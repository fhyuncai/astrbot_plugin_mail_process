import asyncio
import json
import re
import uuid
from datetime import datetime, timezone

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register

from .imap_client import (
    imap_fetch_new,
    imap_query_recent,
    imap_read_uid,
    is_recent_email,
)
from .smtp_client import smtp_send_mail


@register(
    "astrbot_plugin_mail_process",
    "YourName",
    "监控邮箱新邮件并通过 AI 决策通知或发起回复",
    "1.0.0",
)
class MailProcessPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        # Background polling task created during initialize().
        self._check_task: asyncio.Task | None = None
        # Runtime-only status used by /mail_status; not persisted.
        self._last_check_time: dict[str, str] = {}
        self._account_status: dict[str, str] = {}
        self._admin_session_map: dict[str, str] = {}
        self._pending_confirmations: dict[str, dict] = {}
        self._latest_pending_by_session: dict[str, str] = {}

    def _is_send_mail_allowed(self) -> bool:
        if "ai_allow_send_mail" in self.config:
            return bool(self.config.get("ai_allow_send_mail", True))
        return bool(self.config.get("ai_allow_reply", True))

    async def initialize(self):
        """插件初始化后启动后台邮件检查循环"""
        self._admin_session_map = await self.get_kv_data("admin_session_map", {}) or {}
        self._check_task = asyncio.create_task(self._check_loop())
        logger.info("邮件通知插件：后台检查循环已启动。")

    # ── 后台循环 ──────────────────────────────────────────

    async def _check_loop(self):
        await asyncio.sleep(10)  # 等待系统初始化完成
        while True:
            try:
                interval = self.config.get("check_interval", 5)
                accounts = self.config.get("mail_accounts", [])
                admin_targets = await self._get_admin_notify_targets()

                for account in accounts:
                    if not account.get("email") or not account.get("imap_server"):
                        continue
                    try:
                        result = await self._check_account(account, admin_targets)
                        pending_count = int(result.get("pending_count", 0) or 0)
                        delivered_pending = int(
                            result.get("delivered_pending_count", 0) or 0
                        )
                        email_addr = account["email"]
                        if not admin_targets:
                            if pending_count > 0:
                                self._account_status[email_addr] = (
                                    f"⏸️ 已检测到 {pending_count} 封待处理邮件，等待管理员会话"
                                )
                            else:
                                self._account_status[email_addr] = (
                                    "✅ 已检查（当前无管理员会话）"
                                )
                        elif delivered_pending > 0:
                            self._account_status[email_addr] = (
                                f"✅ 正常（已补处理 {delivered_pending} 封待处理邮件）"
                            )
                        else:
                            self._account_status[email_addr] = "✅ 正常"
                    except Exception as e:
                        self._account_status[account["email"]] = f"❌ {str(e)[:80]}"
                        logger.error(
                            f"邮件通知插件：{account['email']} 检查失败: {e}"
                        )
                    self._last_check_time[account["email"]] = (
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )
                await asyncio.sleep(max(interval, 1) * 60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"邮件通知插件：循环异常: {e}")
                await asyncio.sleep(60)

    # ── IMAP逻辑 ───────────────────────────────────────────────

    async def _check_account(self, account: dict, notify_targets: dict[str, str]):
        # 每个邮箱独立存储状态，避免冲突
        account_email = account["email"]
        max_body_len = max(int(self.config.get("max_body_length", 500) or 500), 1)
        filter_body_len = max(
            int(self.config.get("filter_body_length", 3000) or 3000),
            max_body_len,
        )

        uid_key = f"last_uid_{account_email}"
        init_key = f"init_time_{account_email}"
        pending_key = f"pending_mails_{account_email}"
        last_uid = await self.get_kv_data(uid_key, 0) or 0
        init_time = await self.get_kv_data(init_key, "")
        delivered_pending_count = 0

        is_first_run = not init_time
        if is_first_run:
            # 首次运行记录初始化时间和当前UID基线，防止历史邮件被推送
            init_time = datetime.now(timezone.utc).isoformat()
            await self.put_kv_data(init_key, init_time)

        if notify_targets:
            delivered_pending_count = await self._flush_pending_mails(
                account, pending_key, notify_targets
            )

        # imaplib为阻塞操作，实际查询在工作线程中执行
        new_emails, new_max_uid = await asyncio.to_thread(
            imap_fetch_new, account, last_uid, max_body_len, filter_body_len
        )

        if new_max_uid > last_uid:
            await self.put_kv_data(uid_key, new_max_uid)

        if is_first_run:
            if new_max_uid > 0:
                logger.info(
                    f"邮件通知插件：{account_email} 初始化完成，最大UID = {new_max_uid}"
                )
            pending_count = len(await self.get_kv_data(pending_key, []) or [])
            return {
                "pending_count": pending_count,
                "delivered_pending_count": delivered_pending_count,
            }

        init_dt = datetime.fromisoformat(init_time)
        matched_mails = []
        for mail_info in new_emails:
            # 二次校验邮件时间，避免刚拉取的邮件属于历史存量
            if is_recent_email(mail_info, init_dt):
                should_notify, reason = self._should_notify_mail(mail_info)
                log_mail_from = (
                    mail_info.get("from_addr") or mail_info.get("from_name") or "?"
                )
                log_mail_subject = (mail_info.get("subject") or "")[:120]
                if not should_notify:
                    logger.info(
                        "MailProcess: filtered mail for account=%s, reason=%s, from=%s, subject=%s",
                        account_email,
                        reason,
                        log_mail_from,
                        log_mail_subject,
                    )
                    continue
                logger.info(
                    "MailProcess: notification allowed for account=%s, reason=%s, from=%s, subject=%s",
                    account_email,
                    reason,
                    log_mail_from,
                    log_mail_subject,
                )
                matched_mails.append(mail_info)

        if not notify_targets:
            pending_count = await self._queue_pending_mails(
                pending_key, matched_mails, account_email
            )
            return {
                "pending_count": pending_count,
                "delivered_pending_count": delivered_pending_count,
            }

        for mail_info in matched_mails:
            await self._handle_incoming_mail(account, mail_info, notify_targets)

        pending_count = len(await self.get_kv_data(pending_key, []) or [])
        return {
            "pending_count": pending_count,
            "delivered_pending_count": delivered_pending_count,
        }

    async def _queue_pending_mails(
        self, pending_key: str, mails: list[dict], account_email: str
    ) -> int:
        pending_mails = await self.get_kv_data(pending_key, []) or []
        merged: dict[int, dict] = {}
        for item in pending_mails:
            if isinstance(item, dict):
                try:
                    merged[int(item.get("uid", 0))] = item
                except Exception:
                    continue

        for mail_info in mails:
            if not isinstance(mail_info, dict):
                continue
            try:
                merged[int(mail_info.get("uid", 0))] = mail_info
            except Exception:
                continue

        merged_list = [merged[uid] for uid in sorted(merged.keys()) if uid > 0][-50:]
        await self.put_kv_data(pending_key, merged_list)
        if mails:
            logger.info(
                "MailProcess: queued %s mail(s) for account=%s because no admin session is available",
                len(mails),
                account_email,
            )
        return len(merged_list)

    async def _flush_pending_mails(
        self, account: dict, pending_key: str, notify_targets: dict[str, str]
    ) -> int:
        pending_mails = await self.get_kv_data(pending_key, []) or []
        if not pending_mails:
            return 0

        remaining = []
        delivered_count = 0
        account_email = account.get("email") or account.get("name") or "?"
        for mail_info in pending_mails:
            if not isinstance(mail_info, dict):
                continue
            try:
                await self._handle_incoming_mail(account, mail_info, notify_targets)
                delivered_count += 1
            except Exception as e:
                logger.warning(
                    "MailProcess: failed to flush pending mail for account=%s uid=%s: %s",
                    account_email,
                    mail_info.get("uid", "?"),
                    e,
                )
                remaining.append(mail_info)

        await self.put_kv_data(pending_key, remaining)
        if delivered_count:
            logger.info(
                "MailProcess: flushed %s pending mail(s) for account=%s",
                delivered_count,
                account_email,
            )
        return delivered_count

    async def _get_pending_count(self, account_email: str) -> int:
        pending_key = f"pending_mails_{account_email}"
        pending_mails = await self.get_kv_data(pending_key, []) or []
        if not isinstance(pending_mails, list):
            return 0
        return len([item for item in pending_mails if isinstance(item, dict)])

    # ── Notification ─────────────────────────────────────────────

    def _get_filter_settings(self, prefix: str) -> dict:
        settings = self.config.get(f"{prefix}_settings", {}) or {}
        if isinstance(settings, dict):
            return settings
        return {}

    def _get_filter_enabled(self, prefix: str) -> bool:
        settings = self._get_filter_settings(prefix)
        if "enable" in settings:
            return bool(settings.get("enable", False))
        return bool(self.config.get(f"enable_{prefix}", False))

    def _get_filter_rules(self, prefix: str, field: str) -> list[str]:
        settings = self._get_filter_settings(prefix)
        nested_values = settings.get(f"{field}_rules", [])
        if not nested_values:
            nested_values = self.config.get(f"{field}_{prefix}", []) or []

        values = nested_values or []
        return [
            str(value).strip()
            for value in values
            if isinstance(value, (str, int, float)) and str(value).strip()
        ]

    @staticmethod
    def _matches_sender_rule(mail_info: dict, rule: str) -> bool:
        normalized_rule = rule.strip().casefold()
        if not normalized_rule:
            return False

        from_addr = (mail_info.get("from_addr") or "").strip().casefold()
        from_name = (mail_info.get("from_name") or "").strip().casefold()

        if re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", normalized_rule):
            return from_addr == normalized_rule
        if normalized_rule.startswith("@"):
            return from_addr.endswith(normalized_rule)
        return normalized_rule in from_addr or normalized_rule in from_name

    @staticmethod
    def _matches_contains_rule(text: str, rule: str) -> bool:
        normalized_rule = rule.strip().casefold()
        if not normalized_rule:
            return False
        return normalized_rule in (text or "").casefold()

    def _match_rule_group(
        self, mail_info: dict, prefix: str
    ) -> tuple[bool, str | None]:
        sender_rules = self._get_filter_rules(prefix, "sender")
        for rule in sender_rules:
            if self._matches_sender_rule(mail_info, rule):
                return True, f"sender:{rule}"

        subject_rules = self._get_filter_rules(prefix, "subject")
        subject = mail_info.get("subject") or ""
        for rule in subject_rules:
            if self._matches_contains_rule(subject, rule):
                return True, f"subject:{rule}"

        body_rules = self._get_filter_rules(prefix, "body")
        filter_body = mail_info.get("filter_body") or mail_info.get("body") or ""
        for rule in body_rules:
            if self._matches_contains_rule(filter_body, rule):
                return True, f"body:{rule}"

        return False, None

    def _should_notify_mail(self, mail_info: dict) -> tuple[bool, str]:
        enable_blacklist = self._get_filter_enabled("blacklist")
        enable_whitelist = self._get_filter_enabled("whitelist")

        if enable_blacklist:
            is_blacklisted, rule = self._match_rule_group(mail_info, "blacklist")
            if is_blacklisted:
                return False, f"被黑名单屏蔽 ({rule})"

        if enable_whitelist:
            is_whitelisted, rule = self._match_rule_group(mail_info, "whitelist")
            if not is_whitelisted:
                return False, "被白名单屏蔽"
            return True, f"白名单允许 ({rule})"

        return True, "允许，因为不需要匹配白名单限制"

    async def _handle_incoming_mail(
        self, account: dict, mail_info: dict, notify_targets: dict[str, str]
    ):
        if self.config.get("enable_ai_processing", False):
            ai_result = await self._run_ai_mail_processing(account, mail_info, notify_targets)
            logger.info(
                "MailProcess: AI decision account=%s notify=%s send_mail=%s reason=%s subject=%s",
                account.get("email") or account.get("name") or "?",
                ai_result.get("notify"),
                ai_result.get("send_mail"),
                ai_result.get("reason", "")[:120],
                (mail_info.get("subject") or "")[:120],
            )
            if ai_result.get("notify"):
                await self._send_ai_notification(
                    account,
                    mail_info,
                    notify_targets,
                    ai_result=ai_result,
                )
            return
        await self._send_notification(account, mail_info, notify_targets)

    async def _send_notification(
        self,
        account: dict,
        mail_info: dict,
        notify_targets: dict[str, str],
        ai_decision: str = "",
    ):
        account_name = account.get("name") or account["email"]
        use_ai = self.config.get("ai_summary", False) and not self.config.get(
            "enable_ai_processing", False
        )
        body_text = mail_info["body"]

        provider_umo = next(iter(notify_targets.values()), "")
        if use_ai and body_text and provider_umo:
            body_text = await self._try_ai_summary(mail_info, provider_umo, body_text)

        lines = [
            f"📬 新邮件通知 [{account_name}]",
            "━━━━━━━━━━━━━━━━",
            f"📤 发件人: {mail_info['from_name']}",
        ]
        if mail_info["from_addr"] and mail_info["from_addr"] != mail_info["from_name"]:
            lines[-1] += f" <{mail_info['from_addr']}>"
        lines.append(f"📋 主题: {mail_info['subject']}")
        lines.append(f"🕐 时间: {mail_info['date']}")
        if ai_decision:
            lines.append(f"🤖 AI判断: {ai_decision}")
        if body_text:
            label = "📝 AI摘要" if use_ai else "📝 预览"
            lines.append(f"{label}: {body_text}")

        chain = MessageChain().message("\n".join(lines))
        await self._broadcast_message(notify_targets, chain)

    async def _try_ai_summary(
        self, mail_info: dict, notify_umo: str, fallback: str
    ) -> str:
        try:
            prompt = (
                "请用简洁的中文（不超过100字）总结以下邮件内容，只输出摘要：\n"
                f"主题：{mail_info['subject']}\n"
                f"正文：{fallback}"
            )
            result = await self._generate_text_with_session_provider(
                notify_umo, prompt
            )
            if result:
                return result
        except Exception as e:
            logger.warning(f"MailProcess: AI summary failed: {e}")
        return fallback

    def _get_account_by_name_or_email(self, account_name: str) -> dict | None:
        accounts = self.config.get("mail_accounts", [])
        target_name = account_name.strip()
        for acc in accounts:
            name = (acc.get("name") or "").strip()
            addr = (acc.get("email") or "").strip()
            if target_name in (name, addr):
                return acc
        return None

    def _list_mail_accounts(self) -> list[dict]:
        accounts = self.config.get("mail_accounts", []) or []
        return [
            acc
            for acc in accounts
            if isinstance(acc, dict) and acc.get("email") and acc.get("imap_server")
        ]

    def _resolve_account_for_query(
        self, account_name: str = "", allow_default: bool = True
    ) -> tuple[dict | None, str | None]:
        accounts = self._list_mail_accounts()
        if not accounts:
            return None, "未配置任何可查询的邮箱账户。"

        normalized_name = (account_name or "").strip()
        if normalized_name:
            account = self._get_account_by_name_or_email(normalized_name)
            if not account:
                available_accounts = ", ".join(
                    (acc.get("name") or acc.get("email") or "?") for acc in accounts
                )
                return (
                    None,
                    f"未找到邮箱账户: {normalized_name}。当前可用账户: {available_accounts}",
                )
            if not account.get("imap_server"):
                return None, f"邮箱账户 {normalized_name} 未配置 IMAP，无法查询。"
            return account, None

        if allow_default and len(accounts) == 1:
            return accounts[0], None

        available_accounts = ", ".join(
            (acc.get("name") or acc.get("email") or "?") for acc in accounts
        )
        return (
            None,
            "当前配置了多个邮箱账户，请明确指定 account_name。"
            f"可用账户: {available_accounts}",
        )

    @staticmethod
    def _normalize_mail_preview(mail_info: dict) -> dict:
        return {
            "uid": int(mail_info.get("uid", 0) or 0),
            "subject": str(mail_info.get("subject", "") or "").strip(),
            "from_name": str(mail_info.get("from_name", "") or "").strip(),
            "from_addr": str(mail_info.get("from_addr", "") or "").strip(),
            "date": str(mail_info.get("date", "") or "").strip(),
            "summary": str(mail_info.get("body", "") or "").strip(),
        }

    def _get_mail_query_page_size(self) -> int:
        configured = int(self.config.get("mail_query_page_size", 10) or 10)
        return max(1, min(configured, 20))

    def _get_mail_query_max_items(self) -> int:
        configured = int(self.config.get("mail_query_max_items", 50) or 50)
        return max(1, min(configured, 100))

    def _get_admin_uids(self) -> set[str]:
        admin_uids = self.config.get("admin_uids", []) or []
        return {
            str(uid).strip()
            for uid in admin_uids
            if isinstance(uid, (str, int)) and str(uid).strip()
        }

    async def _record_admin_session(self, event: AstrMessageEvent) -> None:
        sender_id = str(event.get_sender_id()).strip()
        if not sender_id or sender_id not in self._get_admin_uids():
            return
        umo = getattr(event, "unified_msg_origin", "") or ""
        if not umo:
            return
        if self._admin_session_map.get(sender_id) == umo:
            return
        self._admin_session_map[sender_id] = umo
        await self.put_kv_data("admin_session_map", self._admin_session_map)

    async def _get_admin_notify_targets(self) -> dict[str, str]:
        admin_uids = self._get_admin_uids()
        targets = {
            uid: umo
            for uid, umo in self._admin_session_map.items()
            if uid in admin_uids and isinstance(umo, str) and umo.strip()
        }
        return targets

    async def _broadcast_message(
        self, notify_targets: dict[str, str], chain: MessageChain
    ) -> None:
        sent = set()
        for umo in notify_targets.values():
            if not umo or umo in sent:
                continue
            await self.context.send_message(umo, chain)
            sent.add(umo)

    def _get_personality(self, unified_msg_origin: str):
        try:
            return self.context.persona_manager.get_default_persona_v3(
                unified_msg_origin
            )
        except Exception:
            return None

    def _get_persona_prompt(self, unified_msg_origin: str) -> str:
        personality = self._get_personality(unified_msg_origin)
        if isinstance(personality, dict):
            return str(personality.get("prompt", "") or "").strip()
        for attr in ("prompt", "system_prompt"):
            value = getattr(personality, attr, "")
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _get_persona_begin_dialogs(self, unified_msg_origin: str) -> list[str]:
        personality = self._get_personality(unified_msg_origin)
        dialogs = []
        if isinstance(personality, dict):
            dialogs = personality.get("begin_dialogs", []) or []
        else:
            dialogs = getattr(personality, "begin_dialogs", []) or []
        return [str(item).strip() for item in dialogs if str(item).strip()]

    @staticmethod
    def _merge_stream_text(current: str, new_text: str) -> str:
        current = current or ""
        new_text = (new_text or "").strip()
        if not new_text:
            return current
        if not current:
            return new_text
        if new_text == current:
            return current
        if new_text.startswith(current) or current in new_text:
            return new_text
        if current.startswith(new_text) or new_text in current:
            return current

        max_overlap = min(len(current), len(new_text))
        for size in range(max_overlap, 0, -1):
            if current.endswith(new_text[:size]):
                return current + new_text[size:]

        import difflib

        similarity = difflib.SequenceMatcher(None, current, new_text).ratio()
        if similarity >= 0.75:
            return new_text if len(new_text) >= len(current) else current

        return current + new_text

    async def _get_provider_for_session(self, unified_msg_origin: str):
        getter = getattr(self.context, "get_using_provider", None)
        if callable(getter):
            try:
                provider = getter(umo=unified_msg_origin)
                if provider is not None:
                    return provider
            except TypeError:
                try:
                    provider = getter(unified_msg_origin)
                    if provider is not None:
                        return provider
                except Exception:
                    pass
            except Exception:
                pass

        provider_id = await self.context.get_current_chat_provider_id(
            umo=unified_msg_origin
        )
        if not provider_id:
            return None
        getter = getattr(self.context, "get_provider_by_id", None)
        if callable(getter):
            try:
                return getter(provider_id)
            except Exception:
                pass
        provider_manager = getattr(self.context, "provider_manager", None)
        if provider_manager and hasattr(provider_manager, "get_provider_by_id"):
            try:
                return provider_manager.get_provider_by_id(provider_id)
            except Exception:
                pass
        return None

    @staticmethod
    def _extract_llm_text(llm_resp) -> str:
        if not llm_resp:
            return ""
        if isinstance(llm_resp, str):
            return MailProcessPlugin._extract_text_from_stream_payload(llm_resp)
        if isinstance(llm_resp, dict):
            if "text" in llm_resp and isinstance(llm_resp["text"], str):
                return llm_resp["text"].strip()
            if "content" in llm_resp and isinstance(llm_resp["content"], str):
                return llm_resp["content"].strip()
        completion_text = getattr(llm_resp, "completion_text", "")
        if isinstance(completion_text, str):
            stream_text = MailProcessPlugin._extract_text_from_stream_payload(
                completion_text
            )
            return stream_text or completion_text.strip()
        for attr in ("text", "content", "delta", "message"):
            value = getattr(llm_resp, attr, "")
            if isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, dict):
                nested = value.get("text") or value.get("content") or ""
                if isinstance(nested, str) and nested.strip():
                    return nested.strip()
        return ""

    @staticmethod
    def _extract_text_from_stream_payload(raw_text: str) -> str:
        raw_text = (raw_text or "").strip()
        if not raw_text:
            return ""
        if "data:" not in raw_text:
            return raw_text

        chunks: list[str] = []
        for line in raw_text.splitlines():
            line = line.strip()
            if not line.startswith("data:"):
                continue
            payload = line[5:].strip()
            if not payload or payload == "[DONE]":
                continue
            try:
                data = json.loads(payload)
            except Exception:
                continue
            for choice in data.get("choices", []) or []:
                message = choice.get("message") or {}
                delta = choice.get("delta") or {}
                text = (
                    message.get("content")
                    or delta.get("content")
                    or choice.get("text")
                    or ""
                )
                if isinstance(text, list):
                    for part in text:
                        if isinstance(part, dict):
                            value = part.get("text") or part.get("content") or ""
                            if value:
                                chunks.append(str(value))
                elif text:
                    chunks.append(str(text))

        return "".join(chunks).strip()

    async def _generate_text_with_session_provider(
        self,
        unified_msg_origin: str,
        prompt: str,
        system_prompt: str = "",
    ) -> str:
        provider = await self._get_provider_for_session(unified_msg_origin)
        if not provider:
            raise ValueError("未找到当前会话对应的聊天提供商。")
        kwargs = {"prompt": prompt}
        if system_prompt:
            kwargs["system_prompt"] = system_prompt
        stream_fn = getattr(provider, "text_chat_stream", None)
        if callable(stream_fn):
            merged_text = ""
            async for chunk in stream_fn(**kwargs):
                text = self._extract_llm_text(chunk)
                if text:
                    merged_text = self._merge_stream_text(merged_text, text)
            merged_text = merged_text.strip()
            if merged_text:
                return merged_text
        llm_resp = await provider.text_chat(**kwargs)
        text = self._extract_llm_text(llm_resp)
        if not text:
            raise ValueError("AI 返回内容为空。")
        return text

    def _build_mail_processing_prompt(
        self, account: dict, mail_info: dict, unified_msg_origin: str
    ) -> str:
        default_prompt = "你收到了一封新邮件，请阅读后决定是否回复或通知。"
        base_prompt = (
            self.config.get("ai_processing_prompt", "") or default_prompt
        ).strip() or default_prompt
        persona_prompt = self._get_persona_prompt(unified_msg_origin)
        begin_dialogs = self._get_persona_begin_dialogs(unified_msg_origin)
        notify_enabled = bool(self.config.get("ai_allow_notify", True))
        send_mail_enabled = self._is_send_mail_allowed()
        account_name = account.get("name") or account.get("email") or "未命名账户"
        instructions = [
            base_prompt,
            "你现在要以当前会话中的 AI 助手身份处理一封新邮件。",
            "你不是在写说明文档，也不是在扮演插件；请按当前会话的人格、语气、偏好和判断标准来决定是否需要通知用户，或是否需要直接发送邮件。",
            "你必须只输出一个 JSON 对象，不要输出解释、Markdown 或代码块。",
            (
                '{"should_notify": false, "notify_reason": "", '
                '"should_send_mail": false, "mail_to": "", "mail_subject": "", "mail_body": ""}'
            ),
            f"当前邮箱账户: {account_name}",
            f"允许通知管理员: {'是' if notify_enabled else '否'}",
            f"允许发送邮件: {'是' if send_mail_enabled else '否'}",
            "规则：",
            "1. should_notify 表示是否需要通知管理员。",
            "2. should_send_mail 表示是否需要立即发送邮件，通常用于自动回复当前邮件。",
            "3. 如果 should_send_mail 为 true，mail_to、mail_subject、mail_body 必须填写完整。",
            "4. 如果无需通知或回复，请把对应字段置空或 false。",
            "5. notify_reason 用一句简洁中文说明原因。",
            "6. 判断时要结合当前人格风格与用户体验，不要机械地见到邮件就通知。",
            "7. 如果邮件明显是广告、垃圾、无关提醒，通常不通知也不发信。",
        ]
        if persona_prompt:
            instructions.append(f"当前会话人格设定: {persona_prompt}")
        if begin_dialogs:
            instructions.append("当前会话说话风格参考:")
            instructions.extend(begin_dialogs[:8])
        instructions.extend(
            [
                f"邮件发件人: {mail_info['from_name']} <{mail_info['from_addr']}>",
                f"邮件主题: {mail_info['subject']}",
                f"邮件时间: {mail_info['date']}",
                f"邮件正文: {mail_info['body']}",
            ]
        )
        return "\n".join(instructions)

    @staticmethod
    def _parse_ai_json(text: str) -> dict:
        raw = (text or "").strip()
        if not raw:
            raise ValueError("AI 返回为空。")
        candidates = [raw]
        fenced = re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", raw, flags=re.S)
        candidates.extend(fenced)
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidates.append(raw[start : end + 1])

        decoder = json.JSONDecoder()
        json_objects: list[dict] = []
        for candidate in candidates:
            stripped = candidate.strip()
            try:
                data = json.loads(stripped)
            except Exception:
                data = None
            if isinstance(data, dict):
                json_objects.append(data)
                continue

            idx = 0
            length = len(stripped)
            while idx < length:
                brace_pos = stripped.find("{", idx)
                if brace_pos == -1:
                    break
                try:
                    obj, end_pos = decoder.raw_decode(stripped, brace_pos)
                except Exception:
                    idx = brace_pos + 1
                    continue
                if isinstance(obj, dict):
                    json_objects.append(obj)
                idx = end_pos

        preferred_keys = {
            "should_notify",
            "notify_reason",
            "should_send_mail",
            "mail_to",
            "mail_subject",
            "mail_body",
        }
        for data in reversed(json_objects):
            if preferred_keys.intersection(data.keys()):
                return data
        for data in reversed(json_objects):
            if isinstance(data, dict):
                return data
        raise ValueError(f"无法解析 AI JSON 输出: {raw[:200]}")

    def _normalize_ai_mail_result(self, data: dict) -> dict:
        return {
            "notify": bool(data.get("should_notify", False)),
            "reason": str(data.get("notify_reason", "") or "").strip(),
            "send_mail": bool(
                data.get("should_send_mail", data.get("should_reply", False))
            ),
            "mail_to": str(
                data.get("mail_to", data.get("reply_to", "")) or ""
            ).strip(),
            "mail_subject": str(
                data.get("mail_subject", data.get("reply_subject", "")) or ""
            ).strip(),
            "mail_body": str(
                data.get("mail_body", data.get("reply_body", "")) or ""
            ).strip(),
            "raw": data,
        }

    async def _run_ai_mail_processing(
        self, account: dict, mail_info: dict, notify_targets: dict[str, str]
    ) -> dict:
        decision_umo = next(iter(notify_targets.values()), "")
        if not decision_umo:
            return {"notify": True, "reason": "未找到管理员会话，退回普通通知"}
        try:
            prompt = self._build_mail_processing_prompt(account, mail_info, decision_umo)
            raw_text = await self._generate_text_with_session_provider(
                decision_umo,
                prompt,
                system_prompt=self._get_persona_prompt(decision_umo),
            )
            parsed = self._parse_ai_json(raw_text)
            result = self._normalize_ai_mail_result(parsed)
            if result["send_mail"]:
                try:
                    payload = {
                        "account_name": account.get("name")
                        or account.get("email")
                        or "",
                        "to_addr": result["mail_to"] or mail_info.get("from_addr", ""),
                        "subject": result["mail_subject"],
                        "body": result["mail_body"],
                    }
                    payload["account_name"], payload["to_addr"], payload["subject"], payload[
                        "body"
                    ] = self._validate_reply_payload(
                        payload["account_name"],
                        payload["to_addr"],
                        payload["subject"],
                        payload["body"],
                    )
                    account_display, _ = await self._send_mail_payload(payload)
                    result["reply_sent"] = True
                    if not result["reason"]:
                        result["reason"] = (
                            f"AI 已自动回复。账户: {account_display}，收件人: {payload['to_addr']}"
                        )
                except Exception as e:
                    logger.warning(f"MailProcess: auto reply failed: {e}")
                    result["send_mail"] = False
                    result["reply_sent"] = False
                    result["notify"] = True
                    result["reason"] = f"AI 判断需要处理，但自动回复失败: {e}"
            return result
        except Exception as e:
            logger.warning(f"MailProcess: AI processing failed: {e}")
            return {"notify": True, "reason": f"AI处理失败，退回普通通知: {e}"}

    def _build_ai_notify_prompt(
        self, account: dict, mail_info: dict, ai_result: dict, unified_msg_origin: str
    ) -> str:
        persona_prompt = self._get_persona_prompt(unified_msg_origin)
        begin_dialogs = self._get_persona_begin_dialogs(unified_msg_origin)
        account_name = account.get("name") or account.get("email") or "未命名账户"
        lines = [
            "请直接以当前会话中的 AI 助手身份，给用户发送一条正常聊天消息。",
            "你必须只输出发给用户的正文，不要输出 JSON、代码块、标题、小节或额外说明。",
            "语气必须明显符合当前会话的人格设定，像平时聊天一样自然。",
            "不要写成固定通知模板，不要逐项罗列“邮箱账户、发件人、主题、时间、摘要、处理建议”等标签。",
            "你需要自然地告诉用户：刚收到一封新邮件，以及这封邮件的大致内容。",
            "只有在确实有必要时，才自然提到发件人、主题或邮箱账户；不要机械复述所有字段。",
            "如果这封邮件已经被自动回复，要自然告诉用户；如果没有自动回复，也只需自然带过，不要写成系统提示。",
        ]
        if persona_prompt:
            lines.append(f"当前会话人格设定: {persona_prompt}")
        lines.extend(
            [
                f"当前邮箱账户: {account_name}",
                f"邮件发件人: {mail_info['from_name']} <{mail_info['from_addr']}>",
                f"邮件主题: {mail_info['subject']}",
                f"邮件时间: {mail_info['date']}",
                f"邮件大致内容: {mail_info['body']}",
                f"本次处理原因: {ai_result.get('reason', '')}",
                f"是否已自动回复: {'是' if ai_result.get('reply_sent') else '否'}",
            ]
        )
        if begin_dialogs:
            lines.append("当前会话说话风格参考:")
            lines.extend(begin_dialogs[:8])
        return "\n".join(lines)

    def _build_conversation_mail_context(
        self, account: dict, mail_info: dict, ai_result: dict
    ) -> str:
        account_name = account.get("name") or account.get("email") or "未命名账户"
        lines = [
            "[系统邮件事件]",
            f"邮箱账户: {account_name}",
            f"发件人: {mail_info['from_name']} <{mail_info['from_addr']}>",
            f"主题: {mail_info['subject']}",
            f"时间: {mail_info['date']}",
            f"正文摘要: {mail_info['body']}",
        ]
        if ai_result.get("reply_sent"):
            lines.append("处理结果: 已自动回复")
        else:
            lines.append("处理结果: 已通知用户")
        if ai_result.get("reason"):
            lines.append(f"处理原因: {ai_result['reason']}")
        return "\n".join(lines)

    async def _append_to_conversation(
        self, unified_msg_origin: str, user_text: str, assistant_text: str
    ) -> None:
        try:
            from astrbot.core.agent.message import (
                AssistantMessageSegment,
                TextPart,
                UserMessageSegment,
            )

            conv_mgr = self.context.conversation_manager
            curr_cid = await conv_mgr.get_curr_conversation_id(unified_msg_origin)
            if not curr_cid:
                curr_cid = await conv_mgr.new_conversation(unified_msg_origin)
            await conv_mgr.add_message_pair(
                cid=curr_cid,
                user_message=UserMessageSegment(content=[TextPart(text=user_text)]),
                assistant_message=AssistantMessageSegment(
                    content=[TextPart(text=assistant_text)]
                ),
            )
        except Exception as e:
            logger.warning(f"MailProcess: append conversation failed: {e}")

    async def _send_ai_notification(
        self, account: dict, mail_info: dict, notify_targets: dict[str, str], ai_result: dict
    ) -> None:
        sent = set()
        for unified_msg_origin in notify_targets.values():
            if not unified_msg_origin or unified_msg_origin in sent:
                continue
            try:
                prompt = self._build_ai_notify_prompt(
                    account, mail_info, ai_result, unified_msg_origin
                )
                notify_text = await self._generate_text_with_session_provider(
                    unified_msg_origin,
                    prompt,
                    system_prompt=self._get_persona_prompt(unified_msg_origin),
                )
                if not notify_text:
                    raise ValueError("AI 通知内容为空")
                await self.context.send_message(
                    unified_msg_origin, MessageChain().message(notify_text)
                )
                await self._append_to_conversation(
                    unified_msg_origin,
                    self._build_conversation_mail_context(account, mail_info, ai_result),
                    notify_text,
                )
            except Exception as e:
                logger.warning(f"MailProcess: AI notify failed for {unified_msg_origin}: {e}")
                await self._send_notification(
                    account,
                    mail_info,
                    {unified_msg_origin: unified_msg_origin},
                    ai_decision=ai_result.get("reason", ""),
                )
            sent.add(unified_msg_origin)

    def _create_pending_mail(
        self, payload: dict, event: AstrMessageEvent | None = None
    ) -> str:
        request_id = uuid.uuid4().hex[:8]
        stored_payload = dict(payload)
        if event is not None:
            stored_payload["_owner_umo"] = getattr(event, "unified_msg_origin", "") or ""
            stored_payload["_owner_uid"] = str(event.get_sender_id()).strip()
        stored_payload["_created_at"] = datetime.now().isoformat()
        self._pending_confirmations[request_id] = stored_payload
        owner_umo = stored_payload.get("_owner_umo", "")
        if owner_umo:
            self._latest_pending_by_session[owner_umo] = request_id
        return request_id

    def _pop_pending_mail(self, mail_id: str) -> dict | None:
        normalized_mail_id = (mail_id or "").strip()
        payload = self._pending_confirmations.pop(normalized_mail_id, None)
        if not payload:
            return None
        owner_umo = str(payload.get("_owner_umo", "") or "").strip()
        if owner_umo and self._latest_pending_by_session.get(owner_umo) == normalized_mail_id:
            self._latest_pending_by_session.pop(owner_umo, None)
        return payload

    def _resolve_pending_mail_for_session(
        self, event: AstrMessageEvent, mail_id: str
    ) -> tuple[str | None, dict | None, str]:
        requested_id = (mail_id or "").strip()
        current_umo = getattr(event, "unified_msg_origin", "") or ""
        current_uid = str(event.get_sender_id()).strip()

        if requested_id:
            payload = self._pending_confirmations.get(requested_id)
            if payload:
                owner_umo = str(payload.get("_owner_umo", "") or "").strip()
                owner_uid = str(payload.get("_owner_uid", "") or "").strip()
                if (not owner_umo or owner_umo == current_umo) and (
                    not owner_uid or owner_uid == current_uid
                ):
                    return requested_id, payload, ""

        latest_id = self._latest_pending_by_session.get(current_umo, "")
        if latest_id:
            payload = self._pending_confirmations.get(latest_id)
            if payload:
                owner_uid = str(payload.get("_owner_uid", "") or "").strip()
                if not owner_uid or owner_uid == current_uid:
                    note = ""
                    if requested_id and requested_id != latest_id:
                        note = "已忽略历史或无效 mail_id，改用当前会话最新待确认邮件。"
                    return latest_id, payload, note

        for pending_id, payload in reversed(list(self._pending_confirmations.items())):
            if not isinstance(payload, dict):
                continue
            owner_umo = str(payload.get("_owner_umo", "") or "").strip()
            owner_uid = str(payload.get("_owner_uid", "") or "").strip()
            if owner_umo == current_umo and (not owner_uid or owner_uid == current_uid):
                note = ""
                if requested_id and requested_id != pending_id:
                    note = "已忽略历史或无效 mail_id，改用当前会话待确认邮件。"
                self._latest_pending_by_session[current_umo] = pending_id
                return pending_id, payload, note

        reason = (
            "当前会话没有可确认的待发送邮件。"
            "不要复用历史 mail_id。"
            "如果用户现在是要新回复邮件或新发邮件，请先调用 send_mail(account_name, to_addr, subject, body)"
            " 创建待确认草稿；只有在本轮已经生成草稿且用户明确同意后，才能调用 send_mail_confirm(mail_id)。"
        )
        return None, None, reason

    async def _send_mail_payload(self, payload: dict) -> tuple[str, dict]:
        account = self._get_account_by_name_or_email(payload["account_name"])
        if not account:
            raise ValueError("对应邮箱账户已不存在，无法发送。")
        if not account.get("smtp_server"):
            raise ValueError("该账户未配置 SMTP 服务器。")
        await asyncio.to_thread(
            smtp_send_mail,
            account,
            payload["to_addr"],
            payload["subject"],
            payload["body"],
        )
        account_display = (
            account.get("name") or account.get("email") or payload["account_name"]
        )
        return account_display, account

    def _get_admin_denied_message(self) -> str:
        if not self._get_admin_uids():
            return "❌ 还未指定插件管理员。\n请在插件web设置的admin_uid中添加用户id。"
        return "❌ 无权限使用该命令。"

    def _is_plugin_admin(self, event: AstrMessageEvent) -> bool:
        admin_uids = self._get_admin_uids()
        sender_id = str(event.get_sender_id()).strip()
        return bool(sender_id and sender_id in admin_uids)

    def _validate_reply_payload(
        self, account_name: str, to_addr: str, subject: str, body: str
    ) -> tuple[str, str, str, str]:
        account_name = (account_name or "").strip()
        to_addr = (to_addr or "").strip()
        subject = (subject or "").strip()
        body = (body or "").strip()
        if not account_name:
            raise ValueError("账户名不能为空。")
        if not to_addr or "@" not in to_addr:
            raise ValueError("收件人邮箱格式错误。")
        if not subject:
            raise ValueError("邮件主题不能为空。")
        if not body:
            raise ValueError("邮件正文不能为空。")
        if len(subject) > 200:
            raise ValueError("邮件主题过长（最多 200 字符）。")
        if len(body) > 5000:
            raise ValueError("邮件正文过长（最多 5000 字符）。")
        return account_name, to_addr, subject, body

    # ── Commands ─────────────────────────────────────────────────

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def capture_admin_session(self, event: AstrMessageEvent):
        await self._record_admin_session(event)

    @filter.command("mail_status")
    async def mail_status(self, event: AstrMessageEvent):
        await self._record_admin_session(event)
        if not self._is_plugin_admin(event):
            yield event.plain_result(self._get_admin_denied_message())
            return
        """查看所有邮箱的监控状态"""
        # Read current config plus runtime cache to render a status snapshot.
        accounts = self.config.get("mail_accounts", [])
        interval = self.config.get("check_interval", 5)
        notify_targets = await self._get_admin_notify_targets()

        if not accounts:
            yield event.plain_result(
                "📭 未配置任何邮箱账户，请在 WebUI 插件配置中添加。"
            )
            return

        lines = [
            f"📊 邮箱监控状态 (间隔: {interval}分钟)",
            f"🔔 可通知管理员会话: {len(notify_targets)}/{len(self._get_admin_uids())}",
            "━━━━━━━━━━━━━━━━",
        ]
        for acc in accounts:
            addr = acc.get("email", "?")
            name = acc.get("name") or addr
            status = self._account_status.get(addr, "⏳ 等待首次检查")
            last = self._last_check_time.get(addr, "尚未检查")
            pending_count = await self._get_pending_count(addr)
            lines.append(f"📧 {name} ({addr})")
            lines.append(f"   状态: {status}")
            lines.append(f"   最近检查: {last}")
            if pending_count > 0:
                lines.append(f"   待处理邮件: {pending_count}")

        yield event.plain_result("\n".join(lines))

    @filter.command("mail_check")
    async def mail_check(self, event: AstrMessageEvent):
        await self._record_admin_session(event)
        if not self._is_plugin_admin(event):
            yield event.plain_result(self._get_admin_denied_message())
            return
        """立即手动检查所有邮箱"""
        accounts = self.config.get("mail_accounts", [])
        if not accounts:
            yield event.plain_result(
                "📭 未配置任何邮箱账户，请在 WebUI 插件配置中添加。"
            )
            return

        notify_targets = await self._get_admin_notify_targets()
        if notify_targets:
            yield event.plain_result("🔍 正在检查所有邮箱...")
        else:
            yield event.plain_result(
                "🔍 正在检查所有邮箱...\n当前没有可通知的管理员会话，新邮件会先暂存，等管理员再次和机器人对话后补处理。"
            )

        # Manual check reuses the same account-checking path as the background loop.
        errors = []
        pending_summary = []
        for account in accounts:
            if not account.get("email") or not account.get("imap_server"):
                continue
            email_addr = account["email"]
            try:
                result = await self._check_account(account, notify_targets)
                pending_count = int(result.get("pending_count", 0) or 0)
                delivered_pending = int(
                    result.get("delivered_pending_count", 0) or 0
                )
                if not notify_targets:
                    if pending_count > 0:
                        self._account_status[email_addr] = (
                            f"⏸️ 已检测到 {pending_count} 封待处理邮件，等待管理员会话"
                        )
                    else:
                        self._account_status[email_addr] = "✅ 已检查（当前无管理员会话）"
                elif delivered_pending > 0:
                    self._account_status[email_addr] = (
                        f"✅ 正常（已补处理 {delivered_pending} 封待处理邮件）"
                    )
                else:
                    self._account_status[email_addr] = "✅ 正常"
                if pending_count > 0:
                    pending_summary.append(
                        f"{account.get('name') or email_addr}: {pending_count} 封待处理"
                    )
            except Exception as e:
                self._account_status[email_addr] = f"❌ {str(e)[:80]}"
                errors.append(f"{account.get('name') or email_addr}: {e}")
            self._last_check_time[email_addr] = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        if errors:
            yield event.plain_result("⚠️ 部分邮箱检查失败:\n" + "\n".join(errors))
        elif pending_summary and not notify_targets:
            yield event.plain_result(
                "✅ 所有邮箱检查完成。\n当前无管理员会话，以下邮件已暂存：\n"
                + "\n".join(pending_summary)
            )
        else:
            yield event.plain_result("✅ 所有邮箱检查完成。")

    @filter.llm_tool(name="notify_mail_admin")
    async def notify_mail_admin(self, event: AstrMessageEvent, reason: str = ""):
        """通知管理员。

        Args:
            reason(string): 通知管理员的原因
        """
        if not self.config.get("ai_allow_notify", True):
            return {"notify": False, "reason": "配置已禁用 AI 通知"}
        final_reason = (reason or "").strip() or "AI 判断需要通知管理员"
        return {
            "notify": True,
            "reason": final_reason,
        }

    @filter.llm_tool(name="mail_query")
    async def mail_query(
        self,
        event: AstrMessageEvent,
        account_name: str = "",
        page: int = 1,
        page_size: int = 0,
    ):
        """查询指定邮箱最近邮件列表，只返回简短摘要，支持分页。

        Args:
            account_name(string): 邮箱账户名或邮箱地址；若仅配置了一个邮箱，可留空
            page(int): 页码，从 1 开始
            page_size(int): 每页数量；传 0 时使用配置默认值
        """
        if not self._is_plugin_admin(event):
            return {"ok": False, "reason": self._get_admin_denied_message()}

        account, error = self._resolve_account_for_query(account_name, allow_default=True)
        if not account:
            return {"ok": False, "reason": error}

        max_items = self._get_mail_query_max_items()
        default_page_size = self._get_mail_query_page_size()
        safe_page_size = int(page_size or default_page_size)
        safe_page_size = max(1, min(safe_page_size, 20))
        safe_page = max(int(page or 1), 1)

        preview_len = min(int(self.config.get("mail_query_preview_length", 120) or 120), 300)
        preview_len = max(preview_len, 20)

        try:
            emails = await asyncio.to_thread(
                imap_query_recent,
                account,
                preview_len,
                max_items,
            )
        except Exception as e:
            return {"ok": False, "reason": f"查询邮件失败: {e}"}

        total = len(emails)
        total_pages = max((total + safe_page_size - 1) // safe_page_size, 1)
        if safe_page > total_pages:
            return {
                "ok": False,
                "reason": f"页码超出范围。当前共有 {total_pages} 页。",
            }

        start = (safe_page - 1) * safe_page_size
        end = start + safe_page_size
        page_items = [self._normalize_mail_preview(item) for item in emails[start:end]]
        account_display = account.get("name") or account.get("email") or ""

        return {
            "ok": True,
            "account_name": account_display,
            "account_email": account.get("email") or "",
            "page": safe_page,
            "page_size": safe_page_size,
            "total": total,
            "total_pages": total_pages,
            "max_items": max_items,
            "items": page_items,
            "reason": "如需查看某封邮件完整内容，请调用 mail_read(account_name, mail_uid)。",
        }

    @filter.llm_tool(name="mail_read")
    async def mail_read(
        self,
        event: AstrMessageEvent,
        account_name: str,
        mail_uid: int,
    ):
        """读取指定邮箱中某封邮件的详细内容。

        Args:
            account_name(string): 邮箱账户名或邮箱地址
            mail_uid(int): 目标邮件 UID，可从 mail_query 返回结果中获取
        """
        if not self._is_plugin_admin(event):
            return {"ok": False, "reason": self._get_admin_denied_message()}

        account, error = self._resolve_account_for_query(account_name, allow_default=False)
        if not account:
            return {"ok": False, "reason": error}

        try:
            safe_uid = int(mail_uid)
        except Exception:
            return {"ok": False, "reason": "mail_uid 必须是有效整数。"}
        if safe_uid <= 0:
            return {"ok": False, "reason": "mail_uid 必须大于 0。"}

        body_len = max(int(self.config.get("mail_read_body_length", 4000) or 4000), 200)
        body_len = min(body_len, 20000)

        try:
            mail_info = await asyncio.to_thread(imap_read_uid, account, safe_uid, body_len)
        except Exception as e:
            return {"ok": False, "reason": f"读取邮件失败: {e}"}

        if not mail_info:
            return {"ok": False, "reason": f"未找到 UID 为 {safe_uid} 的邮件。"}

        return {
            "ok": True,
            "account_name": account.get("name") or account.get("email") or "",
            "account_email": account.get("email") or "",
            "mail": {
                "uid": int(mail_info.get("uid", 0) or 0),
                "subject": str(mail_info.get("subject", "") or "").strip(),
                "from_name": str(mail_info.get("from_name", "") or "").strip(),
                "from_addr": str(mail_info.get("from_addr", "") or "").strip(),
                "date": str(mail_info.get("date", "") or "").strip(),
                "body": str(mail_info.get("body", "") or "").strip(),
            },
        }

    @filter.llm_tool(name="send_mail")
    async def send_mail(
        self,
        event: AstrMessageEvent,
        account_name: str,
        to_addr: str,
        subject: str,
        body: str,
    ):
        """创建一封待确认邮件草稿。

        Args:
            account_name(string): 发送所用邮箱账户名或邮箱地址
            to_addr(string): 收件人邮箱地址
            subject(string): 邮件主题
            body(string): 邮件正文
        """
        if not self._is_plugin_admin(event):
            return {"ok": False, "reason": self._get_admin_denied_message()}
        if not self._is_send_mail_allowed():
            return {"ok": False, "reason": "配置已禁用 AI 发信"}
        try:
            account_name, to_addr, subject, body = self._validate_reply_payload(
                account_name, to_addr, subject, body
            )
        except ValueError as e:
            return {"ok": False, "reason": str(e)}

        account = self._get_account_by_name_or_email(account_name)
        if not account:
            return {"ok": False, "reason": f"未找到邮箱账户: {account_name}"}
        if not account.get("smtp_server"):
            return {"ok": False, "reason": "目标账户未配置 SMTP，无法发信"}

        payload = {
            "account_name": account_name,
            "to_addr": to_addr,
            "subject": subject,
            "body": body,
        }
        mail_id = self._create_pending_mail(payload, event)
        return {
            "ok": True,
            "mail_id": mail_id,
            "account_name": account_name,
            "to_addr": to_addr,
            "subject": subject,
            "body": body,
            "status": "pending_confirmation",
            "reason": "已创建当前会话的待确认邮件草稿。请你先用自然语言向用户确认是否发送；只有在用户明确同意后，才能调用 send_mail_confirm(mail_id)。不要复用历史 mail_id。",
        }

    @filter.llm_tool(name="send_mail_confirm")
    async def send_mail_confirm(self, event: AstrMessageEvent, mail_id: str):
        """确认并发送当前会话中刚创建的待确认邮件。

        Args:
            mail_id(string): 待确认邮件 ID。仅用于本会话中刚通过 send_mail 返回的 mail_id，不要复用历史 mail_id。
        """
        if not self._is_plugin_admin(event):
            return {"ok": False, "reason": self._get_admin_denied_message()}
        if not self._is_send_mail_allowed():
            return {"ok": False, "reason": "配置已禁用 AI 发信"}

        resolved_mail_id, payload, resolution_note = self._resolve_pending_mail_for_session(
            event, mail_id
        )
        if not payload or not resolved_mail_id:
            return {"ok": False, "reason": resolution_note}

        try:
            payload = self._pop_pending_mail(resolved_mail_id)
            if not payload:
                return {
                    "ok": False,
                    "reason": "待确认邮件已不存在，可能已发送、已取消或已过期。若需要重新发送，请先重新调用 send_mail 创建新草稿。",
                }
            account_display, _ = await self._send_mail_payload(payload)
        except Exception as e:
            return {"ok": False, "reason": f"发送失败: {e}"}

        result = {
            "ok": True,
            "mail_id": resolved_mail_id,
            "status": "sent",
            "account_name": account_display,
            "to_addr": payload["to_addr"],
            "subject": payload["subject"],
            "reason": "邮件已发送。",
        }
        if resolution_note:
            result["resolution_note"] = resolution_note
        return result

    # ── Lifecycle ────────────────────────────────────────────────

    async def terminate(self):
        """Cancel background task on plugin unload."""
        if self._check_task and not self._check_task.done():
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
        logger.info("MailProcess: plugin terminated.")
