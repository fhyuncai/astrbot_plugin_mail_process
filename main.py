import asyncio
import json
import re
import uuid
from datetime import datetime, timezone

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register

from .imap_client import imap_fetch_new, imap_query_since, is_recent_email
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
                admin_targets = await self._get_admin_notify_targets()
                if admin_targets:
                    accounts = self.config.get("mail_accounts", [])
                    for account in accounts:
                        if not account.get("email") or not account.get("imap_server"):
                            continue
                        try:
                            await self._check_account(account, admin_targets)
                            self._account_status[account["email"]] = "✅ 正常"
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
        last_uid = await self.get_kv_data(uid_key, 0) or 0
        init_time = await self.get_kv_data(init_key, "")

        is_first_run = not init_time
        if is_first_run:
            # 首次运行记录初始化时间和当前UID基线，防止历史邮件被推送
            init_time = datetime.now(timezone.utc).isoformat()
            await self.put_kv_data(init_key, init_time)

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
            return

        init_dt = datetime.fromisoformat(init_time)
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
                await self._handle_incoming_mail(account, mail_info, notify_targets)

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

    def _get_persona_prompt(self, unified_msg_origin: str) -> str:
        try:
            personality = self.context.persona_manager.get_default_persona_v3(
                unified_msg_origin
            )
        except Exception:
            return ""
        if isinstance(personality, dict):
            return str(personality.get("prompt", "") or "").strip()
        return ""

    async def _get_provider_for_session(self, unified_msg_origin: str):
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
        completion_text = getattr(llm_resp, "completion_text", "")
        if isinstance(completion_text, str):
            stream_text = MailProcessPlugin._extract_text_from_stream_payload(
                completion_text
            )
            return stream_text or completion_text.strip()
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
        notify_enabled = bool(self.config.get("ai_allow_notify", True))
        reply_enabled = bool(self.config.get("ai_allow_reply", True))
        account_name = account.get("name") or account.get("email") or "未命名账户"
        instructions = [
            base_prompt,
            "你正在处理一个邮箱插件的新邮件事件。",
            "你必须只输出一个 JSON 对象，不要输出解释、Markdown 或代码块。",
            (
                '{"should_notify": false, "notify_reason": "", '
                '"should_reply": false, "reply_to": "", "reply_subject": "", "reply_body": ""}'
            ),
            f"当前邮箱账户: {account_name}",
            f"允许通知管理员: {'是' if notify_enabled else '否'}",
            f"允许自动回复: {'是' if reply_enabled else '否'}",
            "规则：",
            "1. should_notify 表示是否需要通知管理员。",
            "2. should_reply 表示是否需要立即自动回复原邮件。",
            "3. 如果 should_reply 为 true，reply_to、reply_subject、reply_body 必须填写完整。",
            "4. 如果无需通知或回复，请把对应字段置空或 false。",
            "5. notify_reason 用一句简洁中文说明原因。",
        ]
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
        for candidate in candidates:
            try:
                data = json.loads(candidate)
            except Exception:
                continue
            if isinstance(data, dict):
                return data
        raise ValueError(f"无法解析 AI JSON 输出: {raw[:200]}")

    def _normalize_ai_mail_result(self, data: dict) -> dict:
        return {
            "notify": bool(data.get("should_notify", False)),
            "reason": str(data.get("notify_reason", "") or "").strip(),
            "reply": bool(data.get("should_reply", False)),
            "reply_to": str(data.get("reply_to", "") or "").strip(),
            "reply_subject": str(data.get("reply_subject", "") or "").strip(),
            "reply_body": str(data.get("reply_body", "") or "").strip(),
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
                decision_umo, prompt
            )
            parsed = self._parse_ai_json(raw_text)
            result = self._normalize_ai_mail_result(parsed)
            if result["reply"]:
                try:
                    payload = {
                        "account_name": account.get("name")
                        or account.get("email")
                        or "",
                        "to_addr": result["reply_to"] or mail_info.get("from_addr", ""),
                        "subject": result["reply_subject"],
                        "body": result["reply_body"],
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
                    result["reply"] = False
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
        account_name = account.get("name") or account.get("email") or "未命名账户"
        lines = [
            "请像当前会话中的正常 AI 助手一样，向用户发送一条新消息。",
            "你必须直接输出发给用户的正文，不要输出 JSON、代码块或额外解释。",
            "这是一条由插件触发的邮件处理通知。",
            f"邮箱账户: {account_name}",
            f"邮件发件人: {mail_info['from_name']} <{mail_info['from_addr']}>",
            f"邮件主题: {mail_info['subject']}",
            f"邮件时间: {mail_info['date']}",
            f"邮件正文摘要: {mail_info['body']}",
            f"处理原因: {ai_result.get('reason', '')}",
        ]
        if ai_result.get("reply_sent"):
            lines.append("这封邮件已经被 AI 自动回复，请明确告诉用户这一点。")
        else:
            lines.append("这封邮件目前尚未自动回复。")
        if persona_prompt:
            lines.append(f"请遵守当前会话人格设定: {persona_prompt}")
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
                provider_id = await self.context.get_current_chat_provider_id(
                    umo=unified_msg_origin
                )
                if not provider_id:
                    raise ValueError("未找到聊天提供商")
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

    def _build_confirm_message(self, request_id: str, payload: dict) -> str:
        lines = [
            "📨 AI 生成了一封待确认邮件",
            f"请求ID: {request_id}",
            f"账户: {payload['account_name']}",
            f"收件人: {payload['to_addr']}",
            f"主题: {payload['subject']}",
            "正文:",
            payload["body"],
            "",
            "回复 /mail_confirm " + request_id + " 发送",
            "回复 /mail_reject " + request_id + " 取消",
        ]
        return "\n".join(lines)

    async def _enqueue_confirmation(
        self, event: AstrMessageEvent, payload: dict
    ) -> str:
        request_id = uuid.uuid4().hex[:8]
        self._pending_confirmations[request_id] = payload
        chain = MessageChain().message(self._build_confirm_message(request_id, payload))
        notify_targets = getattr(event, "notify_targets", None)
        if isinstance(notify_targets, dict) and notify_targets:
            await self._broadcast_message(notify_targets, chain)
        else:
            await self.context.send_message(event.unified_msg_origin, chain)
        return request_id

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
            lines.append(f"📧 {name} ({addr})")
            lines.append(f"   状态: {status}")
            lines.append(f"   最近检查: {last}")

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
        if not notify_targets:
            yield event.plain_result(
                "❌ 当前没有可通知的管理员会话。\n请至少让一个管理员先给机器人发送一条消息。"
            )
            return
        yield event.plain_result("🔍 正在检查所有邮箱...")

        # Manual check reuses the same account-checking path as the background loop.
        errors = []
        for account in accounts:
            if not account.get("email") or not account.get("imap_server"):
                continue
            email_addr = account["email"]
            try:
                await self._check_account(account, notify_targets)
                self._account_status[email_addr] = "✅ 正常"
            except Exception as e:
                self._account_status[email_addr] = f"❌ {str(e)[:80]}"
                errors.append(f"{account.get('name') or email_addr}: {e}")
            self._last_check_time[email_addr] = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        if errors:
            yield event.plain_result("⚠️ 部分邮箱检查失败:\n" + "\n".join(errors))
        else:
            yield event.plain_result("✅ 所有邮箱检查完成。")

    @filter.command("mail_query")
    async def mail_query(
        self, event: AstrMessageEvent, account_name: str, since_date: str
    ):
        await self._record_admin_session(event)
        if not self._is_plugin_admin(event):
            yield event.plain_result(self._get_admin_denied_message())
            return
        """查询指定邮箱自某日期以来的邮件，如 /mail_query qq邮箱 2026-03-01"""
        accounts = self.config.get("mail_accounts", [])

        # Resolve the target account by either display name or full email address.
        target = None
        for acc in accounts:
            name = acc.get("name", "")
            addr = acc.get("email", "")
            if account_name in (name, addr):
                target = acc
                break
        if not target:
            yield event.plain_result(
                f'❌ 未找到名为 "{account_name}" 的邮箱账户。\n'
                f"已配置的账户: {', '.join(a.get('name') or a.get('email', '?') for a in accounts)}"
            )
            return

        # The command accepts only YYYY-MM-DD to keep parsing deterministic.
        try:
            since_dt = datetime.strptime(since_date, "%Y-%m-%d")
        except ValueError:
            yield event.plain_result(
                "❌ 日期格式错误，请使用 YYYY-MM-DD，如 2026-03-01"
            )
            return

        yield event.plain_result(
            f"🔍 正在查询 {account_name} 自 {since_date} 以来的邮件..."
        )

        try:
            max_body_len = self.config.get("max_body_length", 500)
            # History query also uses a worker thread because IMAP access is blocking.
            emails = await asyncio.to_thread(
                imap_query_since, target, since_dt, max_body_len
            )
        except Exception as e:
            yield event.plain_result(f"❌ 查询失败: {e}")
            return

        if not emails:
            yield event.plain_result(
                f"📭 {account_name} 自 {since_date} 以来没有邮件。"
            )
            return

        lines = [
            f"📬 {account_name} 自 {since_date} 以来共 {len(emails)} 封邮件：",
            "━━━━━━━━━━━━━━━━",
        ]
        for i, m in enumerate(emails, 1):
            lines.append(f"{i}. 📋 {m['subject']}")
            lines.append(f"   📤 {m['from_name']}  🕐 {m['date']}")
        yield event.plain_result("\n".join(lines))

    @filter.command("mail_confirm")
    async def mail_confirm(self, event: AstrMessageEvent, request_id: str):
        await self._record_admin_session(event)
        if not self._is_plugin_admin(event):
            yield event.plain_result(self._get_admin_denied_message())
            return
        payload = self._pending_confirmations.pop(request_id.strip(), None)
        if not payload:
            yield event.plain_result("❌ 未找到待确认的邮件请求。")
            return
        try:
            account_display, _ = await self._send_mail_payload(payload)
        except Exception as e:
            yield event.plain_result(f"❌ 发送失败: {e}")
            return
        yield event.plain_result(
            f"✅ 发送成功\n账户: {account_display}\n收件人: {payload['to_addr']}\n主题: {payload['subject']}"
        )

    @filter.command("mail_reject")
    async def mail_reject(self, event: AstrMessageEvent, request_id: str):
        await self._record_admin_session(event)
        if not self._is_plugin_admin(event):
            yield event.plain_result(self._get_admin_denied_message())
            return
        removed = self._pending_confirmations.pop(request_id.strip(), None)
        if not removed:
            yield event.plain_result("❌ 未找到待确认的邮件请求。")
            return
        yield event.plain_result("✅ 已取消该邮件发送请求。")

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

    @filter.llm_tool(name="send_mail_reply")
    async def send_mail_reply(
        self,
        event: AstrMessageEvent,
        account_name: str,
        to_addr: str,
        subject: str,
        body: str,
    ):
        """发送邮件。

        Args:
            account_name(string): 发送所用邮箱账户名或邮箱地址
            to_addr(string): 收件人邮箱地址
            subject(string): 邮件主题
            body(string): 邮件正文
        """
        if not self.config.get("ai_allow_reply", True):
            return {"reply": False, "reason": "配置已禁用 AI 回复"}
        try:
            account_name, to_addr, subject, body = self._validate_reply_payload(
                account_name, to_addr, subject, body
            )
        except ValueError as e:
            return {"reply": False, "reason": str(e)}

        account = self._get_account_by_name_or_email(account_name)
        if not account:
            return {"reply": False, "reason": f"未找到邮箱账户: {account_name}"}
        if not account.get("smtp_server"):
            return {"reply": False, "reason": "目标账户未配置 SMTP，无法回复"}

        payload = {
            "account_name": account_name,
            "to_addr": to_addr,
            "subject": subject,
            "body": body,
        }
        request_id = await self._enqueue_confirmation(event, payload)
        return {
            "reply": True,
            "notify": False,
            "reason": f"已创建待确认邮件，请管理员使用 /mail_confirm {request_id} 或 /mail_reject {request_id}",
        }

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
