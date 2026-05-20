<div align="center">

# 📬 邮件处理插件

[![Plugin Version](https://img.shields.io/badge/Latest_Version-v1.0.0-blue.svg?style=for-the-badge&color=76bad9)](https://github.com/fhyuncai/astrbot_plugin_mail_process)
[![AstrBot](https://img.shields.io/badge/AstrBot-Plugin-ff69b4?style=for-the-badge)](https://github.com/AstrBotDevs/AstrBot)
[![License](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge)](LICENSE)

<img src="https://count.getloli.com/@astrbot-plugin-mail-process?name=astrbot-plugin-mail-process&theme=booru-jaypee&padding=6&offset=0&align=top&scale=1&pixelated=1&darkmode=auto" alt="count" />

_✨ 监控多个 IMAP 邮箱的新邮件，支持 AI 自动判断是否通知管理员或发起回复。✨_

</div>

## 功能特性

- 多邮箱监控：同时监控多个 IMAP 邮箱
- 自动推送：后台定时轮询新邮件
- AI 邮件处理：可选把邮件交给 AI 决定是否通知、是否发送邮件
- AI 自动回复：处理新邮件时，AI 可直接发送回复
- AI 代发确认：用户要求 AI 回复邮件或新发邮件时，发送前需要确认
- AI 通知拟人化：AI 接管时，通知会按目标会话对应的模型和人格设定生成
- AI 邮件查询：可让 AI 分页查询最近邮件，并按需读取单封邮件全文
- 黑白名单过滤：支持按发件人、主题、正文过滤
- WebUI 配置：全部配置项可在 AstrBot WebUI 管理
- 零额外依赖：仅使用 Python 标准库

## 配置说明

在 AstrBot WebUI 的插件配置中填写：

### 邮箱账户

每个账户需要配置：

- `name`：账户备注名
- `imap_server` / `imap_port` / `use_ssl`
- `email` / `password`
- `smtp_server` / `smtp_port` / `smtp_use_ssl`
- `smtp_password`：留空时复用 `password`

### 通用配置

- `check_interval`：轮询间隔，单位分钟
- `admin_uids`：管理员 UID 列表
- `max_body_length`：通知预览长度
- `filter_body_length`：正文过滤长度
- `mail_query_max_items`：AI 查询最近邮件时最多读取的邮件数
- `mail_query_page_size`：AI 查询邮件的默认每页条数
- `mail_query_preview_length`：AI 查询邮件列表时的摘要长度
- `mail_read_body_length`：AI 读取单封邮件时的最大正文长度

### AI 配置

- `ai_summary`：仅做摘要，不做 AI 决策
- `enable_ai_processing`：启用 AI 邮件处理
- `ai_processing_prompt`：AI 处理提示词
- `ai_allow_notify`：允许 AI 决定通知管理员
- `ai_allow_send_mail`：允许 AI 发送邮件

默认 AI 处理提示词：

```text
你收到了一封新邮件，请阅读后决定是否回复或通知。
```

## 使用方式

### 1. 管理员先建立可通知会话

通知目标基于 `admin_uids` 自动匹配管理员最近一次会话。也就是说：

- 先在配置里填好管理员 UID
- 每个需要接收通知的管理员，至少先给机器人发过一条消息
- 插件会记录该管理员最近一次会话，并向该会话发送通知

### 2. 手动检查

```text
/mail_check
```

### 3. 查看状态

```text
/mail_status
```

## 指令列表

| 指令 | 说明 |
|------|------|
| `/mail_check` | 立即检查所有邮箱 |
| `/mail_status` | 查看邮箱状态和管理员会话覆盖情况 |

## AI 回复流程

启用 `enable_ai_processing` 后：

1. 插件收到新邮件
2. 按你的 `ai_processing_prompt` 把邮件交给 AI
3. AI 可以做两件事：
   - 调用通知工具，要求插件通知管理员
   - 调用回复工具，直接完成自动回复

如果是管理员在聊天里要求 AI 去回复某封邮件，或者给某个地址新发邮件，则应走待确认流程：

1. AI 调用 `send_mail` 生成待发送邮件
2. AI 以自然语言询问用户是否确认发送
3. 用户明确同意后，AI 再调用 `send_mail_confirm(mail_id)` 真正发送

如果管理员想让 AI 帮忙查看最近邮件，则应走查询流程：

1. AI 调用 `mail_query(account_name, page, page_size)` 查询指定邮箱最近邮件列表
2. 列表仅返回简短摘要，并支持分页
3. 需要查看某封邮件完整内容时，AI 再调用 `mail_read(account_name, mail_uid)`

说明：

- 已移除手动 `/mail_reply`、`/mail_query`、`/mail_confirm`、`/mail_reject` 指令
- 自动处理新邮件时，AI 回复不需要额外确认
- 用户主动要求 AI 发送邮件时，需要确认
- 自动处理新邮件时，如果 AI 决定通知，通知文本会按目标会话当前 AI 设定生成
- 若关闭 `ai_allow_send_mail`，AI 只能通知，不能发起发信
- 若关闭 `ai_allow_notify`，AI 不能要求插件发通知

## 邮件通知示例

```text
📬 新邮件通知 [qq邮箱]
━━━━━━━━━━━━━━━━
📤 发件人: 张三 <zhangsan@example.com>
📋 主题: 关于项目进度汇报
🕐 时间: 2026-03-07 14:30
🤖 AI判断: 这是一封需要你关注的项目邮件
📝 预览: 你好，本周项目已完成模块A的开发...
```

## 常见问题

**1. 为什么没有收到通知？**

- 确认 `admin_uids` 已正确配置
- 确认管理员至少先给机器人发过一条消息
- 用 `/mail_status` 查看当前可通知管理员会话数量

**2. 为什么 AI 没有发出回复？**

- 自动处理新邮件时，确认 `ai_allow_send_mail` 已开启，且目标账户已配置 SMTP
- 用户主动要求 AI 发送邮件时，需要先明确确认
- 确认目标邮箱已配置 SMTP

**3. 怎么查询历史邮件？**

- 直接在会话里让 AI 帮你查询即可
- AI 会使用 `mail_query` 查询最近邮件列表
- 需要看具体某封内容时，AI 会继续调用 `mail_read`

**4. 连接超时或认证失败？**

- 检查 IMAP/SMTP 服务器地址和端口
- Gmail、QQ、163 等通常需要授权码或应用专用密码

## 权限

- 只有 `admin_uids` 中的用户可以使用插件命令
- 只有管理员最近一次会话会被用于主动通知

## License

MIT
