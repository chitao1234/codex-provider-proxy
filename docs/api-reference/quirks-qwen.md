# 阿里百炼 Qwen API quirks

> 核对日期：2026-08-12。  
> 对照基准：`messages-api.md`、`responses-api.md`、`chat-completions-api.md`。本文只记录官方文档可直接支撑的偏差；未在官方文档找到的能力不推断。

## 官方来源与访问状态

| 来源 | 状态 | 用途 | 官方摘录 |
| --- | --- | --- | --- |
| https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses | 可访问；已缓存并抽取为 `/tmp/codex_relay_official/qwen-responses.redownload.html.extracted.txt` | OpenAI Responses 兼容接口 | `请求将仅处理本文档明确列出的参数` |
| https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions | 可访问；已缓存并抽取为 `/tmp/codex_relay_official/qwen-chat.redownload.html.extracted.txt` | OpenAI Chat Completions 兼容接口 | `POST .../compatible-mode/v1/chat/completions` |
| https://help.aliyun.com/zh/model-studio/anthropic-api-messages | 可访问；已缓存并抽取为 `/tmp/codex_relay_official/qwen-anthropic.redownload.html.extracted.txt` | Anthropic Messages 兼容接口 | `base_url="https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/apps/anthropic"` |
| https://help.aliyun.com/zh/model-studio/context-cache | 可访问；已缓存并抽取为 `/tmp/codex_relay_official/qwen-context-cache.redownload.html.extracted.txt` | 显式/隐式上下文缓存 | `cache_control: {"type": "ephemeral"}` |

## 1. 端点 URL 与认证

| 协议 | 是否存在 | base_url / path | 认证 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- | --- | --- |
| OpenAI Responses | 存在 | 区域专属：`https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/responses` 等 | `Authorization: Bearer $DASHSCOPE_API_KEY` | 路径是 OpenAI 兼容模式，但 base_url 含业务空间和地域，不是 `api.openai.com/v1`。 | `POST https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/responses`；`Authorization: Bearer $DASHSCOPE_API_KEY`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| OpenAI Chat Completions | 存在 | 区域专属：`https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/chat/completions` 等；调试页仍展示 `https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions` | `Authorization: Bearer $DASHSCOPE_API_KEY` | base_url 需要业务空间；旧域名仍可用但官方建议迁移。 | `POST https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/chat/completions`；`现有域名仍可正常使用`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| Anthropic Messages | 存在 | `https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/apps/anthropic/v1/messages`（Anthropic SDK 在 base_url 后拼 `/v1/messages`） | 示例用 `default_headers={"Authorization": f"Bearer {api_key}"}`，不是 Anthropic 基准 `x-api-key` | Anthropic 基准要求 `x-api-key` 和 `anthropic-version`；百炼示例使用 Bearer。 | `base_url="https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/apps/anthropic"`；`default_headers={"Authorization": f"Bearer {api_key}"}`（https://help.aliyun.com/zh/model-studio/context-cache） |

## 2. 请求参数偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Responses 只处理文档列出的参数 | Responses | OpenAI Responses 基准参数很多；百炼明确未提及参数会忽略。 | `请求将仅处理本文档明确列出的参数，任何未提及的 OpenAI 参数都会被忽略`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Responses 不支持 `background` | Responses | OpenAI Responses 基准支持后台执行；百炼当前仅同步。 | `不支持部分 OpenAI Responses API 参数，例如异步执行参数 background（当前仅支持同步调用）`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Responses session cache 走 Header | Responses | OpenAI Responses 基准使用 prompt cache key/options；百炼 Responses 可用 `x-dashscope-session-cache` header。 | `x-dashscope-session-cache: enable（默认值为 disable）`；`服务端即可自动缓存对话上下文`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Responses thinking 通过 `reasoning.effort` | Responses | 与 OpenAI Responses 基准同名；百炼把它列为关键差异点。 | `思考强度控制：通过 reasoning.effort 参数控制模型的思考强度`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Chat `max_tokens` 即将废弃 | Chat | OpenAI Chat 基准也标 `max_tokens` 废弃；百炼保留但建议新接入用 `max_completion_tokens`。 | `max_tokens integer（可选，即将废弃）`；`新接入请使用 max_completion_tokens`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| Chat `max_tokens` 与 `max_completion_tokens` 语义不同 | Chat | OpenAI Chat 基准中 `max_completion_tokens` 是完整输出上限；百炼说明 `max_tokens` 不含思维链。 | `max_tokens...模型回答的最大长度（不包含思维链内容）`；`max_completion_tokens 限制模型完整输出（思维链 + 回答）`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| Chat 非流式超时行为 | Chat | OpenAI Chat 基准未说明 300 秒中断并返回已生成内容；百炼明确该行为。 | `非流式调用若超过 300 秒未完成，服务将中断请求并返回已生成的内容（而非报错）`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| Chat JSON 模式需提示词明确 | Chat | OpenAI Chat 基准也建议明确 JSON；百炼文档说否则会报错。 | `需在提示词中明确指示模型输出 JSON...否则会报错`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| Anthropic 显式缓存 | Anthropic / Chat | Anthropic 基准支持 `cache_control`；百炼文档支持 OpenAI Chat 与 Anthropic 显式缓存，但 Responses 用 Session cache。 | `Responses uses Session cache` 对应正文：`Responses API 支持...Session 缓存`；显式例子为 `cache_control: {"type":"ephemeral"}`（https://help.aliyun.com/zh/model-studio/context-cache） |

## 3. 消息与内容格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Responses role 支持四类 | Responses | 与 OpenAI Responses 基准一致；转换层可保留 developer。 | `消息角色，可选值：user、assistant、system、developer`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Responses content 类型按 role 受限 | Responses | OpenAI Responses 基准支持更多输入块；百炼限定 system/developer 只能 `input_text`，user 才能图/文件。 | `role 为 system/developer 时，array 元素类型为 input_text；role 为 user 时...input_text、input_image 或 input_file`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Responses 暂不支持视频或语音 | Responses | OpenAI Responses 基准可承载更多多模态；百炼 Responses 明确不支持视频/语音，需用 Chat 或 DashScope。 | `当前 Responses API 暂不支持传入视频或语音，您可以通过 Chat Completions API 或 DashScope API 传入`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Responses `input_file` 限制 | Responses | OpenAI Responses 基准支持 input_file；百炼只列 PDF 和图片，且当前仅 qwen3.5-ocr。 | `支持 PDF 文件（最大 50 页、100 MB）和图片文件（最大 20 MB）。目前仅 qwen3.5-ocr 支持此类型`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Chat content 多模态类型较多 | Chat | OpenAI Chat 基准用户 content part 有 text/image_url/input_audio/file；百炼 Chat 还列 `video/video_url`。 | `可选值：text、image_url、input_audio、video、video_url`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| Chat 不支持 `developer` role | Chat | OpenAI Chat 基准支持 `developer`；百炼 Chat 消息类型只列 System/User/Assistant/Tool。 | `System Message`；`User Message`；`Assistant Message`；`Tool Message`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| QwQ/QVQ system 限制 | Chat | OpenAI Chat 基准没有模型族特定 system 无效说明。 | `QwQ 模型不建议设置 System Message，QVQ 模型设置 System Message 不会生效`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |

## 4. 工具格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Responses 内置工具扩展 | Responses | OpenAI Responses 基准有 built-in tools；百炼列 `web_search`、`code_interpreter`、`web_extractor`。 | `tools=[{"type": "web_search"}, {"type": "code_interpreter"}, {"type": "web_extractor"}]`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Responses function tool 为扁平 shape | Responses | 与 OpenAI Responses function tool 接近；不同于 OpenAI Chat 的 nested `function`。 | `tools = [` 后示例含 `{"type": "function", "name": ... "parameters": ...}`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Chat function tool 与 OpenAI Chat shape | Chat | 与 OpenAI Chat 基准一致：assistant 返回 `tool_calls`，tool message 需 `tool_call_id`。 | `工具类型，当前只支持设为 function`；`arguments 入参信息，为 JSON 格式字符串`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| Chat Tool Message content 必须字符串 | Chat | OpenAI Chat 基准 tool content 可为 string 或 text parts；百炼要求字符串，结构化数据需序列化。 | `工具函数的输出内容，必须为字符串`；`若工具返回结构化数据...需将其序列化为字符串`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| 显式缓存不能加在 tools 上 | Chat / Anthropic | Anthropic 基准可在 tools 上加 `cache_control`；百炼上下文缓存文档说明 Chat 显式缓存只能加 messages content。 | `cache_control 只能加在 messages 的 content 上，不能加在 tools 上`（https://help.aliyun.com/zh/model-studio/context-cache） |

## 5. 响应、usage 与错误格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Responses 可回传 `reasoning` item | Responses | OpenAI Responses 基准也有 reasoning item；百炼说明可直接传回 input。 | `模型的思考内容。可直接将上一轮响应的 output 中的 reasoning 项传回 input`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Responses 可回传 `web_search_call` item | Responses | OpenAI Responses 基准有 tool call outputs；百炼明确 web_search_call 可用于多轮搜索上下文。 | `可直接将上一轮响应的 output 中的 web_search_call 项传回 input`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Responses Session cache usage 字段 | Responses | OpenAI Responses 基准是 `input_tokens_details.cached_tokens`；百炼示例同名。 | `输入 Token: {usage.input_tokens}`；`缓存命中 Token: {usage.input_tokens_details.cached_tokens}`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Chat stream usage 只能最后 chunk | Chat | OpenAI Chat 基准也有最终 usage chunk；百炼明确“仅可出现在最后一个数据块”。 | `流式输出时，Token 消耗信息仅可出现在响应的最后一个数据块`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| Chat 显式缓存 usage 字段 | Chat | OpenAI Chat 基准使用 `prompt_tokens_details.cached_tokens`；百炼还展示 `cache_creation_input_tokens`。 | `usage.prompt_tokens_details.cache_creation_input_tokens`；`usage.prompt_tokens_details.cached_tokens`（https://help.aliyun.com/zh/model-studio/context-cache） |
| Anthropic 缓存 usage 字段 | Anthropic | 与 Anthropic 基准字段名一致：创建和读取分开。 | `cache_creation_input_tokens`；`cache_read_input_tokens`（https://help.aliyun.com/zh/model-studio/context-cache） |
| 错误格式 | 全部 | 本次已访问页面没有抽到统一 error schema 原文；不推断 OpenAI/Anthropic 风格。 | `未在官方文档找到统一错误格式` |

## 6. 流式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Responses 流式为 OpenAI Responses 事件对象 | Responses | 与 OpenAI Responses 基准相近；示例遍历 `event.type`。 | `for event in stream`；`if event.type == "response.completed"`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Chat 流式开启与 usage | Chat | 与 OpenAI Chat 基准相近；最后 usage chunk 受 `stream_options.include_usage` 控制。 | `stream 为 true 时生效`；`include_usage...最后一个数据块包含 Token 消耗信息`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| Chat `[DONE]` | Chat | OpenAI Chat 基准用 `data: [DONE]`；百炼文档同样提到。 | `data: [DONE] 消息之前额外发送一个 chunk`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| Anthropic 流式事件集 | Anthropic | 本次访问的 Anthropic 页面/缓存示例重点为缓存，未抽到完整 SSE 事件集；不推断差异。 | `未在官方文档找到完整 Anthropic 流式事件集` |

## 7. 已知限制

| 限制 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Responses 未列 OpenAI 参数会忽略 | Responses | 转换层不能把 OpenAI Responses 全量参数直接透传并期待生效。 | `任何未提及的 OpenAI 参数都会被忽略`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Responses 文件页数/大小 | Responses | OpenAI Responses 基准不含百炼 qwen3.5-ocr 限定。 | `PDF 文件（最大 50 页、100 MB）和图片文件（最大 20 MB）`；`目前仅 qwen3.5-ocr 支持此类型`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-responses） |
| Chat 非流式 300 秒 | Chat | 输出长文本建议流式，非流式可能中断并返回部分。 | `非流式调用若超过 300 秒未完成`；`建议输出较长的场景务必使用流式调用`（https://help.aliyun.com/zh/model-studio/qwen-api-via-openai-chat-completions） |
| 显式缓存块限制 | Chat / Anthropic | Anthropic 基准显式 cache marker 最多 4 个；百炼同样要求最多 4 个，并回看 20 个 content block。 | `系统会向前查找最多 20 个 content blocks`；`最多 4 个 cache_control 标记`（https://help.aliyun.com/zh/model-studio/context-cache） |
| 显式缓存最小 token 与 TTL | Chat / Anthropic | 百炼显式缓存最小 1024 tokens，TTL 5 分钟且命中刷新。 | `最小 1024 tokens`；`TTL 5 min reset on hit` 对应正文：`有效期为 5 分钟，每次命中后刷新`（https://help.aliyun.com/zh/model-studio/context-cache） |
| 缓存不保证 100% 命中 | 全部 | 转换层不能把 cache hit 当确定性行为。 | `上下文缓存命中概率并非 100%，即使请求上下文完全一致，仍可能未命中`（https://help.aliyun.com/zh/model-studio/context-cache） |

