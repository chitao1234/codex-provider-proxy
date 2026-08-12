# DeepSeek API quirks

> 核对日期：2026-08-12。  
> 对照基准：`messages-api.md`、`responses-api.md`、`chat-completions-api.md`。本文只记录官方文档可直接支撑的偏差；未在官方文档找到的能力不推断。

## 官方来源与访问状态

| 来源 | 状态 | 用途 | 官方摘录 |
| --- | --- | --- | --- |
| https://api-docs.deepseek.com/zh-cn/api/create-chat-completion | 可访问；已缓存为 `/tmp/codex_relay_official/deepseek-chat-cn.txt` | OpenAI Chat Completions 兼容端点 | `POST /chat/completions`；`消息流以 data: [DONE] 结尾` |
| https://api-docs.deepseek.com/zh-cn/api/create-response | 可访问；已缓存为 `/tmp/codex_relay_official/deepseek-responses-cn.txt` | OpenAI Responses 兼容端点 | `支持的输入 item 类型为 message / function_call / function_call_output / reasoning / web_search_call` |
| https://api-docs.deepseek.com/zh-cn/guides/anthropic_api | 可访问；已缓存为 `/tmp/codex_relay_official/deepseek-anthropic-cn.txt` | Anthropic Messages 兼容说明 | `base_url 为 https://api.deepseek.com/anthropic` |
| https://api-docs.deepseek.com/zh-cn/quick_start/error_codes | 可访问；已缓存为 `/tmp/codex_relay_official/deepseek-errors-cn.txt` | HTTP 错误码 | `400 - 格式错误`；`429 - 请求速率达到上限` |

## 1. 端点 URL 与认证

| 协议 | 是否存在 | base_url / path | 认证 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- | --- | --- |
| OpenAI Chat Completions | 存在 | `https://api.deepseek.com/chat/completions`；Beta 前缀续写用 `https://api.deepseek.com/beta` | `Authorization: Bearer`，示例环境变量 `DEEPSEEK_API_KEY` | 路径与 Chat 基准相同；Beta 功能需换 base_url，不是 OpenAI Chat 基准字段。 | `POST /chat/completions`；`您必须设置 base_url="https://api.deepseek.com/beta"`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| OpenAI Responses | 存在 | `https://api.deepseek.com/responses` | `Authorization: Bearer` | 路径与 Responses 基准相同；官方强调无 `[DONE]` 结束。 | `最后一个事件是 response.completed / response.incomplete / response.failed（没有 data: [DONE] 消息）`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Anthropic Messages | 存在 | `https://api.deepseek.com/anthropic/v1/messages`（Anthropic SDK 在 base_url 后拼 `/v1/messages`） | `x-api-key` 支持；示例用 `ANTHROPIC_API_KEY` | Anthropic 基准的 `anthropic-version` 被忽略，且模型名会映射。 | `base_url 为 https://api.deepseek.com/anthropic`；`x-api-key Fully Supported`；`anthropic-version Ignored`（https://api-docs.deepseek.com/zh-cn/guides/anthropic_api） |

## 2. 请求参数偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 使用 `max_tokens` | Chat | OpenAI Chat 基准推荐 `max_completion_tokens`，且 `max_tokens` 已废弃；DeepSeek Chat 文档仍使用 `max_tokens`。 | `max_tokens integer nullable`；`限制一次请求中模型生成 completion 的最大 token 数`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Responses 使用 `max_output_tokens` | Responses | 与 OpenAI Responses 基准字段名一致；计入可见输出和思维链。 | `响应可生成的 token 数上限，包含可见的输出 token 与思维链 token`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Chat thinking 控制为 `thinking.type` | Chat | OpenAI Chat 基准没有顶层 `thinking`；Anthropic thinking 需要 `budget_tokens`，DeepSeek Chat 只有开关。 | `thinking object`；`Possible values: enabled, disabled`；`Default value: enabled`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Chat `reasoning_effort` 枚举变窄 | Chat | OpenAI Responses 基准为 `reasoning.effort`，Chat 基准没有 `reasoning_effort`；DeepSeek Chat 用顶层 `reasoning_effort`，且 `medium/xhigh` 会映射。 | `Possible values: low, high, max`；`medium、xhigh 会映射为 high`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Responses thinking 与采样 | Responses | OpenAI Responses 基准允许 `temperature/top_p`；DeepSeek 明确 thinking 模式下不生效。 | `temperature...思考模式下不生效`；`top_p...思考模式下不生效`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Responses 只处理部分输入 item | Responses | OpenAI Responses 基准支持更多 item/content 类型；DeepSeek 文档列出支持项，并说其他会忽略。 | `支持的输入 item 类型为 message / function_call / function_call_output / reasoning / web_search_call，其他类型会被忽略`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Anthropic `thinking.budget_tokens` 忽略 | Anthropic | Anthropic Messages 基准要求 enabled thinking 配 `budget_tokens`，且计入 `max_tokens`；DeepSeek 兼容端点忽略预算字段。 | `thinking Supported (budget_tokens is ignored)`（https://api-docs.deepseek.com/zh-cn/guides/anthropic_api） |
| Anthropic 额外/高级字段被忽略 | Anthropic | Anthropic 基准支持 `container`、`mcp_servers`、`service_tier`、`top_k` 等；DeepSeek 标为忽略。 | `container Ignored`；`mcp_servers Ignored`；`service_tier Ignored`；`top_k Ignored`（https://api-docs.deepseek.com/zh-cn/guides/anthropic_api） |
| Anthropic `metadata` 只保留 `user_id` | Anthropic | Anthropic 基准 `metadata` 是对象；DeepSeek 只支持 `user_id`。 | `metadata user_id is supported, others are ignored`（https://api-docs.deepseek.com/zh-cn/guides/anthropic_api） |
| `stop` / `stop_sequences` | Chat / Anthropic | Chat 支持字符串或最多 16 个字符串；Anthropic 兼容端点支持 `stop_sequences`。 | `一个 string 或最多包含 16 个 string 的 list`；`stop_sequences Fully Supported`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion；https://api-docs.deepseek.com/zh-cn/guides/anthropic_api） |

## 3. 消息与内容格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat role 不含 `developer` | Chat | OpenAI Chat 基准 role 包含 `developer`；DeepSeek Chat schema 只列 system/user/assistant/tool 四类。 | `System message`、`User message`、`Assistant message`、`Tool message`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Responses `developer` 视同 `system` | Responses | OpenAI Responses 基准区分 `developer` 与 `system`；DeepSeek 接收但合并语义。 | `消息角色支持 user / assistant / system / developer（developer 视同 system）`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Responses 不支持图片和文件输入 | Responses | OpenAI Responses 基准支持 `input_image`、`input_file`；DeepSeek 不支持，且 `input_image` 会替换为占位文本。 | `不支持图片、文件输入（input_image 内容块不会报错，但会被替换为占位文本）`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Chat assistant prefix 扩展 | Chat | OpenAI Chat 基准没有 `partial`/`prefix` 语义；DeepSeek 用 assistant message 的 `prefix:true`。 | `prefix (Beta) 设置此参数为 true`；`以此 assistant 消息中提供的前缀内容开始`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Chat thinking 回传字段 | Chat | OpenAI Chat 基准没有请求侧 `reasoning_content`；DeepSeek Beta 可把最后一条 assistant 思维链作为输入。 | `reasoning_content (Beta) 用于思考模式下在对话前缀续写功能下...prefix 参数必须设置为 true`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Anthropic 多模态块缺失 | Anthropic | Anthropic Messages 基准支持 image/document/search_result/redacted_thinking 等；DeepSeek 兼容端点多项不支持或忽略 cache。 | `array, type="image" Not Supported`；`type = "document" Not Supported`；`type = "search_result" Not Supported`；`type="redacted_thinking" Not Supported`（https://api-docs.deepseek.com/zh-cn/guides/anthropic_api） |
| Anthropic tool/result 局部支持 | Anthropic | Anthropic 基准 `tool_result.is_error` 有语义；DeepSeek 忽略。 | `tool_use id Fully Supported`；`tool_result...is_error Ignored`（https://api-docs.deepseek.com/zh-cn/guides/anthropic_api） |

## 4. 工具格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 仅支持 function tool | Chat | OpenAI Chat 基准还包含 custom tool、allowed_tools 等；DeepSeek Chat 只支持 `function`。 | `目前，仅支持 function 作为工具`；`最多支持 128 个 function`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Chat function shape | Chat | 与 OpenAI Chat function tool shape 基本一致：`{type:"function", function:{...}}`。 | `type...function`；`function...name`；`parameters...JSON Schema 对象`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Responses function tool 是扁平 shape | Responses | OpenAI Responses 基准 function tool 为扁平 `{type:"function", name, parameters}`，DeepSeek 与 Responses 基准一致；与 Chat 基准不同。 | `type...function, web_search, web_search_2025_08_26`；`name 用于 function 工具`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Responses 内置工具只列 web search | Responses | OpenAI Responses 基准有 web/file/computer/code/MCP 等多类；DeepSeek 只列 function 与 web_search，其他内置工具忽略。 | `除 function 外，还支持内置的 web_search 工具（服务端执行），其他内置工具类型会被忽略`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Responses 可强制 web_search | Responses | OpenAI Responses 基准 `tool_choice` 支持多种工具选择；DeepSeek 明确强制 web_search 时必须在 `tools` 中声明。 | `通过 {"type": "web_search"}...可强制模型执行联网搜索`；`tools 中必须包含 web_search 工具，否则返回 400 错误`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Anthropic `tool_choice.disable_parallel_tool_use` 忽略 | Anthropic | Anthropic Messages 基准中该字段会限制并行工具调用；DeepSeek 忽略。 | `auto Supported (disable_parallel_tool_use is ignored)`；`any Supported (disable_parallel_tool_use is ignored)`（https://api-docs.deepseek.com/zh-cn/guides/anthropic_api） |

## 5. 响应、usage 与错误格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat `finish_reason` 额外值 | Chat | OpenAI Chat 基准没有 `insufficient_system_resource`。 | `Possible values: stop, length, content_filter, tool_calls, insufficient_system_resource`；`系统推理资源不足`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Chat usage 缓存字段不按 OpenAI 细分命名 | Chat | OpenAI Chat 基准使用 `prompt_tokens_details.cached_tokens`；DeepSeek Chat 使用顶层 `prompt_cache_hit_tokens/miss_tokens`。 | `prompt_tokens 该值等于 prompt_cache_hit_tokens + prompt_cache_miss_tokens`；`prompt_cache_hit_tokens`；`prompt_cache_miss_tokens`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Chat response 返回 `reasoning_content` | Chat | OpenAI Chat 基准没有标准 `message.reasoning_content`；DeepSeek thinking 模式返回。 | `reasoning_content 仅适用于思考模式`；`最终答案之前的推理内容`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Responses error 在响应对象内 | Responses | OpenAI Responses 基准也有 `response.error`，但 DeepSeek 明确只含 `code/message`。 | `响应失败时的错误对象，包含 code 和 message 字段`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Responses incomplete reason 枚举 | Responses | 与 OpenAI Responses 基准存在交集；DeepSeek 只列两项。 | `reason 字段可能为 max_output_tokens 或 content_filter`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Responses usage 缓存字段 | Responses | OpenAI Responses 基准为 `input_tokens_details.cached_tokens`；DeepSeek 同名支持。 | `cached_tokens 命中上下文缓存的输入 token 数`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| HTTP 错误码 | 全部 | 官方错误页按 HTTP 状态列原因；不是 Anthropic `type:error` 格式说明。 | `400 - 格式错误`；`401 - 认证失败`；`429 - 请求速率达到上限`；`503 - 服务器繁忙`（https://api-docs.deepseek.com/zh-cn/quick_start/error_codes） |

## 6. 流式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 流式结束 | Chat | 与 OpenAI Chat 基准一致，用 `data: [DONE]`。 | `消息流以 data: [DONE] 结尾`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Chat usage chunk | Chat | 与 OpenAI Chat 的 `stream_options.include_usage` 兼容；最终 usage chunk 在 `[DONE]` 前。 | `data: [DONE] 之前将会传输一个额外的块`；`choices 字段将始终是一个空数组`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Responses 无 `[DONE]` | Responses | OpenAI Responses 基准本身也是语义 SSE，不用 `[DONE]`；DeepSeek 明确最后事件集合。 | `最后一个事件是 response.completed / response.incomplete / response.failed（没有 data: [DONE] 消息）`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Responses 事件带 `sequence_number` | Responses | OpenAI Responses 基准事件也有类型化事件；DeepSeek 文档明确每事件含 `sequence_number`。 | `每个事件带有表示事件类型的 event 字段和递增的 sequence_number`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Anthropic stream | Anthropic | DeepSeek 标注 `stream Fully Supported`，但没有在该页逐项列出完整 Anthropic SSE 事件集；转换层不要假定未列事件。 | `stream Fully Supported`（https://api-docs.deepseek.com/zh-cn/guides/anthropic_api） |

## 7. 已知限制

| 限制 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 工具数量 | Chat | OpenAI Chat 基准未固定为 DeepSeek 的 128 上限；DeepSeek 明确最多 128。 | `最多支持 128 个 function`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Chat `stop` 数量 | Chat | OpenAI Chat 基准通常兼容多停用词；DeepSeek 明确最多 16 个。 | `最多包含 16 个 string 的 list`（https://api-docs.deepseek.com/zh-cn/api/create-chat-completion） |
| Responses 图片/文件限制 | Responses | OpenAI Responses 基准支持图片/文件；DeepSeek 不支持。 | `不支持图片、文件输入`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Responses input/instructions 至少一个 | Responses | OpenAI Responses 基准 schema 未强制同一规则；DeepSeek 文档明确。 | `input 与 instructions 至少传一个`（https://api-docs.deepseek.com/zh-cn/api/create-response） |
| Anthropic 模型名映射 | Anthropic | Anthropic Messages 基准不会把 Claude 模型名映射到 DeepSeek 模型；DeepSeek 会映射或默认 flash。 | `不支持的模型名...自动将其映射到 deepseek-v4-flash`；`claude-opus...映射到 deepseek-v4-pro`（https://api-docs.deepseek.com/zh-cn/guides/anthropic_api） |
| 速率限制错误 | 全部 | 具体 RPM/TPM 数值未在错误页列出；官方只给 429 原因。 | `429 - 请求速率达到上限`；`请求速率（TPM 或 RPM）达到上限`（https://api-docs.deepseek.com/zh-cn/quick_start/error_codes） |

