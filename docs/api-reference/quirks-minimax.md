# MiniMax API quirks

> 核对日期：2026-08-13。  
> 对照基准：`messages-api.md`、`responses-api.md`、`chat-completions-api.md`。本文只记录官方文档可直接支撑的偏差；未在官方文档找到的能力不推断。

## 官方来源与访问状态

| 来源 | 状态 | 用途 | 官方摘录 |
| --- | --- | --- | --- |
| https://platform.minimaxi.com/docs/llms.txt | 可访问 | 文档索引与 OpenAPI spec 地址 | `OpenAPI Specs`；`openapi-responses`；`openapi-chat-openai`；`openapi-chat-anthropic` |
| https://platform.minimaxi.com/docs/api-reference/text-chat-openai | 可访问 | OpenAI Chat Completions 兼容接口 | `POST /v1/chat/completions`；`工具定义列表，当前支持 function 工具` |
| https://platform.minimaxi.com/docs/api-reference/responses-create | 可访问 | OpenAI Responses 兼容接口 | `POST /v1/responses`；`OpenAI Responses API 兼容的主接口` |
| https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic | 可访问 | Anthropic Messages 兼容接口 | `POST /anthropic/v1/messages`；`仅支持 auto 和 none` |
| https://platform.minimaxi.com/docs/api-reference/anthropic-api-compatible-cache | 可访问 | Anthropic 主动缓存 | `cache_control`；`生命周期为 5 分钟` |
| https://platform.minimaxi.com/docs/api-reference/text-prompt-caching | 可访问 | 自动 Prompt 缓存 | `自动识别重复内容并缓存`；`512 个及以上` |
| https://platform.minimaxi.com/docs/guides/rate-limits | 可访问 | 速率限制 | `RPM`；`TPM` |
| https://platform.minimaxi.com/docs/api-reference/errorcode | 可访问 | 错误码 | `接口响应基本结构`；`base_resp` |

## 1. 端点 URL 与认证

| 协议 | 是否存在 | base_url / path | 认证 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- | --- | --- |
| OpenAI Chat Completions | 存在 | OpenAPI server `https://api.minimaxi.com` + `POST /v1/chat/completions`；SDK 示例还写国内 `https://api.minimaxi.com/v1`、国际 `https://api.minimax.io/v1` | `Authorization: Bearer <API_KEY>` | 路径与 OpenAI Chat 基准相同；base URL 是 MiniMax 域名。 | `POST /v1/chat/completions`；`国内用户使用 https://api.minimaxi.com/v1，国际用户使用 https://api.minimax.io/v1`；`HTTP Authorization Scheme: Bearer API_key`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai；https://platform.minimaxi.com/docs/api-reference/text-prompt-caching） |
| OpenAI Responses | 存在 | OpenAPI server `https://api.minimaxi.com` + `POST /v1/responses` | `Authorization: Bearer <API_KEY>` | 路径与 OpenAI Responses 基准相同；Responses 文档只列 Bearer。 | `POST /v1/responses`；`HTTP Authorization Scheme: Bearer API_key`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Anthropic Messages | 存在 | OpenAPI server `https://api.minimaxi.com` + `POST /anthropic/v1/messages`；Anthropic SDK 示例 base `https://api.minimaxi.com/anthropic` | 支持 `Authorization: Bearer <API_KEY>` 和 `x-api-key: <API_KEY>`；Bearer 优先且推荐 | Anthropic 基准使用 `x-api-key` 与 `anthropic-version`；MiniMax 未要求 `anthropic-version`，并推荐 Bearer。 | `POST /anthropic/v1/messages`；`base_url="https://api.minimaxi.com/anthropic"`；`如果 Authorization 和 x-api-key 同时存在，优先使用 Authorization`；`推荐使用 Authorization: Bearer <API_KEY>`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic；https://platform.minimaxi.com/docs/api-reference/anthropic-api-compatible-cache） |

## 2. 请求参数偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 使用 `max_completion_tokens`，`max_tokens` 已废弃 | Chat | OpenAI Chat 基准仍列 `max_tokens` 但标废弃；MiniMax Chat 明确要求改用 `max_completion_tokens`，且上限模型相关。 | `旧版生成长度限制参数。已弃用，请改用 max_completion_tokens`；`MiniMax-M3 推荐值为 131072（128K），上限为 524288（512K）`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Responses 使用 `max_output_tokens` | Responses | 与 OpenAI Responses 基准同名；MiniMax 文档只写“最大输出 token 数”。 | `max_output_tokens`；`最大输出 token 数`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Anthropic 使用 `max_tokens` | Anthropic | 与 Anthropic Messages 基准同名；上限与 MiniMax 模型相关。 | `max_tokens`；`MiniMax-M3 推荐值为 131072（128K），上限为 524288（512K）`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| Chat `thinking` 默认开启 adaptive | Chat | OpenAI Chat 基准无 `thinking` 对象；MiniMax Chat 省略时默认开启 M3 adaptive thinking。 | `控制 MiniMax-M3 thinking。省略时默认开启 adaptive thinking`；`type...disabled, adaptive`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Anthropic `thinking` 默认关闭 | Anthropic | Anthropic 基准 enabled thinking 需要 `budget_tokens`；MiniMax Anthropic 只列 `disabled/adaptive`，且省略时关闭 M3 thinking。 | `省略时默认关闭 thinking`；`disabled`；`adaptive`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| Responses `reasoning.effort` 非 `none` 只作为开关 | Responses | OpenAI Responses 基准用 `reasoning.effort` 控制推理强度；MiniMax 对 M3 不细分强度。 | `minimal、low、medium 和 high 这些取值会被兼容接收并开启推理输出，但不会调节 MiniMax-M3 的推理深度`；`none`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| M2.x thinking 无法关闭 | Chat / Responses / Anthropic | 三协议基准不会按 MiniMax M2 系列保留 thinking；MiniMax 明确即使传 disabled/none 也无法关闭。 | `对于 M2.x 模型，thinking 无法关闭`；`即使传入 reasoning: {"effort": "none"}，推理仍会保持开启`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai；https://platform.minimaxi.com/docs/api-reference/responses-create） |
| `temperature/top_p` 范围 | Chat / Anthropic | OpenAI Chat/Responses 基准常见 `temperature` 为 0 到 2；MiniMax Chat/Anthropic 同为 `[0,2]`，`top_p` 为 `[0,1]`。Responses 只允许 `(0,1]`。 | Chat: `temperature...取值范围 [0, 2]`；`top_p...取值范围 [0, 1]`；Responses: `采样温度，取值范围 (0, 1]`；`核采样，取值范围 (0, 1]`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai；https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Chat `stop` 未在 OpenAPI 请求体列出 | Chat | OpenAI Chat 基准有 `stop`；本次 MiniMax Chat OpenAPI 请求体未列 `stop`，只在 finish reason 说明自然停止/长度/工具等。 | 未在官方文档找到请求参数 `stop`；核对页面：`POST /v1/chat/completions`；`生成停止的原因：stop、length、content_filter 或 tool_calls`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Anthropic `stop_sequences` 未在请求体列出 | Anthropic | Anthropic 基准支持 `stop_sequences`；MiniMax Anthropic OpenAPI 本次未列该字段。 | 未在官方文档找到请求参数 `stop_sequences`；核对页面：`POST /anthropic/v1/messages`；响应字段含 `stop_sequence: null`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| Responses `tool_choice` 仅 `none/auto` | Responses | OpenAI Responses 基准支持强制指定工具等多种对象；MiniMax Responses 只列字符串 `none/auto`。 | `tool_choice`；`none`；`auto`；`工具选择策略：none 表示不调用任何工具；auto 表示由模型自动判断是否调用工具`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Anthropic `tool_choice` 仅 `auto/none` | Anthropic | Anthropic Messages 基准支持 `auto/any/tool/none` 以及 `disable_parallel_tool_use`；MiniMax Anthropic 只列 `auto/none`。 | `工具选择策略。仅支持 auto 和 none`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| `service_tier` 是 MiniMax 扩展 | Chat / Responses / Anthropic | OpenAI/Anthropic 基准有各自 service tier 枚举；MiniMax 使用 `standard/priority`，且 priority 价格 1.5 倍。 | `支持的取值为 standard 和 priority`；`priority 的价格为 standard 的 1.5 倍`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |

## 3. 消息与内容格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat role 不含 `developer` | Chat | OpenAI Chat 基准支持 `developer`；MiniMax Chat role 枚举只有 system/user/assistant/tool。 | `消息发送者的角色`；`system`、`user`、`assistant`、`tool`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Chat content 支持视频 | Chat | OpenAI Chat 基准 content part 支持 text/image/audio/file；MiniMax Chat content part 为 text/image_url/video_url。 | `内容块类型：text、image_url、video_url`；`MiniMax-M3 支持文本、图片和视频输入`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Chat 多模态大小限制 | Chat | OpenAI Chat 基准不使用 MiniMax 的 `mm_file://` 和这些大小限制。 | `视频文件 ≤ 50 MB，图片文件 ≤ 10 MB，请求体 ≤ 64 MB`；`Files API 视频最大 512 MB`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Responses role 包含 `tool` | Responses | OpenAI Responses 基准 message role 为 user/assistant/system/developer；工具结果通常是 item，不是 `role:"tool"` message。MiniMax InputItem role 枚举还列 `tool`。 | `role...user, assistant, system, developer, tool`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Responses input item 类型较少 | Responses | OpenAI Responses 基准支持更多 tool/built-in/MCP item；MiniMax 列 `message/function_call/function_call_output/reasoning`。 | `type 字段决定具体形态：message / function_call / function_call_output / reasoning`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Responses content 支持视频但不列文件 | Responses | OpenAI Responses 基准支持 `input_file`；MiniMax content part 为 input_text/output_text/input_image/input_video。 | `input_text`、`output_text`、`input_image`、`input_video`；`input_video: 视频输入`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Anthropic 普通 message role 是 MiniMax 自有集合 | Anthropic | Anthropic Messages 基准只有 user/assistant 输入消息，system 是顶层；MiniMax schema 还列 `user_system/group/sample_message_user/sample_message_ai`，并说明 M3 使用 user/assistant 交替。 | `role...user, assistant, user_system, group, sample_message_user, sample_message_ai`；`MiniMax-M3 使用 user / assistant 交替消息`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| Anthropic content 块扩展视频和 mid-conversation system | Anthropic | Anthropic 基准有 document/search_result/server tools 等；MiniMax 请求块列 text/image/video/tool_use/tool_result/thinking/mid_conv_system。 | `内容块类型：text...image...video...tool_use...tool_result...thinking...mid_conv_system`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| Anthropic response 不会出现图片/视频/tool_result | Anthropic | Anthropic 基准响应可出现服务端工具结果等；MiniMax 明确响应只输出 text、tool_use、thinking。 | `响应中不会出现 image、video、tool_result 或 mid_conv_system 块`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |

## 4. 工具格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 仅支持 function tool | Chat | OpenAI Chat 基准还支持 custom tool 等；MiniMax Chat 只列 function。 | `工具定义列表，当前支持 function 工具`；`工具类型，固定为 function`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Chat function shape 是 OpenAI Chat 嵌套格式 | Chat | 与 OpenAI Chat 基准一致；不同于 Responses 扁平工具格式。 | `function.name`；`function.parameters`；`函数参数 JSON Schema`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Responses 仅 function tool，且扁平字段 | Responses | OpenAI Responses 基准有 built-in、MCP、custom/grammar 等；MiniMax Responses 只列 `type:"function"`、`name`、`parameters`。 | `工具类型`；`function`；`函数参数定义，使用 JSON Schema 格式`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Anthropic client tool 用 `input_schema` | Anthropic | 与 Anthropic Messages 基准的客户端工具字段一致；MiniMax 未列服务端工具。 | `Anthropic 兼容工具调用的工具定义`；`input_schema`；`工具输入参数 JSON Schema`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| Anthropic tool `cache_control` 可加在工具定义 | Anthropic | Anthropic 基准支持工具定义缓存；MiniMax 主动缓存文档也支持。 | `工具：tools 数组中的工具定义`；`cache_control 参数放置在最后一个工具`（https://platform.minimaxi.com/docs/api-reference/anthropic-api-compatible-cache） |
| 服务端工具未在三套主接口 OpenAPI 中列出 | 全部 | OpenAI Responses / Anthropic 基准有 web/code/MCP 等服务端工具；MiniMax 官方索引另有“服务端工具”指南，但本次三个主接口 schema 未列服务端工具类型。 | 未在官方文档找到 `web_search`、`code_interpreter`、`mcp` 等工具类型；已核对 `tools` 说明：Chat `当前支持 function 工具`；Responses `function`；Anthropic `工具输入参数 JSON Schema`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai；https://platform.minimaxi.com/docs/api-reference/responses-create；https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |

## 5. 响应、usage 与错误格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat `reasoning_content/reasoning_details` 需 `reasoning_split` | Chat | OpenAI Chat 基准没有这些标准字段；MiniMax 可把 thinking 拆到非标准字段。 | `reasoning_split`；`启用后将 thinking 内容拆分到 reasoning_content 和 reasoning_details 字段`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Chat `finish_reason` 枚举 | Chat | 与 OpenAI Chat 基准类似但没有 legacy `function_call`；MiniMax 列 stop/length/content_filter/tool_calls。 | `生成停止的原因：stop、length、content_filter 或 tool_calls`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Chat usage schema 文档只抽到 `total_tokens`，示例含 cache 细节 | Chat | OpenAI Chat 基准有完整 prompt/completion details；MiniMax OpenAPI schema 的 Usage 只列 `total_tokens`，Prompt 缓存页示例含 `prompt_tokens_details.cached_tokens`。 | `total_tokens`；`响应包含上下文缓存的 Token 使用信息`；`prompt_tokens_details...cached_tokens`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai；https://platform.minimaxi.com/docs/api-reference/text-prompt-caching） |
| Chat 错误可能出现在 `base_resp` | Chat | OpenAI Chat 基准错误通常 HTTP error body；MiniMax Chat 响应 schema 还列 `base_resp.status_code/status_msg`。 | `base_resp`；`status_code`；`status_msg`；`1027: 输出内容错误`；`1039: Token 超出限制`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Responses `id` 子项格式 | Responses | OpenAI Responses 基准不要求 MiniMax 的 `<id>_msg/_rs/_fc_<index>` 描述。 | `id`；`<id>_msg 格式`；`<id>_rs 格式`；`<id>_fc_<index> 格式`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Responses status 集合较窄 | Responses | OpenAI Responses 基准还有 cancelled/queued/in_progress 等；MiniMax schema 只列 completed/incomplete/failed。 | `响应状态`；`completed`、`incomplete`、`failed`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Responses usage cache 字段 | Responses | 与 OpenAI Responses 基准同名；MiniMax 只列 `cached_tokens`，没有 cache write 字段。 | `input_tokens_details.cached_tokens`；`Prompt 缓存命中的 token 数`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Responses error 在 response 对象内 | Responses | OpenAI Responses 基准也有 `error`，MiniMax 只列 `code/message`。 | `错误信息，仅在 status=failed 时返回`；`code`；`message`（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Anthropic usage cache 字段 | Anthropic | 与 Anthropic Messages 基准的读/写字段一致；MiniMax 主动缓存和自动缓存都使用这两个字段。 | `cache_creation_input_tokens`；`cache_read_input_tokens`；`命中 prompt cache 的输入 token 数`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| Anthropic 错误格式 | Anthropic | 与 Anthropic 基准类似但枚举是 MiniMax 文档列出的集合。 | `统一使用 HTTP 状态码 + JSON body`；`event: error`；`invalid_request_error`、`authentication_error`、`rate_limit_error`、`overloaded_error`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |

## 6. 流式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 流式 chunk 是 `chat.completion.chunk` | Chat | 与 OpenAI Chat 基准相近；MiniMax schema 说明 usage 仅最后 chunk 返回，但未在正文抽到 `[DONE]` 结束哨兵。 | `对象类型，固定为 chat.completion.chunk`；`Token 使用情况（仅在最后一个 chunk 中返回）`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Chat `stream_options.include_usage` | Chat | 与 OpenAI Chat 基准相近，但 MiniMax 只列 include_usage 一个子字段。 | `stream_options`；`include_usage`；`是否在流式响应中包含 token 用量`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| Responses 流式只说明 SSE，未列事件集合 | Responses | OpenAI Responses 基准有语义事件全集；MiniMax Responses OpenAPI 只说明 `stream:true` 启用 SSE，没有抽到事件名枚举。 | `设置为 true 启用 SSE 流式响应`；未在官方文档找到 `response.completed` 等事件名枚举（https://platform.minimaxi.com/docs/api-reference/responses-create） |
| Anthropic 流式事件集合 | Anthropic | 与 Anthropic Messages 基准事件名相近，但 MiniMax 枚举不含 `error`，错误另以 `event:error` 说明；delta 枚举不含 `input_json_delta`。 | `message_start`、`ping`、`content_block_start`、`content_block_delta`、`content_block_stop`、`message_delta`、`message_stop`；`text_delta、thinking_delta 或 signature_delta`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| Anthropic 流式错误 | Anthropic | Anthropic 基准也可能在 HTTP 200 后流式 error；MiniMax 明确收到 error 后停止读取并清理状态。 | `流式过程中出现错误时，会以 event: error SSE 事件下发`；`客户端应在收到 error 后停止读取并清理本次会话状态`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |

## 7. 已知限制

| 限制 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| 请求体必填字段 | Chat / Responses / Anthropic | 三协议字段名不同，转换层不能只按一个协议补字段：Chat 和 Anthropic 必填 `model/messages`，Responses 必填 `model/input`。 | Chat `required: model, messages`；Responses `required: model, input`；Anthropic `required: model, messages`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai；https://platform.minimaxi.com/docs/api-reference/responses-create；https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| MiniMax-M3 上下文 | Chat / Responses / Anthropic | 三协议基准按 OpenAI/Anthropic 模型窗口定义；MiniMax 文档强调 M3 1M 超长上下文。 | `1M 超长上下文`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| 输出上限 | Chat / Anthropic | OpenAI/Anthropic 基准没有 MiniMax M3 512K 输出上限；MiniMax M3 推荐 128K，上限 512K。 | `MiniMax-M3 推荐值为 131072（128K），上限为 524288（512K）`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai；https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| 其它模型输出上限 | Chat / Anthropic | MiniMax M2.x 推荐 64K，上限 200K。 | `其他模型推荐值为 65536（64K），上限为 204800（200K）`（https://platform.minimaxi.com/docs/api-reference/text-chat-openai） |
| 多模态模型限制 | Chat / Anthropic | MiniMax 只让 M3 支持图像/视频，M2 系列不支持。 | `M2.7、M2.5、M2.1 和 M2 系列仅支持文本与工具调用，不支持图片和视频输入`（https://platform.minimaxi.com/docs/api-reference/text-chat-anthropic） |
| 自动缓存最小 token | 全部 | OpenAI/Anthropic 基准的缓存阈值不同；MiniMax 自动缓存要求至少 512 输入 token。 | `缓存适用于包含 512 个及以上的输入 token 数量的 API 调用`（https://platform.minimaxi.com/docs/api-reference/text-prompt-caching） |
| 自动缓存前缀顺序 | 全部 | 转换层应保持工具、系统、历史顺序稳定。 | `缓存采用前缀匹配的方式，以「工具定义-系统提示词-历史对话内容」为顺序构建`（https://platform.minimaxi.com/docs/api-reference/text-prompt-caching） |
| Anthropic 主动缓存 TTL | Anthropic | Anthropic 基准可有 5m/1h；MiniMax 主动缓存只写 5 分钟且命中刷新。 | `生命周期为 5 分钟`；`每次命中缓存内容时，缓存生命周期都会自动刷新`（https://platform.minimaxi.com/docs/api-reference/anthropic-api-compatible-cache） |
| Anthropic 主动缓存断点限制 | Anthropic | 与 Anthropic 基准类似但 MiniMax 明确超过 4 个取最近 4 个。 | `20 块回溯窗口`；`一次调用最多支持 4 个 cache_control 参数，若超过 4 个，只取从后向前最近的 4 个`（https://platform.minimaxi.com/docs/api-reference/anthropic-api-compatible-cache） |
| 速率限制按 RPM/TPM | 全部 | 具体额度随模型/账号；转换层应处理 429。 | `RPM`；`TPM`；`速率限制`（https://platform.minimaxi.com/docs/guides/rate-limits） |
