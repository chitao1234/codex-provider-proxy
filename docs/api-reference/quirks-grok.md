# xAI Grok API quirks

> 核对日期：2026-08-13。  
> 对照基准：`messages-api.md`、`responses-api.md`、`chat-completions-api.md`。本文只记录官方文档可直接支撑的偏差；未在官方文档找到的能力不推断。

## 官方来源与访问状态

| 来源 | 状态 | 用途 | 官方摘录 |
| --- | --- | --- | --- |
| https://docs.x.ai/llms.txt | 可访问 | 官方整站文档与章节原文 | `base_url="https://api.x.ai/v1"`；`POST /v1/responses` |
| https://docs.x.ai/developers/rest-api-reference/inference/chat | 可访问 | Chat Completions 与 Responses REST reference | `POST /v1/chat/completions`；`POST /v1/responses` |
| https://docs.x.ai/developers/advanced-api-usage/prompt-caching | 可访问 | Prompt caching | `automatically`；`x-grok-conv-id`；`prompt_cache_key` |
| https://docs.x.ai/developers/advanced-api-usage/prompt-caching/usage-and-pricing | 可访问 | 缓存 usage 字段 | `usage.prompt_tokens_details.cached_tokens`；`usage.input_tokens_details.cached_tokens` |
| https://docs.x.ai/developers/advanced-api-usage/prompt-caching/how-it-works | 可访问 | 缓存命中保证 | `Prompt caching is not 100% guaranteed` |
| https://docs.x.ai/developers/advanced-api-usage/prompt-caching/maximizing-cache-hits | 可访问 | sticky routing | `x-grok-conv-id HTTP header`；`prompt_cache_key field` |
| https://docs.x.ai/developers/advanced-api-usage/prompt-caching/multi-turn | 可访问 | 多轮缓存要求 | `Any change to earlier messages breaks the cache`；`must include reasoning_content` |
| https://docs.x.ai/developers/advanced-api-usage/prompt-caching/best-practices | 可访问 | 缓存淘汰 | `Cache entries can be evicted at any time` |
| https://docs.x.ai/developers/rest-api-reference/inference/legacy | 可访问 | Deprecated Anthropic 兼容 | `Anthropic SDK compatibility is fully deprecated`；`POST /v1/messages` |
| https://docs.x.ai/developers/model-capabilities/text/generate-text | 可访问 | Responses 推荐、状态化、30 天保存 | `Responses API is the preferred way`；`stored for 30 days` |
| https://docs.x.ai/developers/model-capabilities/text/comparison | 可访问 | Chat 与 Responses 对比 | `Chat Completions API (Deprecated)`；`No reasoning content returned` |
| https://docs.x.ai/developers/model-capabilities/text/reasoning | 可访问 | Grok reasoning | `Reasoning cannot be disabled`；`reasoning_effort` |
| https://docs.x.ai/developers/model-capabilities/text/streaming | 可访问 | SSE streaming | `Server-Sent Events (SSE)`；`data: [DONE]` |
| https://docs.x.ai/developers/grok-4-6 | 可访问 | Grok 4.6 模型窗口与输出限制 | `Context window: 500,000 tokens`；`Output limit: No text output limit` |
| https://docs.x.ai/developers/models | 可访问 | 模型列表与上下文窗口 | `grok-4.6`；`500k` |
| https://docs.x.ai/developers/tools/function-calling | 可访问 | Function calling | `parallel function calling`；`strict mode` |
| https://docs.x.ai/developers/tools/web-search | 可访问 | Web Search tool | `OpenAI Responses API`；`web_search` |
| https://docs.x.ai/developers/tools/remote-mcp | 可访问 | MCP 工具 | `OpenAI compatible Responses API`；`type: "mcp"` |
| https://docs.x.ai/developers/tools/tool-usage-details | 可访问 | 服务端工具 usage | `web_search_call output items`；`server_side_tool_usage` |
| https://docs.x.ai/developers/rate-limits | 可访问 | 速率限制 | `You are unable to concurrently run your requests beyond the rate limits shown in the API console` |
| https://docs.x.ai/developers/debugging | 可访问 | HTTP 错误码与认证错误 | `Authorization: Bearer <XAI_API_KEY>`；`429 Too Many Requests` |

## 1. 端点 URL 与认证

| 协议 | 是否存在 | base_url / path | 认证 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- | --- | --- |
| OpenAI Responses | 存在；官方推荐 | `https://api.x.ai/v1/responses` | `Authorization: Bearer $XAI_API_KEY` | 路径与 OpenAI Responses 基准相同；xAI 文档称 Responses 是首选，并默认保存 30 天。 | `curl https://api.x.ai/v1/responses`；`Authorization: Bearer $XAI_API_KEY`；`Responses API is the preferred way`（https://docs.x.ai/developers/model-capabilities/text/generate-text） |
| OpenAI Chat Completions | 存在；文档对比页称 deprecated | `https://api.x.ai/v1/chat/completions` | `Authorization: Bearer $XAI_API_KEY` | 路径与 OpenAI Chat 基准相同；xAI 建议新项目用 Responses。 | `POST /v1/chat/completions`；`Chat Completions API (Deprecated)`（https://docs.x.ai/developers/rest-api-reference/inference/chat；https://docs.x.ai/developers/model-capabilities/text/comparison） |
| Anthropic Messages 兼容 | 存在但 deprecated | `https://api.x.ai/v1/messages`（legacy page 下 `POST /v1/messages`） | `Authorization: Bearer <XAI_API_KEY>` | Anthropic 基准端点为 `/v1/messages` 但通常 base `api.anthropic.com` 且要求 `x-api-key`；xAI 明确 Anthropic SDK compatibility 已 fully deprecated。 | `Anthropic SDK compatibility is fully deprecated`；`POST /v1/messages`；`This endpoint is compatible with the Anthropic API`；`Supply an Authorization: Bearer <XAI_API_KEY> header`（https://docs.x.ai/developers/rest-api-reference/inference/legacy；https://docs.x.ai/developers/debugging） |

## 2. 请求参数偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Responses `store` 默认状态化且保存 30 天 | Responses | OpenAI Responses 基准也支持 store，但 xAI 文档强调默认保存、可用 response id 续接 30 天。 | `previous input prompts, reasoning content, and model responses are saved and stored on xAI's servers`；`This behavior is on by default`；`stored for 30 days`（https://docs.x.ai/developers/model-capabilities/text/generate-text） |
| Responses 可 `store:false` | Responses | 转换层如不想服务端保存，需要显式设置。 | `If you do not want to store your previous request/response on the server, you can set store: false`（https://docs.x.ai/developers/model-capabilities/text/generate-text） |
| Responses `previous_response_id` 续接会话 | Responses | 与 OpenAI Responses 基准同名；xAI 强调无需重复 prior context。 | `send the id of the previous response, and the new messages to append to it`；`previous_response_id`（https://docs.x.ai/developers/model-capabilities/text/generate-text） |
| Chat `max_completion_tokens` 只限制可见输出 | Chat | OpenAI Chat 基准 `max_completion_tokens` 包含 visible output 与 reasoning tokens；xAI Chat 明确不包含 reasoning/function call tokens。 | `only applies to visible output tokens (i.e. does not apply to tokens used for reasoning or function calls)`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Chat `max_tokens` 已废弃 | Chat | OpenAI Chat 基准同样废弃；xAI 写 deprecated in favor of `max_completion_tokens`。 | `max_tokens`；`[DEPRECATED]`；`Deprecated in favor of max_completion_tokens`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Responses `max_output_tokens` 包含 reasoning | Responses | 与 OpenAI Responses 基准一致；默认未设为 128,000。 | `includes both output and reasoning tokens`；`Defaults to 128,000 when unset`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Responses `reasoning_effort` 是非标准替代字段 | Responses | OpenAI Responses 基准使用 `reasoning.effort`；xAI REST reference 还接受非标准 `reasoning_effort`，仅在 reasoning 字段未设置时生效。 | `reasoning_effort alternative to reasoning configuration`；`non-standard field`；`We only look at this if the reasoning field is unset`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Grok 4.6/4.5 reasoning 不能关闭 | Responses / Chat | OpenAI Responses 基准允许 `reasoning.effort` 多档；xAI reasoning 文档说明 `grok-4.6`/`grok-4.5` 默认 high 且不能禁用。 | `grok-4.6 and grok-4.5 support the reasoning_effort parameter`；`defaults to "high"`；`Reasoning cannot be disabled`（https://docs.x.ai/developers/model-capabilities/text/reasoning） |
| Reasoning 模型不支持 `stop`、presence/frequency penalty | Chat / Responses | OpenAI Chat/Responses 基准支持这些采样/惩罚字段；xAI reasoning 文档明确会报错。 | `presencePenalty, frequencyPenalty, and stop cannot be used with reasoning models`；`return an error`（https://docs.x.ai/developers/model-capabilities/text/reasoning） |
| Chat `stop` 最多 4 个且不支持 reasoning models | Chat | OpenAI Chat 基准也最多 4 个，但 xAI 明确 reasoning models 不支持。 | `stop`；`(Not supported by reasoning models)`；`Up to 4 sequences`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Anthropic `stop_sequences` 最多 4 个且 reasoning models 不支持 | Anthropic legacy | Anthropic 基准支持 stop_sequences；xAI legacy 对 reasoning models 限制。 | `stop_sequences`；`(Not supported by reasoning models)`；`Up to 4 sequences`（https://docs.x.ai/developers/rest-api-reference/inference/legacy） |
| Responses 不支持/仅兼容若干 OpenAI 字段 | Responses | OpenAI Responses 基准支持 `background`、metadata、truncation 等；xAI 多项写 Unsupported 或 compatibility only。 | `background... (Unsupported)`；`metadata... Not supported. Only maintained for compatibility reasons`；`truncation... Not supported`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| `temperature/top_p` 范围 | Chat / Responses / Anthropic legacy | xAI Chat/Responses/legacy 均写 temperature 0 到 2；top_p 为 nucleus sampling。 | `temperature...between 0 and 2`；`top_p...generally recommended to alter this or temperature but not both`（https://docs.x.ai/developers/rest-api-reference/inference/chat；https://docs.x.ai/developers/rest-api-reference/inference/legacy） |

## 3. 消息与内容格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Responses input 支持 text/image prompts | Responses | OpenAI Responses 基准支持更多文件类型和多种 content blocks；xAI REST reference 摘要写 text or image prompts。 | `Generates a response based on text or image prompts`；`input...required`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Responses instructions 是 system/developer 替代方式 | Responses | 与 OpenAI Responses 基准相近；xAI 明确不能和 `previous_response_id` 同用。 | `instructions...An alternate way to specify the system prompt`；`cannot be used alongside previous_response_id`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Responses output 是 typed `output[]` | Responses | 与 OpenAI Responses 基准相近；xAI 对比页强调不要按 Chat `choices` 读取。 | `Responses API returns content in an output array with typed items`；`output_text`（https://docs.x.ai/developers/model-capabilities/text/comparison） |
| Responses reasoning item | Responses | OpenAI Responses 基准有 reasoning item；xAI 示例返回 `type:"reasoning"`，summary_text。 | `type: "reasoning"`；`summary_text`；`status: "completed"`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Chat message roles 未在 REST reference 展开完整枚举 | Chat | OpenAI Chat 基准有 developer/system/user/assistant/tool/function；xAI Chat REST reference 只在示例确认 system/user，响应 role 固定 assistant。 | `messages...Different models support different message types`；示例 `role: "system"`、`role: "user"`；`response from model is always "assistant"`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Chat 响应仍列 `reasoning_content` 但对比页称 Chat 不返回 reasoning content | Chat | 官方文档内部存在差异：REST schema 列 `reasoning_content`，对比页写 Chat Completions 不返回 reasoning content。转换层应以模型/实际返回容错，不把它当稳定必有字段。 | `message.reasoning_content...reasoning trace generated by the model`；`No reasoning content returned`（https://docs.x.ai/developers/rest-api-reference/inference/chat；https://docs.x.ai/developers/model-capabilities/text/comparison） |
| Anthropic legacy role 包含 `system` message | Anthropic legacy | Anthropic Messages 基准正文通常 system 是顶层；xAI legacy role 说明可用 `"system"`。 | `role... "system" for system prompt, "user" for user prompt, and "assistant" for response from the model`（https://docs.x.ai/developers/rest-api-reference/inference/legacy） |
| Anthropic legacy content block 未完整展开 | Anthropic legacy | 用户要求逐项但官方 legacy 页只给 array<object...> 类型摘要，未列 image/tool_result/thinking 具体块字段；不推断。 | `content` 为 `string` 或 `array<object...>`，且标 `required`（https://docs.x.ai/developers/rest-api-reference/inference/legacy） |

## 4. 工具格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 只支持 functions 工具 | Chat | OpenAI Chat 基准还支持 custom tools；xAI Chat REST reference 只支持 function，最多 128。 | `Currently, only functions are supported as a tool`；`A max of 128 functions are supported`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Chat `parallel_tool_calls` false 限制最多一个工具 | Chat | 与 OpenAI Chat 基准相近；xAI 明确行为。 | `If set to false, the model can perform maximum one tool call`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Function calling 文档称支持并行和 strict mode | Chat / Responses | OpenAI 基准也有并行工具和 strict；xAI 文档强调 default 并行、strict 模式。 | `parallel function calling`；`By default, parallel function calling is enabled`；`strict mode`（https://docs.x.ai/developers/tools/function-calling） |
| Responses REST reference 只写 functions 与 web search | Responses | OpenAI Responses 基准支持更多 built-in/MCP/custom；xAI REST reference 此处写 currently only functions and web search，最多 128。 | `Currently, only functions and web search are supported as tools`；`A max of 128 tools are supported`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| xAI 工具指南还说明 MCP/code/image 等服务端工具 | Responses / xAI SDK | REST reference 与工具指南覆盖面不同：REST reference 写 only functions/web search，但工具页列出 MCP、code、image 等 Responses 工具。转换层应按具体目标端点和工具页分别适配。 | `Remote MCP tools are supported in the xAI native SDK, the OpenAI compatible Responses API`；工具页表格列 `OpenAI Responses API` 对应 `code_interpreter` 与 `image_generation`（https://docs.x.ai/developers/tools/remote-mcp；https://docs.x.ai/developers/tools/code-execution；https://docs.x.ai/developers/tools/image-generation） |
| Web Search 是 `tools[].type:"web_search"` | Responses | OpenAI Responses 基准有 web search tool；xAI 也支持，但 activity 输出为 `web_search_call`。 | 工具页表格列 `OpenAI Responses API` 对应 `web_search`；`In the Responses API, Web Search activity is represented as web_search_call output items`（https://docs.x.ai/developers/tools/web-search；https://docs.x.ai/developers/tools/tool-usage-details） |
| Anthropic legacy 只支持 function tools | Anthropic legacy | Anthropic Messages 基准支持客户端工具与服务端工具；xAI legacy 只列 functions，最多 128。 | `Currently, only functions are supported as a tool`；`A max of 128 functions are supported`（https://docs.x.ai/developers/rest-api-reference/inference/legacy） |
| custom/namespace 工具未在 xAI REST reference 找到 | Chat / Responses / Anthropic legacy | OpenAI Chat/Responses 基准有 custom/namespace 等；xAI 本次官方 REST reference 未列。 | 未在官方文档找到 `custom` 或 `namespace` 工具；REST reference 的正向说明为 `Currently, only functions and web search are supported as tools`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |

## 5. 响应、usage 与错误格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat id 是 UUID 样式 | Chat | OpenAI Chat 基准通常 `chatcmpl-...`；xAI 示例是 UUID。不要依赖 OpenAI 前缀。 | `"id": "a3d1008e-4544-40d4-d075-11527e794e4a"`；`object: "chat.completion"`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Responses id 是 UUID 样式，message id 加 `msg_` | Responses | OpenAI Responses 基准通常 `resp_...`；xAI 示例 response id 是 UUID，message item id 是 `msg_<uuid>`。 | `"id": "ad5663da-63e6-86c6-e0be-ff15effa8357"`；`"id": "msg_ad5663da-63e6-86c6-e0be-ff15effa8357"`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Chat usage 额外 cost 与 source 字段 | Chat | OpenAI Chat 基准没有 `cost_in_usd_ticks`、`num_sources_used`。 | `cost_in_usd_ticks`；`num_sources_used`；`prompt_tokens_details.cached_tokens`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Responses usage 额外 cost/context/server tool 字段 | Responses | OpenAI Responses 基准没有 xAI 的 cost ticks、context_details、server_side_tool_usage_details 等字段。 | `cost_in_usd_ticks`；`context_details`；`num_server_side_tools_used`；`server_side_tool_usage_details`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| 缓存命中字段 | Chat / Responses | 与 OpenAI 基准字段名一致；xAI prompt caching 文档要求看 `cached_tokens`。 | Chat: `usage.prompt_tokens_details.cached_tokens`；Responses: `usage.input_tokens_details.cached_tokens`；`Token cached by xAI from previous requests and reused`（https://docs.x.ai/developers/advanced-api-usage/prompt-caching/usage-and-pricing；https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Anthropic legacy cache write unsupported | Anthropic legacy | Anthropic 基准有 `cache_creation_input_tokens`；xAI legacy 标 Unsupported，但 `cache_read_input_tokens` 支持。 | `cache_creation_input_tokens... (Unsupported)`；`cache_read_input_tokens... Number of tokens retrieved from the cache`（https://docs.x.ai/developers/rest-api-reference/inference/legacy） |
| Responses `status` 集合较窄 | Responses | OpenAI Responses 基准有 queued/cancelled/failed 等；xAI response body 写 completed/in_progress/incomplete。 | `Status of the response. One of completed, in_progress or incomplete`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| HTTP/API 错误 schema 未在已访问章节完整展开 | 全部 | 官方 snippets 只说明请求包含不支持字段会报错或 response 内有 `error` object；本次未找到统一 HTTP error body schema。 | `error object returned when the model fails to generate a response`；`Requests that include them return an error`；`If there has been an error...return an error code with error message`（https://docs.x.ai/developers/rest-api-reference/inference/chat；https://docs.x.ai/developers/model-capabilities/text/reasoning；https://docs.x.ai/developers/debugging） |

## 6. 流式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 流式结束 `[DONE]` | Chat | 与 OpenAI Chat 基准一致；usage chunk 在 `[DONE]` 前。 | `data-only server-sent events`；`terminated by a data: [DONE] message`；`include_usage...additional chunk...before the data: [DONE]`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Responses REST reference 也写 `[DONE]` | Responses | OpenAI Responses 基准是语义事件结束（`response.completed/failed/incomplete`）且无 `[DONE]`；xAI REST reference 对 `/v1/responses` 的 `stream` 说明写 data-only SSE 与 `[DONE]`。 | `stream...partial message deltas will be sent`；`terminated by a data: [DONE] message`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Streaming guide 示例也以 `[DONE]` 结束 | Chat / Responses | 与 OpenAI Responses 基准不同；转换层要容忍 `[DONE]`。 | `Server-Sent Events (SSE)`；`data: [DONE]`（https://docs.x.ai/developers/model-capabilities/text/streaming） |
| Responses reasoning 流式事件 | Responses | 与 OpenAI Responses reasoning 事件相近；xAI reasoning 文档示例监听 reasoning text/summary delta。 | `response.reasoning_text.delta`；`response.reasoning_summary_text.delta`；`event.delta`（https://docs.x.ai/developers/model-capabilities/text/reasoning） |
| Anthropic legacy 流式也用 data-only SSE + `[DONE]` | Anthropic legacy | Anthropic Messages 基准使用 named event 如 `message_start/message_stop`；xAI deprecated Anthropic legacy 页写 data-only SSE 与 `[DONE]`，不是 Anthropic named event 集。 | `partial message deltas will be sent`；`data-only server-sent events`；`terminated by a data: [DONE] message`（https://docs.x.ai/developers/rest-api-reference/inference/legacy） |
| Anthropic legacy `/v1/complete` stream 标 Unsupported | Anthropic legacy | 旧 Anthropic completions 不是本项目主基准，但若兼容层支持需注意。 | `stream... (Unsupported)`（https://docs.x.ai/developers/rest-api-reference/inference/legacy） |

## 7. 已知限制

| 限制 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| 请求体必填字段 | Chat / Responses / Anthropic legacy | xAI REST reference 没有统一的 request required 数组；Chat Request Body 列 `messages/model`，Responses 明确 `input` required 并列 `model`，Anthropic legacy 列 `max_tokens/messages/model`。转换层应至少保证这些入口字段按协议存在。 | Chat `messages...A list of messages`、`model...Model name`；Responses `input...required`、`model...Model name`；Anthropic legacy `max_tokens (integer)`、`messages (array<object>)`、`model (string)`（https://docs.x.ai/developers/rest-api-reference/inference/chat；https://docs.x.ai/developers/rest-api-reference/inference/legacy） |
| Grok 4.6 上下文与输出 | Chat / Responses | 基准协议不规定 xAI 模型窗口；Grok 4.6 页面写 500,000 token context，文本输出无单独上限。 | `Context window: 500,000 tokens`；`Output limit: No text output limit`；模型表 `grok-4.6...500k`（https://docs.x.ai/developers/grok-4-6；https://docs.x.ai/developers/models） |
| Chat function/tools 最大 128 | Chat / Anthropic legacy | OpenAI Chat 基准常见 tools 最多 128；xAI Chat/legacy Anthropic 都写 128。 | `A max of 128 functions are supported`（https://docs.x.ai/developers/rest-api-reference/inference/chat；https://docs.x.ai/developers/rest-api-reference/inference/legacy） |
| Responses tools 最大 128 | Responses | OpenAI Responses 基准工具数量可能不同；xAI REST reference 写 max 128。 | `A max of 128 tools are supported`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Chat/Responses 默认最大输出 128,000 | Chat / Responses | 基准协议不规定 xAI 默认值；xAI 未设置时默认 128,000。 | `Defaults to 128,000 when unset`（https://docs.x.ai/developers/rest-api-reference/inference/chat） |
| Responses 保存期限 30 天 | Responses | OpenAI Responses 基准的 retention 由 OpenAI 平台规则决定；xAI 明确 30 天。 | `stored for 30 days, after which they will be removed`（https://docs.x.ai/developers/model-capabilities/text/generate-text） |
| Prompt caching 自动但不保证命中 | Chat / Responses | OpenAI 基准 prompt caching 机制不同；xAI 自动缓存消息前缀，并建议 sticky routing。 | `performs prompt caching automatically`；`Prompt caching is not 100% guaranteed`；`Use x-grok-conv-id to maximize cache hit rates`（https://docs.x.ai/developers/advanced-api-usage/prompt-caching；https://docs.x.ai/developers/advanced-api-usage/prompt-caching/how-it-works） |
| Chat 用 `x-grok-conv-id`，Responses 用 `prompt_cache_key` | Chat / Responses | OpenAI Chat 基准用 `prompt_cache_key`；xAI Chat 另有 header，Responses body 字段同名。 | `x-grok-conv-id HTTP header routes requests with the same conversation ID to the same server`；`For the Responses API, use the prompt_cache_key field directly`（https://docs.x.ai/developers/advanced-api-usage/prompt-caching/maximizing-cache-hits） |
| 缓存前缀不能改历史消息 | Chat / Responses | 转换层必须 append-only；对 reasoning models 要带 reasoning_content。 | `Any change to earlier messages breaks the cache`；`must include reasoning_content from previous responses`（https://docs.x.ai/developers/advanced-api-usage/prompt-caching/multi-turn） |
| 缓存 TTL 不固定，可随时淘汰 | Chat / Responses | 不要按 Anthropic 5m/1h 断点 TTL 实现。 | `Cache entries can be evicted at any time due to server load or restarts`（https://docs.x.ai/developers/advanced-api-usage/prompt-caching/best-practices） |
| 速率限制在控制台显示，不能超过并发限制 | 全部 | 官方公开文档没有在本次抓取中给出固定 RPM/TPM 表；按控制台额度处理。 | `You are unable to concurrently run your requests beyond the rate limits shown in the API console`（https://docs.x.ai/developers/rate-limits） |
| Anthropic 兼容已 fully deprecated | Anthropic legacy | 转换层不应为新功能依赖此端点。 | `Anthropic SDK compatibility is fully deprecated`；`Please migrate to the Responses API or gRPC`（https://docs.x.ai/developers/rest-api-reference/inference/legacy） |
