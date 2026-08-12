# 小米 MiMo API quirks

> 核对日期：2026-08-13。  
> 对照基准：`messages-api.md`、`responses-api.md`、`chat-completions-api.md`。本文只记录官方文档可直接支撑的偏差；未在官方文档找到的能力不推断。

## 官方来源与访问状态

| 来源 | 状态 | 用途 | 官方摘录 |
| --- | --- | --- | --- |
| https://mimo.mi.com/llms.txt | 可访问 | 文档索引与静态 Markdown 地址 | `compatible with OpenAI and Anthropic API formats`；`OpenAI Chat Completion API`；`OpenAI Responses API`；`Anthropic API` |
| https://mimo.mi.com/docs/zh-CN/api/chat/openai-api | 可访问；也可访问对应静态 Markdown | OpenAI Chat Completions 兼容接口 | `https://api.xiaomimimo.com/v1/chat/completions` |
| https://mimo.mi.com/docs/zh-CN/api/chat/responses | 可访问；也可访问对应静态 Markdown | OpenAI Responses 兼容接口 | `https://api.xiaomimimo.com/v1/responses` |
| https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api | 可访问；也可访问对应静态 Markdown | Anthropic Messages 兼容接口 | `https://api.xiaomimimo.com/anthropic/v1/messages` |
| https://mimo.mi.com/static/docs/api/guidance/rate-limit.md | 可访问 | RPM/TPM 与 429 | `model concurrency limit`；`429 error may occur` |
| https://mimo.mi.com/static/docs/api/guidance/model-hyperparameters.md | 可访问 | thinking 下采样参数限制 | `do not support customizing the temperature and top_p parameters` |
| https://mimo.mi.com/static/docs/api/guidance/error-codes.md | 可访问 | HTTP 错误码 | `common error codes and solutions`；`400 - Invalid Format`；`429 - Too Many Requests` |
| https://mimo.mi.com/static/docs/price/pay-as-you-go.md | 可访问 | Prompt Cache 计费存在性 | `Cache Hit`；`Cache Write: Limited-time Free` |
| https://mimo.mi.com/static/docs/quick-start/usage-guide/text-generation/deep-thinking.md | 可访问 | thinking、reasoning 回传与 `[DONE]` 示例 | `must completely pass back the reasoning_content field`；`data: [DONE]` |
| https://mimo.mi.com/static/docs/quick-start/faq/api-integration.md | 可访问 | auth header、工具调用建议 | `api-key: $MIMO_API_KEY`；`Authorization: Bearer $MIMO_API_KEY` |
| https://mimo.mi.com/static/docs/quick-start/usage-guide/text-generation/tool-calling/web-search.md | 可访问 | Web Search 插件限制 | `Web Search Plugin`；`5-minute cache period`；`multiple keywords concurrently` |
| https://mimo.mi.com/static/docs/tokenplan/integration/openclaw.md | 可访问 | OpenClaw 集成与模型窗口样例 | `contextWindow: 1048576`；`maxTokens: 131072` |

## 1. 端点 URL 与认证

| 协议 | 是否存在 | base_url / path | 认证 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- | --- | --- |
| OpenAI Chat Completions | 存在 | `https://api.xiaomimimo.com/v1/chat/completions` | 页面示例 `Authorization: Bearer $MIMO_API_KEY`；FAQ 还写 `api-key: $MIMO_API_KEY` | 路径与 OpenAI Chat 基准相同；MiMo FAQ 额外接受 `api-key` header。 | `https://api.xiaomimimo.com/v1/chat/completions`；`Authorization: Bearer $MIMO_API_KEY`；`api-key: $MIMO_API_KEY or Authorization: Bearer $MIMO_API_KEY`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api；https://mimo.mi.com/static/docs/quick-start/faq/api-integration.md） |
| OpenAI Responses | 存在 | `https://api.xiaomimimo.com/v1/responses` | `Authorization: Bearer $MIMO_API_KEY`；FAQ 也写 `api-key` | 路径与 OpenAI Responses 基准相同；未定义参数可能被过滤或报错。 | `https://api.xiaomimimo.com/v1/responses`；`Authorization: Bearer $MIMO_API_KEY`；`Only the parameters documented here will be processed normally`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Anthropic Messages | 存在 | `https://api.xiaomimimo.com/anthropic/v1/messages` | `Authorization: Bearer $MIMO_API_KEY`；FAQ 也写 `api-key` | Anthropic 基准使用 `x-api-key` 和 `anthropic-version`；MiMo Anthropic 页面只展示 Bearer。 | `https://api.xiaomimimo.com/anthropic/v1/messages`；`Authorization: Bearer $MIMO_API_KEY`；`Anthropic interface /anthropic/v1/messages follows Claude format`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api；https://mimo.mi.com/static/docs/quick-start/faq/api-integration.md） |

## 2. 请求参数偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 使用 `max_completion_tokens` | Chat | OpenAI Chat 基准也支持该字段，但 MiMo 没有把 `max_tokens` 列为主字段；范围 `[1,131072]`。 | `max_completion_tokens`；`including visible output tokens and reasoning tokens`；`Required range: [1, 131072]`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Responses 使用 `max_output_tokens` | Responses | 与 OpenAI Responses 基准同名；范围 `[1,131072]`。 | `max_output_tokens`；`including visible output tokens and reasoning tokens`；`Required range: [1, 131072]`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Anthropic 使用 `max_tokens` | Anthropic | 与 Anthropic Messages 基准同名；MiMo 写明模型默认值和范围。 | `max_tokens`；`mimo-v2.5-pro: default 131072`；`Required range: [1, 131072]`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| Responses 不支持若干 OpenAI 参数 | Responses | OpenAI Responses 基准支持 `background`、`previous_response_id`、`context_management`；MiMo 明确不支持，可能忽略或报错。 | `Fields such as background, previous_response_id, and context_management are not currently supported`；`will be ignored or trigger an error`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Responses 只处理文档列出的参数 | Responses | OpenAI Responses 基准字段很多；MiMo 对未定义参数更严格。 | `Only the parameters documented here will be processed normally`；`undefined parameters will be filtered out and may cause request errors`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Chat `thinking` 是非标准 OpenAI 参数 | Chat | OpenAI Chat 基准无 `thinking` 顶层对象；MiMo 要通过 SDK `extra_body` 传。 | `The thinking field is not a standard OpenAI parameter`；`must be included in extra_body`（https://mimo.mi.com/static/docs/quick-start/usage-guide/text-generation/deep-thinking.md） |
| Chat `thinking.type` 使用 enabled/disabled | Chat | MiniMax Chat 用 adaptive/disabled；MiMo 示例用 enabled/disabled。 | `thinking: {"type": "enabled"}`；`thinking: {"type": "disabled"}`（https://mimo.mi.com/static/docs/quick-start/usage-guide/text-generation/deep-thinking.md） |
| Responses `reasoning.effort` 只有开关效果 | Responses | OpenAI Responses 基准把 effort 当强度；MiMo `none` 关闭，其它值启用但行为相同。 | `none disables thinking; low / medium / high all enable reasoning with identical behavior`；`fine-grained reasoning intensity differentiation is not yet supported`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Anthropic `thinking` 不使用 `budget_tokens` | Anthropic | Anthropic 基准 enabled thinking 要 `budget_tokens`；MiMo Anthropic 只描述启用 extended thinking，没有列预算字段。 | `Configuration for enabling model's extended thinking`；未在官方文档找到 `budget_tokens`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| thinking 模式下不能自定义 `temperature/top_p` | Chat / Responses / Anthropic | OpenAI/Anthropic 基准通常允许采样参数；MiMo thinking 模式会强制默认值。 | `do not support customizing the temperature and top_p parameters`；`forcibly set by the model to its recommended default values of 1.0 and 0.95`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api；https://mimo.mi.com/static/docs/api/guidance/model-hyperparameters.md） |
| `temperature/top_p` 范围 | Chat / Responses / Anthropic | MiMo `temperature` 为 `[0,1.5]`，`top_p` 为 `[0.01,1.0]`；不同于 OpenAI Chat 常见 0 到 2。 | `Required range: [0, 1.5]`；`Required range: [0.01, 1.0]`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Chat `tool_choice` 只支持 `auto` 且非 auto 会被移除 | Chat | OpenAI Chat 基准支持 none/auto/required/强制工具；MiMo 文档只列 auto。 | `Available options: auto`；`When a value other than auto is passed to tool_choice, the backend will remove this field by default`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Responses `tool_choice` 只支持 `auto` 且非 auto 会被移除 | Responses | OpenAI Responses 基准支持对象强制选择；MiMo 只列 auto。 | `Available options: auto`；`backend will remove this field by default`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Anthropic `tool_choice.type` 只支持 `auto` | Anthropic | Anthropic 基准支持 auto/any/tool/none；MiMo 只列 auto，但保留 `disable_parallel_tool_use`。 | `Available options: auto`；`disable_parallel_tool_use`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| `stop` / `stop_sequences` | Chat / Anthropic | Chat 支持最多 4 个；Anthropic 支持 `stop_sequences`，未写数量上限。 | Chat: `Up to 4 sequences`；Anthropic: `Custom text sequences that will cause the model to stop generating`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api；https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |

## 3. 消息与内容格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat role 支持 `developer` | Chat | 与新版 OpenAI Chat 基准一致；不同于许多兼容厂商。 | `Available options: developer`；`Available options: system`；`assistant`；`tool`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Chat user content 支持 audio/video | Chat | OpenAI Chat 基准支持 text/image/audio/file；MiMo 支持 text/image_url/input_audio/video_url，且只有 `mimo-v2.5` 支持多模态输入。 | `Can contain text, image, audio or video inputs`；`Currently, only the mimo-v2.5 model supports image, audio, or video input`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Chat tool message 只支持 text content part | Chat | OpenAI Chat 基准 tool content 可为字符串或 text parts；MiMo 明确 tool message 只支持 text。 | `For tool messages, only type text is supported`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Responses role 支持四类 | Responses | 与 OpenAI Responses 基准相近：user/assistant/system/developer。 | `Available options: user, assistant, system, developer`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Responses 输入只写 text/image | Responses | OpenAI Responses 基准支持 file 等；MiMo 顶部写 `Text, image inputs`，且只有 `mimo-v2.5` 支持 image。 | `Text, image inputs to the model`；`Currently, only the mimo-v2.5 model supports image input`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Responses output 包含 `reasoning` item | Responses | 与 OpenAI Responses reasoning item 类似；MiMo 要后续手动管理上下文时回传。 | `A description of the chain of thought used by a reasoning model`；`Be sure to include these items in your input...for subsequent turns`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Anthropic message role 仅 `user/assistant` | Anthropic | Anthropic 基准普通 input message 也是 user/assistant；system 用顶层参数。 | `Role of the message`；`Available options: user, assistant`；`system`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| Anthropic content 块未列 document/search_result/server_tool | Anthropic | Anthropic 基准支持 document、search_result、server_tool_use 等；MiMo 页面列 text/image/tool_use/tool_result/thinking。 | `Such text, image, tool use, tool result, and thinking`；`Only the mimo-v2.5 model supports image input`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| Anthropic thinking 块要保留 signature/thinking | Anthropic | Anthropic 基准也要求 thinking block 原样回传；MiMo 明确工具调用 thinking 模式需要保留。 | `signature of the thinking block`；`thinking content block alongside tool_use content block`；`keep all previous thinking content block`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |

## 4. 工具格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat function tool 是 OpenAI Chat 嵌套格式 | Chat | 与 OpenAI Chat 基准 function shape 一致；MiMo 没有承诺 custom tool。 | `A function tool that can be used to generate a response`；`function.name`；`function.parameters`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Chat function name 限制 | Chat | OpenAI Chat 基准也有名称限制；MiMo 明确最大 64 且字符集。 | `Must be a-z, A-Z, 0-9, or contain underscores and dashes`；`maximum length of 64`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Chat 内置 web_search 是 MiMo 扩展 | Chat | OpenAI Chat 基准没有 `tools[].type:"web_search"` 的同名标准；MiMo Chat tools 中列 Web search tool，且需插件启用。 | `Web search tool`；`Web Search plugin must be activated before use`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Responses function tool 是扁平字段 | Responses | 与 OpenAI Responses function tool shape 类似：`type/name/parameters`；不同于 Chat 嵌套。 | `A JSON schema object describing the parameters of the function`；`Available options: function`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Responses 支持 `namespace` 工具 | Responses | OpenAI Responses 基准有 custom/MCP/server tools；MiMo 额外列 `namespace`，用于给 function tools 分组。 | `Groups function tools under a shared namespace`；`Available options: namespace`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Anthropic tool 使用 `input_schema` | Anthropic | 与 Anthropic Messages 基准一致；MiMo 没有列服务端工具。 | `input_schema: JSON schema for the tool input shape`；`tool_use output content blocks`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| thinking + tool 调用必须保留 reasoning/thinking | Chat / Responses / Anthropic | 三协议转换时不能丢掉 reasoning 状态；OpenAI Chat 基准没有该强制回传语义。 | Chat: `keep all previous reasoning_content in the messages array`；Responses: `keep all previous reasoning content in the input array`；Anthropic: `keep all previous thinking content block`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api；https://mimo.mi.com/docs/zh-CN/api/chat/responses；https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| 工具调用场景建议关闭 thinking | Chat | 不是协议字段差异，但直接影响转换策略；否则 reasoning_content 可能不稳定。 | `It is recommended to disable thinking when calling tool calls`（https://mimo.mi.com/static/docs/quick-start/faq/api-integration.md） |

## 5. 响应、usage 与错误格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat `finish_reason` 额外值 `repetition_truncation` | Chat | OpenAI Chat 基准没有该值。 | `repetition_truncation if the model detects repetition`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Chat 响应含 `reasoning_content` | Chat | OpenAI Chat 基准没有标准 `message.reasoning_content`；MiMo thinking 模式返回。 | `reasoning_content`；`The reasoning contents of the assistant message, before the final answer`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Chat usage 多模态与 web_search 字段 | Chat | OpenAI Chat 基准 usage 没有 MiMo 的 `web_search_usage`，也不列 `image_tokens/video_tokens` 为同样结构。 | `cached_tokens`；`image_tokens`；`video_tokens`；`web_search_usage`；`tool_usage`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Responses status/error | Responses | 与 OpenAI Responses 基准相近；MiMo 文档只列 response 内 `error.code/message`。 | `An error object returned when the model fails to generate a Response`；`The error code for the response`；`human-readable description`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Responses usage cache 字段 | Responses | 与 OpenAI Responses 基准同名。 | `input_tokens_details.cached_tokens`；`The number of cached input tokens`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Responses reasoning token 字段 | Responses | 与 OpenAI Responses 基准同名，但 MiMo 的 reasoning effort 不细分。 | `output_tokens_details.reasoning_tokens`；`The number of reasoning tokens`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Anthropic `stop_reason` 额外值 | Anthropic | Anthropic 基准没有 `content_filter` 和 `repetition_truncation` 这两个 MiMo 值。 | `content_filter`；`repetition_truncation`；`Available options: end_turn, max_tokens, tool_use, content_filter, repetition_truncation`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| Anthropic usage 只列 cache read | Anthropic | Anthropic 基准有 `cache_creation_input_tokens`；MiMo 页面本次只列 `cache_read_input_tokens`。 | `cache_read_input_tokens`；`The number of input tokens read from the cache`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| Prompt Cache 计费存在但无显式控制参数 | 全部 | OpenAI/Anthropic 基准有 prompt cache key / cache_control；MiMo API 页面未列这些显式缓存控制字段。 | `Cache Hit: When the requested prefix content hits the Prompt Cache`；`Cache Write: Limited-time Free`；`未在 Chat/Responses/Anthropic 请求参数中找到 prompt_cache_key 或 cache_control`（https://mimo.mi.com/static/docs/price/pay-as-you-go.md；https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| 统一 HTTP 错误码 | 全部 | 具体 error schema 未在三协议页完整展开；错误码页列常见错误与 429。 | `common error codes and solutions`；`429`（https://mimo.mi.com/static/docs/api/guidance/error-codes.md） |

## 6. 流式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 流式为 SSE，结束 `[DONE]` | Chat | 与 OpenAI Chat 基准相近；MiMo deep thinking 示例明确最终 `data: [DONE]`。 | `server-sent events`；`data: [DONE]`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api；https://mimo.mi.com/static/docs/quick-start/usage-guide/text-generation/deep-thinking.md） |
| Chat 流式 reasoning 先于 answer | Chat | OpenAI Chat 基准没有 `delta.reasoning_content`；MiMo thinking 流中先输出 reasoning，再输出 content。 | `thinking content and answer content are output sequentially`；`reasoning_content`；`content`（https://mimo.mi.com/static/docs/quick-start/usage-guide/text-generation/deep-thinking.md） |
| Chat chunk delta 含 `reasoning_content` 和 tool_calls | Chat | OpenAI Chat 基准 delta 没有标准 reasoning_content。 | `Chat response chunk object`；`reasoning_content`；`tool_calls`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api） |
| Responses 使用 OpenAI Responses 语义事件 | Responses | 与 OpenAI Responses 基准相近；MiMo 明确列 `response.created/in_progress/completed/incomplete` 等。 | `server will emit server-sent events`；`response.completed`；`response.incomplete`；`response.output_text.delta`；`response.reasoning_text.delta`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Responses usage 在最终 `response.completed` 中 | Responses | 与 OpenAI Responses 基准相近；MiMo completion event 的 response 与非流式 Response object 相同。 | `The response that was created...identical to those returned by...non-streaming mode`；`response.completed`（https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| Anthropic SSE 事件集不含 `ping` | Anthropic | Anthropic 基准有 ping；MiMo 页面列 message_start/content_block_start/content_block_delta/content_block_stop/message_delta/message_stop。 | `Each server-sent event includes a named event type`；`message_start`、`content_block_start`、`content_block_delta`、`content_block_stop`、`message_delta`、`message_stop`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| Anthropic delta 支持 `input_json_delta` | Anthropic | 与 Anthropic 工具参数流相近；MiMo 明确列 text/thinking/input_json 三种增量。 | `Available options: text_delta, thinking_delta, input_json_delta`（https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |

## 7. 已知限制

| 限制 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| 请求体必填字段 | Chat / Responses / Anthropic | 三协议必填入口不同：Chat 必填 `messages/model`，Responses 必填 `input/model`，Anthropic 必填 `messages/model`；`max_*` 字段在 MiMo 页面标为非必填但有取值范围。 | Chat `messages...required: true`、`model...required: true`、`max_completion_tokens...required: false`；Responses `input...required: true`、`model...required: true`、`max_output_tokens...required: false`；Anthropic `messages...required: true`、`model...required: true`、`max_tokens...required: false`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api；https://mimo.mi.com/docs/zh-CN/api/chat/responses；https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| 模型上下文样例为 1M | 全部 | 基准协议不规定 MiMo 模型窗口；OpenClaw 配置给出 1048576。 | `contextWindow: 1048576`；`maxTokens: 131072`（https://mimo.mi.com/static/docs/tokenplan/integration/openclaw.md） |
| 输出上限 131072 | Chat / Responses / Anthropic | MiMo 三协议都列 `[1,131072]`。 | `Required range: [1, 131072]`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api；https://mimo.mi.com/docs/zh-CN/api/chat/responses；https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
| 多模态只支持 `mimo-v2.5` | Chat / Responses / Anthropic | 转换层不能把图片/音频/视频任意发给 `mimo-v2.5-pro`。 | `Currently, only the mimo-v2.5 model supports image, audio, or video input`；`only the mimo-v2.5 model supports image input`（https://mimo.mi.com/docs/zh-CN/api/chat/openai-api；https://mimo.mi.com/docs/zh-CN/api/chat/responses） |
| thinking + tool 缺 reasoning 会 400 | Chat / Anthropic | OpenAI Chat 基准无此状态回传要求；Anthropic 基准保留 thinking，但 MiMo 明确缺失会 400。 | `must completely pass back the reasoning_content field, otherwise the API will return a 400 error`；`absence of reasoning_content...API will return a 400 error`（https://mimo.mi.com/static/docs/quick-start/usage-guide/text-generation/deep-thinking.md；https://mimo.mi.com/static/docs/tokenplan/integration/openclaw.md） |
| Web Search 插件开关有 5 分钟缓存期 | Chat | 不属于 OpenAI Chat 基准；启停不会立刻生效。 | `There is a 5-minute cache period after enabling / disabling online search`（https://mimo.mi.com/static/docs/quick-start/usage-guide/text-generation/tool-calling/web-search.md） |
| Web Search 单轮可能并发多个关键词 | Chat | 计费和限流需按工具调用次数处理。 | `A single search round...may initiate multiple keywords concurrently`；`max_keyword`（https://mimo.mi.com/static/docs/quick-start/usage-guide/text-generation/tool-calling/web-search.md） |
| 账户按模型有并发/RPM/TPM 限制 | 全部 | 基准协议不规定 MiMo 额度；MiMo 明确高并发可能 429。 | `model concurrency limit for each account`；`429 error may occur`；`RPM`；`TPM`（https://mimo.mi.com/static/docs/api/guidance/rate-limit.md） |
| 缓存无公开 TTL/断点参数 | 全部 | MiniMax/Anthropic 基准有显式断点 TTL；MiMo 只证明 prefix cache 命中计费和 usage 字段。 | `requested prefix content hits the Prompt Cache`；`Cache Write: Limited-time Free`；未在官方 API 请求参数中找到 `cache_control` 或 `prompt_cache_key`（https://mimo.mi.com/static/docs/price/pay-as-you-go.md；https://mimo.mi.com/docs/zh-CN/api/chat/openai-api；https://mimo.mi.com/docs/zh-CN/api/chat/responses；https://mimo.mi.com/docs/zh-CN/api/chat/anthropic-api） |
