# 智谱 GLM API quirks

> 核对日期：2026-08-13。
> 对照基准：`messages-api.md`、`responses-api.md`、`chat-completions-api.md`。本文只记录官方文档或官方 OpenAPI 可直接支撑的差异；未在官方文档找到的能力不推断。

## 官方来源与访问状态

| 来源 | 状态 | 用途 | 官方摘录 |
| --- | --- | --- | --- |
| https://docs.bigmodel.cn/llms.txt | 可访问 | 官方文档索引 | `对话补全`；`Claude API 兼容`；`OpenAI API 兼容`；`上下文缓存` |
| https://docs.bigmodel.cn/openapi/openapi.json | 可访问 | OpenAPI 路径、请求/响应 schema、认证、错误 schema | `servers: https://open.bigmodel.cn/api/`；`/paas/v4/chat/completions` |
| https://docs.bigmodel.cn/api-reference/模型-api/对话补全.md | 可访问 | Chat Completions 端点与内嵌 OpenAPI | `post /paas/v4/chat/completions`；`流式输出结束时会返回 data: [DONE] 消息` |
| https://docs.bigmodel.cn/cn/guide/develop/openai/introduction.md | 可访问 | OpenAI SDK 兼容 base_url 示例 | `base_url="https://open.bigmodel.cn/api/paas/v4/"`；`client.chat.completions.create` |
| https://docs.bigmodel.cn/cn/guide/develop/claude/introduction.md | 可访问 | Anthropic / Claude 兼容入口 | `base_url 为 https://open.bigmodel.cn/api/anthropic`；`curl https://open.bigmodel.cn/api/anthropic/v1/messages` |
| https://docs.bigmodel.cn/cn/guide/start/model-overview.md | 可访问 | 上下文窗口与输出上限 | `GLM-5.2...上下文 1M...最大输出 128K` |
| https://docs.bigmodel.cn/cn/guide/capabilities/cache.md | 可访问 | 上下文缓存字段 | `隐式缓存`；`usage.prompt_tokens_details.cached_tokens` |
| https://docs.bigmodel.cn/cn/guide/capabilities/thinking-mode.md | 可访问 | 思考模式、Preserved thinking | `GLM-5.2 GLM-5.1 GLM-5 GLM-4.7 系列默认开启 Thinking` |
| https://docs.bigmodel.cn/cn/guide/capabilities/function-calling.md | 可访问 | 工具调用说明 | `工具调用`（用于交叉核对；具体 schema 以 OpenAPI 为准） |
| https://docs.bigmodel.cn/cn/guide/tools/web-search.md | 可访问 | Web Search in Chat | `Web Search in Chat`；`tools = [{... "type": "web_search" ...}]` |
| https://docs.bigmodel.cn/cn/api/api-code.md | 可访问 | 错误码与错误响应示例 | `{"error":{"code":"1001","message":"Header 中未收到 Authentication 参数，无法进行身份验证"}}` |
| https://docs.bigmodel.cn/cn/api/rate-limit.md | 可访问 | 速率/并发限制 | `速率限制主要体现在：并发请求数限制` |

## 1. 端点 URL 与认证

| 协议 | 是否存在 | base_url / path | 认证 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- | --- | --- |
| OpenAI Chat Completions | 存在 | `https://open.bigmodel.cn/api/paas/v4/chat/completions`；OpenAI SDK base_url 可设为 `https://open.bigmodel.cn/api/paas/v4/` | `Authorization: Bearer <API Key>` | 路径不是 OpenAI 基准的 `/v1/chat/completions`；使用 `/api/paas/v4`。 | `servers: https://open.bigmodel.cn/api/`；`/paas/v4/chat/completions`（https://docs.bigmodel.cn/openapi/openapi.json）；`base_url="https://open.bigmodel.cn/api/paas/v4/"`；`client.chat.completions.create`（https://docs.bigmodel.cn/cn/guide/develop/openai/introduction.md） |
| OpenAI Responses | 未在官方 OpenAPI/索引中找到 | 未在官方 OpenAPI paths 找到 `/responses`；只看到 `/paas/v4/chat/completions`、`/paas/v4/async/chat/completions` 等 | 未在官方文档找到 | OpenAI Responses 基准的 `/v1/responses`、`input`、`max_output_tokens`、语义 SSE 等不应假定可透传。 | OpenAPI paths 摘录：`/paas/v4/chat/completions`；`/paas/v4/async/chat/completions`；未出现 `/responses`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Anthropic Messages | 存在兼容入口 | `https://open.bigmodel.cn/api/anthropic/v1/messages`；Anthropic SDK base_url 为 `https://open.bigmodel.cn/api/anthropic` | `x-api-key: YOUR_API_KEY`；示例还建议设置 `ANTHROPIC_API_KEY` 环境变量 | 与 Anthropic 基准同为 `/v1/messages` 形态；官方页未列完整 Messages schema，只给迁移示例并提示仍有差异。 | `替换您访问的 base_url 为 https://open.bigmodel.cn/api/anthropic`；`curl https://open.bigmodel.cn/api/anthropic/v1/messages`；`--header "x-api-key: YOUR_API_KEY"`；`某些场景下智谱与 Claude 接口仍存在差异`（https://docs.bigmodel.cn/cn/guide/develop/claude/introduction.md） |

## 2. 请求参数偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 必填字段 | Chat | 与 OpenAI Chat 基准相近，`model` 与 `messages` 必填；不同于 Responses 基准的 `input`。 | `required: ["model","messages"]`；`普通对话模型请求，支持纯文本对话和工具调用`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 输出上限字段为 `max_tokens` | Chat / Anthropic 示例 | OpenAI Chat 基准推荐 `max_completion_tokens`，`max_tokens` 已废弃；GLM Chat OpenAPI 使用 `max_tokens`。Anthropic 兼容示例也使用 `max_tokens`。 | `max_tokens`；`模型输出的最大令牌 token 数量限制`；`maximum: 131072`（https://docs.bigmodel.cn/openapi/openapi.json）；`max_tokens=1024`（https://docs.bigmodel.cn/cn/guide/develop/claude/introduction.md） |
| 未找到 `max_completion_tokens` / `max_output_tokens` | Chat / Responses | GLM Chat schema 未使用 OpenAI Chat 的新字段 `max_completion_tokens`；未找到 Responses 的 `max_output_tokens`。 | Chat schema 摘录仅列 `max_tokens`；未在 OpenAPI 中找到 `max_completion_tokens` 或 `/responses`（https://docs.bigmodel.cn/openapi/openapi.json） |
| `thinking` 形状 | Chat | OpenAI Chat 基准用 `reasoning_effort`；Anthropic Messages 基准的 thinking 需要 `budget_tokens`。GLM 用 `thinking: {type, clear_thinking}`，无 `budget_tokens`。 | `仅 GLM-4.5 及以上模型支持此参数配置. 控制大模型是否开启思维链`；`type...enabled, disabled`；`clear_thinking...默认 True`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 默认 thinking 行为 | Chat | 与基准协议的模型无关默认值不同；GLM 按模型族说明默认开启或自动开启。 | `GLM-5.2 GLM-5.1 GLM-5 GLM-4.7 系列默认开启 Thinking`；`GLM-4.6 的默认“混合 thinking（自动开启）”`（https://docs.bigmodel.cn/cn/guide/capabilities/thinking-mode.md） |
| `reasoning_effort` 映射 | Chat | OpenAI Chat 基准枚举可包含 `none/minimal/low/medium/high/xhigh/max`；GLM 仅 `GLM-5.2` 支持，并会映射部分值。 | `仅 GLM-5.2 支持`；`传入 none 或 minimal 模型会放弃思考；传入 low medium 将映射为 high；传入 xhigh 将映射为 max`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Preserved thinking 控制 | Chat | Anthropic 基准要求保留 thinking blocks；GLM 用 `thinking.clear_thinking:false` 控制历史 `reasoning_content` 是否保留。 | `false：保留历史 turns 的 reasoning_content 并随上下文一同提供给模型`；`必须在 messages 中完整、未修改、按原顺序透传历史 reasoning_content`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 采样开关 `do_sample` | Chat | OpenAI Chat 基准没有同名字段；GLM 关闭采样时忽略 `temperature/top_p`。 | `do_sample`；`当设置为 false 时...temperature 和 top_p 参数将被忽略`（https://docs.bigmodel.cn/openapi/openapi.json） |
| `temperature` 范围较窄 | Chat | OpenAI Chat 基准范围为 0 到 2；GLM Chat 范围为 0 到 1。 | `temperature...取值范围为 [0.0, 1.0]`；`minimum: 0`；`maximum: 1`（https://docs.bigmodel.cn/openapi/openapi.json） |
| `top_p` 范围 | Chat | OpenAI Chat 基准通常为 0 到 1；GLM 要求 0.01 到 1。 | `top_p...取值范围为 [0.01, 1.0]`；`minimum: 0.01`；`maximum: 1`（https://docs.bigmodel.cn/openapi/openapi.json） |
| `stop` 数量说明矛盾 | Chat | OpenAI Chat 基准最多 4 个；GLM schema 有 `maxItems:4`，但描述写“目前仅支持单个停止词”。转换层应按官方矛盾保守处理。 | `目前仅支持单个停止词，格式为["stop_word1"]`；同 schema `maxItems: 4`（https://docs.bigmodel.cn/openapi/openapi.json） |
| `tool_choice` 仅 `auto` | Chat | OpenAI Chat 基准支持 `none/auto/required`、指定函数、allowed_tools；GLM Chat schema 只列 `auto`。 | `tool_choice...enum: ["auto"]`；`默认 auto 且仅支持 auto`（https://docs.bigmodel.cn/openapi/openapi.json） |
| `tool_stream` 扩展 | Chat streaming | OpenAI Chat 基准没有同名字段；GLM 用于 Function Calls 流式响应，且模型受限。 | `是否开启流式响应 Function Calls`；`仅限 GLM-5.2 GLM-5.1 GLM-5 GLM-5-Turbo GLM-4.7 GLM-4.6 系列支持`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 缓存无显式请求参数 | Chat | OpenAI Responses/Chat 基准有 `prompt_cache_key/options`；Anthropic 有块级 `cache_control`。GLM 官方写隐式缓存，无需手动配置。 | `自动缓存识别：隐式缓存...无需手动配置`；`系统会复用之前的计算结果`（https://docs.bigmodel.cn/cn/guide/capabilities/cache.md） |

## 3. 消息格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat role 不含 `developer` | Chat | OpenAI Chat 基准支持 `developer`；GLM Chat 文本模型只列 `system/user/assistant/tool`。 | `支持四种角色：system...user...assistant...tool`；`注意不能只包含系统消息或助手消息`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Tool message 必填项 | Chat | 与 OpenAI Chat 基准类似需要 `tool_call_id`，但 GLM schema 的 required 只列 `role/content`；仍有字段说明对应 ID。 | `工具消息...content`；`tool_call_id...指示此消息对应的工具调用 ID`；`required: ["role","content"]`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Assistant message 可携带 `reasoning_content` | Chat | OpenAI Chat 基准没有标准历史 `reasoning_content` 输入；GLM Preserved thinking 要求原样回传。 | `请记得返回历史的 reasoning_content，以保持推理连贯性`（https://docs.bigmodel.cn/cn/guide/capabilities/thinking-mode.md）；`reasoning_content...完整、未修改、按原顺序透传`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 文本模型 content 主要是字符串 | Chat 文本模型 | OpenAI Chat 基准 user content 可含 `text/image_url/input_audio/file`；GLM 文本模型 schema 写普通对话模型主要支持纯文本。 | `普通对话模型主要支持纯文本内容`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 多模态 content 类型扩展 | Chat 视觉/音频模型 | OpenAI Chat 基准没有 `video_url` 同名标准块；GLM Chat 示例和 schema 支持图片、视频、文件、音频。 | `支持多模态（文本、图片、音频、视频、文件）`（https://docs.bigmodel.cn/api-reference/模型-api/对话补全.md）；示例含 `image_url`、`video_url`、`file_url`、`input_audio`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 视觉模型图片限制 | Chat 视觉模型 | OpenAI Chat 基准不含 GLM 具体大小/张数限制。 | `图像大小上传限制为每张图像 5M 以下，且像素不超过 6000*6000`；`GLM-5V-Turbo GLM4.6V GLM4.5V 系列限制 50 张`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 视觉模型视频限制 | Chat 视觉模型 | OpenAI Chat 基准不含 GLM 视频大小/格式限制。 | `视频大小限制为 200M 以内`；`视频类型：mp4，mkv，mov`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Anthropic Messages schema 未完整列出 | Anthropic | 官方 Claude 兼容页仅给 SDK/curl 示例；未在该页找到 role/content block 全量支持矩阵。 | 示例仅含 `messages: [{"role": "user", "content": "Hello, ZHIPU"}]`；`某些场景下智谱与 Claude 接口仍存在差异`（https://docs.bigmodel.cn/cn/guide/develop/claude/introduction.md） |

## 4. 工具格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat `tools` 支持多类 | Chat | OpenAI Chat 基准只标准化 function/custom；GLM Chat 支持 function、retrieval、web_search、mcp。 | `支持函数调用、知识库检索和网络搜索`；schema `FunctionToolSchema`、`RetrievalToolSchema`、`WebSearchToolSchema`、`MCPToolSchema`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Function tool shape | Chat | 与 OpenAI Chat function tool 嵌套 shape 相近：`{type:"function", function:{...}}`，不是 Responses 的扁平 `{name, parameters}`。 | `Function Call`；`type: function`；`function`；`name`、`description`、`parameters` 必填（https://docs.bigmodel.cn/openapi/openapi.json） |
| Function 名称限制 | Chat | OpenAI Chat 基准最大 64 且可下划线/短横线；GLM 要求正则不同，不允许点号/美元符号。 | `必须是 a-z、A-Z、0-9，或包含下划线和破折号，最大长度为 64`；`pattern: ^[a-zA-Z0-9_-]+$`（https://docs.bigmodel.cn/openapi/openapi.json） |
| `tools` 数量限制 | Chat | GLM 明确最多 128 个函数。 | `最多支持 128 个函数`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Web Search in Chat 是 `tools[]` | Chat | OpenAI Chat 基准的 web search 不是这个 shape；GLM 使用 `{"type":"web_search","web_search":{...}}`。 | `tools = [{... "type": "web_search", "web_search": {...}}]`（https://docs.bigmodel.cn/cn/guide/tools/web-search.md）；`WebSearchToolSchema` required `type, web_search`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Web Search 参数 | Chat | GLM Web Search 参数与 OpenAI built-in web_search 不同。 | `search_engine...search_std、search_pro、search_pro_sogou、search_pro_quark`；`count...1-50`；`search_recency_filter...oneDay...noLimit`；`search_result...是否返回搜索来源的详细信息`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Retrieval tool | Chat | OpenAI Chat 基准没有 GLM `retrieval` tool shape。 | `Retrieval`；`knowledge_id`；`prompt_template`；`required: ["knowledge_id"]`（https://docs.bigmodel.cn/openapi/openapi.json） |
| MCP tool | Chat | OpenAI Responses 基准有 MCP tool；GLM Chat 的 MCP shape 为 `mcp` 嵌套对象。 | `MCP`；`server_label`；`server_url`；`transport_type...sse, streamable-http`；`allowed_tools`；`headers`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Custom/namespace 工具 | Chat / Responses | OpenAI Chat/Responses 基准有 `custom` tool 或 namespace 字段；GLM Chat OpenAPI 未列 `custom` tool 类型或 `namespace` 字段。 | `tools` anyOf 仅列 `FunctionToolSchema`、`RetrievalToolSchema`、`WebSearchToolSchema`、`MCPToolSchema`；未找到 `custom` tool 类型（https://docs.bigmodel.cn/openapi/openapi.json） |
| Anthropic `input_schema` 工具 | Anthropic | 官方 Claude 兼容页未给 tool schema 示例；不推断是否支持 Anthropic `input_schema`、server tools。 | `某些场景下智谱与 Claude 接口仍存在差异`；未在该页找到 `input_schema` 或 server tool 说明（https://docs.bigmodel.cn/cn/guide/develop/claude/introduction.md） |

## 5. 响应格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 响应含 `request_id` | Chat | OpenAI Chat 基准不含标准顶层 `request_id`；GLM response schema 有。 | `id`；`request_id`；`created`；`model`；`choices`（https://docs.bigmodel.cn/openapi/openapi.json） |
| `finish_reason` 扩展 | Chat | OpenAI Chat 基准无 `sensitive`、`network_error`、`model_context_window_exceeded`。 | `stop`；`tool_calls`；`length`；`sensitive`；`network_error`；`model_context_window_exceeded`（https://docs.bigmodel.cn/openapi/openapi.json） |
| `usage.prompt_tokens_details.cached_tokens` | Chat | 与 OpenAI Chat 缓存字段名相近；不同于 Kimi 的顶层 `cached_tokens` 和 Anthropic 的 `cache_read_input_tokens`。 | `prompt_tokens_details.cached_tokens`；`命中的缓存 Token 数量`（https://docs.bigmodel.cn/openapi/openapi.json）；`响应包含上下文缓存的 Token 使用信息`（https://docs.bigmodel.cn/cn/guide/capabilities/cache.md） |
| 响应 `reasoning_content` | Chat | OpenAI Chat 基准没有标准 `message.reasoning_content`；GLM 在思考模型返回。 | `reasoning_content`；`思维链内容，仅在使用 glm-4.5 系列, glm-4.1v-thinking 系列模型时返回`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Web Search 结果顶层字段 | Chat | OpenAI Chat 基准 web search citation 在 message annotations；GLM 返回顶层 `web_search` 数组。 | `web_search`；`返回与网页搜索相关的信息，使用 WebSearchToolSchema 时返回`；字段含 `icon/title/link/media/publish_date/content/refer`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 内容安全结果 | Chat | OpenAI Chat 基准没有 GLM 顶层 `content_filter` shape。 | `content_filter`；`安全生效环节，包括 role = assistant...user...history`；`level 0-3`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 错误对象 shape | 全部 | OpenAI 基准错误对象含 `message/type/param/code`；GLM schema 只列 `error.code/message`。 | `{"error":{"code":"1001","message":"Header 中未收到 Authentication 参数，无法进行身份验证"}}`（https://docs.bigmodel.cn/cn/api/api-code.md）；schema `error.code`、`error.message`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Anthropic 响应格式 | Anthropic | 官方 Claude 兼容页未列完整 Message response、usage、error schema；不推断完全等同 Anthropic。 | 示例 `print(message.content)`；`某些场景下智谱与 Claude 接口仍存在差异`（https://docs.bigmodel.cn/cn/guide/develop/claude/introduction.md） |

## 6. 流式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 流式结束为 `[DONE]` | Chat | 与 OpenAI Chat 基准一致；不同于 OpenAI Responses 语义事件结束。 | `流式输出结束时会返回 data: [DONE] 消息`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Chat chunk 对象 | Chat | 与 OpenAI Chat chunk 形态接近，`object` 字段未在 GLM schema 中作为必有字段列出；schema 核心为 `choices[].delta`。 | `ChatCompletionChunk`；`choices`；`delta`；`finish_reason`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 流式工具调用需 `tool_stream` | Chat | OpenAI Chat 基准通常直接在 stream 中增量给 `tool_calls`；GLM 另有 `tool_stream` 开关。 | `tool_stream`；`是否开启流式响应 Function Calls`；`默认值 false`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 流式 `reasoning_content` | Chat | OpenAI Chat 基准没有标准 delta reasoning_content；GLM chunk delta 可有。 | `delta.reasoning_content`；`思维链内容, 仅 glm-4.5 系列支持`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 流式 usage | Chat | OpenAI Chat 基准通过 `stream_options.include_usage` 控制最终 usage chunk；GLM Chat OpenAPI 未列 `stream_options`，但 chunk schema 有 `usage`。 | `ChatCompletionChunk.usage`；`本次模型调用的 tokens 数量统计`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 流式错误 | Chat | OpenAI Chat 基准错误仍为标准错误对象；GLM 官方说明推理中异常不返回上述错误码，而看 `finish_reason`。 | `使用流式（SSE）调用时，如果 API 在推理过程中异常终止，不会返回上述错误码，而是在响应体的 finish_reason 参数中返回异常原因`（https://docs.bigmodel.cn/cn/api/api-code.md） |
| Responses SSE | Responses | 未找到 `/responses`，不写 Responses 语义事件支持。 | OpenAPI paths 未出现 `/responses`（https://docs.bigmodel.cn/openapi/openapi.json） |
| Anthropic SSE | Anthropic | Claude 兼容页示例 `stream:true`，但未列 Anthropic `message_start/content_block_delta/message_stop` 事件集。 | curl 示例含 `"stream": true`；未找到完整 SSE 事件表（https://docs.bigmodel.cn/cn/guide/develop/claude/introduction.md） |

## 7. 已知限制

| 限制 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| 上下文窗口和输出上限按模型变化 | Chat / Anthropic 兼容模型 | 基准协议不规定供应商模型窗口；GLM 模型窗口差异大。 | `GLM-5.2...上下文 1M...最大输出 128K`；`GLM-5.1...200K...128K`；`GLM-4.5-Air...128K...96K`（https://docs.bigmodel.cn/cn/guide/start/model-overview.md） |
| `max_tokens` 最大值 | Chat | GLM Chat schema 最大 `131072`，并建议不小于 `1024`；OpenAI Chat 基准字段名和模型限制不同。 | `maximum: 131072`；`建议设置不小于 1024`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 工具数量 | Chat | GLM 明确最多 128 个函数。 | `最多支持 128 个函数`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 停止词 | Chat | GLM 文档描述“仅支持单个停止词”，schema 又写 `maxItems:4`；实现应保守。 | `目前仅支持单个停止词`；`maxItems: 4`（https://docs.bigmodel.cn/openapi/openapi.json） |
| 速率限制按账户/模型并发 | 全部 | OpenAI 基准常以 RPM/TPM；GLM 文档强调并发请求数，具体数值需到控制台查看。 | `速率限制主要体现在：并发请求数限制`；`不同模型设有独立的并发限制`；`可通过速率限制查看您的账户目前各模型可调用的速率`（https://docs.bigmodel.cn/cn/api/rate-limit.md） |
| 1302/1305 限流错误 | 全部 | 错误码与 OpenAI/Anthropic 不同。 | `1302...您的账户已达到速率限制，请您控制请求频率`；`1305...该模型当前访问量过大，请您稍后再试`（https://docs.bigmodel.cn/cn/api/rate-limit.md） |
| 上下文缓存 TTL 未公开 | Chat | OpenAI/Anthropic 基准有 TTL 或 cache 字段；GLM 只说明隐式缓存和命中字段，未给具体 TTL。 | `隐式缓存...无需手动配置`；`缓存有合理的时效性`；未找到具体 TTL 数值（https://docs.bigmodel.cn/cn/guide/capabilities/cache.md） |
| Anthropic 兼容能力细项未公开 | Anthropic | 官方只给迁移入口和示例；未找到完整参数支持矩阵，因此不能假定 `cache_control`、server tools、thinking budget 等完全支持。 | `某些场景下智谱与 Claude 接口仍存在差异`；示例仅列 `model/max_tokens/messages/stream`（https://docs.bigmodel.cn/cn/guide/develop/claude/introduction.md） |
