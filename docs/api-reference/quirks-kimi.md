# 月之暗面 Kimi API quirks

> 核对日期：2026-08-13。
> 对照基准：`messages-api.md`、`responses-api.md`、`chat-completions-api.md`。本文只记录官方文档或官方 OpenAPI 可直接支撑的差异；未在官方文档找到的能力不推断。

## 官方来源与访问状态

| 来源 | 状态 | 用途 | 官方摘录 |
| --- | --- | --- | --- |
| https://platform.kimi.com/docs/llms.txt | 可访问 | 官方文档索引 | `创建对话补全`；`在 Claude Code 中使用 Kimi`；`常见错误码说明` |
| https://platform.kimi.com/docs/api/chat.md | 可访问 | Chat Completions API、内嵌 OpenAPI、流式、工具、thinking、Partial Mode | `POST /v1/chat/completions`；`data: [DONE]` |
| https://platform.kimi.com/docs/openapi.json | 可访问 | OpenAPI 路径、schema、认证、错误对象 | `servers: https://api.moonshot.cn`；`/v1/chat/completions` |
| https://platform.kimi.com/docs/guide/claude-code-kimi.md | 可访问 | Anthropic 兼容端点（Claude Code 接入） | `ANTHROPIC_BASE_URL="https://api.moonshot.cn/anthropic"`；`ANTHROPIC_AUTH_TOKEN` |
| https://platform.kimi.com/docs/models.md | 可访问 | 模型列表与上下文窗口 | `kimi-k3...100 万 token 上下文窗口`；`kimi-k2.6...上下文 256k` |
| https://platform.kimi.com/docs/api/models-overview.md | 可访问 | 模型参数差异、thinking、tool_choice、采样固定值 | `不同模型系列对 Chat Completions API 参数有不同的默认值和约束` |
| https://platform.kimi.com/docs/guide/use-context-caching-feature-of-kimi-api.md | 可访问 | Context Caching | `对所有模型请求自动启用`；`无需手动创建` |
| https://platform.kimi.com/docs/guide/use-web-search.md | 可访问 | `$web_search` 内置联网搜索 | `$web_search（builtin_function 类型）` |
| https://platform.kimi.com/docs/guide/use-official-tools.md | 可访问 | Formula 官方工具 | `GET /v1/formulas/{uri}/tools`；`POST /v1/formulas/{uri}/fibers` |
| https://platform.kimi.com/docs/guide/use-tool-choice.md | 可访问 | tool_choice 行为与限制 | `auto`；`none`；`required`；`tool_choice 'specified' is incompatible with thinking enabled` |
| https://platform.kimi.com/docs/guide/use-dynamic-tool-loading.md | 可访问 | K3 动态工具加载 | `在 messages 中插入一条 role 为 system 的消息，并通过该消息的 tools 字段声明` |
| https://platform.kimi.com/docs/api/errors.md | 可访问 | 错误格式和错误类型 | `API 会返回包含错误信息的 JSON 响应` |
| https://platform.kimi.com/docs/pricing/limits.md | 可访问 | 充值 tier、并发、RPM/TPM/TPD | `基于账户的累计充值金额进行速率限制` |

## 1. 端点 URL 与认证

| 协议 | 是否存在 | base_url / path | 认证 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- | --- | --- |
| OpenAI Chat Completions | 存在 | `https://api.moonshot.cn/v1/chat/completions`；OpenAI SDK base_url 为 `https://api.moonshot.cn/v1` | `Authorization: Bearer $MOONSHOT_API_KEY` | 路径与 OpenAI Chat 基准同为 `/v1/chat/completions`，但模型参数、工具和 usage 有 Kimi 差异。 | `curl https://api.moonshot.cn/v1/chat/completions`；`Authorization: Bearer $MOONSHOT_API_KEY`（https://platform.kimi.com/docs/api/chat.md）；OpenAPI server `https://api.moonshot.cn`（https://platform.kimi.com/docs/openapi.json） |
| OpenAI Responses | 未在官方 OpenAPI/索引中找到 | OpenAPI paths 只列 `/v1/chat/completions`、files、batches、models、tokenizers、balance 等；未列 `/v1/responses` | 未在官方文档找到 | OpenAI Responses 基准的 `input`、`max_output_tokens`、Responses 语义 SSE 不应假定可用。 | OpenAPI paths 摘录：`/v1/chat/completions`；未出现 `/v1/responses`（https://platform.kimi.com/docs/openapi.json） |
| Anthropic Messages / Claude Code 兼容 | 存在兼容端点说明 | `https://api.moonshot.cn/anthropic`；Claude Code 通过环境变量转发 | `ANTHROPIC_AUTH_TOKEN` 使用 Kimi API Key；未在该页给出 `x-api-key`/`Authorization` HTTP 细节 | 与 Anthropic 基准的 `https://api.anthropic.com/v1/messages` 不同；官方页面面向 Claude Code，不是完整 Messages API reference。 | `export ANTHROPIC_BASE_URL="https://api.moonshot.cn/anthropic"`；`export ANTHROPIC_AUTH_TOKEN="${YOUR_MOONSHOT_API_KEY}"`；`将模型请求转发到 Kimi 的 Anthropic 兼容端点`（https://platform.kimi.com/docs/guide/claude-code-kimi.md） |
| Formula 官方工具 | 存在 | `https://api.moonshot.cn/v1/formulas/{uri}/tools`；`/v1/formulas/{uri}/fibers` | `Authorization: Bearer $MOONSHOT_API_KEY` | 这不是 OpenAI/Anthropic 基准端点；用于取得官方工具声明和执行工具。 | `GET /v1/formulas/{uri}/tools`；`POST /v1/formulas/{uri}/fibers`；`Authorization: Bearer $MOONSHOT_API_KEY`（https://platform.kimi.com/docs/guide/use-official-tools.md） |

## 2. 请求参数偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat 必填字段 | Chat | 与 OpenAI Chat 基准一样至少需要消息；Kimi OpenAPI 用 `allOf` 组合请求 schema：公共 base 要求 `messages`，各模型分支要求 `model`；K3 分支还直接要求 `model/messages`。 | `required: ["messages"]`（ChatRequestBase）；`required: ["model","messages"]`（KimiK3ChatRequest）；K2.x 与 moonshot-v1 分支 `required: ["model"]`（https://platform.kimi.com/docs/openapi.json） |
| `max_tokens` 已废弃 | Chat | OpenAI Chat 基准也废弃 `max_tokens`；Kimi 明确要求用 `max_completion_tokens`。 | `max_tokens...已弃用，请使用 max_completion_tokens`（https://platform.kimi.com/docs/openapi.json；https://platform.kimi.com/docs/api/chat.md） |
| `max_completion_tokens` 默认/最大 | Chat | OpenAI Chat 基准字段同名，但 Kimi K3 默认和最大值非常大；且输入加输出超上下文会报错。 | `Kimi K3 默认为 131072，最大可设置为 1048576`；`如果输入加 max_completion_tokens 超出模型上下文窗口，将返回 invalid_request_error`（https://platform.kimi.com/docs/openapi.json） |
| 未找到 `max_output_tokens` | Responses | Kimi 没有官方 `/responses`，未找到 Responses 字段。 | OpenAPI paths 未出现 `/v1/responses`；Chat schema 使用 `max_completion_tokens`（https://platform.kimi.com/docs/openapi.json） |
| K3 reasoning 控制 | Chat | OpenAI Chat 基准 `reasoning_effort` 枚举更宽；Kimi K3 只支持 `low/high/max`，默认 max，且始终启用思考。 | `Kimi K3 始终启用思考`；`推理强度支持 low、high 和 max，默认为 max`（https://platform.kimi.com/docs/openapi.json）；`K3 支持顶层 reasoning_effort，可选值为 "low" / "high" / "max"`（https://platform.kimi.com/docs/api/models-overview.md） |
| K2.x thinking 控制 | Chat | Anthropic thinking 使用 `budget_tokens`；Kimi K2.x 用 `thinking.type` 和 `thinking.keep`，没有 budget。 | `kimi-k2.6...thinking...默认值为 {"type": "enabled"}`；`type...enabled, disabled`；`keep...all 或 null`（https://platform.kimi.com/docs/openapi.json） |
| K2.7 Code thinking 固定 | Chat / Anthropic 兼容模型 | OpenAI Chat 基准不含此限制；Kimi K2.7 Code 不允许禁用 thinking。 | `kimi-k2.7-code...始终开启思考、不可禁用`；`仅支持 {"type":"enabled","keep":"all"}`（https://platform.kimi.com/docs/api/models-overview.md） |
| Anthropic 兼容 thinking 限制 | Anthropic / Claude Code | 官方 Claude Code 页说明 K2.7 Code 需要显式 Thinking on；但未给 Messages API 字段级 schema。 | `未开启时请求会被拒绝（invalid thinking: only type=enabled is allowed for this model）`；`请在 Claude Code 中按 Tab 开启 Thinking on`（https://platform.kimi.com/docs/guide/claude-code-kimi.md） |
| `temperature/top_p/n/penalty` 多数 Kimi 模型固定 | Chat | OpenAI Chat 基准允许传采样参数；Kimi K3/K2.7/K2.6 多数不可改，传入会报错。 | `temperature...不可修改`；`top_p...0.95 不可改`；`n...1 不可改`；`presence_penalty / frequency_penalty...固定 0`；`表中"固定"表示该参数不可修改：传入其他值会报错`（https://platform.kimi.com/docs/api/models-overview.md） |
| Moonshot V1 参数不同 | Chat | Kimi 的 legacy Moonshot V1 与 Kimi K 系列参数限制不同；例如 `n` 最大 5。 | `moonshot-v1 系列...temperature 0.0`；`n...1（最大 5）`；`上下文长度 8k/32k/128k`（https://platform.kimi.com/docs/api/models-overview.md；https://platform.kimi.com/docs/models.md） |
| `stop` 上限 | Chat | OpenAI Chat 基准最多 4 个；Kimi 最多 5 个，且每个不超过 32 字节。 | `stop...最多允许 5 个字符串，每个不超过 32 字节`（https://platform.kimi.com/docs/openapi.json） |
| `stream_options.include_usage` | Chat streaming | 与 OpenAI Chat 基准相近；最终 usage chunk 在 `[DONE]` 前。 | `将在 data: [DONE] 消息之前额外发送一个 chunk`；`usage 字段显示整个请求的 Token 使用统计，choices 字段为空数组`（https://platform.kimi.com/docs/openapi.json） |
| `prompt_cache_key` | Chat | OpenAI Chat/Responses 基准有同名字段；Kimi 支持但语义是相似请求缓存命中率优化，且 Kimi Code Plan 必填。 | `用于缓存相似请求的响应以优化缓存命中率`；`对于 Kimi Code Plan，此字段为必填以提高缓存命中率`（https://platform.kimi.com/docs/openapi.json） |
| 未找到 `prompt_cache_options` / `cache_control` | Chat / Anthropic | OpenAI Responses 基准有 `prompt_cache_options`；Anthropic 基准有块级 `cache_control`。Kimi 官方文档描述自动 Context Caching，未列这两个字段。 | `无需手动创建`；`无需引用缓存 ID`；`无需管理 TTL`（https://platform.kimi.com/docs/guide/use-context-caching-feature-of-kimi-api.md）；OpenAPI 未列 `prompt_cache_options` 或 `cache_control`（https://platform.kimi.com/docs/openapi.json） |

## 3. 消息格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat role 不含 `developer` | Chat | OpenAI Chat 基准支持 `developer`；Kimi Chat role 只支持 `system/user/assistant/tool`。 | `role 支持 system、user、assistant、tool 其一`；`消息发送者的角色，支持 system、user、assistant、tool`（https://platform.kimi.com/docs/openapi.json；https://platform.kimi.com/docs/api/chat.md） |
| content 多模态类型 | Chat | OpenAI Chat 基准支持 `text/image_url/input_audio/file`；Kimi Chat content 数组支持 `text/image_url/video_url`，未列 `input_audio/file` 作为 Chat content part。 | `内容类型 "text" | "image_url" | "video_url"`；`base64 编码...文件引用 ms://<file_id>`（https://platform.kimi.com/docs/api/chat.md） |
| `image_url`/`video_url` 字符串简写 | Chat | OpenAI Chat 基准 `image_url` 为对象；Kimi 允许对象或字符串。 | `image_url 和 video_url 也支持直接传入字符串，效果等同于对象形式中的 url 字段`（https://platform.kimi.com/docs/api/chat.md） |
| Dynamic tool message | Chat K3 | OpenAI Chat 基准没有“system 消息带 tools 且无 content”的动态工具声明；Kimi K3 支持。 | `除标准消息外，还可在任意对话位置插入 {"role": "system", "tools": [...]} 消息动态加载工具`；`该动态工具消息不包含 content 字段`（https://platform.kimi.com/docs/openapi.json） |
| Dynamic tool 只 K3 支持 | Chat K3 | 其他 Kimi 模型不支持动态工具消息。 | `动态加载工具目前仅 kimi-k3 支持，在其他模型（如 kimi-k2.6）上请求会返回 tokenization failed 错误`（https://platform.kimi.com/docs/guide/use-dynamic-tool-loading.md） |
| 带 tools 的 system 消息不能有 content | Chat K3 | OpenAI Chat 基准没有该消息形态；Kimi 明确同一消息不能同时带 content。 | `携带 tools 的 system 消息不能再携带 content 字段，否则请求会以 400 报错（cannot be used with content）`（https://platform.kimi.com/docs/guide/use-dynamic-tool-loading.md） |
| Partial Mode | Chat | OpenAI Chat 基准没有标准 `partial:true`；Kimi 在最后 assistant 消息上启用。 | `在 messages 的最后一条 assistant 消息中预填输出前缀`；`设置 partial: true`（https://platform.kimi.com/docs/api/chat.md） |
| thinking 历史回传 | Chat | OpenAI Chat 基准没有 `reasoning_content` 历史回传要求；Kimi thinking 多轮需要原样保留。 | `请务必将每一轮 assistant 消息的 reasoning_content 原样保留在 messages 中，否则模型可能丢失推理上下文`（https://platform.kimi.com/docs/api/chat.md） |
| Anthropic Messages schema 未完整公开 | Anthropic | Claude Code 指南只说明环境变量和模型行为，未提供 `messages` content block、tool_result、cache_control 等支持矩阵。 | `本文说明一种通用接入方案：通过环境变量将 Claude Code 的模型请求转发到 Kimi API`；未在该页找到完整 Messages API schema（https://platform.kimi.com/docs/guide/claude-code-kimi.md） |

## 4. 工具格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| 普通工具为 OpenAI Chat function shape | Chat | 与 OpenAI Chat function tool shape 相近：`{type:"function", function:{name, parameters}}`；不同于 Anthropic 的 `input_schema` 和 Responses 的扁平 `parameters`。 | `ToolDefinition`；`type...function`；`function.name`；`function.parameters`；`required: ["name","parameters"]`（https://platform.kimi.com/docs/openapi.json） |
| Function 名称限制 | Chat | OpenAI Chat 基准允许最大 64；Kimi 正则要求首字符字母/下划线且长度至少 3。 | `函数名称。必须符合正则表达式：^[a-zA-Z_][a-zA-Z0-9-_]{2,63}$`（https://platform.kimi.com/docs/openapi.json） |
| `parameters` 需 MFJS | Chat | OpenAI Chat 基准使用 JSON Schema；Kimi 要求 Moonshot Flavored JSON Schema。 | `函数参数，JSON Schema 格式。需符合 MFJS（Moonshot Flavored JSON Schema）规范`（https://platform.kimi.com/docs/openapi.json） |
| `strict` 默认 true | Chat | OpenAI Chat function strict 可选；Kimi 明确默认 true，false 时只保证合法 JSON 对象。 | `strict...默认为 true`；`设为 false 时仅保证输出为合法 JSON 对象，不强制约束内部结构`（https://platform.kimi.com/docs/openapi.json） |
| `tool_choice` 支持三枚举和指定函数 | Chat | 与 OpenAI Chat 基准接近，但无 `allowed_tools`；模型支持范围不同。 | `auto：模型自行决定是否调用工具；none：不调用工具；required：强制调用工具`；`也可传入特定函数对象强制调用指定工具`（https://platform.kimi.com/docs/openapi.json） |
| 指定函数与 thinking 不兼容 | Chat | OpenAI Chat 基准没有该限制；Kimi 指定函数工具选择在 thinking 开启时报 400。 | `指定函数调用当前与思考开启不兼容：思考开启时传入会返回 400 错误（tool_choice 'specified' is incompatible with thinking enabled）`（https://platform.kimi.com/docs/guide/use-tool-choice.md） |
| `required` 只部分模型支持 | Chat | OpenAI Chat 基准不按 Kimi 模型族限制；Kimi K2.6/K2.7 不支持 `required`。 | `kimi-k3 支持 auto / none / required`；`kimi-k2.6 与 kimi-k2.7-code 不支持 required，传入会报错`（https://platform.kimi.com/docs/api/models-overview.md） |
| `$web_search` 内置工具是 `builtin_function` | Chat | OpenAI Chat 基准没有 `builtin_function` tool type；Kimi `$web_search` 不需要参数 schema。 | `$web_search（builtin_function 类型）`；`只需在 tools 中声明 type 和 function.name 即可成功注册`；`type = builtin_function`（https://platform.kimi.com/docs/guide/use-web-search.md） |
| `$web_search` 名称带 `$` | Chat | Kimi 普通 function 名称正则不允许 `$`；内置函数以 `$` 前缀区分。 | `$web_search 以美元符号 $ 作为前缀`；`在普通的 function 定义中，不允许出现美元符号 $`（https://platform.kimi.com/docs/guide/use-web-search.md） |
| `$web_search` 执行流程特殊 | Chat | 普通 tool 由客户端执行；Kimi `$web_search` 由 Kimi 大模型定义和执行，调用方把 arguments 作为 tool 结果回传。 | `搜索本身由 Kimi 大模型定义并执行`；`调用方只需要将 tool_call.function.arguments 原封不动地提交给 Kimi 大模型`（https://platform.kimi.com/docs/guide/use-web-search.md） |
| Formula 官方工具通道 | Chat / Tools API | OpenAI/Anthropic 基准没有 Formula；Kimi 官方工具需要先取声明、再执行 fiber。 | `GET /v1/formulas/{uri}/tools — 获取工具声明`；`POST /v1/formulas/{uri}/fibers — 按 tool_calls 原样执行`（https://platform.kimi.com/docs/guide/use-official-tools.md） |
| Formula 工具列表 | Chat / Tools API | Kimi 官方工具集合是供应商扩展，不是 OpenAI built-ins。 | `convert`、`web-search`、`rethink`、`memory`、`excel`、`fetch`、`quickjs`、`code-runner` 等（https://platform.kimi.com/docs/guide/use-official-tools.md） |
| Dynamic tools 与顶层 tools 并存 | Chat K3 | OpenAI Chat 基准没有中途注入工具声明；Kimi 允许并存。 | `动态加载的工具与请求顶层 tools 字段声明的全局工具并存`；`声明格式与请求顶层 tools 字段的格式完全一致`（https://platform.kimi.com/docs/guide/use-dynamic-tool-loading.md） |
| 未找到 custom/namespace/MCP tool | Chat / Responses | OpenAI Chat/Responses 基准有 custom tool、namespace 或 MCP；Kimi Chat OpenAPI 未列这些 tool shape。 | `ToolDefinition.type enum ["function"]`；`$web_search` 文档另列 `builtin_function`，OpenAPI 未列 custom/MCP（https://platform.kimi.com/docs/openapi.json；https://platform.kimi.com/docs/guide/use-web-search.md） |
| Anthropic `input_schema` / server tools | Anthropic | Claude Code 指南未列 Anthropic tool schema 支持；不推断 `input_schema` 或 server tools。 | 未在 https://platform.kimi.com/docs/guide/claude-code-kimi.md 找到 `input_schema`、`server_tool_use` 或 `cache_control` 字段说明。 |

## 5. 响应格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat id 格式示例 | Chat | OpenAI Chat 基准 ID 通常 `chatcmpl-...`；Kimi 示例为 `cmpl-...`。不应依赖 OpenAI ID 前缀。 | `"id": "cmpl-04ea926191a14749b7f2c7a48a68abc6"`（https://platform.kimi.com/docs/api/chat.md） |
| `usage.cached_tokens` 顶层字段 | Chat | OpenAI Chat 基准使用 `prompt_tokens_details.cached_tokens`；Kimi Chat response usage 直接有 `cached_tokens`。 | `usage...cached_tokens`；`命中缓存的 Token 数量`（https://platform.kimi.com/docs/openapi.json）；示例 `"cached_tokens": 10`（https://platform.kimi.com/docs/api/chat.md） |
| `reasoning_content` 响应字段 | Chat | OpenAI Chat 基准没有标准 `message.reasoning_content`；Kimi thinking 模式返回。 | `reasoning_content`；`推理过程（仅在思考模式启用时返回）`（https://platform.kimi.com/docs/openapi.json） |
| `finish_reason` 枚举较少 | Chat | OpenAI Chat 基准还有 `content_filter/function_call` 等；Kimi OpenAPI 只列 `stop/length/tool_calls`。 | `finish_reason enum: stop, length, tool_calls`（https://platform.kimi.com/docs/openapi.json） |
| Tool call arguments 是 JSON 字符串 | Chat / Formula | 与 OpenAI Chat function tool call 类似；Kimi 官方工具强调不要二次转义。 | `arguments`；`函数参数的 JSON 字符串`（https://platform.kimi.com/docs/openapi.json）；`function.arguments 虽然内容是合法的 JSON，但格式上仍然是一个 encoded string，你不需要转义`（https://platform.kimi.com/docs/guide/use-official-tools.md） |
| `$web_search` 搜索内容 token 统计在 arguments 内 | Chat `$web_search` | OpenAI Chat 基准 usage 不会在 tool call arguments 内报告内置搜索 token。 | `arguments 时，会在其中的 usage 对象下额外添加一个 total_tokens 字段`；`读取路径为 arguments.usage.total_tokens`（https://platform.kimi.com/docs/guide/use-web-search.md） |
| Formula protected tool 输出 | Tools API | OpenAI/Anthropic 基准没有 `encrypted_output`；Kimi Formula protected 工具结果可能需回传 encrypted output。 | `web-search 在创建时被设置为 protected`；`结果会出现在 context.encrypted_output 字段中`；`该内容可以直接塞到 tool 调用里使用`（https://platform.kimi.com/docs/guide/use-official-tools.md） |
| 错误格式 | 全部 | OpenAI 基准错误对象字段为 `message/type/param/code`；Kimi 为顶层 `error`，常见字段 `message/type/code`，`message` required。 | `API 会返回包含错误信息的 JSON 响应`；示例 `{"error":{"type":"content_filter","message":"..."}}`（https://platform.kimi.com/docs/api/errors.md）；OpenAPI `ErrorResponse.error.message/type/code`（https://platform.kimi.com/docs/openapi.json） |
| 错误类型 | 全部 | Kimi error.type 包含自有名称。 | `invalid_request_error`；`invalid_authentication_error`；`incorrect_api_key_error`；`engine_overloaded_error`；`rate_limit_reached_error`（https://platform.kimi.com/docs/api/errors.md） |
| Anthropic 响应格式 | Anthropic | Claude Code 指南未列 Messages response、usage、error schema；不推断完全等同 Anthropic。 | `本文说明一种通用接入方案`；未在该页找到完整响应 schema（https://platform.kimi.com/docs/guide/claude-code-kimi.md） |

## 6. 流式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Chat stream 为 OpenAI Chat chunk + `[DONE]` | Chat | 与 OpenAI Chat 基准相近；不同于 Responses 语义 SSE。 | 示例 `object:"chat.completion.chunk"`；最终 `data: [DONE]`（https://platform.kimi.com/docs/api/chat.md） |
| usage final chunk | Chat | 与 OpenAI Chat 的 `stream_options.include_usage` 相近；Kimi 文档明确最终 chunk 在 `[DONE]` 前。 | `通过 stream_options: {"include_usage": true} 可在最后一个 chunk（data: [DONE] 之前）额外获取 usage 字段`（https://platform.kimi.com/docs/api/chat.md） |
| final usage 字段含 `cached_tokens` | Chat | OpenAI Chat 基准 final usage 用 `prompt_tokens_details.cached_tokens`；Kimi final chunk usage 可直接有 `cached_tokens`。 | `usage":{"prompt_tokens":19,"completion_tokens":13,"total_tokens":32,"cached_tokens":12}`（https://platform.kimi.com/docs/api/chat.md） |
| 非 usage chunk 的 usage 为 null | Chat | 与 OpenAI Chat 基准相近，但字段说明来自 Kimi OpenAPI。 | `其他所有 chunk 也会包含 usage 字段，但值为 null`（https://platform.kimi.com/docs/openapi.json） |
| 流式中断可能无总 usage | Chat | 与 OpenAI Chat 基准同类限制；Kimi 明确说明。 | `如果流中断，可能无法收到包含总 Token 用量的最终 chunk`（https://platform.kimi.com/docs/openapi.json） |
| Tool call delta | Chat | 与 OpenAI Chat tool_calls delta 相近，Kimi OpenAPI 列 `delta.tool_calls`。 | `delta.tool_calls`；`模型发起的工具调用片段`；`arguments...函数参数的 JSON 字符串`（https://platform.kimi.com/docs/openapi.json） |
| Responses SSE | Responses | 未找到 `/responses`，不写 Responses 事件支持。 | OpenAPI paths 未出现 `/v1/responses`（https://platform.kimi.com/docs/openapi.json） |
| Anthropic SSE | Anthropic | Claude Code 指南未列 Anthropic SSE 事件名；不推断 `message_start/content_block_delta/message_stop`。 | 未在 https://platform.kimi.com/docs/guide/claude-code-kimi.md 找到完整 SSE 事件集。 |

## 7. 已知限制

| 限制 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| 模型上下文窗口 | Chat / Anthropic 兼容模型 | 基准协议不规定 Kimi 模型窗口；Kimi K3 为 1M，K2.6/K2.7 为 256K。 | `kimi-k3...100 万 token 上下文窗口`；`kimi-k2.7-code...上下文 256k`；`kimi-k2.6...上下文 256k`（https://platform.kimi.com/docs/models.md） |
| Anthropic 兼容自动压缩窗口配置 | Anthropic / Claude Code | Claude Code 需手动把 auto compact window 与模型窗口对齐。 | `CLAUDE_CODE_AUTO_COMPACT_WINDOW`；`kimi-k3 为 1M（1048576），kimi-k2.7-code 为 256K（262144）`（https://platform.kimi.com/docs/guide/claude-code-kimi.md） |
| K3 `max_completion_tokens` 最大 1,048,576 | Chat | OpenAI Chat 基准模型上限不同；Kimi K3 schema 明确最大值。 | `Kimi K3 默认为 131072，最大可设置为 1048576`（https://platform.kimi.com/docs/openapi.json） |
| K2.x/K3 固定采样参数 | Chat | 传 OpenAI 常见采样参数可能报错。 | `固定表示该参数不可修改：传入其他值会报错，建议不要显式传入`（https://platform.kimi.com/docs/api/models-overview.md） |
| K2.6/K2.7 不支持 `tool_choice:"required"` | Chat | OpenAI Chat 基准支持 `required`；Kimi 按模型限制。 | `kimi-k2.6 与 kimi-k2.7-code 不支持 required，传入会报错`（https://platform.kimi.com/docs/api/models-overview.md） |
| 指定函数 tool_choice + thinking 报 400 | Chat | OpenAI Chat 基准没有该限制。 | `tool_choice 'specified' is incompatible with thinking enabled`（https://platform.kimi.com/docs/guide/use-tool-choice.md） |
| 动态工具只 K3 支持 | Chat | 基准协议无此功能。 | `动态加载工具目前仅 kimi-k3 支持`；`其他模型...tokenization failed`（https://platform.kimi.com/docs/guide/use-dynamic-tool-loading.md） |
| Context Caching 自动启用，无手动 TTL | Chat | Anthropic/OpenAI 基准有显式 cache 字段或 TTL 控制；Kimi 自动前缀缓存。 | `Context Caching 对所有模型请求自动启用`；`无需手动创建`；`无需管理 TTL`（https://platform.kimi.com/docs/guide/use-context-caching-feature-of-kimi-api.md） |
| Context Caching 最小门槛 | Chat | OpenAI/Anthropic 基准最小缓存长度不同；Kimi 为前一个请求 prompt tokens 大于 256 才能命中。 | `当前一个请求的 prompt tokens 大于 256 时，新的请求才能命中前缀缓存`；`小于 256 时，请求不会被缓存而是被丢弃`（https://platform.kimi.com/docs/guide/use-context-caching-feature-of-kimi-api.md） |
| Dynamic tools 与缓存前缀 | Chat K3 | Kimi 文档要求追加动态工具以保持前缀缓存；这是供应商特定优化。 | `追加，不要插入`；`在 messages 末尾追加工具声明：不影响已有前缀缓存`（https://platform.kimi.com/docs/guide/use-dynamic-tool-loading.md） |
| 充值 tier 限速 | 全部 | OpenAI/Anthropic 基准不使用 Kimi 的充值 tier 表。 | Tier 表含 `并发`、`RPM`、`TPM`、`TPD`；`Tier0...并发 1...RPM 3...TPM 500,000...TPD 1,500,000`；`Tier5...并发 1,000...RPM 10,000...TPM 5,000,000`（https://platform.kimi.com/docs/pricing/limits.md） |
| 集群负载临时限流 | 全部 | 即使未触达账号 tier，也可能临时限流。 | `当集群负载达到容量上限时，我们可能会采取临时的限流措施`（https://platform.kimi.com/docs/pricing/limits.md） |
| 504 非流式超时 | Chat | OpenAI Chat 基准未规定 Kimi 网关 900 秒；Kimi 错误页建议长请求改用流式。 | `504 Gateway Time-out`；`服务端 900 秒无响应`；`建议改用流式输出（stream: true）`（https://platform.kimi.com/docs/api/errors.md） |
| Anthropic 端点 `max_tokens` 必填等字段级要求未找到 | Anthropic | 用户关注的 Anthropic `max_tokens` 必填，在 Kimi 指定 Claude Code 官方页未找到字段级 schema；只保留“无法从该官方页核实”的结论。 | Claude Code 页仅列 `ANTHROPIC_BASE_URL`、`ANTHROPIC_AUTH_TOKEN`、模型变量和 thinking 行为；未在官方页找到 Anthropic Messages 请求字段表（https://platform.kimi.com/docs/guide/claude-code-kimi.md） |
