# Anthropic Messages API 参考

> 核对日期：2026-08-12。  
> 说明：本文只使用官方文档与仓库中已核验调研作为线索；协议事实以官方文档为准。每个小节给出官方 URL 与短摘录，字段含义用中文归纳。未访问到的官方页面会明确标注“无法访问”。

## 官方来源与访问状态

| URL | 状态 | 用途 | 官方短摘录 |
| --- | --- | --- | --- |
| https://platform.claude.com/docs/en/api/messages | 可访问；已使用 `.md` 官方页面 | Create Message API reference | `post /v1/messages`；`Send a structured list of input messages` |
| https://platform.claude.com/docs/en/manage-claude/authentication | 可访问 | 认证方式 | `Set the x-api-key header`；`anthropic-version: 2023-06-01` |
| https://platform.claude.com/docs/en/build-with-claude/streaming | 可访问 | SSE 流式事件 | `Each server-sent event includes a named event type` |
| https://platform.claude.com/docs/en/build-with-claude/prompt-caching | 可访问 | Prompt cache | `automatic caching or explicit breakpoints` |
| https://platform.claude.com/docs/en/build-with-claude/thinking | 可访问 | Thinking 行为 | `thinking content blocks ahead of the response` |
| https://platform.claude.com/docs/en/build-with-claude/thinking-tool-workflows | 可访问 | Thinking + tool 工作流 | `preserving thinking blocks` |
| https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview | 可访问 | 客户端工具 | `client tools and functions` |
| https://platform.claude.com/docs/en/agents-and-tools/tool-use/parallel-tool-use | 可访问 | 并行工具 | `parallel tool use` |
| https://platform.claude.com/docs/en/api/errors | 可访问 | 错误格式与错误码 | `The API always returns errors as JSON` |

## 1. 端点 URL 与认证方式

| 项 | 值 | 官方依据 |
| --- | --- | --- |
| HTTP 方法与路径 | `POST https://api.anthropic.com/v1/messages` | Messages API：`post /v1/messages`（https://platform.claude.com/docs/en/api/messages） |
| Content-Type | `content-type: application/json` | Authentication 示例：`content-type: application/json`（https://platform.claude.com/docs/en/manage-claude/authentication） |
| API key 认证 | Header `x-api-key: $ANTHROPIC_API_KEY` | Authentication：`Static ... secret in the x-api-key header`（https://platform.claude.com/docs/en/manage-claude/authentication） |
| 版本 Header | Header `anthropic-version: 2023-06-01` | Authentication 示例：`anthropic-version: 2023-06-01`（https://platform.claude.com/docs/en/manage-claude/authentication） |
| 可选用户归因 Header | `anthropic-user-profile-id: string`，需要 `user-profiles` beta header | Messages API：`anthropic-user-profile-id: optional string`（https://platform.claude.com/docs/en/api/messages） |

## 2. 请求格式

### 顶层参数

| 参数 | 类型 | 必填 | 默认值 | 约束/说明 | 官方依据 |
| --- | --- | --- | --- | --- | --- |
| `model` | `Model` 或 `string` | 是 | 无 | 生成响应的模型；官方枚举示例包含 `claude-sonnet-5`、`claude-fable-5`、`claude-mythos-5`、`claude-opus-5`、`claude-opus-4-8`、`claude-sonnet-4-6`、`claude-haiku-4-5` 等，也允许字符串。 | `model: Model`；`The model that will complete your prompt.`（https://platform.claude.com/docs/en/api/messages） |
| `max_tokens` | `number` | 是 | 无 | 最大生成 token 数；模型可在达到此值前停止。设为 `0` 可预热 prompt cache 且不生成输出；不同模型上限不同。 | `The maximum number of tokens to generate before stopping.`；`Set to 0 to populate the prompt cache`（https://platform.claude.com/docs/en/api/messages） |
| `messages` | `array<MessageParam>` | 是 | 无 | 输入消息数组；官方说明模型按交替 `user`/`assistant` turns 工作；连续同角色 turn 会合并；单请求最多 100,000 条消息。 | `Input messages.`；`There is a limit of 100,000 messages`（https://platform.claude.com/docs/en/api/messages） |
| `cache_control` | `CacheControlEphemeral` | 否 | 无 | 顶层自动缓存开关；自动给请求中最后一个可缓存块加 cache marker。和显式块级断点共用最多 4 个断点槽。 | `Top-level cache control automatically applies...`（https://platform.claude.com/docs/en/api/messages）；`automatic cache breakpoint uses one of the 4 available`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| `container` | `string` | 否 | 无 | 复用 code execution container 的标识。 | `Container identifier for reuse across requests.`（https://platform.claude.com/docs/en/api/messages） |
| `inference_geo` | `string` | 否 | workspace 默认值 | 指定推理地理区域；未指定时使用 workspace 的 `default_inference_geo`。 | `Specifies the geographic region for inference processing.`（https://platform.claude.com/docs/en/api/messages） |
| `metadata` | `object` | 否 | 无 | 请求元数据。当前子字段 `user_id?: string`，应为 uuid/hash/opaque id，不应包含姓名、邮箱、电话等身份信息。 | `An object describing metadata`；`user_id: optional string`（https://platform.claude.com/docs/en/api/messages） |
| `output_config` | `object` | 否 | 无 | 输出配置。`effort?: "low"|"medium"|"high"|"xhigh"|"max"`；`format?: {type:"json_schema", schema: object}`。 | `Configuration options for the model's output`；`type: "json_schema"`（https://platform.claude.com/docs/en/api/messages） |
| `service_tier` | `"auto"` 或 `"standard_only"` | 否 | 未注明 | 控制优先容量或标准容量。 | `service_tier: optional "auto" or "standard_only"`（https://platform.claude.com/docs/en/api/messages） |
| `stop_sequences` | `array<string>` | 否 | 无 | 自定义停止字符串；命中时 `stop_reason` 为 `"stop_sequence"`，响应 `stop_sequence` 为匹配值。 | `Custom text sequences`；`stop_reason value will be "stop_sequence"`（https://platform.claude.com/docs/en/api/messages） |
| `stream` | `boolean` | 否 | 未注明 | 为 `true` 时使用 SSE 增量流。 | `Whether to incrementally stream the response using server-sent events.`（https://platform.claude.com/docs/en/api/messages） |
| `system` | `string` 或 `array<TextBlockParam>` | 否 | 无 | 顶层 system prompt；Messages API 的普通输入消息不使用 Chat 风格 system role。注意：同页 schema 的 `MessageParam.role` 又列出 `"system"`，见下文“官方差异”。 | `System prompt.`；`there is no "system" role for input messages`（https://platform.claude.com/docs/en/api/messages） |
| `temperature` | `number` | 否 | `1.0` | 范围 `0.0` 到 `1.0`；`0.0` 也不保证完全确定。 | `Defaults to 1.0. Ranges from 0.0 to 1.0.`（https://platform.claude.com/docs/en/api/messages） |
| `thinking` | `ThinkingConfigParam` | 否 | 依模型而定 | 开启/关闭/自适应 extended thinking；启用预算最小 1,024，且小于 `max_tokens`，计入 `max_tokens`。 | `Requires a minimum budget of 1,024 tokens`；`counts towards your max_tokens`（https://platform.claude.com/docs/en/api/messages） |
| `tool_choice` | `ToolChoice` | 否 | 模型自行决定 | 控制是否、如何使用工具。形状见工具章节。 | `How the model should use the provided tools.`（https://platform.claude.com/docs/en/api/messages） |
| `tools` | `array<ToolUnion>` | 否 | 无 | 客户端工具与服务端工具定义。响应可返回 `tool_use`，调用方再以 `tool_result` 回传结果。 | `Definitions of tools that the model may use.`（https://platform.claude.com/docs/en/api/messages） |
| `top_k` | `number` | 否 | 无 | 从 top K token 候选采样；官方建议仅高级场景使用。 | `Only sample from the top K options`（https://platform.claude.com/docs/en/api/messages） |
| `top_p` | `number` | 否 | 无 | nucleus sampling；官方建议不要同时调 `temperature` 与 `top_p`。 | `Use nucleus sampling.`（https://platform.claude.com/docs/en/api/messages） |

### `thinking` 参数形状

| 形状 | 字段 | 约束 | 官方依据 |
| --- | --- | --- | --- |
| `ThinkingConfigEnabled` | `type:"enabled"`；`budget_tokens:number`；`display?:"summarized"|"omitted"` | `budget_tokens >= 1024` 且小于 `max_tokens`；`display` 默认 `summarized`（API reference）；thinking 文档说明新模型可能默认 omitted，属于模型级默认差异。 | `type: "enabled"`；`Must be ≥1024 and less than max_tokens`（https://platform.claude.com/docs/en/api/messages） |
| `ThinkingConfigDisabled` | `type:"disabled"` | 禁用 thinking。 | `type: "disabled"`（https://platform.claude.com/docs/en/api/messages） |
| `ThinkingConfigAdaptive` | `type:"adaptive"`；`display?:"summarized"|"omitted"` | 模型根据请求决定是否/多深 thinking；`display` 控制 thinking 内容是否返回。 | `type: "adaptive"`；`display: optional "summarized" or "omitted"`（https://platform.claude.com/docs/en/api/messages） |

## 3. 消息与内容块格式

### `MessageParam`

| 字段 | 类型 | 必填 | 约束/说明 | 官方依据 |
| --- | --- | --- | --- | --- |
| `role` | `"user"` 或 `"assistant"`；同页 schema 还列出 `"system"` | 是 | 官方正文说明输入消息只有 `user`/`assistant`，system prompt 用顶层 `system`；但同一 API reference 的 schema 行列出 `role: "user" or "assistant" or "system"`。转换实现应保留此官方差异，不把 `system` role 当作无来源事实。 | `user and assistant conversational turns`；`no "system" role`；`role: "user" or "assistant" or "system"`（https://platform.claude.com/docs/en/api/messages） |
| `content` | `string` 或 `array<ContentBlockParam>` | 是 | 字符串是单个 `{type:"text"}` block 的简写。 | `Using a string for content is shorthand`（https://platform.claude.com/docs/en/api/messages） |

### 输入 `ContentBlockParam` 类型

| `type` | 形状 | 主要字段 | 可缓存 | 官方依据 |
| --- | --- | --- | --- | --- |
| `text` | `TextBlockParam` | `text:string`；`cache_control?`；`citations?` | 是 | `TextBlockParam object { text, type, cache_control, citations }`（https://platform.claude.com/docs/en/api/messages） |
| `image` | `ImageBlockParam` | `source` 为 base64 或 URL；base64 `media_type` 为 `image/jpeg`、`image/png`、`image/gif`、`image/webp` | 是 | `ImageBlockParam object { source, type, cache_control }`（https://platform.claude.com/docs/en/api/messages） |
| `document` | `DocumentBlockParam` | `source` 为 base64 PDF、plain text、content blocks、URL PDF；`citations?`、`context?`、`title?` | 是 | `DocumentBlockParam object { source, type, cache_control, 3 more }`（https://platform.claude.com/docs/en/api/messages） |
| `search_result` | `SearchResultBlockParam` | `content: array<TextBlockParam>`；`source`；`title`；`citations?` | 是 | `SearchResultBlockParam object { content, source, title, 3 more }`（https://platform.claude.com/docs/en/api/messages） |
| `thinking` | `ThinkingBlockParam` | `signature:string`；`thinking:string` | 不能直接标记 `cache_control` | `ThinkingBlockParam object { signature, thinking, type }`（https://platform.claude.com/docs/en/api/messages）；`Thinking blocks cannot be cached directly`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| `redacted_thinking` | `RedactedThinkingBlockParam` | `data:string` | 未标为可直接缓存 | `RedactedThinkingBlockParam object { data, type }`（https://platform.claude.com/docs/en/api/messages） |
| `tool_use` | `ToolUseBlockParam` | `id:string`；`name:string`；`input: object`；`caller?` | 是 | `ToolUseBlockParam object { id, input, name, 3 more }`（https://platform.claude.com/docs/en/api/messages） |
| `tool_result` | `ToolResultBlockParam` | `tool_use_id:string`；`content?: string | array<...>`；`is_error?: boolean` | 是 | `ToolResultBlockParam object { tool_use_id, type, cache_control, 2 more }`（https://platform.claude.com/docs/en/api/messages） |
| `server_tool_use` | `ServerToolUseBlockParam` | `id`、`input`、`name`；`name` 可为 `web_search`、`web_fetch`、`code_execution`、`bash_code_execution`、`text_editor_code_execution`、`tool_search_tool_regex`、`tool_search_tool_bm25` | 是 | `ServerToolUseBlockParam object`；`name: "web_search" or "web_fetch"...`（https://platform.claude.com/docs/en/api/messages） |
| `web_search_tool_result` | `WebSearchToolResultBlockParam` | `tool_use_id`；`content` 为结果数组或错误对象；错误码见下表 | 是 | `WebSearchToolResultBlockParam object`（https://platform.claude.com/docs/en/api/messages） |
| `web_fetch_tool_result` | `WebFetchToolResultBlockParam` | `content` 为 `web_fetch_result` 或错误对象 | 是 | `WebFetchToolResultBlockParam object`（https://platform.claude.com/docs/en/api/messages） |
| `code_execution_tool_result` | `CodeExecutionToolResultBlockParam` | `content` 为执行结果、加密执行结果或错误 | 是 | `CodeExecutionToolResultBlockParam object`（https://platform.claude.com/docs/en/api/messages） |
| `bash_code_execution_tool_result` | `BashCodeExecutionToolResultBlockParam` | `content` 为 bash 执行结果或错误 | 是 | `BashCodeExecutionToolResultBlockParam object`（https://platform.claude.com/docs/en/api/messages） |
| `text_editor_code_execution_tool_result` | `TextEditorCodeExecutionToolResultBlockParam` | `content` 为 view/create/str_replace 结果或错误 | 是 | `TextEditorCodeExecutionToolResultBlockParam object`（https://platform.claude.com/docs/en/api/messages） |
| `tool_search_tool_result` | `ToolSearchToolResultBlockParam` | `content` 为 tool references 或错误 | 是 | `ToolSearchToolResultBlockParam object`（https://platform.claude.com/docs/en/api/messages） |
| `container_upload` | `ContainerUploadBlockParam` | `file_id:string` | 是 | `ContainerUploadBlockParam object { file_id, type, cache_control }`（https://platform.claude.com/docs/en/api/messages） |
| `mid_conv_system` | `MidConversationSystemBlockParam` | `content: array<TextBlockParam>` | 是 | `System instructions that appear mid-conversation.`（https://platform.claude.com/docs/en/api/messages） |

### 来源与引用子结构

| 对象 | 字段/枚举 | 官方依据 |
| --- | --- | --- |
| `Base64ImageSource` | `type:"base64"`；`media_type:"image/jpeg"|"image/png"|"image/gif"|"image/webp"`；`data:string` | `media_type: "image/jpeg" or ...`（https://platform.claude.com/docs/en/api/messages） |
| `URLImageSource` | `type:"url"`；`url:string` | `URLImageSource object { type, url }`（https://platform.claude.com/docs/en/api/messages） |
| `Document source` | `base64` PDF、`text/plain`、`content`、URL PDF | `Base64PDFSource`；`PlainTextSource`；`ContentBlockSource`；`URLPDFSource`（https://platform.claude.com/docs/en/api/messages） |
| `TextCitationParam` | `char_location`、`page_location`、`content_block_location`、`web_search_result_location`、`search_result_location` | `The type of citation returned will depend...`（https://platform.claude.com/docs/en/api/messages） |
| `ToolResultBlockParam.content` | 字符串或 `TextBlockParam`、`ImageBlockParam`、`SearchResultBlockParam`、`DocumentBlockParam`、`ToolReferenceBlockParam` 数组 | `content: optional string or array`（https://platform.claude.com/docs/en/api/messages） |

## 4. 工具定义格式

### `tool_choice`

| 形状 | 字段 | 行为 | 官方依据 |
| --- | --- | --- | --- |
| `ToolChoiceAuto` | `type:"auto"`；`disable_parallel_tool_use?: boolean` | 模型自动决定是否用工具；禁用并行时最多一个 tool use。 | `The model will automatically decide`；`output at most one tool use`（https://platform.claude.com/docs/en/api/messages） |
| `ToolChoiceAny` | `type:"any"`；`disable_parallel_tool_use?: boolean` | 模型会使用任意可用工具；禁用并行时正好一个 tool use。 | `The model will use any available tools.`（https://platform.claude.com/docs/en/api/messages） |
| `ToolChoiceTool` | `type:"tool"`；`name:string`；`disable_parallel_tool_use?: boolean` | 强制使用指定工具。 | `The model will use the specified tool`（https://platform.claude.com/docs/en/api/messages） |
| `ToolChoiceNone` | `type:"none"` | 不允许使用工具。 | `The model will not be allowed to use tools.`（https://platform.claude.com/docs/en/api/messages） |

### 客户端工具 `Tool`

| 字段 | 类型 | 必填 | 说明 | 官方依据 |
| --- | --- | --- | --- | --- |
| `name` | `string` | 是 | 工具名称；出现在 `tool_use.name`。 | `Name of the tool.`（https://platform.claude.com/docs/en/api/messages） |
| `input_schema` | JSON Schema object | 是 | 工具输入 schema；定义模型在 `tool_use.input` 中生成的 shape。 | `JSON schema... for this tool's input`（https://platform.claude.com/docs/en/api/messages） |
| `description` | `string` | 否 | 强烈建议填写；说明工具用途和用法。 | `Optional, but strongly-recommended`（https://platform.claude.com/docs/en/api/messages） |
| `type` | `"custom"` | 否 | 客户端自定义工具可带此 discriminator。 | `type: optional "custom"`（https://platform.claude.com/docs/en/api/messages） |
| `cache_control` | `CacheControlEphemeral` | 否 | 可在工具定义上设置缓存断点。 | `Create a cache control breakpoint`（https://platform.claude.com/docs/en/api/messages） |
| `allowed_callers` | `array<"direct"|"code_execution_20250825"|"code_execution_20260120"|"code_execution_20260521">` | 否 | 限制哪些 caller 可调用工具。 | `allowed_callers: optional array`（https://platform.claude.com/docs/en/api/messages） |
| `defer_loading` | `boolean` | 否 | 设为 true 时初始 system prompt 不加载该工具，仅由 tool search 引用加载。 | `will not be included in initial system prompt`（https://platform.claude.com/docs/en/api/messages） |
| `eager_input_streaming` | `boolean` | 否 | 开启后工具参数可增量流出；`null`/缺省时按 beta header 默认行为。 | `tool input parameters will be streamed incrementally`（https://platform.claude.com/docs/en/api/messages） |
| `input_examples` | `array<object>` | 否 | 输入示例。 | `input_examples: optional array`（https://platform.claude.com/docs/en/api/messages） |
| `strict` | `boolean` | 否 | 为 true 时保证工具名和输入 schema 校验。 | `guarantees schema validation`（https://platform.claude.com/docs/en/api/messages） |

### 服务端/内置工具 `ToolUnion`

| 工具类型 | 固定字段 | 可选字段摘要 | 官方依据 |
| --- | --- | --- | --- |
| `bash_20250124` | `type:"bash_20250124"`；`name:"bash"` | `allowed_callers`、`cache_control`、`defer_loading`、`input_examples`、`strict` | `ToolBash20250124 object`（https://platform.claude.com/docs/en/api/messages） |
| `code_execution_20250522` / `20250825` / `20260120` / `20260521` | `type` 对应版本；`name:"code_execution"` | `allowed_callers`、`cache_control`、`defer_loading`、`strict` | `CodeExecutionTool... object`（https://platform.claude.com/docs/en/api/messages） |
| `memory_20250818` | `type:"memory_20250818"`；`name:"memory"` | `allowed_callers`、`cache_control`、`defer_loading`、`input_examples`、`strict` | `MemoryTool20250818 object`（https://platform.claude.com/docs/en/api/messages） |
| `text_editor_20250124` | `type:"text_editor_20250124"`；`name:"str_replace_editor"` | 通用可选字段 | `ToolTextEditor20250124 object`（https://platform.claude.com/docs/en/api/messages） |
| `text_editor_20250429` / `20250728` | `type` 对应版本；`name:"str_replace_based_edit_tool"` | `20250728` 还支持 `max_characters?: number` | `ToolTextEditor20250728 object`（https://platform.claude.com/docs/en/api/messages） |
| `web_search_20250305` / `20260209` / `20260318` | `type` 对应版本；`name:"web_search"` | `allowed_domains` 与 `blocked_domains` 互斥；`max_uses`；`user_location`；`20260318` 支持 `response_inclusion:"full"|"excluded"` | `WebSearchTool... object`（https://platform.claude.com/docs/en/api/messages） |
| `web_fetch_20250910` / `20260209` / `20260309` / `20260318` | `type` 对应版本；`name:"web_fetch"` | `allowed_domains`、`blocked_domains`、`citations`、`max_content_tokens`、`max_uses`；`20260309+` 支持 `use_cache`; `20260318` 支持 `response_inclusion` | `WebFetchTool... object`（https://platform.claude.com/docs/en/api/messages） |
| `tool_search_tool_bm25_20251119` 或 `tool_search_tool_bm25` | `name:"tool_search_tool_bm25"` | 通用可选字段 | `ToolSearchToolBm25_20251119 object`（https://platform.claude.com/docs/en/api/messages） |
| `tool_search_tool_regex_20251119` 或 `tool_search_tool_regex` | `name:"tool_search_tool_regex"` | 通用可选字段 | `ToolSearchToolRegex20251119 object`（https://platform.claude.com/docs/en/api/messages） |

## 5. 非流式响应格式

### `Message` 对象

| 字段 | 类型 | 必填/可空 | 说明 | 官方依据 |
| --- | --- | --- | --- | --- |
| `id` | `string` | 必有 | 响应对象唯一 ID；格式和长度可能变化。 | `Unique object identifier.`（https://platform.claude.com/docs/en/api/messages） |
| `type` | `"message"` | 必有 | Messages 响应固定对象类型。 | `For Messages, this is always "message".`（https://platform.claude.com/docs/en/api/messages） |
| `role` | `"assistant"` | 必有 | 生成消息角色固定为 assistant。 | `This will always be "assistant".`（https://platform.claude.com/docs/en/api/messages） |
| `model` | `Model` 或 `string` | 必有 | 生成响应的模型。 | `The model that will complete your prompt.`（https://platform.claude.com/docs/en/api/messages） |
| `content` | `array<ContentBlock>` | 必有 | 模型生成内容块；若请求最后一条是 assistant turn，响应会直接续写该 turn。 | `Content generated by the model.`（https://platform.claude.com/docs/en/api/messages） |
| `stop_reason` | 枚举 | 非流式非 null | 停止原因。 | `In non-streaming mode this value is always non-null.`（https://platform.claude.com/docs/en/api/messages） |
| `stop_sequence` | `string` | 可能 null | 当命中自定义 `stop_sequences` 时为匹配字符串。 | `Which custom stop sequence was generated`（https://platform.claude.com/docs/en/api/messages） |
| `stop_details` | `RefusalStopDetails` | 可选 | 拒绝时的结构化信息；`type:"refusal"`，`category` 包含 `cyber`、`bio`、`frontier_llm`、`reasoning_extraction`、`general_harms`。 | `Structured information about a refusal.`（https://platform.claude.com/docs/en/api/messages） |
| `container` | `{id, expires_at}` | 可选 | code execution tool 使用的 container 信息。 | `Information about the container used`（https://platform.claude.com/docs/en/api/messages） |
| `usage` | `Usage` | 必有 | 计费与限流 token 用量。 | `Billing and rate-limit usage.`（https://platform.claude.com/docs/en/api/messages） |

### `stop_reason` 枚举

| 值 | 含义 | 官方依据 |
| --- | --- | --- |
| `end_turn` | 模型到达自然停止点。 | `the model reached a natural stopping point`（https://platform.claude.com/docs/en/api/messages） |
| `max_tokens` | 超过请求 `max_tokens` 或模型最大值。 | `we exceeded the requested max_tokens`（https://platform.claude.com/docs/en/api/messages） |
| `stop_sequence` | 生成了自定义停止序列。 | `one of your provided custom stop_sequences`（https://platform.claude.com/docs/en/api/messages） |
| `tool_use` | 模型调用了一个或多个工具。 | `the model invoked one or more tools`（https://platform.claude.com/docs/en/api/messages） |
| `pause_turn` | 长运行 turn 被暂停；可把响应原样放回后续请求继续。 | `we paused a long-running turn`（https://platform.claude.com/docs/en/api/messages） |
| `refusal` | 流式分类器介入处理潜在政策违规。 | `streaming classifiers intervene`（https://platform.claude.com/docs/en/api/messages） |
| `model_context_window_exceeded` | 超过模型上下文窗口。 | `we exceeded the model's context window`（https://platform.claude.com/docs/en/api/messages） |

### 响应 `content` 块

响应 `content` 的主要 block 类型与输入相近，但响应侧不包含 `cache_control`，并会出现模型生成的 `tool_use`、`server_tool_use`、tool result、`thinking`、`redacted_thinking`、`container_upload` 等对象。官方摘录：`Content generated by the model.`；`array of content blocks`（https://platform.claude.com/docs/en/api/messages）。

| `type` | 主要字段 | 官方依据 |
| --- | --- | --- |
| `text` | `text:string`；`citations: array<TextCitation>` | `TextBlock object { citations, text, type }`（https://platform.claude.com/docs/en/api/messages） |
| `thinking` | `signature:string`；`thinking:string` | `ThinkingBlock object { signature, thinking, type }`（https://platform.claude.com/docs/en/api/messages） |
| `redacted_thinking` | `data:string` | `RedactedThinkingBlock object { data, type }`（https://platform.claude.com/docs/en/api/messages） |
| `tool_use` | `id`、`caller`、`input`、`name`、`type` | `ToolUseBlock object { id, caller, input, 2 more }`（https://platform.claude.com/docs/en/api/messages） |
| `server_tool_use` | `id`、`caller`、`input`、`name`、`type` | `ServerToolUseBlock object`（https://platform.claude.com/docs/en/api/messages） |
| `web_search_tool_result` / `web_fetch_tool_result` / `code_execution_tool_result` / `bash_code_execution_tool_result` / `text_editor_code_execution_tool_result` / `tool_search_tool_result` | 对应服务端工具结果或错误对象 | 各 `...ToolResultBlock object`（https://platform.claude.com/docs/en/api/messages） |
| `container_upload` | `file_id:string` | `ContainerUploadBlock object { file_id, type }`（https://platform.claude.com/docs/en/api/messages） |

### `usage`

| 字段 | 类型 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| `input_tokens` | `number` | 未从 cache 读取、也未用于 cache 创建的输入 token；总输入 token 不是此字段单独值。 | `input_tokens: number`；`Total input tokens... summation`（https://platform.claude.com/docs/en/api/messages） |
| `cache_creation_input_tokens` | `number` | 创建 cache entry 的输入 token 数。 | `The number of input tokens used to create the cache entry.`（https://platform.claude.com/docs/en/api/messages） |
| `cache_read_input_tokens` | `number` | 从 cache 读取的输入 token 数。 | `The number of input tokens read from the cache.`（https://platform.claude.com/docs/en/api/messages） |
| `cache_creation.ephemeral_1h_input_tokens` | `number` | 1 小时 TTL cache 写入 token 明细。 | `The number of input tokens used to create the 1 hour cache entry.`（https://platform.claude.com/docs/en/api/messages） |
| `cache_creation.ephemeral_5m_input_tokens` | `number` | 5 分钟 TTL cache 写入 token 明细。 | `The number of input tokens used to create the 5 minute cache entry.`（https://platform.claude.com/docs/en/api/messages） |
| `output_tokens` | `number` | 输出 token 数；即使空字符串响应也可能非零。 | `output_tokens will be non-zero`（https://platform.claude.com/docs/en/api/messages） |
| `output_tokens_details.thinking_tokens` | `number` | 内部 reasoning token 数，包含 thinking block delimiter；`<= output_tokens`。 | `thinking_tokens: number`（https://platform.claude.com/docs/en/api/messages） |
| `server_tool_use` | `ServerToolUsage` | 服务端工具请求数，例如 web search。 | `The number of server tool requests.`（https://platform.claude.com/docs/en/api/messages） |
| `inference_geo` | `string` | 实际推理地理区域。 | `The geographic region where inference was performed`（https://platform.claude.com/docs/en/api/messages） |

总输入 token 官方公式：`cache_read_input_tokens + cache_creation_input_tokens + input_tokens`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching）。

## 6. 流式 SSE

### 事件顺序

| 顺序 | 事件 | `data.type` | 结构 | 官方依据 |
| --- | --- | --- | --- | --- |
| 1 | `message_start` | `"message_start"` | `{type, message}`；`message.content` 初始为空；`stop_reason:null`。 | `message_start: contains a Message object with empty content`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| 2 | `content_block_start` | `"content_block_start"` | `{type, index, content_block}`；`index` 对应最终 `content` 数组下标。 | `content_block_start... index corresponds`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| 3 | `content_block_delta` | `"content_block_delta"` | `{type, index, delta}`；delta 类型见下表。 | `contains a delta of a type`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| 4 | `content_block_stop` | `"content_block_stop"` | `{type, index}`；结束当前 content block。 | `content_block_stop event`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| 5 | `message_delta` | `"message_delta"` | `{type, delta:{stop_reason, stop_sequence}, usage}`；`usage` token count 为累计。 | `usage field ... are cumulative`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| 6 | `message_stop` | `"message_stop"` | `{type:"message_stop"}`；最终结束事件。 | `A final message_stop event.`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| 任意 | `ping` | `"ping"` | `{type:"ping"}`；可出现任意数量。 | `may also include any number of ping events`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| 任意 | `error` | `"error"` | `{type:"error", error:{type,message}}`；可能在 HTTP 200 后流中出现。 | `error can occur after the API returns a 200 response`（https://platform.claude.com/docs/en/api/errors） |

### `content_block_delta.delta` 类型

| `delta.type` | 字段 | 用途 | 官方依据 |
| --- | --- | --- | --- |
| `text_delta` | `text:string` | 文本增量。 | `text_delta`；`text`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| `input_json_delta` | `partial_json:string` | `tool_use.input` 的增量 JSON 字符串；最终 `tool_use.input` 始终为 object。 | `partial JSON strings`；`final tool_use.input is always an object`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| `thinking_delta` | `thinking:string` | thinking block 的增量内容。 | `thinking content through thinking_delta events`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| `signature_delta` | `signature:string` | thinking block 结束前发送，用于完整性校验。 | `signature_delta event is sent just before`（https://platform.claude.com/docs/en/build-with-claude/streaming） |

### 流式 usage 上报

| 位置 | 字段 | 语义 | 官方依据 |
| --- | --- | --- | --- |
| `message_start.message.usage` | `input_tokens`、`output_tokens`，可包含 cache 字段 | 初始 token 用量；示例中 `output_tokens` 初始为 1 或 2。 | `message_start` 示例含 `usage`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| `message_delta.usage` | `output_tokens` 等 | 累计 token count。 | `token counts ... are cumulative`（https://platform.claude.com/docs/en/build-with-claude/streaming） |
| cache usage | `cache_creation_input_tokens`、`cache_read_input_tokens` 等 | prompt cache 命中/写入也在流式 `message_start` 中可见。 | `within usage in the response (or message_start event if streaming)`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |

## 7. 缓存

### 开启方式与断点

| 机制 | 写法 | 约束/行为 | 官方依据 |
| --- | --- | --- | --- |
| 自动缓存 | 顶层 `cache_control: {"type":"ephemeral", "ttl"?: "5m"|"1h"}` | 自动把断点放在最后一个可缓存块；多轮对话中断点会随请求增长前移。 | `Add a single cache_control field at the top level`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| 显式缓存断点 | 在 content block、tool definition、system block 等可缓存对象上加 `cache_control` | 在标记 block 结尾写入前缀 cache；最多 4 个 cache breakpoints。 | `place cache_control directly on individual content blocks`；`up to 4 cache breakpoints`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| TTL | `ttl:"5m"` 或 `"1h"` | `ephemeral` 是当前唯一 cache type；默认 5 分钟；1 小时 TTL 需显式写 `ttl:"1h"`。 | `ephemeral is the only supported cache type`；`default has a 5-minute lifetime`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| 前缀顺序 | `tools` → `system` → `messages` | cache prefix 覆盖上述顺序直到含 `cache_control` 的 block。 | `tools, system, and messages (in that order)`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| lookback | 每个断点最多回看 20 个 block | 只寻找以前请求已经写入的 cache entry，不自动写入更早位置。 | `The lookback window is 20 blocks.`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| 最小长度 | 依模型：Opus 5/Fable 5/Mythos 5 为 512；Mythos Preview/Opus 4.7 为 2,048；Opus 4.6/4.5 为 4,096；Opus 4.8/Sonnet 5/Sonnet 4.6/Sonnet 4.5 等为 1,024；Haiku 4.5 为 4,096；Haiku 3.5 为 2,048 | 低于阈值即使标记也不会缓存，且不报错；用 usage cache 字段确认。 | `minimum cacheable prompt length`；`Shorter prompts cannot be cached`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| 预热 | `max_tokens: 0` | 只读取 prompt 并写 cache，不生成输出；不能与 `stream:true`、enabled thinking、structured outputs、强制工具 choice 等一起用。 | `Set max_tokens: 0`；`returns immediately without generating`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |

### 可缓存与不可缓存

| 类别 | 是否可缓存 | 官方依据 |
| --- | --- | --- |
| `tools` 数组中的工具定义 | 是 | `Tools: Tool definitions in the tools array`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| `system` 数组中的 content blocks | 是 | `System messages: Content blocks in the system array`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| `messages.content` 文本块 | 是 | `Text messages: Content blocks`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| 用户 turn 中的 images/documents | 是 | `Images & Documents`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| tool use 与 tool results | 是 | `Tool use and tool results`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| thinking blocks | 不能直接加 `cache_control`；但作为历史 assistant turn 与其它内容一起可被缓存 | `Thinking blocks cannot be cached directly`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| citations 等 sub-content blocks | 不能直接缓存；缓存顶层 block | `Sub-content blocks ... cannot be cached directly`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| 空 text blocks | 不可缓存 | `Empty text blocks cannot be cached.`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |

### 命中报告字段

| 字段 | 位置 | 含义 | 官方依据 |
| --- | --- | --- | --- |
| `usage.cache_creation_input_tokens` | 非流式 response 或流式 `message_start` | 写入 cache 的 token。 | `Number of tokens written to the cache`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| `usage.cache_read_input_tokens` | 同上 | 从 cache 读取的 token。 | `Number of tokens retrieved from the cache`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| `usage.input_tokens` | 同上 | 最后一个 cache breakpoint 之后的未缓存输入 token。 | `tokens that come after the last cache breakpoint`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| `usage.cache_creation.ephemeral_5m_input_tokens` | 同上 | 5 分钟 TTL cache 写入明细。 | `ephemeral_5m_input_tokens`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |
| `usage.cache_creation.ephemeral_1h_input_tokens` | 同上 | 1 小时 TTL cache 写入明细。 | `ephemeral_1h_input_tokens`（https://platform.claude.com/docs/en/build-with-claude/prompt-caching） |

## 8. 错误格式

### HTTP 错误码

| HTTP | `error.type` | 说明 | 官方依据 |
| --- | --- | --- | --- |
| 400 | `invalid_request_error` | 请求格式或内容问题；也可能用于未列出的其它 4XX。 | `issue with the format or content`（https://platform.claude.com/docs/en/api/errors） |
| 401 | `authentication_error` | API key 或平台凭证问题。 | `issue with your API key`（https://platform.claude.com/docs/en/api/errors） |
| 402 | `billing_error` | 账单或付款信息问题。 | `billing or payment information`（https://platform.claude.com/docs/en/api/errors） |
| 403 | `permission_error` | key 无权使用指定资源。 | `does not have permission`（https://platform.claude.com/docs/en/api/errors） |
| 404 | `not_found_error` | endpoint 或资源不存在。 | `requested resource was not found`（https://platform.claude.com/docs/en/api/errors） |
| 409 | `conflict_error` | 请求与资源当前状态冲突。 | `conflicts with the current state`（https://platform.claude.com/docs/en/api/errors） |
| 413 | `request_too_large` | 请求超过大小限制；Messages API 最大 32 MB。 | `Request exceeds the maximum allowed`；`Messages API 32 MB`（https://platform.claude.com/docs/en/api/errors） |
| 429 | `rate_limit_error` | 账号达到 rate limit。 | `account has hit a rate limit`（https://platform.claude.com/docs/en/api/errors） |
| 500 | `api_error` | Anthropic 内部意外错误。 | `unexpected error ... internal`（https://platform.claude.com/docs/en/api/errors） |
| 504 | `timeout_error` | 请求处理超时。 | `request timed out while processing`（https://platform.claude.com/docs/en/api/errors） |
| 529 | `overloaded_error` | API 临时过载。 | `temporarily overloaded`（https://platform.claude.com/docs/en/api/errors） |

### 错误响应结构

```json
{
  "type": "error",
  "error": {
    "type": "not_found_error",
    "message": "The requested resource could not be found."
  },
  "request_id": "req_011CSHoEeqs5C35K2UUqR7Fy"
}
```

官方依据：`always returns errors as JSON`；`top-level error object`；`request_id field`（https://platform.claude.com/docs/en/api/errors）。

### 流式错误

```sse
event: error
data: {"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}}
```

官方依据：流式文档示例 `event: error`；`overloaded_error`（https://platform.claude.com/docs/en/build-with-claude/streaming）。
