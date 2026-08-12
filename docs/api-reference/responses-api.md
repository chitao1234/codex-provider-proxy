# OpenAI Responses API 参考

> 核对日期：2026-08-12。  
> 说明：OpenAI `platform.openai.com` API reference 页面在本环境返回 403，因此本文把这些页面标为“无法访问”，并以可访问的官方 `developers.openai.com` API reference、官方 OpenAPI schema 与官方 guides 为依据。每项给出官方 URL 与短摘录；协议含义用中文归纳。

## 官方来源与访问状态

| URL | 状态 | 用途 | 官方短摘录 |
| --- | --- | --- | --- |
| https://platform.openai.com/docs/api-reference/responses/create | 无法访问：HTTP 403 Cloudflare challenge（2026-08-12 实测） | 用户指定的 platform API reference | 无法访问 |
| https://developers.openai.com/api/reference/resources/responses/methods/create | 可访问；MCP 搜索与本地 `.md`/OpenAPI 已核对 | Create Response API reference | `Create a model response`；`POST /v1/responses` |
| https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml | 可访问 | 官方 OpenAPI schema | `/responses:`；`$ref: "#/components/schemas/CreateResponse"` |
| https://developers.openai.com/api/reference/overview | 可访问 | 认证、headers、request id | `HTTP Bearer authentication` |
| https://developers.openai.com/api/docs/guides/streaming-responses | 可访问 | HTTP SSE streaming | `HTTP streaming (stream=true) over server-sent events` |
| https://developers.openai.com/api/docs/guides/prompt-caching | 可访问 | Prompt caching | `works automatically for eligible requests` |
| https://developers.openai.com/api/docs/guides/function-calling | 可访问 | Function/custom tools | `Function calls (custom tools)` |
| https://developers.openai.com/api/docs/guides/error-codes | 可访问 | 错误码与 SDK 错误类型 | `API errors` |

## 1. 端点 URL 与认证方式

| 项 | 值 | 官方依据 |
| --- | --- | --- |
| HTTP 方法与路径 | `POST https://api.openai.com/v1/responses` | OpenAPI：`/responses:` + `post:`；示例 `curl https://api.openai.com/v1/responses`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Content-Type | `Content-Type: application/json` | OpenAPI 示例：`-H "Content-Type: application/json"`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| 认证 | Header `Authorization: Bearer $OPENAI_API_KEY` 或 access token | API overview：`Provide API credentials with HTTP Bearer authentication`；`Authorization: Bearer OPENAI_API_KEY_OR_ACCESS_TOKEN`（https://developers.openai.com/api/reference/overview） |
| 成功响应媒体类型 | `application/json` 或 `text/event-stream` | OpenAPI：`application/json` schema `Response`；`text/event-stream` schema `ResponseStreamEvent`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 2. 请求格式

### 顶层参数

| 参数 | 类型 | 必填 | 默认值 | 约束/说明 | 官方依据 |
| --- | --- | --- | --- | --- | --- |
| `model` | `ModelIdsResponses` | 否（schema 未列 required） | 无 | 用于生成响应的模型 ID，例如 `gpt-4o` 或 `o3`。 | `Model ID used to generate the response`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `input` | `string` 或 `array<InputItem>` | 否（schema 未列 required） | 无 | 文本、图片、文件输入；字符串等价于 `user` role 的文本输入。 | `Text, image, or file inputs to the model`；`equivalent to a text input with the user role`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `instructions` | `string` 或 `array<InputItem>` 或 `null` | 否 | `null` | 插入模型上下文的 system/developer 消息；与 `previous_response_id` 一起使用时，前一响应的 instructions 不自动带到下一响应。 | `A system (or developer) message inserted`；`will not be carried over`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `stream` | `boolean` 或 `null` | 否 | 未注明 | 为 `true` 时用 SSE 流式返回。 | `If set to true... streamed to the client`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `stream_options` | `ResponseStreamOptions` 或 `null` | 否 | `null` | 仅在 `stream:true` 时设置；当前子字段 `include_obfuscation?: boolean`。 | `Only set this when you set stream: true`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `background` | `boolean` 或 `null` | 否 | 未注明 | 后台运行 model response。 | `Whether to run the model response in the background`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `conversation` | `ConversationParam` 或 `null` | 否 | `null` | 指定 response 所属 conversation；该 conversation 的 items 会前置到本次 input，完成后本次 input/output 会自动加入 conversation。不能与 `previous_response_id` 同用。 | `The conversation that this response belongs to`；`Cannot be used in conjunction with previous_response_id`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `previous_response_id` | `string` 或 `null` | 否 | `null` | 前一 response ID，用于多轮会话；不能与 `conversation` 同用。 | `The unique ID of the previous response`；`Cannot be used in conjunction with conversation`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `context_management` | `array<ContextManagementParam>` 或 `null` | 否 | `null` | 本次请求的 context management 配置。 | `Context management configuration for this request`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `include` | `array<IncludeEnum>` 或 `null` | 否 | `null` | 指定额外输出数据。枚举见下表。 | `Specify additional output data to include`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `max_output_tokens` | `integer` 或 `null` | 否 | `null` | 响应可生成 token 上限，包含可见输出与 reasoning tokens。 | `including visible output tokens and reasoning tokens`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `max_tool_calls` | `integer` 或 `null` | 否 | `null` | 内置工具调用总数上限，跨所有 built-in tool 计数，不是每个工具单独上限。 | `maximum number of total calls to built-in tools`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `metadata` | `object` | 否 | `{}` 或未注明 | 最多 16 个 key-value；key 最大 64 字符，value 最大 512 字符。 | `Set of 16 key-value pairs`；`Keys...64`；`Values...512`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `moderation` | `ModerationParam` 或 `null` | 否 | `null` | 对输入和输出运行 moderation 的配置。 | `Configuration for running moderation`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `parallel_tool_calls` | `boolean` 或 `null` | 否 | 响应对象默认 `true` | 是否允许模型并行运行 tool calls。 | `Whether to allow the model to run tool calls in parallel`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `prompt` | `Prompt` | 否 | 无 | prompt template 与 variables 的引用。 | `Reference to a prompt template and its variables`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `prompt_cache_key` | `string` 或 `null` | 否 | `null` | OpenAI 用于给相似请求分桶以优化 cache hit；替代 `user` 的缓存用途。GPT-5.6+ 更可靠匹配要求设置该 key。 | `Used by OpenAI to cache responses`；`must set prompt_cache_key`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| `prompt_cache_options` | `{ttl?, mode?}` | 否 | `mode:"implicit"`；`ttl:"30m"` | GPT-5.6+ 支持；可控制 implicit/explicit 断点。`ttl` 当前唯一支持 `30m`。 | `Supported for gpt-5.6 and later`；`ttl defaults to 30m`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `prompt_cache_retention` | `"in_memory"` 或 `"24h"` 或 `null` | 否 | 依模型/组织 ZDR | 已弃用；使用 `prompt_cache_options.ttl`。旧模型可用于最大保留策略。 | `Deprecated. Use prompt_cache_options.ttl instead.`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `reasoning` | `Reasoning` 或 `null` | 否 | `null` | GPT-5 和 o-series reasoning 模型配置；字段见下表。 | `gpt-5 and o-series models only`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `safety_identifier` | `string` 或 `null` | 否 | `null` | 用于帮助检测违反使用政策的 end-user 稳定标识，最大 64 字符；官方建议 hash 用户名/邮箱。 | `maximum length of 64 characters`；`recommend hashing`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `service_tier` | `"auto"|"default"|"flex"|"scale"|"priority"|"fast"` 或 `null` | 否 | `auto` | 请求服务层；响应会显示实际使用层级，可能不同于请求值。 | `Specifies the processing type`；`default behavior is 'auto'`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `store` | `boolean` 或 `null` | 否 | 示例响应为 `true` | 是否存储生成的 model response 以便后续 API 检索。 | `Whether to store the generated model response`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `temperature` | `number` 或 `null` | 否 | 未注明；示例响应 `1.0` | 采样温度，范围 `0` 到 `2`；官方建议改 `temperature` 或 `top_p` 其中一个。 | `between 0 and 2`；`altering this or top_p but not both`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `text` | `ResponseTextParam` | 否 | 默认 plain text | 文本响应配置，可为 plain text 或 structured JSON。 | `Configuration options for a text response`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `tool_choice` | `ToolChoiceParam` | 否 | 示例为 `"auto"` | 控制模型选择哪些工具。形状见工具章节。 | `How the model should select which tool`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `tools` | `array<Tool>` | 否 | `[]` | 模型可调用工具数组；包括 built-in tools、MCP tools、function/custom tools。 | `We support the following categories of tools`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `top_logprobs` | `integer` | 否 | 无 | 0 到 20，返回每个 token 位置最可能 tokens 与 log probability；返回数量可能少于请求。 | `An integer between 0 and 20`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `top_p` | `number` 或 `null` | 否 | 示例响应 `1.0` | nucleus sampling；官方建议不要和 `temperature` 同时调整。 | `An alternative to sampling with temperature`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `truncation` | `"auto"` 或 `"disabled"` 或 `null` | 否 | `disabled` | `auto` 超过上下文窗口时从 conversation 开头丢 items；`disabled` 超限返回 400。 | `disabled (default)`；`request will fail with a 400 error`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `user` | `string` | 否 | 无 | 已被 `safety_identifier` 和 `prompt_cache_key` 替代。 | `This field is being replaced`（https://developers.openai.com/api/reference/resources/responses/methods/create） |

### `include` 枚举

| 值 | 含义 | 官方依据 |
| --- | --- | --- |
| `file_search_call.results` | 包含 file search tool call 结果。 | `Include the search results`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `web_search_call.results` | 包含 web search tool call 结果。 | `web_search_call.results`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `web_search_call.action.sources` | 包含 web search action sources。 | `Include the sources`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `message.input_image.image_url` | 包含 input message 的 image URL。 | `Include image urls from the input message`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `computer_call_output.output.image_url` | 包含 computer call output 的 image URL。 | `computer_call_output.output.image_url`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `code_interpreter_call.outputs` | 包含 code interpreter tool call outputs。 | `Includes the outputs of python code execution`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `reasoning.encrypted_content` | 包含加密 reasoning tokens，以便 stateless 多轮续用 reasoning items。 | `encrypted version of reasoning tokens`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `message.output_text.logprobs` | assistant messages 带 output text logprobs。 | `Include logprobs with assistant messages`（https://developers.openai.com/api/reference/resources/responses/methods/create） |

### `reasoning` 参数

| 字段 | 类型 | 默认值 | 说明 | 官方依据 |
| --- | --- | --- | --- | --- |
| `effort` | `"minimal"|"low"|"medium"|"high"|"xhigh"|"max"` 等，或 `null` | 未注明 | 限制 reasoning effort；支持值依模型。 | `Configuration options for reasoning models`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `summary` | `"auto"|"concise"|"detailed"` 或 `null` | 未注明 | reasoning summary 配置。 | `summary` enum（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `context` | `"auto"|"current_turn"|"all_turns"` 或 `null` | 未注明 | reasoning context 范围。 | `context` enum（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `mode` | `ReasoningModeEnum` | 未注明 | reasoning 执行模式；响应中为实际生效值。 | `effective execution mode`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `generate_summary` | `"auto"|"concise"|"detailed"` 或 `null` | 未注明 | 旧/兼容 summary 生成字段。 | `generate_summary`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### `text` 参数

| 字段 | 类型 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| `format` | `{"type":"text"}`、`{"type":"json_schema", ...}` 或 `{"type":"json_object"}` | plain text 或 structured JSON 输出；`json_schema` 要求 `name`、`schema`，可带 `description`、`strict`。 | `Can be plain text or structured JSON data`；`json_schema`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `verbosity` | `"low"|"medium"|"high"` 或 `null` | 控制回复详略，默认 `medium`。 | `default is medium`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 3. 输入格式

### `input`

| 形状 | 说明 | 官方依据 |
| --- | --- | --- |
| `string` | 文本输入，等价于 `user` role 文本 input。 | `A text input... equivalent... user role`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `array<InputItem>` | 一个或多个 input items，可含消息、已返回 item、tool output、item reference 等。 | `A list of one or many input items`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### 消息 item

| 对象 | 字段 | 角色枚举/约束 | 官方依据 |
| --- | --- | --- | --- |
| `EasyInputMessage` | `type?:"message"`；`role`；`content`；`phase?` | `role:"user"|"assistant"|"system"|"developer"`；`content` 可为字符串或 content list。 | `role... user, assistant, system, or developer`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `InputMessage` | `type?:"message"`；`role`；`status?`；`content` | `role:"user"|"system"|"developer"`；status 为 `in_progress`、`completed`、`incomplete`。 | `role... user, system, or developer`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### 输入 content block

| `type` | 字段 | 必填 | 约束/说明 | 官方依据 |
| --- | --- | --- | --- | --- |
| `input_text` | `type`；`text:string`；`prompt_cache_breakpoint?` | `type`,`text` | 文本输入块。 | `The type... Always input_text`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `input_image` | `type`；`image_url?`；`file_id?`；`detail`；`prompt_cache_breakpoint?` | `type`,`detail` | 图片输入；`detail:"low"|"high"|"auto"|"original"`，默认 `auto`。 | `One of high, low, auto, or original`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `input_file` | `type`；`file_id?`；`filename?`；`file_data?`；`file_url?`；`detail?`；`prompt_cache_breakpoint?` | `type` | 文件输入；`detail` 可为 `auto`/`low`/`high` 等，GPT-5.6+ 的 `auto` 使用高质量渲染。 | `The type... Always input_file`；`auto uses high-quality rendering`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### 工具输出作为输入 item

| `type` | 字段 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| `function_call_output` | `call_id?`；`output`；`name?`；`namespace?`；`status?` | 把开发者代码执行的 function output 回传模型；`output` 可为字符串或 text/image/file output 内容数组。 | `The output of a function tool call`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `custom_tool_call_output` | `call_id`；`output` | 把 custom tool output 回传模型。 | `output of a custom tool call from your code`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `item_reference` | item id 引用 | 引用之前 conversation/response 中的 item。 | `ItemReferenceParam`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 4. 工具定义格式

### 工具类别

官方 create reference 将工具分为三类：`Built-in tools`、`MCP Tools`、`Function calls (custom tools)`（https://developers.openai.com/api/reference/resources/responses/methods/create）。

| 工具 | 请求形状 | 必填字段 | 可选字段/约束 | 官方依据 |
| --- | --- | --- | --- | --- |
| Function tool | `{type:"function", name, description?, parameters?, output_schema?, strict?, defer_loading?, allowed_callers?}` | `type`、`name`；响应 schema 中 `parameters`、`strict` 为 required，但 request param schema 中它们为 optional | `parameters` 是 JSON Schema 参数；`strict` 控制 schema 严格性；可 `defer_loading` 和限制 `allowed_callers`。 | `Defines a function in your own code`；`type... Always function`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Custom tool | `{type:"custom", name, description?, format?, defer_loading?, allowed_callers?}` | `type`、`name` | `format` 可为 unconstrained text 或 grammar format；用于自定义输入格式。 | `A custom tool that processes input`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Web search | `{type:"web_search"|"web_search_2025_08_26", filters?, user_location?, search_context_size?}` | `type` | `search_context_size:"low"|"medium"|"high"`，默认 `medium`；filters 可含 allowed domains。 | `Search the Internet for sources`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| File search | `{type:"file_search", vector_store_ids, max_num_results?, ranking_options?, filters?}` | `type`、`vector_store_ids` | `max_num_results` 1 到 50。 | `searches for relevant content from uploaded files`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Computer use preview | `{type:"computer_use_preview", environment, display_width, display_height}` | 全部 | 控制虚拟 computer。 | `controls a virtual computer`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Code interpreter | `{type:"code_interpreter", container, allowed_callers?}` | `type`、`container` | `container` 可为 container id 或带 uploaded file ids / memory limit 的对象。 | `runs Python code`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| MCP tool | `{type:"mcp", server_label, server_url?|connector_id?|tunnel_id?, authorization?, server_description?, headers?, allowed_tools?, require_approval?, defer_loading?, allowed_callers?}` | `type`、`server_label` | `server_url`、`connector_id`、`tunnel_id` 三者之一必须提供；connector ids 包含 Dropbox/Gmail/Google Drive/Microsoft Teams/Outlook/SharePoint 等。 | `Give the model access... via remote Model Context Protocol`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Image generation | 官方 OpenAPI stream/output item 中包含 `image_generation_call.*` 事件与 item 类型 | 依具体 schema | 用于生成图像；事件见流式章节。 | `response.image_generation_call.completed`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### `tool_choice`

| 形状 | 字段 | 行为 | 官方依据 |
| --- | --- | --- | --- |
| 字符串 | `"none"`、`"auto"`、`"required"` | `none` 不调用工具；`auto` 可生成消息或工具调用；`required` 必须调用一个或多个工具。 | `none means...`; `auto means...`; `required means...`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `allowed_tools` | `{type:"allowed_tools", mode:"auto"|"required", tools:[...]}` | 限制模型只能在预定义工具集合中选择。 | `Constrains the tools available`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Function choice | `{type:"function", name}` | 强制调用指定 function。 | `force the model to call a specific function`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Custom choice | `{type:"custom", name}` | 强制调用指定 custom tool。 | `force the model to call a specific custom tool`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| MCP choice | `{type:"mcp", server_label, name?}` | 强制调用指定 MCP server 上的 tool。 | `force the model to call a specific tool on a remote MCP server`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| 专用 programmatic choices | `SpecificProgrammaticToolCallingParam`、`SpecificApplyPatchParam`、`SpecificFunctionShellParam` | 官方 OpenAPI union 列出，create reference 未展开；转换实现应保留透传能力。 | `ToolChoiceParam oneOf ... SpecificApplyPatchParam`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 5. 非流式响应格式

### `Response` 对象

| 字段 | 类型 | required（schema） | 说明 | 官方依据 |
| --- | --- | --- | --- | --- |
| `id` | `string` | 是 | response 唯一 ID。 | `Unique identifier for this Response.`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `object` | `"response"` | 是 | 对象类型固定为 `response`。 | `always set to response`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `status` | `"completed"|"failed"|"in_progress"|"cancelled"|"queued"|"incomplete"` | 否 | response generation 状态。 | `One of completed, failed...`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `created_at` | Unix seconds number | 是 | 创建时间。 | `Unix timestamp... when this Response was created`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `completed_at` | Unix seconds number 或 `null` | 否 | 仅 completed 时出现。 | `Only present when the status is completed`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `error` | `ResponseError` 或 `null` | 是 | model 未能生成 response 时的错误对象。 | `error: ResponseError`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `incomplete_details` | `{reason}` 或 `null` | 是 | incomplete 原因；`reason:"max_output_tokens"|"content_filter"`。 | `Details about why the response is incomplete`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `instructions` | `string|array<InputItem>|null` | 是 | 生效的 system/developer instructions。 | `A system (or developer) message inserted`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `model` | model id | 是 | 生成所用模型。 | `model: ModelIdsResponses`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `output` | `array<OutputItem>` | 是 | 模型生成的内容 item 数组；长度和顺序依模型响应而定。 | `length and order... dependent`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `output_text` | `string|null` | 否 | SDK-only convenience property，聚合所有 `output_text`。 | `SDK-only convenience property`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `parallel_tool_calls` | `boolean` | 是 | 是否允许并行 tool calls；默认 `true`。 | `Whether to allow... parallel`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `previous_response_id` | `string|null` | 否 | 前一 response id。 | `previous_response_id`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `reasoning` | `Reasoning|null` | 否 | reasoning 配置/生效信息。 | `reasoning`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `store` | `boolean|null` | 否 | 是否存储。 | `store: true` 示例（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `temperature` | `number|null` | 是 | 实际 temperature。 | `temperature`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `text` | `ResponseTextParam` | 否 | 文本输出配置。 | `text: { format: { type: text } }` 示例（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `tool_choice` | `ToolChoiceParam` | 是 | 实际 tool choice。 | `tool_choice: auto` 示例（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `tools` | `array<Tool>` | 是 | 本次可用工具。 | `tools: []` 示例（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `top_p` | `number|null` | 是 | 实际 top_p。 | `top_p: 1` 示例（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `truncation` | `"auto"|"disabled"|null` | 否 | 实际 truncation 策略。 | `truncation: disabled` 示例（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `usage` | `ResponseUsage` | 否 | token 用量。 | `usage: ResponseUsage`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `metadata` | `object` | 是 | metadata。 | `metadata: {}` 示例（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `service_tier` | service tier 或 `null` | 否 | 实际服务层。 | `response body will include the service_tier value`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| `prompt_cache_options` | `PromptCacheOptions` | 否 | 实际应用的 prompt cache options。 | `prompt-caching options that were applied`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `moderation` | `Moderation|null` | 否 | moderation 结果。 | `Moderation results`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `conversation` | `ResponseConversation|null` | 否 | 关联 conversation。 | `conversation`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `max_output_tokens` | `integer|null` | 否 | 生成上限。 | `max_output_tokens`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### `output` item 类型

| 类型 | 关键字段 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| `message` | `id`、`type:"message"`、`role:"assistant"`、`content`、`status` | assistant 输出消息；content 是输出 content 数组。 | `An output message from the model.`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `output_text` content | `type:"output_text"`、`text`、`annotations`、`logprobs` | 文本输出块。 | `A text output from the model`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `refusal` content | `type:"refusal"`、`refusal` | 拒绝内容。 | `RefusalContent`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `reasoning` item | `id`、`type:"reasoning"`、`summary`、`encrypted_content?`、`content?`、`status?` | reasoning 模型输出的 reasoning 描述/摘要。 | `A description of the chain of thought`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `function_call` | `id?`、`call_id`、`name`、`arguments`、`status?` | 要运行的 function call；`arguments` 为 JSON 字符串。 | `A tool call to run a function`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `function_call_output` | `call_id?`、`output`、`status?` | function output item。 | `The output of a function tool call`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `custom_tool_call` | `call_id`、`name`、`input` | custom tool call；`input` 为字符串。 | `A call to a custom tool`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `custom_tool_call_output` | `call_id`、`output` | custom tool output。 | `custom tool call output`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| 内置工具 call items | `file_search_call`、`web_search_call`、`computer_call`、`code_interpreter_call`、`image_generation_call` 等 | OpenAPI `OutputItem` union 包含这些工具调用与输出资源。 | `OutputItem oneOf ... FileSearchToolCall ... CodeInterpreterToolCall ... ImageGenToolCall`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| MCP items | `mcp_call`、`mcp_list_tools`、approval request/response 等 | MCP 工具调用、列工具和审批 item。 | `MCPToolCall`；`MCPListTools`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### `usage`

| 字段 | 类型 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| `input_tokens` | `integer` | 输入 token 数。 | `The number of input tokens.`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `input_tokens_details.cached_tokens` | `integer` | 从 cache 读取的 token 数。 | `retrieved from the cache`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `input_tokens_details.cache_write_tokens` | `integer` | 写入 cache 的输入 token 数。 | `written to the cache`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `output_tokens` | `integer` | 输出 token 数。 | `The number of output tokens.`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `output_tokens_details.reasoning_tokens` | `integer` | reasoning token 数。 | `The number of reasoning tokens.`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `total_tokens` | `integer` | 总 token 数。 | `The total number of tokens used.`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 6. 流式 SSE

### 基本机制

| 项 | 说明 | 官方依据 |
| --- | --- | --- |
| 开启 | 请求 `stream:true`。 | `set stream=True in your request`（https://developers.openai.com/api/docs/guides/streaming-responses） |
| 传输 | HTTP SSE。 | `HTTP streaming (stream=true) over server-sent events`（https://developers.openai.com/api/docs/guides/streaming-responses） |
| 事件模型 | Responses API 使用语义事件，每个事件有预定义 schema 和 `type`。 | `uses semantic events`；`Each event is typed`（https://developers.openai.com/api/docs/guides/streaming-responses） |
| 常见文本事件 | `response.created`、`response.output_text.delta`、`response.completed`、`error`。 | `Common events...`（https://developers.openai.com/api/docs/guides/streaming-responses） |
| 结束事件 | 成功为 `response.completed`；失败/不完整为 `response.failed` 或 `response.incomplete`。OpenAPI 未使用 Chat Completions 的 `[DONE]` sentinel。 | `response.completed`；`response.failed`；`response.incomplete`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| usage 上报 | 最终 `response.completed.response.usage` 带完整 `ResponseUsage`；早期生命周期事件中的 `response.usage` 可为 `null`。 | `response.completed` has `response`；Response 示例含 `usage`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### 生命周期与内容事件

| `type` | 字段 | 含义 | 官方依据 |
| --- | --- | --- | --- |
| `response.created` | `response`, `sequence_number` | response 创建。 | `emitted when a response is created`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.queued` | `response`, `sequence_number` | 排队等待处理。 | `queued and waiting to be processed`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.in_progress` | `response`, `sequence_number` | response 处理中。 | `response is in progress`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.completed` | `response`, `sequence_number` | 完成，`response` 中包含最终 output/usage。 | `model response is complete`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.failed` | `response`, `sequence_number` | 失败。 | `response fails`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.incomplete` | `response`, `sequence_number` | 以 incomplete 状态结束。 | `finishes as incomplete`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.output_item.added` | `output_index`, `item`, `sequence_number` | 新 output item 添加。 | `new output item is added`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.output_item.done` | `output_index`, `item`, `sequence_number` | output item 完成。 | `output item is marked done`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.content_part.added` | `item_id`, `output_index`, `content_index`, `part`, `sequence_number` | 新 content part 添加。 | `new content part is added`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.content_part.done` | `item_id`, `output_index`, `content_index`, `part`, `sequence_number` | content part 完成。 | `content part is done`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.output_text.delta` | `item_id`, `output_index`, `content_index`, `delta`, `sequence_number`, `logprobs` | 文本增量。 | `additional text delta`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.output_text.done` | `item_id`, `output_index`, `content_index`, `text`, `sequence_number`, `logprobs` | 文本完成。 | `text content is finalized`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.output_text.annotation.added` | `item_id`, `output_index`, `content_index`, `annotation_index`, `annotation`, `sequence_number` | 文本 annotation 添加。 | `annotation is added`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.refusal.delta` | `item_id`, `output_index`, `content_index`, `delta`, `sequence_number` | refusal 文本增量。 | `partial refusal text`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response.refusal.done` | `item_id`, `output_index`, `content_index`, `refusal`, `sequence_number` | refusal 文本完成。 | `refusal text is finalized`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### 工具、reasoning、audio、MCP 事件

| 类别 | `type` 列表 | 通用字段/说明 | 官方依据 |
| --- | --- | --- | --- |
| Function call args | `response.function_call_arguments.delta`；`response.function_call_arguments.done` | `item_id`、`output_index`、`sequence_number`；delta/done 分别带 `delta` 或最终 `arguments`。 | `partial function-call arguments delta`；`arguments are finalized`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Custom tool input | `response.custom_tool_call_input.delta`；`response.custom_tool_call_input.done` | `output_index`、`item_id`、`delta` 或最终 `input`。 | `partial update to the input`；`input... complete`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Web search | `response.web_search_call.in_progress`；`searching`；`completed` | `output_index`、`item_id`、`sequence_number`。 | `web search call is initiated/executing/completed`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| File search | `response.file_search_call.in_progress`；`searching`；`completed` | `output_index`、`item_id`、`sequence_number`。 | `file search call is initiated`；`currently searching`；`completed`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Code interpreter | `response.code_interpreter_call.in_progress`；`interpreting`；`completed`；`response.code_interpreter_call_code.delta`；`done` | call 状态事件带 `output_index/item_id/sequence_number`；code delta/done 带 `delta` 或最终 `code`。 | `code interpreter call is in progress`；`partial code snippet`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Image generation | `response.image_generation_call.in_progress`；`generating`；`partial_image`；`completed` | partial image 事件带 `partial_image_index` 与 `partial_image_b64`。 | `partial image is available`；`final image is available`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| MCP call | `response.mcp_call.in_progress`；`completed`；`failed`；`response.mcp_call_arguments.delta`；`done` | MCP call 状态与参数增量/最终参数。 | `MCP tool call`；`arguments for an MCP tool call are finalized`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| MCP list tools | `response.mcp_list_tools.in_progress`；`completed`；`failed` | 列出 MCP server 可用工具的状态事件。 | `retrieving the list of available MCP tools`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Reasoning summary | `response.reasoning_summary_part.added`；`done`；`response.reasoning_summary_text.delta`；`done` | reasoning summary part/text 增量与完成事件。 | `reasoning summary`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Reasoning text | `response.reasoning_text.delta`；`done` | reasoning text 增量与完成事件。 | `delta is added to a reasoning text`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Audio | `response.audio.delta`；`done`；`response.audio.transcript.delta`；`done` | audio delta 为 base64 音频 bytes；transcript delta 为文本片段。 | `partial audio response`；`partial transcript`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Error | `error` | `code`、`message`、`param`、`sequence_number`。 | `Emitted when an error occurs.`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 7. 缓存

### 开启方式、断点、TTL

| 项 | Responses API 行为 | 官方依据 |
| --- | --- | --- |
| 自动缓存 | eligible requests 自动启用，无需代码改动；近期模型 `gpt-4o` and newer 启用。 | `works automatically for eligible requests`；`gpt-4o and newer`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| GPT-5.6+ implicit breakpoint | 默认在 latest user 或 tool message 放 implicit breakpoint。 | `places an implicit breakpoint at the latest user or tool message`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| 显式断点 | 在 supported content block 上加 `prompt_cache_breakpoint: {"mode":"explicit"}`。 | `Add prompt_cache_breakpoint`；`mode: explicit`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| Responses 支持断点的 block | `input_text`、`input_image`、`input_file`。 | `Responses API supports breakpoints on input_text, input_image, and input_file`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| `prompt_cache_key` | 对共享长前缀的请求使用相同 key；GPT-5.6+ 更可靠匹配必须设置。 | `Set prompt_cache_key`；`must set prompt_cache_key`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| `prompt_cache_options.mode` | `implicit` 默认；`explicit` 禁用 implicit，只用显式断点。 | `implicit is the default`；`explicit disables`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| `prompt_cache_options.ttl` | GPT-5.6+ 断点最小生命周期；唯一支持值 `30m`，默认 `30m`；后端可保留更久。 | `only supported value is 30m`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| 每请求写入数 | 每个请求最多创建 4 个新的 cache writes；implicit 模式下 implicit breakpoint 用一个写槽。 | `up to four new cache writes`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| 读取回看数量 | 官方 guide 写“latest 50 breakpoints”；create reference 写“latest 80 breakpoints”。这是官方文档差异，本文不做推断合并。 | `latest 50 breakpoints`（https://developers.openai.com/api/docs/guides/prompt-caching）；`latest 80 breakpoints`（https://developers.openai.com/api/reference/resources/responses/methods/create） |
| 最小 cacheable prefix | GPT-5.6+ 至少 1,024 tokens；GPT-5.5 及更早可在 1,024 到 2,048 间变化。 | `at least 1,024 tokens`；`range from 1,024 to 2,048`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| 旧 `prompt_cache_retention` | GPT-5.6+ 用 `prompt_cache_options.ttl`；旧模型可用 `in_memory` 或 `24h`，其中 `24h` 最大保留 24 小时。 | `Deprecated`；`up to a maximum of 24 hours`（https://developers.openai.com/api/docs/guides/prompt-caching） |

### 命中报告字段

| 字段 | 位置 | 含义 | 官方依据 |
| --- | --- | --- | --- |
| `usage.input_tokens_details.cached_tokens` | `Response.usage` | 从 cache 读取的 token。 | `read from cache`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| `usage.input_tokens_details.cache_write_tokens` | `Response.usage` | 写入 cache 的 token；GPT-5.6+ cache write 计费使用此值。 | `reports the number of prompt tokens written to cache`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| `usage.input_tokens` | `Response.usage` | 输入 token 总数。 | `The number of input tokens`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 8. 错误格式

### HTTP/API 错误码

| HTTP/类别 | 代码/类型 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| 401 | Invalid Authentication | 认证凭证无效；需确认 API key 与组织。 | `Invalid Authentication`（https://developers.openai.com/api/docs/guides/error-codes） |
| 401 | Incorrect API key provided | 请求 API key 不正确。 | `Incorrect API key provided`（https://developers.openai.com/api/docs/guides/error-codes） |
| 401 | Organization membership | 账号不属于组织。 | `must be a member of an organization`（https://developers.openai.com/api/docs/guides/error-codes） |
| 401 | IP not authorized | 请求 IP 不在 allowlist。 | `IP not authorized`（https://developers.openai.com/api/docs/guides/error-codes） |
| 403 | Country/region unsupported | 从不支持国家/地区访问。 | `Country, region, or territory not supported`（https://developers.openai.com/api/docs/guides/error-codes） |
| 429 | `credit_balance_exhausted` | 预付 credit 用尽。 | `credit_balance_exhausted`（https://developers.openai.com/api/docs/guides/error-codes） |
| 429 | Rate limit reached | 请求过快；遵循 `Retry-After`。 | `Rate limit reached for requests`（https://developers.openai.com/api/docs/guides/error-codes） |
| 429 | `organization_spend_limit_exceeded` | 组织 spend limit 达到。 | `organization_spend_limit_exceeded`（https://developers.openai.com/api/docs/guides/error-codes） |
| 429 | `project_spend_limit_exceeded` | 项目 spend limit 达到。 | `project_spend_limit_exceeded`（https://developers.openai.com/api/docs/guides/error-codes） |
| 429 | `organization_usage_limit_exceeded` | OpenAI 分配的组织 usage limit 达到。 | `organization_usage_limit_exceeded`（https://developers.openai.com/api/docs/guides/error-codes） |
| 500 | server error | OpenAI server 处理错误；重试。 | `server had an error`（https://developers.openai.com/api/docs/guides/error-codes） |
| 503 | overloaded | 服务器高流量过载。 | `engine is currently overloaded`（https://developers.openai.com/api/docs/guides/error-codes） |
| 503 | Slow Down | 突然请求速率增长影响服务稳定性。 | `Slow Down`（https://developers.openai.com/api/docs/guides/error-codes） |

### 标准错误响应结构

官方 OpenAPI `Error` schema 为顶层 `error` 对象，常用字段为 `message`、`type`、`param`、`code`。官方错误 guide 还要求对 billing/quota 相关错误检查 `error.code`。依据：`inspect error.code`（https://developers.openai.com/api/docs/guides/error-codes）与 `#/components/schemas/Error`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml）。

```json
{
  "error": {
    "message": "string",
    "type": "string",
    "param": "string or null",
    "code": "string or null"
  }
}
```

### Response 对象内错误

`Response.error` 是 `ResponseError|null`；对象字段为 `code` 与 `message`。官方摘录：`An error object returned when the model fails to generate a Response.`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml）。

```json
{
  "id": "resp_...",
  "object": "response",
  "status": "failed",
  "error": {
    "code": "server_error",
    "message": "..."
  }
}
```

### 流式错误事件

| 事件 | 字段 | 官方依据 |
| --- | --- | --- |
| `error` | `code`、`message`、`param`、`sequence_number` | `Emitted when an error occurs.`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
