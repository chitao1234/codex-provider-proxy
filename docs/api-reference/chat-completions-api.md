# OpenAI Chat Completions API 参考

> 核对日期：2026-08-12。  
> 说明：OpenAI `platform.openai.com` API reference 页面在本环境返回 403，因此本文把该页面标为“无法访问”，并以可访问的官方 `developers.openai.com` API reference 索引、官方 OpenAPI schema 与官方 guides 为依据。每项给出官方 URL 与短摘录；协议含义用中文归纳。

## 官方来源与访问状态

| URL | 状态 | 用途 | 官方短摘录 |
| --- | --- | --- | --- |
| https://platform.openai.com/docs/api-reference/chat/create | 无法访问：HTTP 403 Cloudflare challenge（2026-08-12 实测） | 用户指定的 platform API reference | 无法访问 |
| https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create | 可访问；MCP 搜索命中，页面 fetch `.md` 不稳定；用官方 OpenAPI 核对 schema | Create chat completion API reference | `Create chat completion`；`Body Parameters` |
| https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml | 可访问 | 官方 OpenAPI schema | `/chat/completions:`；`createChatCompletion` |
| https://developers.openai.com/api/reference/overview | 可访问 | 认证、headers、request id | `HTTP Bearer authentication` |
| https://developers.openai.com/api/docs/guides/streaming-responses | 可访问 | SSE streaming 总说明 | `server-sent events (SSE)` |
| https://developers.openai.com/api/docs/guides/prompt-caching | 可访问 | Prompt caching | `Chat Completions API supports` |
| https://developers.openai.com/api/docs/guides/function-calling | 可访问 | Function/custom tools | `function tools`；`custom tools` |
| https://developers.openai.com/api/docs/guides/error-codes | 可访问 | 错误码与 SDK 错误类型 | `API errors` |

## 1. 端点 URL 与认证方式

| 项 | 值 | 官方依据 |
| --- | --- | --- |
| HTTP 方法与路径 | `POST https://api.openai.com/v1/chat/completions` | OpenAPI：`/chat/completions:` + `post:`；示例 `curl https://api.openai.com/v1/chat/completions`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Content-Type | `Content-Type: application/json` | OpenAPI 示例：`-H "Content-Type: application/json"`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| 认证 | Header `Authorization: Bearer $OPENAI_API_KEY` 或 access token | API overview：`Provide API credentials with HTTP Bearer authentication`；`Authorization: Bearer OPENAI_API_KEY_OR_ACCESS_TOKEN`（https://developers.openai.com/api/reference/overview） |
| 成功响应媒体类型 | `application/json` 或 `text/event-stream` | OpenAPI：`application/json` schema `CreateChatCompletionResponse`；`text/event-stream` schema `CreateChatCompletionStreamResponse`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 2. 请求格式

### 顶层参数

| 参数 | 类型 | 必填 | 默认值 | 约束/说明 | 官方依据 |
| --- | --- | --- | --- | --- | --- |
| `model` | `ModelIdsShared` | 是 | 无 | 用于生成响应的模型 ID，例如 `gpt-4o` 或 `o3`。 | `Model ID used to generate the response`（https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create） |
| `messages` | `array<ChatCompletionRequestMessage>` | 是 | 无 | 目前对话消息列表；`minItems: 1`；不同模型支持不同 modality，如 text/images/audio。 | `A list of messages comprising the conversation so far`；`minItems: 1`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `stream` | `boolean` 或 `null` | 否 | `false` | 为 true 时使用 SSE 返回 chunk 序列。 | `If set to true... streamed to the client`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `stream_options` | `ChatCompletionStreamOptions` 或 `null` | 否 | `null` | 仅在 `stream:true` 时设置；支持 `include_usage`、`include_obfuscation`。 | `Only set this when you set stream: true`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `max_completion_tokens` | `integer` 或 `null` | 否 | 无 | completion 生成 token 上限，包含可见输出和 reasoning tokens。 | `including visible output tokens and reasoning tokens`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `max_tokens` | `integer` 或 `null` | 否 | 无 | 已弃用，改用 `max_completion_tokens`；不兼容 o-series models。 | `deprecated in favor of max_completion_tokens`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `reasoning_effort` | `"none"|"minimal"|"low"|"medium"|"high"|"xhigh"|"max"` 或 `null` | 否 | 未注明 | reasoning 模型 effort；降低 effort 可更快且减少 reasoning tokens；并非所有模型支持所有值。 | `Currently supported values are none... max`（https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create） |
| `verbosity` | `"low"|"medium"|"high"` 或 `null` | 否 | `medium` | 控制响应详略。 | `default is medium`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `modalities` | `array<"text"|"audio">` 或 `null` | 否 | `["text"]` | 请求输出类型；音频输出需配 `audio`。 | `Most models are capable of generating text, which is the default`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `audio` | `{voice, format}` 或 `null` | 否 | 无 | 当 `modalities:["audio"]` 时必需；`format` 为 `wav`、`aac`、`mp3`、`flac`、`opus`、`pcm16`；voice 支持内置 voices 或 custom voice id。 | `Required when audio output is requested`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `temperature` | `number` 或 `null` | 否 | 未注明 | 0 到 2；更高更随机，更低更聚焦；建议不要和 `top_p` 同时调整。 | `between 0 and 2`（https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create） |
| `top_p` | `number` 或 `null` | 否 | 未注明 | nucleus sampling；建议不要和 `temperature` 同时调整。 | `alternative to sampling with temperature`（https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create） |
| `n` | `integer` 或 `null` | 否 | `1` | 每个 input 生成几个 choices；范围 1 到 128；按所有 choices 的 generated tokens 计费。 | `minimum: 1`；`maximum: 128`；`default: 1`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `stop` | `string` 或 `array<string>` | 否 | 无 | 最多 4 个停止序列；返回文本不包含 stop sequence；最新 reasoning models `o3`、`o4-mini` 不支持。 | `Up to 4 sequences`；`Not supported with latest reasoning models`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `frequency_penalty` | `number` 或 `null` | 否 | `0` | 范围 -2 到 2；正值降低重复同一行的可能性。 | `minimum: -2`；`maximum: 2`；`default: 0`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `presence_penalty` | `number` 或 `null` | 否 | `0` | 范围 -2 到 2；正值增加谈论新主题的可能性。 | `minimum: -2`；`maximum: 2`；`default: 0`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `logit_bias` | `object<token_id, integer>` 或 `null` | 否 | `null` | token id 到 bias 的 map；bias 范围 -100 到 100；加到 sampling 前 logits。 | `maps tokens... to... -100 to 100`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `logprobs` | `boolean` 或 `null` | 否 | `false` | 是否返回输出 token log probabilities。 | `Whether to return log probabilities`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `top_logprobs` | `integer` 或 `null` | 否 | 无 | 0 到 20；使用时 `logprobs` 必须为 true。 | `logprobs must be set to true`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `response_format` | `{"type":"text"}`、`{"type":"json_schema",...}`、`{"type":"json_object"}` | 否 | 无 | 输出格式；`json_schema` 用 Structured Outputs；`json_object` 是旧 JSON mode。 | `json_schema... Structured Outputs`；`json_object... older JSON mode`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `tools` | `array<ChatCompletionTool|CustomToolChatCompletions>` | 否 | 无 | 模型可调用工具；仅支持 function tools 和 custom tools。 | `provide either custom tools or function tools`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `tool_choice` | `ChatCompletionToolChoiceOption` | 否 | 无工具时 `none`，有工具时 `auto` | 控制是否/如何调用工具；形状见工具章节。 | `none is the default when no tools are present. auto is the default if tools are present`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `parallel_tool_calls` | `boolean` | 否 | 未注明 | 是否启用并行 function calling/tool use。 | `Whether to enable parallel function calling`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `functions` | `array<ChatCompletionFunctions>` | 否 | 无 | 已弃用，改用 `tools`；最少 1，最多 128。 | `Deprecated in favor of tools`；`maxItems: 128`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `function_call` | `"none"|"auto"|{"name":string}` | 否 | 无 functions 时 `none`，有 functions 时 `auto` | 已弃用，改用 `tool_choice`；可强制旧 function。 | `Deprecated in favor of tool_choice`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `web_search_options` | `{user_location?, search_context_size?}` | 否 | 无 | Chat 专用 web search 配置；不是 `tools[]` 的一员。 | `This tool searches the web`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `store` | `boolean` 或 `null` | 否 | `false` | 是否存储 chat completion output 供 distillation/evals 使用；支持 text/image input，超过 8MB 的 image input 会被丢弃。 | `default: false`；`image inputs over 8MB will be dropped`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `metadata` | `object` | 否 | 无 | 最多 16 个 key-value；key 最大 64 字符，value 最大 512 字符。 | `Set of 16 key-value pairs`（https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create） |
| `moderation` | `ModerationParam` 或 `null` | 否 | `null` | 对 request input 和 generated output 运行 moderation。 | `Configuration for running moderation`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `prediction` | `PredictionContent` 或 `null` | 否 | `null` | 预测输出配置；当大部分 model response 已知时可改善响应时间。 | `Predicted Output`；`large parts... known ahead of time`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `seed` | `integer` 或 `null` | 否 | 无 | Beta 且已弃用；尽力确定性采样，不保证确定性；用 `system_fingerprint` 监控后端变化。 | `Determinism is not guaranteed`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `prompt_cache_key` | `string` 或 `null` | 否 | `null` | 缓存分桶 key；替代 `user` 的缓存用途；GPT-5.6+ 更可靠匹配需设置。 | `Used by OpenAI to cache responses`；`must set prompt_cache_key`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| `prompt_cache_options` | `{ttl?, mode?}` | 否 | `mode:"implicit"`；`ttl:"30m"` | GPT-5.6+ prompt caching options；可禁用 implicit breakpoint。 | `Supported for gpt-5.6 and later`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| `prompt_cache_retention` | `"in_memory"` 或 `"24h"` 或 `null` | 否 | 依模型/组织 ZDR | 已弃用；旧模型最大保留策略。 | `Deprecated. Use prompt_cache_options.ttl instead`（https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create） |
| `safety_identifier` | `string` 或 `null` | 否 | `null` | end-user 稳定标识，最大 64 字符，建议 hash。 | `maximum length of 64 characters`（https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create） |
| `service_tier` | `"auto"|"default"|"flex"|"scale"|"priority"|"fast"` 或 `null` | 否 | `auto` | 服务层；响应显示实际使用值，可能不同于请求值。 | `default behavior is 'auto'`（https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create） |
| `user` | `string` | 否 | 无 | 已被 `safety_identifier` 和 `prompt_cache_key` 替代。 | `This field is being replaced`（https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create） |

## 3. 消息与内容格式

### role 枚举与 message 形状

| role | 对象 | 字段 | content 支持 | 官方依据 |
| --- | --- | --- | --- | --- |
| `developer` | `ChatCompletionRequestDeveloperMessage` | `role:"developer"`；`content`；`name?` | 字符串或 `text` content parts；o1 及更新模型中 developer messages 替代此前 system messages。 | `developer messages replace the previous system messages`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `system` | `ChatCompletionRequestSystemMessage` | `role:"system"`；`content`；`name?` | 字符串或 `text` content parts；o1 及更新模型建议用 `developer`。 | `use developer messages for this purpose instead`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `user` | `ChatCompletionRequestUserMessage` | `role:"user"`；`content`；`name?` | 字符串或 content parts：`text`、`image_url`、`input_audio`、`file`。 | `Can contain text, image, or audio inputs`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `assistant` | `ChatCompletionRequestAssistantMessage` | `role:"assistant"`；`content?`；`refusal?`；`name?`；`audio?`；`tool_calls?`；`function_call?` | 字符串或 content parts：一个或多个 `text`，或恰好一个 `refusal`；`content` 在没有 `tool_calls`/`function_call` 时需要。 | `Required unless tool_calls or function_call is specified`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `tool` | `ChatCompletionRequestToolMessage` | `role:"tool"`；`content`；`tool_call_id` | 字符串或 `text` content parts。 | `Tool call that this message is responding to`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `function` | `ChatCompletionRequestFunctionMessage` | `role:"function"`；`content`；`name` | 字符串或 null；已弃用。 | `deprecated: true`；`role... function`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

官方 role 总枚举还列出：`developer`、`system`、`user`、`assistant`、`tool`、`function`。摘录：`The role of the author of a message`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml）。

### content part 类型

| `type` | 可出现在 | 字段 | 必填 | 约束/说明 | 官方依据 |
| --- | --- | --- | --- | --- | --- |
| `text` | developer/system/user/assistant/tool | `text:string`；`prompt_cache_breakpoint?` | `type`,`text` | 文本 content part。 | `The type of the content part`；`The text content`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `image_url` | user | `image_url:{url, detail?}`；`prompt_cache_breakpoint?` | `type`,`image_url.url` | `url` 可为图片 URL 或 base64 data；`detail:"auto"|"low"|"high"`，默认 `auto`。 | `Either a URL... or base64 encoded image data`；`default: auto`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `input_audio` | user | `input_audio:{data, format}`；`prompt_cache_breakpoint?` | `type`,`input_audio.data`,`input_audio.format` | `data` 为 base64 audio；`format:"wav"|"mp3"`。 | `Base64 encoded audio data`；`supports "wav" and "mp3"`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `file` | user | `file:{filename?, file_data?, file_id?}`；`prompt_cache_breakpoint?` | `type`,`file` | `file_data` 为 base64 encoded file data；或使用 uploaded file id。 | `base64 encoded file data`；`ID of an uploaded file`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `refusal` | assistant | `refusal:string` | `type`,`refusal` | assistant refusal 内容。 | `The refusal message generated by the model`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### assistant tool call 结构

| 字段 | 结构 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| `tool_calls` | `array<ChatCompletionMessageToolCall | ChatCompletionMessageCustomToolCall>` | 当前工具调用输出；function tool call 通常含 `id`、`type:"function"`、`function:{name, arguments}`；custom tool call 含 custom 输入。 | `tool_calls`；`ChatCompletionMessageToolCall`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `function_call` | `{name, arguments}` | 已弃用，被 `tool_calls` 取代；`arguments` 是模型生成 JSON 字符串，调用前需校验。 | `Deprecated and replaced by tool_calls`；`Validate the arguments`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `audio` | `{id}`（请求历史）或响应中 `{id, expires_at, data, transcript}` | 多轮中引用前一次音频响应，或接收本次音频输出。 | `previous audio response`；`Base64 encoded audio bytes`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 4. 工具定义格式

### `tools[]`

| 工具类型 | 形状 | 必填 | 说明 | 官方依据 |
| --- | --- | --- | --- | --- |
| Function tool | `{type:"function", function:{name, description?, parameters?, strict?}}` | `type`,`function.name` | `function.name` 最大 64，允许字母数字、下划线、短横线；`parameters` 是 JSON Schema；省略 `parameters` 表示空参数列表；`strict` 为 true 时模型遵循参数 schema 的严格子集。 | `A function tool`；`The name... maximum length of 64`；`Omitting parameters defines... empty`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| Custom tool | `{type:"custom", custom:{name, description?, format?}}` | `type`,`custom.name` | `format` 默认 unconstrained text；可为 `{type:"text"}` 或 `{type:"grammar", grammar:{definition, syntax:"lark"|"regex"}}`。 | `A custom tool that processes input`；`Default is unconstrained text`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### `tool_choice`

| 形状 | 字段 | 行为 | 官方依据 |
| --- | --- | --- | --- |
| 字符串 | `"none"`、`"auto"`、`"required"` | `none` 不调用工具；`auto` 可生成消息或工具调用；`required` 必须调用一个或多个工具。 | `none means...`; `auto means...`; `required means...`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| 强制 function | `{type:"function", function:{name}}` | 强制调用指定 function。 | `forces the model to call that tool`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| 强制 custom | `{type:"custom", custom:{name}}` | 强制调用指定 custom tool。 | `force the model to call a specific custom tool`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `allowed_tools` | `{type:"allowed_tools", allowed_tools:{mode:"auto"|"required", tools:[...]}}` | 限制模型可选工具集合；`mode:auto` 可在集合中选择或生成消息，`mode:required` 要求调用集合中的一个或多个工具。 | `Constrains the tools available`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| legacy `function_call` | `"none"`、`"auto"` 或 `{"name": "my_function"}` | 已弃用；无 functions 时默认 `none`，有 functions 时默认 `auto`。 | `Deprecated in favor of tool_choice`；`none is the default... auto...`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 5. 非流式响应格式

### `CreateChatCompletionResponse`

| 字段 | 类型 | required（schema） | 说明 | 官方依据 |
| --- | --- | --- | --- | --- |
| `id` | `string` | 是 | chat completion 唯一 ID。 | `A unique identifier for the chat completion`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `object` | `"chat.completion"` | 是 | 对象类型固定。 | `always chat.completion`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `created` | Unix seconds integer | 是 | 创建时间。 | `Unix timestamp... created`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `model` | `string` | 是 | 实际使用模型。 | `The model used for the chat completion`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `choices` | `array<Choice>` | 是 | choices 列表；`n > 1` 时可多个。 | `Can be more than one if n is greater than 1`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `usage` | `CompletionUsage` | 否 | token 用量。 | `Usage statistics for the completion request`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `service_tier` | service tier 或 `null` | 否 | 实际服务层。 | `service_tier`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `system_fingerprint` | `string` | 否；已弃用 | 后端配置 fingerprint；可配合 `seed` 观察确定性影响。 | `backend configuration`；`deprecated: true`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `moderation` | `ChatCompletionModeration|null` | 否 | 若请求 moderation completions，则包含 input/output moderation 结果。 | `Moderation results`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### `choices[]`

| 字段 | 类型 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| `index` | `integer` | choice 下标。 | `The index of the choice`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `message` | `ChatCompletionResponseMessage` | assistant 生成消息。 | `A chat completion message generated by the model`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `finish_reason` | `"stop"|"length"|"tool_calls"|"content_filter"|"function_call"` | 停止原因；`function_call` 已弃用。 | `reason the model stopped`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `logprobs` | object 或 `null` | content/refusal token logprob 信息。 | `Log probability information for the choice`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### `ChatCompletionResponseMessage`

| 字段 | 类型 | required（schema） | 说明 | 官方依据 |
| --- | --- | --- | --- | --- |
| `role` | `"assistant"` | 是 | 响应消息作者角色。 | `role... assistant`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `content` | `string|null` | 是 | 消息文本内容。 | `The contents of the message`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `refusal` | `string|null` | 是 | refusal 消息。 | `The refusal message generated by the model`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `tool_calls` | array | 否 | 工具调用。 | `tool_calls`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `annotations` | array | 否 | web search 等场景的 URL citation；字段含 `start_index`、`end_index`、`url`、`title`。 | `Annotations for the message`；`url_citation`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `function_call` | `{name, arguments}` | 否；已弃用 | legacy function call。 | `Deprecated and replaced by tool_calls`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `audio` | object 或 `null` | 否 | 音频输出：`id`、`expires_at`、`data`、`transcript`。 | `Base64 encoded audio bytes`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### `usage`

| 字段 | 类型 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| `prompt_tokens` | `integer` | prompt token 数。 | `Number of tokens in the prompt`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `completion_tokens` | `integer` | generated completion token 数。 | `Number of tokens in the generated completion`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `total_tokens` | `integer` | 总 token 数。 | `prompt + completion`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `prompt_tokens_details.audio_tokens` | `integer` | prompt 中的 audio input tokens。 | `Audio input tokens present in the prompt`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `prompt_tokens_details.cached_tokens` | `integer` | prompt 中 cache hit tokens。 | `Cached tokens present in the prompt`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `prompt_tokens_details.cache_write_tokens` | `integer` | 写入 cache 的 prompt tokens。 | `prompt tokens written to cache`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `completion_tokens_details.reasoning_tokens` | `integer` | reasoning tokens。 | `Tokens generated by the model for reasoning`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `completion_tokens_details.audio_tokens` | `integer` | 模型生成的 audio tokens。 | `Audio input tokens generated by the model`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `completion_tokens_details.accepted_prediction_tokens` | `integer` | Predicted Outputs 中出现在 completion 的 prediction tokens。 | `appeared in the completion`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `completion_tokens_details.rejected_prediction_tokens` | `integer` | Predicted Outputs 中未出现在 completion 的 prediction tokens；仍计入 billing/output/context limits。 | `still counted in the total`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 6. 流式 SSE

### 开启与结束

| 项 | 说明 | 官方依据 |
| --- | --- | --- |
| 开启 | 请求 `stream:true`。 | `If set to true... streamed to the client`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| chunk 对象类型 | 每个 chunk 是 `CreateChatCompletionStreamResponse`；`object:"chat.completion.chunk"`。 | `always chat.completion.chunk`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| 结束 sentinel | SSE 最终为 `data: [DONE]`；若 `include_usage` 为 true，usage chunk 在 `[DONE]` 前发送。 | `before the data: [DONE] message`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| usage 上报 | `stream_options.include_usage:true` 时额外 chunk 的 `usage` 为全请求统计，`choices` 为空数组；其它 chunk 也有 `usage` 字段但为 null；流中断可能收不到最终 usage chunk。 | `choices field will always be an empty array`；`usage... null value`；`may not receive`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| moderation | 请求 moderated completions 时，moderation chunk 上出现 `moderation`。 | `Present on the moderation chunk`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### chunk 结构

| 字段 | 类型 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| `id` | `string` | 每个 chunk 相同 ID。 | `Each chunk has the same ID`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `object` | `"chat.completion.chunk"` | 固定对象类型。 | `always chat.completion.chunk`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `created` | Unix seconds integer | 每个 chunk 相同 timestamp。 | `Each chunk has the same timestamp`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `model` | `string` | 模型。 | `model to generate the completion`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `choices` | `array<StreamChoice>` | 可为空；`include_usage` 最后 usage chunk 中为空。 | `Can also be empty for the last chunk`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `usage` | `CompletionUsage|null` | 仅 `include_usage` 时存在，除最后 usage chunk 外为 null。 | `contains a null value except for the last chunk`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `service_tier` | service tier | 实际服务层。 | `service_tier`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `system_fingerprint` | `string` | 后端 fingerprint；已弃用。 | `deprecated: true`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `moderation` | object 或 `null` | moderation chunk 使用。 | `moderation chunk`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### `choices[].delta`

| 字段 | 类型 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| `role` | `"developer"|"system"|"user"|"assistant"|"tool"` | delta message 作者 role。 | `role of the author`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `content` | `string|null` | 文本增量。 | `contents of the chunk message`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `refusal` | `string|null` | refusal 文本增量。 | `refusal message generated`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `tool_calls` | array | tool call chunk 增量。 | `ChatCompletionMessageToolCallChunk`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `function_call` | `{name?, arguments?}` | 已弃用；legacy function call 增量。 | `Deprecated and replaced by tool_calls`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

### stream choice 字段

| 字段 | 类型 | 说明 | 官方依据 |
| --- | --- | --- | --- |
| `index` | `integer` | choice 下标。 | `The index of the choice`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `delta` | `ChatCompletionStreamResponseDelta` | chunk 增量。 | `A chat completion delta`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `finish_reason` | `"stop"|"length"|"tool_calls"|"content_filter"|"function_call"|null` | 该 choice 的结束原因；流中间通常为 null。 | `finish_reason... nullable: true`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `logprobs` | object 或 null | content/refusal token logprob。 | `Log probability information`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

## 7. 缓存

### 开启方式、断点、TTL

| 项 | Chat Completions 行为 | 官方依据 |
| --- | --- | --- |
| 自动缓存 | eligible requests 自动启用，无需代码改动；近期模型 `gpt-4o` and newer 启用。 | `works automatically for eligible requests`；`gpt-4o and newer`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| GPT-5.6+ implicit breakpoint | 默认在 latest user 或 tool message 放 implicit breakpoint。 | `implicit breakpoint at the latest user or tool message`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| 显式断点 | 在 supported content block 加 `prompt_cache_breakpoint: {"mode":"explicit"}`。 | `Add prompt_cache_breakpoint`；`mode: explicit`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| Chat 支持断点的 block | `text`、`image_url`、`input_audio`、`file`、`refusal`。 | `Chat Completions API supports them on text, image_url, input_audio, file, and refusal`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| `prompt_cache_key` | 对共享长前缀的请求使用相同 key；GPT-5.6+ 更可靠匹配必须设置。 | `Set prompt_cache_key`；`must set prompt_cache_key`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| `prompt_cache_options.mode` | `implicit` 默认；`explicit` 禁用 implicit，仅显式断点读写。 | `implicit is the default`；`explicit disables`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| `prompt_cache_options.ttl` | 当前唯一支持 `30m`，默认 `30m`；后端可保留更久。 | `only supported value is 30m`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| 写入数 | 每请求最多 4 个新的 cache writes；implicit 模式下 latest message breakpoint 占 1 个写槽。 | `up to four new cache writes`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| 读取回看数量 | 官方 guide 写“latest 50 breakpoints”；Chat create reference MCP 索引内容写“latest 80 breakpoints”。这是官方文档差异，本文不做推断合并。 | `latest 50 breakpoints`（https://developers.openai.com/api/docs/guides/prompt-caching）；`latest 80 breakpoints`（https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create） |
| 最小 cacheable prefix | GPT-5.6+ 至少 1,024 tokens；GPT-5.5 及更早可在 1,024 到 2,048 间变化。 | `at least 1,024 tokens`；`range from 1,024 to 2,048`（https://developers.openai.com/api/docs/guides/prompt-caching） |
| legacy retention | `prompt_cache_retention` 已弃用；旧模型可用 `in_memory` 或 `24h`，`24h` 最大 24 小时。 | `Deprecated`；`maximum of 24 hours`（https://developers.openai.com/api/docs/guides/prompt-caching） |

### 命中报告字段

| 字段 | 位置 | 含义 | 官方依据 |
| --- | --- | --- | --- |
| `usage.prompt_tokens_details.cached_tokens` | `CreateChatCompletionResponse.usage` 或最终 usage chunk | 从 cache 读取的 prompt tokens。 | `Cached tokens present in the prompt`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `usage.prompt_tokens_details.cache_write_tokens` | 同上 | 写入 cache 的 prompt tokens。 | `prompt tokens written to cache`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |
| `usage.prompt_tokens` | 同上 | prompt token 总数。 | `Number of tokens in the prompt`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml） |

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

官方 OpenAPI `Error` schema 为顶层 `error` 对象，常用字段为 `message`、`type`、`param`、`code`。官方错误 guide 要求 billing/quota 相关错误检查 `error.code`。依据：`inspect error.code`（https://developers.openai.com/api/docs/guides/error-codes）与 `#/components/schemas/Error`（https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml）。

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

### 流式错误

Chat Completions OpenAPI 的 streaming 成功媒体类型为 `text/event-stream`，chunk schema 为 `CreateChatCompletionStreamResponse`；错误响应仍按 OpenAI API 标准错误对象处理。官方错误 guide 说明 `RateLimitError`、`AuthenticationError`、`BadRequestError` 等 SDK 错误类型，HTTP/API 错误码见上表（https://developers.openai.com/api/docs/guides/error-codes）。
