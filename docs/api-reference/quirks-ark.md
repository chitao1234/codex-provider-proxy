# 火山方舟 Ark API quirks

> 核对日期：2026-08-12。  
> 对照基准：`messages-api.md`、`responses-api.md`、`chat-completions-api.md`。本文只记录官方文档或官方 PDF 可直接支撑的偏差；网页无法访问处明确标注。

## 官方来源与访问状态

| 来源 | 状态 | 用途 | 官方摘录 |
| --- | --- | --- | --- |
| https://docs.volcengine.com/docs/82379/1662042 | 无法访问正文：本环境返回文档中心壳页，页面状态含 `errorCode:"InternalError"` | 用户指定在线文档 | `errorCode":"InternalError"`（缓存：`/tmp/codex_relay_official/ark-docs-82379-1662042.html`） |
| `/home/chi/Downloads/火山方舟_创建模型响应_1776169582.pdf` | 可访问；已转文本为 `/tmp/codex_relay_official/ark-responses.txt` | Responses API 参数 | `POST https://ark.cn-beijing.volces.com/api/v3/responses` |
| `/home/chi/Downloads/火山方舟_Web Search（联网内容插件）_1785419476.pdf` | 可访问；已转文本为 `/tmp/codex_relay_official/ark-websearch.txt` | Web Search 工具、usage、QPS | `usage.tool_usage`；`默认支持账号维度 5 QPS` |
| `/home/chi/Downloads/火山方舟_文档指南_1785946827.pdf` | 可访问；已转文本为 `/tmp/codex_relay_official/ark-guide.txt` | 快速开始、Chat 示例、模型窗口 | `curl https://ark.cn-beijing.volces.com/api/v3/chat/completions` |

## 1. 端点 URL 与认证

| 协议 | 是否存在 | base_url / path | 认证 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- | --- | --- |
| OpenAI Responses | 存在 | `https://ark.cn-beijing.volces.com/api/v3/responses` | `Authorization: Bearer $ARK_API_KEY`；PDF 还称支持 API Key / Access Key | 路径带 `/api/v3`，不是 OpenAI 基准的 `/v1/responses`。 | `POST https://ark.cn-beijing.volces.com/api/v3/responses`；`本接口支持 API Key /Access Key 鉴权`（`火山方舟_创建模型响应_1776169582.pdf`） |
| OpenAI Chat Completions | 存在 | `https://ark.cn-beijing.volces.com/api/v3/chat/completions` | `Authorization: Bearer $ARK_API_KEY` | 路径带 `/api/v3`，不是 OpenAI 基准的 `/v1/chat/completions`。 | `curl https://ark.cn-beijing.volces.com/api/v3/chat/completions`；`base_url="https://ark.cn-beijing.volces.com/api/v3"`（`火山方舟_文档指南_1785946827.pdf`） |
| Anthropic Messages | 未在已访问官方 PDF/网页正文中找到 | 未在官方文档找到 | 未在官方文档找到 | 用户指定 Ark 在线页无法访问正文；本次不推断 Anthropic 端点。 | `无法访问正文`；`errorCode":"InternalError"`（https://docs.volcengine.com/docs/82379/1662042） |

## 2. 请求参数偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Responses `input` 扩展视频/文件 | Responses | OpenAI Responses 基准有 text/image/file，Ark PDF 明确 `content` 可含文本、图片、视频或文件。 | `input.content...用于生成回复的文本、图片、视频或文件输入`（`火山方舟_创建模型响应_1776169582.pdf`） |
| Responses role 支持 `developer/system/user/assistant` | Responses | 与 OpenAI Responses 基准 role 接近；不同段落对 assistant 是否可用有分场景差异。 | `输入消息的角色，可以是 user，system，assistant 或 developer`；另处为 `system，user 或 developer`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `instructions` 与缓存互斥 | Responses | OpenAI Responses 基准没有把 `instructions` 与 prompt cache 互斥；Ark 明确互斥。 | `不可与缓存能力一起使用`；`配置了instructions 字段后，本轮请求无法写入缓存和使用缓存`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `previous_response_id` 有连续调用延迟建议 | Responses | OpenAI Responses 基准支持该字段；Ark 额外建议连续多轮之间约 100ms 延迟。 | `建议在每次请求之间加入约 100 毫秒的延迟，否则可能会导致调用失败`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `expire_at` 是 Ark 扩展 | Responses | OpenAI Responses 基准没有同名顶层 `expire_at`；Ark 用于 store/cache 过期。 | `expire_at integer 默认值：创建时刻+259200`；`最多保留7天`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `max_output_tokens` 包含思维链 | Responses | 与 OpenAI Responses 基准一致；转换层应把 Chat `max_completion_tokens` 映射到该字段时计入 thinking。 | `模型输出最大 token 数，包含模型回答和思维链内容`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `thinking.type` 三态 | Responses | OpenAI Responses 基准用 `reasoning` 控制；Anthropic thinking 也不同。Ark 另有 `thinking.type`：enabled/disabled/auto。 | `取值范围：enabled，disabled，auto`；`auto：自动思考模式`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `reasoning.effort` 枚举较窄 | Responses | OpenAI Responses 基准可有 `xhigh/max` 等；Ark PDF 列 `minimal/low/medium/high`。 | `取值范围：minimal，low，medium，high`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `caching` 是 Ark 扩展且互斥工具 | Responses | OpenAI 基准用 prompt cache options / cache breakpoint；Ark 顶层 `caching` 且与非 function tools 互斥。 | `caching object 默认值 {"type": "disabled"}`；`不可与 instructions 字段、tools（除自定义函数 Function Calling 外）字段一起使用`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `store` 默认值 | Responses | OpenAI Responses 基准示例常有 `store:true`，Ark 明确默认 true。 | `store boolean / null 默认值 true`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `temperature/top_p` 模型固定值 | Responses | OpenAI Responses 基准允许传参；Ark 某些模型忽略手动值。 | `字段取值固定为 1，手动指定的参数值将被忽略`；`top_p...固定为 0.95`（`火山方舟_创建模型响应_1776169582.pdf`） |

## 3. 消息与内容格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| `input` 可直接传字符串 | Responses | 与 OpenAI Responses 基准一致。 | `文本输入 string`；`等同于使用 user 角色输入的文本信息`（`火山方舟_创建模型响应_1776169582.pdf`） |
| 图片字段有 Ark 扩展 `image_pixel_limit` | Responses | OpenAI Responses 基准使用 `detail`；Ark 还支持 `image_pixel_limit`，且优先级高于 `detail`。 | `input.content.image_pixel_limit`；`生效优先级：高于 detail 字段`（`火山方舟_创建模型响应_1776169582.pdf`） |
| assistant `partial` 续写 | Responses | OpenAI Responses 基准没有 Ark 的 `partial:true` 续写描述。 | `最后一条消息的 role 为 assistant，并设置 partial 为 true 开启续写模式`（`火山方舟_创建模型响应_1776169582.pdf`） |
| 内容类型覆盖 | Responses | OpenAI Responses 基准未包含视频输入；Ark PDF 明确上下文元素可含视频。 | `上下文元素...可以包含文本、图片和视频输入`（`火山方舟_创建模型响应_1776169582.pdf`） |
| Anthropic 消息格式 | Anthropic | 未在已访问官方 PDF/网页正文找到，不推断支持。 | `未在官方文档找到` |

## 4. 工具格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| 工具类别扩展 | Responses | OpenAI Responses 基准有 built-in/MCP/function；Ark 也有内置工具、MCP、自定义函数，但内置工具类型是方舟自有集合。 | `当前支持多种调用方式，包括 内置工具...MCP工具...自定义工具`（`火山方舟_创建模型响应_1776169582.pdf`） |
| Function tool 是扁平字段 | Responses | 与 OpenAI Responses function tool 类似；不同于 OpenAI Chat 的 `{function:{...}}` 嵌套 shape。 | `tools.type...function`；`tools.name`；`tools.parameters`；`tools.strict...默认值 true`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `strict` 必选且默认 true | Responses | OpenAI Responses 基准 `strict` 可选；Ark PDF 标 `%%require%% 默认值 true`。 | `tools.strict boolean %%require%% 默认值 true`（`火山方舟_创建模型响应_1776169582.pdf`） |
| Web Search tool 参数 | Responses | OpenAI Responses 基准 web_search 参数不同；Ark 有 `sources/limit/user_location/max_keyword`。 | `工具类型...应为 web_search`；`tools.sources`；`tools.limit integer 默认值 10`；`tools.max_keyword`（`火山方舟_创建模型响应_1776169582.pdf`） |
| Web Search 名称冲突 | Responses | OpenAI 基准不要求避免自定义函数名 `web_search`；Ark 明确避免。 | `自定义函数名称应避免使用 web_search，否则模型将按内置优先级判断调用逻辑`（`火山方舟_Web Search（联网内容插件）_1785419476.pdf`） |
| MCP tool 字段 | Responses | OpenAI Responses MCP tool 字段不完全等同；Ark 用 `server_label/server_url/headers/require_approval/allowed_tools`。 | `tools.type`；`tools.server_label string 必选`；`tools.server_url string 必选`；`tools.headers object`（`火山方舟_创建模型响应_1776169582.pdf`） |
| Knowledge Search 是 Ark 内置工具 | Responses | OpenAI Responses 基准没有 `knowledge_search` 同名工具；Ark 有私域知识库搜索工具。 | `tools.knowledge_resource_id 必选`；`Knowledge Search 私域知识库搜索工具`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `tool_choice` 模型受限 | Responses | OpenAI Responses 基准普遍支持 tool_choice；Ark 标明仅 doubao-seed-1.6 系列支持。 | `tool_choice string / object`；`仅 doubao-seed-1.6 系列模型支持此字段`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `max_tool_calls` 尽力而为 | Responses | OpenAI Responses 基准定义最大工具调用数；Ark 明确 best effort 且按工具默认不同。 | `取值范围：[1, 10]`；`尽力而为（best effort）机制，不保证成功`（`火山方舟_创建模型响应_1776169582.pdf`） |

## 5. 响应、usage 与错误格式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Web Search 输出 item 和 annotations | Responses | OpenAI Responses 基准也有 web_search_call，但 Ark 文档强调 `message.content[0].annotations`。 | `web_search_call：包含搜索调用的 ID 以及执行的操作`；`message.content[0].annotations`（`火山方舟_Web Search（联网内容插件）_1785419476.pdf`） |
| Web Search usage 扩展 | Responses | OpenAI Responses 基准 usage 没有 Ark 的 `tool_usage/tool_usage_details`。 | `usage.tool_usage：显示插件的总调用次数`；`usage.tool_usage_details：显示每个搜索源...详细调用次数`（`火山方舟_Web Search（联网内容插件）_1785419476.pdf`） |
| 缓存命中与 `previous_response_id` | Responses | Ark 文档说明带缓存 previous response 时 `cached_tokens` 为 0。 | `传入带缓存的 previous_response_id 时，缓存输入（cached_tokens）为0`（`火山方舟_创建模型响应_1776169582.pdf`） |
| 错误格式 | Responses | 官方 PDF 未在已抽取段落给出完整 error schema；转换层不要编造 OpenAI/Anthropic 错误对象。 | `未在官方文档找到完整错误格式` |

## 6. 流式偏差

| 项 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Responses 用 `[DONE]` 结束 | Responses | OpenAI Responses 基准是语义 SSE，最终 `response.completed/failed/incomplete`，没有 Chat 风格 `[DONE]`；Ark 明确用 `[DONE]`。 | `true：按 SSE 协议逐块返回模型生成内容，并以一条 data: [DONE] 消息结束`（`火山方舟_创建模型响应_1776169582.pdf`） |
| 同时仍出现语义事件 | Responses | Ark SDK 示例使用 `response.completed`、`response.reasoning_summary_text.delta` 等事件名；因此转换层需处理语义事件和 `[DONE]` 终止符并存。 | `EventType_response_reasoning_summary_text_delta`；`EventType_response_completed`（`火山方舟_Web Search（联网内容插件）_1785419476.pdf`） |
| 流式 Web Search 最终 usage | Responses | usage/tool usage 在最终事件取。 | `Store the final event (for extracting usage info)`；`final_event.response.usage.tool_usage`（`火山方舟_Web Search（联网内容插件）_1785419476.pdf`） |
| Chat stream | Chat | 官方指南有 Chat curl，但本次未在可访问官方 PDF 中抽到完整 Chat 流式事件集；不推断额外差异。 | `未在官方文档找到完整 Chat 流式事件集` |

## 7. 已知限制

| 限制 | 适用协议 | 对照基准差异 | 官方摘录 |
| --- | --- | --- | --- |
| Web Search 默认并发 | Responses | OpenAI Responses 基准没有 Ark 账号维度 5 QPS 说明。 | `默认支持账号维度 5 QPS`（`火山方舟_Web Search（联网内容插件）_1785419476.pdf`） |
| Web Search + caching 返回错误 | Responses | Ark Web Search 文档说明缓存不兼容。 | `caching 返回 400`（`火山方舟_Web Search（联网内容插件）_1785419476.pdf`） |
| `expire_at` 最长 7 天 | Responses | OpenAI Responses 基准不使用该过期字段。 | `取值范围：(创建时刻, 创建时刻+604800]，即最多保留7天`（`火山方舟_创建模型响应_1776169582.pdf`） |
| `retrieve_count >= limit` | Responses / Knowledge Search | Ark 私域知识库工具特有限制。 | `retrieve_count 需要大于等于 limit，否则会抛出错误`（`火山方舟_创建模型响应_1776169582.pdf`） |
| 上下文窗口模型相关 | Responses / Chat | 方舟模型窗口按模型不同；PDF 指南示例列 128k、200k、256k、1024k。 | `上下文窗口: 1024k`；`上下文窗口: 256k`；`上下文窗口: 128k`（`火山方舟_文档指南_1785946827.pdf`） |
| Anthropic 端点 | Anthropic | 在线正文无法访问，本地 PDF 未找到 Anthropic Messages 端点；不写支持性结论。 | `无法访问正文`；`未在官方文档找到` |

