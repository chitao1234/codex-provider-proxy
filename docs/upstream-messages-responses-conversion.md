# 上游 Messages / 上游第三方 Responses 转换层设计

> 状态：设计中（调研进行中，偏差表待填充）
> 对应任务：#32 火山方舟 Responses 偏差、#33 各家 Messages 上游、#34 各家第三方 Responses 偏差

## 1. 目标与范围

当前转换层只支持 **上游 OpenAI Chat Completions**（下游 Messages 或 Responses 都转成 chat 发出）。
本设计扩展上游协议，支持：

| 上游 | 下游 | 方向 |
|---|---|---|
| Anthropic Messages（各家 /anthropic 端点） | OpenAI Responses（Codex） | Messages→Responses |
| Anthropic Messages | Anthropic Messages | 透传/最小适配 |
| OpenAI Responses（第三方实现） | OpenAI Responses | **归一化到官方标准** |
| OpenAI Responses（第三方实现） | Anthropic Messages | Responses→Messages |

**关键背景**（用户明确指出）：
- **Responses API 是重灾区**：每家第三方实现都和 OpenAI 官方 Responses 文档有偏差（火山方舟尤其严重）。必须逐项归一化。
- **Messages API 偏差小**：各家以兼容 Claude Code 为目标，偏差较小，但仍有硬性要求（MiMo 必须回传 reasoning_content 否则 400、MiniMax 交错思维链完整回传）。
- **"第三方 responses 转 openai 格式的 responses"** 是核心场景：下游 Codex 说官方协议，上游第三方 Responses 有偏差——转换层把上游偏差消化，输出官方标准。

## 2. 现状（已实现）

```
下游 Messages (Claude Code) ──convert_messages_request──▶ chat ──ChatToMessagesStream──▶ 下游
下游 Responses (Codex) ──convert_responses_request──▶ chat ──ChatToResponsesStream──▶ 下游
```

- `UpstreamApi`: Passthrough | OpenAiChatCompletions
- 请求转换：`convert_messages_request`（Messages→chat）、`convert_responses_request`（Responses→chat）
- 响应转换：`ChatStreamConverter`（chat→Messages SSE）、`ResponsesStreamConverter`（chat→Responses SSE）
- chat 中间表示：标准 Chat Completions 形状（choices[].delta 增量、reasoning_content、tool_calls 分片、usage 尾 chunk、[DONE]）

## 3. 架构决策：语义事件流（SemanticEvent）中间层

**否决方案：以 chat SSE 为中间表示。** chat SSE 无法表达以下语义（转换会丢信息或失真）：

- Anthropic `thinking` 块的 `signature`（prompt caching 验证）与 `redacted_thinking`（加密推理块）
- Anthropic `cache_control` 断点（prompt caching 语义）
- Responses 的 `item_id`（function_call id、item 引用、previous_response_id 状态关联）
- Responses 内置工具调用 items（`web_search_call` / `file_search_call` / `code_interpreter_call`）——chat 只有 function tool_calls
- Anthropic `tool_result` 的块结构（可含 image/document）vs chat tool 消息的字符串
- usage 形状差异（cache_creation/cache_read、input_tokens_details 细分）
- Anthropic `stop_sequence` 与 Responses `incomplete_details` 的细粒度原因

**采用方案：协议无关的语义事件流。** 定义覆盖 Messages + Responses 全部语义的事件接口，
上游协议解析器产出事件流，下游协议渲染器消费事件流。转换器数量从 N×M（协议对）降到 N+M（解析器+渲染器）。

```
上游协议解析器（上游 SSE → 语义事件）      下游协议渲染器（语义事件 → 下游 SSE）
┌──────────────────────────┐            ┌──────────────────────────┐
│ ChatCompletionsParser    │            │ MessagesRenderer         │
│ MessagesParser           │──▶ 事件流 ──│ ResponsesRenderer        │
│ ResponsesParser(第三方)   │  (偏差归一化) │ (输出官方标准)            │
└──────────────────────────┘            └──────────────────────────┘
```

### 3.1 语义事件流定义（StreamEvent）

```rust
/// 协议无关的流式响应事件（覆盖 Messages + Responses 语义）。
pub enum StreamEvent {
    /// 响应开始（id/model 用于下游消息 id 合成）。
    Start { id: String, model: String },
    /// 推理文本增量。signature 仅上游 Messages 提供（透传给 Messages 下游）。
    ReasoningDelta { text: String, signature: Option<String> },
    /// 正文文本增量。
    TextDelta { text: String },
    /// 工具调用开始（首 chunk 带 id/name）。
    ToolCallStart { index: usize, id: String, name: String },
    /// 工具调用参数增量。
    ToolCallArgsDelta { index: usize, args: String },
    /// 工具调用结束。
    ToolCallEnd { index: usize },
    /// 内置工具调用（Responses web_search_call 等；chat/Messages 无此语义）。
    ServerToolCall { name: String, input: Value },
    /// 响应结束（stop_reason + usage）。
    End { stop_reason: StopReason, usage: Usage },
    /// 错误。
    Error { message: String, code: Option<String> },
}

pub enum StopReason { Stop, ToolUse, MaxTokens, Length, ContentFilter, Other }
```

### 3.2 解析器（上游 → 事件流）

| 解析器 | 输入 | 输出事件 | 偏差消化 |
|---|---|---|---|
| ChatCompletionsParser | chat SSE | Start/ReasoningDelta/TextDelta/ToolCall*/End/Error | 现有 ChatStreamConverter 重构 |
| MessagesParser | Messages SSE | Start/ReasoningDelta(signature)/TextDelta/ToolCall*/End/Error | 直接透传语义，无损失 |
| ResponsesParser | 第三方 Responses SSE | 同上 + ServerToolCall | **归一化偏差**（改名/缺事件/字段差异） |

### 3.3 渲染器（事件流 → 下游）

| 渲染器 | 输入 | 输出 | 复用 |
|---|---|---|---|
| MessagesRenderer | 事件流 | Messages SSE | 现有 ChatStreamConverter 输出逻辑抽出 |
| ResponsesRenderer | 事件流 | Responses SSE | 现有 ResponsesStreamConverter 输出逻辑抽出 |

**关键收益**：
1. 语义无损：signature/redacted_thinking/cache_control/item_id/ServerToolCall 全程保留
2. 线性扩展：新增上游协议只写解析器，新增下游协议只写渲染器
3. 现有代码可重构：ChatStreamConverter 的 on_chunk 产出本就是事件流雏形，渲染器逻辑从现有转换器抽出
4. 第三方 Responses 偏差集中在一个解析器消化，下游渲染器输出始终是官方标准

### 3.4 请求侧（下游请求 → 上游请求）

请求侧同理：定义**语义请求 IR**（协议无关的消息数组 + 参数），
下游请求解析器（Messages/Responses → IR）与上游请求渲染器（IR → chat/Messages/Responses）分离。

```rust
/// 协议无关的请求消息块。
pub enum RequestBlock {
    Text(String),
    Image(ImageSource),           // base64 / url
    Thinking(String),             // 历史 thinking（回传）
    RedactedThinking,
    ToolUse { id: String, name: String, input: Value },
    ToolResult { id: String, content: Vec<RequestBlock>, is_error: bool },
    ServerToolUse { name: String, input: Value },  // 声明或调用
    Reasoning(String),            // Responses reasoning item（回传）
}
```

请求侧收益：
- 现有 `convert_messages_request`/`convert_responses_request` 重构为"下游请求解析器 → IR"，输出不变
- 上游请求渲染器按上游协议产出（chat 渲染器已有，Messages/Responses 渲染器新增）
- 硬性要求（MiMo reasoning_content 回传、MiniMax 交错思维链）在 IR 层统一处理，不散落各协议

### 3.5 thinking 注入（调研发现的新能力需求）

Claude Code 对 subagent / structured-output 请求会**省略 thinking 字段**（anthropics/claude-code#69379），
而部分上游 Messages 端点**要求显式 thinking**（Kimi k2.7-code 不传则 400）或**要求特定形态**
（GLM 需要 enabled/disabled、MiniMax M2.x 强制开）。

转换层需提供 `thinking_policy`（按 provider 配置）：

```rust
pub enum ThinkingPolicy {
    /// 透传下游的 thinking 请求（默认）。
    Passthrough,
    /// 强制注入 thinking（下游省略时补 `{"type":"enabled"}`）。
    ForceEnable,
    /// 强制注入 thinking.disabled。
    ForceDisable,
    /// 强制注入且固定形态（GLM clear_thinking 等）。
    Force { type: String, extra: Value },
}
```

- 请求渲染器按 policy 补/改 thinking 字段
- 与现有 `thinking_param`（TopLevel/Adaptive/EnableThinking/None）配合：policy 决定**是否**注入，`thinking_param` 决定**形态**
- Kimi 需 `ForceEnable` + `thinking_param=TopLevel`；MiniMax M2.x 需 `ForceEnable` + `Adaptive`；GLM-5.2 需 `Force{type:"enabled", clear_thinking:false}`

## 6. UpstreamApi 扩展与配置

```rust
pub enum UpstreamApi {
    Passthrough,
    OpenAiChatCompletions,
    /// Anthropic Messages (`POST /v1/messages`), e.g. DeepSeek/MiniMax/MiMo /anthropic.
    AnthropicMessages,
    /// OpenAI Responses, possibly a third-party implementation with deviations.
    OpenAIResponses,
}
```

- `upstream_path_for(provider, downstream_path)`：上游协议 → 上游路径
  - chat → `/chat/completions`
  - Messages 上游 → `/v1/messages`（或 provider base_url 直接拼）
  - Responses 上游 → `/v1/responses`
- 响应侧选择：`upstream_parser(provider)` → ChatCompletionsParser / MessagesParser / ResponsesParser（产出语义事件流），再按下游协议选渲染器
- 转换矩阵合法性检查：`provider_converts_path` 需校验"上游协议 × 下游协议"组合是否支持

## 7. 错误处理

- 上游 Messages 错误：Anthropic 信封 `{"type":"error","error":{...}}` → 解析器转 `StreamEvent::Error` → 下游渲染器按协议输出
- 上游第三方 Responses 错误：各家形状不一（方舟待调研）→ ResponsesParser 归一化到语义 Error → 下游渲染
- HTTP 200 + body 内错误码（MiniMax base_resp 类）→ 解析器检查并转 Error

## 8. 实施阶段

1. **Phase 0（重构）**：定义 StreamEvent / 请求 IR，把现有 `ChatStreamConverter`/`ResponsesStreamConverter` 重构为"chat 解析器 + 下游渲染器"。回归测试保证行为不变。
2. **Phase 1**：`UpstreamApi::AnthropicMessages` + MessagesParser + 下游双路渲染。DeepSeek/MiniMax/MiMo /anthropic 实测。
3. **Phase 2**：`UpstreamApi::OpenAIResponses` + ResponsesParser + 下游双路渲染。DeepSeek /responses 实测。
4. **Phase 3**：火山方舟归一化（重灾区，按调研偏差表逐项适配）。codex + curl 实测。
5. **Phase 4**：其余各家（GLM/Kimi/Qwen/MiniMax）归一化 + 全矩阵实测。

## 9. 调研结果附录

### 9.1 各家 Anthropic Messages 上游端点（subagent 调研完成 ✅）

| 平台 | 端点 | 认证 | 模型 |
|---|---|---|---|
| DeepSeek | `https://api.deepseek.com/anthropic` | x-api-key | claude-opus*→deepseek-v4-pro、claude-sonnet/haiku*→deepseek-v4-flash（自动映射） |
| MiniMax | `https://api.minimaxi.com/anthropic` | ANTHROPIC_API_KEY | MiniMax-M3（1M）/M2.x（204.8K） |
| MiMo | `https://api.xiaomimimo.com/anthropic` | sk- | mimo-v2.5-pro（1M）/mimo-v2.5（多模态）；V2 已下线 |
| 火山方舟 | `https://ark.cn-beijing.volces.com/api/coding`（Coding Plan）或 `/api/plan`（Agent Plan）；**无通用按量端点** | ANTHROPIC_AUTH_TOKEN | doubao-seed-2.1-turbo/minimax-m3/glm-5.2/deepseek-v4-flash/kimi-k2.7-code 等 |
| GLM | `https://open.bigmodel.cn/api/anthropic` | x-api-key | glm-5.2[1m]、GLM-4.7 |
| Kimi | `https://api.moonshot.cn/anthropic` | ANTHROPIC_AUTH_TOKEN | kimi-k3[1m]、kimi-k2.7-code（必须开思考） |
| Qwen | `https://{...}.maas.aliyuncs.com/apps/anthropic`（Token/Coding/按量 3 种） | ANTHROPIC_AUTH_TOKEN | qwen3.8-max/qwen3.7-max/qwen3.6-flash/qwen3.7-plus |

### 9.2 请求偏差清单（Messages 上游）

| 平台 | thinking | 思维链回传 | image | server tools | 其他 |
|---|---|---|---|---|---|
| DeepSeek | budget_tokens 忽略，signature 不强制 | **必须回传 reasoning_content 否则 400** | ❌ 不支持 | web_search 识别 | image/document 不支持 |
| MiniMax | M3: adaptive/disabled；**M2.x 强制开** | 交错思维链完整回传 | M3 支持 | 无 | stop_sequences/top_k 忽略 |
| MiMo | 默认开；thinking.enabled | **必须回传 reasoning_content 否则 400**（思考模式下） | v2.5 支持 | flat tool 声明（type-only） | cache_control/budget_tokens 忽略 |
| 火山方舟 | 默认开；可 disabled | 待实测 | 待实测 | **web_search 仅 Claude 模型**；第三方走豆包搜索 | 套餐 Key 专用 |
| GLM | enabled/disabled；无 budget_tokens | **GLM-5.2 默认 clear_thinking=true 剥除回传**（需 clear_thinking=false）；4.6 读回传 | 待实测 | 社区有 web_search | 响应字段缺失（Xcode 代理需补） |
| Kimi | **k2.7-code 必须显式开否则 400**；k3 默认开 | 待实测 | 支持 | WebSearch ✅、**WebFetch ❌**、document ❌ | temperature ×0.6 映射 |
| Qwen | 待实测 | 待实测 | 待实测 | 无标准 server tool | 无模型列表端点 |

**横向结论（对转换层最关键）**：
1. **thinking 注入是普遍需求**——Claude Code 对 subagent 请求会省略 thinking 字段（anthropics/claude-code#69379），而 Kimi k2.7-code 必须显式开思考否则 400、GLM 需要 enabled/disabled。代理需按 provider 补 thinking（`always_enable_tools` 类机制扩展到 thinking）
2. **思维链回传硬性要求**：MiMo、DeepSeek（400）；MiniMax 完整回传；GLM-5.2 需 clear_thinking=false
3. **temperature 映射**：Kimi ×0.6；各家 thinking budget_tokens 全忽略
4. **server tools 支持参差**：Kimi（WebSearch✅/WebFetch❌）、DeepSeek（识别 web_search）、MiMo（flat tool）、火山/百炼（无标准 server tool）

### 9.3 火山方舟 Responses 偏差（PDF 调研完成 ✅，2175 页文档）

**端点**：`POST https://ark.cn-beijing.volces.com/api/v3/responses`，Bearer 鉴权；基础 URL `/api/v3`。
仅 doubao-seed-1-6-250615+ 支持；模型用方舟 Model ID（doubao-seed-2-1-pro-260628 / glm-5-2-260617 / deepseek-v4-*）。

**与官方一致**：input/instructions/max_output_tokens/previous_response_id/store/text.format/reasoning.effort/include；
函数工具扁平格式 `{"type":"function","name":...,"parameters":...}`；function_call item 结构；SSE 事件名 + sequence_number；
output[] items（reasoning/message，rs_*/msg_* id）；usage 结构；结构化输出 json_schema strict。

**方舟专有偏差（转换层必须消化）**：

| # | 偏差 | 方舟行为 | 官方 Responses | 归一化策略 |
|---|---|---|---|---|
| 1 | `thinking` 参数 | `{"type":"enabled\|disabled\|auto"}` 控制深度思考（官方 Responses 无此参数） | 用 `reasoning` | 请求侧：官方 reasoning.effort → 方舟 thinking（effort 非 none → enabled） |
| 2 | `caching` 参数+响应字段 | 显式开关前缀缓存 `{type,prefix}`，响应回显 | 无（自动缓存） | 请求侧：透传/忽略；响应侧：剥离回显字段 |
| 3 | `expire_at` | 控制存储 TTL 最大 7 天，响应带 | 无 | 请求侧可透传；响应侧剥离 |
| 4 | `store` 默认 true | 默认保留 3 天 | 默认 false | 请求侧：下游未传 store 时按方舟默认（透传） |
| 5 | `partial: true` | input assistant 消息可带 partial（续写 PreFill） | 无 | 请求侧：下游无此字段，忽略 |
| 6 | `web_search` 工具参数 | **`max_keyword`** | **`search_context_size`** | 请求侧：归一化 `search_context_size` → `max_keyword`；响应侧：web_search_call item 透传 |
| 7 | 内置工具类型 | `image_process`/`knowledge_search`/`mcp` + `ark-beta-*` 头 | 官方无 | 请求侧：未知工具 drop 或透传 + 注入 beta 头 |
| 8 | `context_management.edits` | 上下文编辑 beta（clear_thinking/clear_tool_uses） | 无 | 请求侧：透传/忽略 |
| 9 | 前缀缓存门槛 | 需输入 ≥256 tokens | 无 | 请求侧：忽略（上游自行处理） |
| 10 | 思考流式事件 | `response.reasoning_summary_text.delta`（与官方同名） | 同 | 解析器：标准处理 |

**关键**：流式事件名与官方一致（response.created/in_progress/output_item.added/reasoning_summary_text.delta/output_text.delta/output_item.done/completed + sequence_number）——
**方舟 Responses 解析器主要做请求侧参数归一化，响应侧几乎透传**（除剥离 caching/expire_at 回显字段）。

### 9.4 各家第三方 Responses 偏差（subagent 实测完成 ✅，带 apikey）

**总览**：DeepSeek/MiniMax/Qwen 有 /responses 端点；GLM/Kimi 无（只能走 chat 转换）。

| 平台 | /responses 端点 | 状态 |
|---|---|---|
| DeepSeek | `api.deepseek.com/responses` | ✅ 可用，偏差小 |
| GLM | open.bigmodel.cn 全路径 404 | ❌ 无（官方 issue 仍 open） |
| Kimi | `api.moonshot.cn/v1/responses` → 403 | ❌ 未开放 |
| Qwen | `dashscope.aliyuncs.com/compatible-mode/v1/responses` | ✅ 可用，**偏差最大** |
| MiniMax | `api.minimaxi.com/v1/responses` | ✅ 可用，最接近官方 |

**事件序列**：
- DeepSeek/MiniMax：与官方**完全一致**（response.created→in_progress→output_item.added→content_part.added→reasoning_text.delta→done→...→response.completed + sequence_number）
- Qwen：**思考走 `reasoning_summary_text.delta`**（无 reasoning_text.delta/done）；SSE 帧带 `id:NN` + `:HTTP_STATUS/200` 注释行；`response.created` status 是 `"queued"`；**截断仍报 completed**（需自行判断）

**previous_response_id**：
- Qwen：**真实实现**（服务端有状态，store 默认 true）
- DeepSeek/MiniMax：接受但**忽略**（无状态）→ 转换层自管状态

**id 前缀**：
- DeepSeek/MiniMax：id 无 `resp_` 前缀（裸 UUID）；item id 无 `msg_`/`rs_`（DeepSeek 裸 UUID、MiniMax `<rid>_msg`/`<rid>_rs` 后缀）
- Qwen：有 `resp_` 前缀；reasoning item id 是 `msg_`（官方 rs_）

**错误格式（三家三种）**：
- DeepSeek：`{error:{message,type,param,code}}`（code==type）
- Qwen：`{code,message,request_id}` **无 error 包裹**；流式错误 HTTP 200 + SSE `event:error` + `:HTTP_STATUS/400` 注释——需从注释恢复状态码
- MiniMax：`{error:{message,code}}` 缺 type（code 自有值如 invalid_prompt）
- GLM 404：Spring Boot 格式；Kimi 404：code/scode 系

**web_search**：
- DeepSeek：原生可用（坑：流式 output_item.done 标 failed vs 非流式 completed；queries 混入 `ws_call_id=` marker；URL 追加 `#ws_call_id=`）
- Qwen：原生可用（action 多非标 `query` 单数 + `sources[]`）
- MiniMax：**静默忽略**（声明接受但不执行）
- GLM/Kimi：无端点

**多轮工具调用**：
- DeepSeek：**必须回传 reasoning item 否则 400**（`reasoning_text in the thinking mode must be passed back`）——与 chat 端点同样约束
- Qwen/MiniMax：不需要

**其他**：Qwen max_output_tokens ≥16 下限；MiniMax 默认 reasoning 关闭（官方默认开，需显式传 effort）；Qwen usage 有非标 `x_details`。

**归一化策略（跨平台）**：
1. 响应侧解析器：DeepSeek/MiniMax 几乎透传；Qwen 需把 `reasoning_summary_text.delta` 当 reasoning 处理 + 剥离 SSE 注释行 + 截断判断 + id 前缀归一化
2. 请求侧：DeepSeek 强制回传 reasoning item；MiniMax 显式传 reasoning.effort
3. 错误归一化：三家三种 → 统一 OpenAI `{error:{message,type,param,code}}` 再转下游

## 10. 参考

- docs/messages-to-chat-completions-conversion.md（现有 Messages→chat 设计）
- docs/chat-to-responses-conversion.md（现有 chat→Responses 设计）
