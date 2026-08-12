# 设计：Chat Completions（上游）→ Responses API（下游）转换

> 状态：设计 v1（2026-08-06）。方向：上游是 OpenAI Chat Completions 兼容供应商（DeepSeek/glm/kimi/火山/千问），
> 下游客户端是 OpenAI Responses API 客户端（如 Codex CLI）。
> 资料来源：codex 调研 OpenAI 官方文档（2026-08-06）+ DeepSeek /responses 端点实测。

## 0. 目标

```text
下游客户端（OpenAI Responses API，如 Codex CLI）──► 本代理 ──► 上游（Chat Completions 兼容）
  POST /v1/responses                       POST <base_url>/chat/completions
```

与现有 Messages→Chat 方向正交：新增 `DownstreamApi::OpenAIResponses`，provider 声明
`accept_downstream_apis = ["openai_responses"]` 时启用。

## 1. 下游侧：Responses API 协议（codex 调研 + 实测）

### 1.1 请求（下游 → 转换 → 上游 chat）

| Responses 请求字段 | Chat Completions 字段 | 规则 |
|---|---|---|
| `model` | `model` | 透传 |
| `input` 字符串 | `messages: [{role:"user", content: input}]` | 字符串等价于 user 文本 |
| `input` item 数组 | `messages` | 逐 item 展开（见 1.2） |
| `instructions` | 首条 `system` 消息 | 有则放最前 |
| `max_output_tokens` | `max_tokens` 或 `max_completion_tokens` | 按 `max_tokens_field` 能力配置 |
| `temperature`/`top_p` | 透传 | |
| `reasoning.effort` | `reasoning_effort` / `thinking` | 按能力配置钳制 |
| `text.format` | `response_format` | `json_object`/`json_schema` → 对应 |
| `tools` | `tools` | 扁平 function → 嵌套 function；内置工具按 server_tools 策略 |
| `tool_choice` | `tool_choice` | `"auto"/"required"/"none"` 字符串直接对应；`{type:"function",name}` → `{type:"function",function:{name}}` |
| `parallel_tool_calls` | `parallel_tool_calls` | 透传 |
| `stream` | `stream` | 透传；流式加 `stream_options.include_usage` |
| `include`/`previous_response_id`/`store`/`metadata`/`user` | （丢弃） | 上游无对应；state 由代理侧管理（见 §5） |

### 1.2 input item 展开（响应→chat 消息）

| Responses input item | Chat 消息 |
|---|---|
| `{type:"message", role:"user"/"developer"/"system", content}` | system→`role:"system"`（或合并进 instructions）；user→`role:"user"`；developer→`role:"system"`（上游无 developer 时） |
| `{type:"message", role:"assistant", content}` | `role:"assistant"`（text 合并，tool_use 见下） |
| `{type:"function_call", call_id, name, arguments}` | assistant 消息的 `tool_calls[]`（id=call_id） |
| `{type:"function_call_output", call_id, output}` | `role:"tool"` 消息（tool_call_id=call_id） |
| `{type:"reasoning", ...}` | 丢弃（或映射 thinking） |
| `{type:"item_reference", id}` | 需代理状态展开（§5） |
| `{type:"web_search_call"}` 等内置 | 丢弃 |

**注意**：`function_call` 和同属一个 assistant turn 的文本，需合并进同一 assistant 消息
（OpenAI 要求 tool 消息紧跟 tool_calls）。`function_call_output` 对应 tool 消息，先于后续 user 文本发射。

### 1.3 响应（上游 chat → 转换 → 下游 responses）

**非流式**：Chat `chat.completion` → Responses `response` 对象：

| Chat 字段 | Responses 字段 |
|---|---|
| `id` | `id`（合成 `resp_` 前缀；上游 uuid → `resp_<uuid>`） |
| `object:"chat.completion"` | `object:"response"` |
| `created` | `created_at` |
| `model` | `model` |
| `choices[0].message.content` | `output[]` 中 message item 的 `content[].output_text` |
| `choices[0].message.reasoning_content` | `output[]` 中 reasoning item（`summary` 或直接 text） |
| `choices[0].message.tool_calls[]` | `output[]` 中 function_call items（`call_id`=tool_call.id） |
| `choices[0].finish_reason` | `status`: stop→completed、length→incomplete、tool_calls→completed、content_filter→incomplete |
| `usage.prompt_tokens` | `usage.input_tokens` |
| `usage.completion_tokens` | `usage.output_tokens` |
| `usage.prompt_tokens_details.cached_tokens` | `usage.input_tokens_details.cached_tokens` |
| `usage.completion_tokens_details.reasoning_tokens` | `usage.output_tokens_details.reasoning_tokens` |

**流式**：Chat SSE chunks → Responses SSE 事件（实测 DeepSeek /responses 形状）：

```
response.created        ← 首 chunk（含 response 对象，status:in_progress）
response.in_progress
response.output_item.added     ← 每个 output item（reasoning/message/function_call）
response.content_part.added    ← 文本 part
response.reasoning_text.delta  ← thinking 增量
response.output_text.delta     ← 文本增量
response.function_call_arguments.delta ← 工具参数增量（分片拼接）
response.function_call_arguments.done
response.output_item.done
response.completed     ← 结束（含完整 response + usage）
```

- **结束信号是 `response.completed`**，不是 `[DONE]`（实测确认）。
- 事件带 `sequence_number`（递增）、`output_index`/`content_index`/`item_id`。
- `output_text.delta` 按 `(output_index, content_index)` 聚合，不能只按全局顺序。

## 2. 上游侧：Chat Completions 各家形状（已有）

完全复用现有 Messages→Chat 的上游适配：max_tokens 字段、thinking、reasoning_effort、server_tools
策略（搜索/代码/图片）。**上游行为不变，只是下游从 Messages 换成 Responses。**

## 3. 配置

```toml
[providers.ds]
base_url = "https://api.deepseek.com/"
api_key = "..."
upstream_api = "openai_chat_completions"
accept_downstream_apis = ["anthropic_messages", "openai_responses"]  # 可同时接受两种下游
# capabilities 复用现有（max_tokens_field/thinking_param/server_tools/...）
```

新增 `DownstreamApi::OpenAIResponses`；转换选择逻辑：path 是 `/responses` 且
`accept_downstream_apis` 含 `openai_responses` → 用 Responses 转换。

## 4. 转换选择与路径

- 请求路径 `/responses`（或 `responses`）→ 上游路径 `/chat/completions`（provider 转换时）。
- 与 Messages 并存：同一 provider 可同时接受两种下游，按 path 区分。
- 现有 `drop_responses_slow_down_errors` 只作用于 `*/responses` SSE——新方向下该功能
  需审视（它假设 Responses 流式是 `response.failed` 事件，实测 DeepSeek 是 `error` 事件）。

## 5. 状态：previous_response_id（关键差异）

Responses 客户端（Codex）用 `previous_response_id` 续接，而 Chat 上游无状态。
代理需维护本地状态（同旧计划 §Stateful Responses Compatibility）：

```rust
pub struct ResponseState {
    pub response_id: String,          // 合成的 resp_* id
    pub provider_name: String,
    pub model: String,
    pub chat_messages: Vec<ChatMessage>, // 完整 chat 转录
    pub created_at_unix: u64,
    pub expires_at_unix: u64,
}
```

- 每次转换后存 chat 转录；`previous_response_id` 时加载转录、追加新 input，再转换。
- TTL + 最大条数淘汰；绑定 provider（跨 provider 续接拒绝）。
- **非流式必须生成完整 output[]**（Codex 依赖 reasoning/tool item），不能只返回 message。

## 6. 流式状态机（chat→responses）

复用 Messages→Chat 的 ChatStreamConverter 思路，但输出 Responses 事件：

- 状态：`response_id`、`output_index`、`item_id`、`content_index`、函数参数缓冲、`sequence_number`。
- 首 chunk → `response.created` + `response.in_progress`。
- `delta.reasoning_content` → reasoning item（`output_item.added` + `reasoning_text.delta`）。
- `delta.content` → message item（`content_part.added` + `output_text.delta`）。
- `delta.tool_calls[]` → function_call item（`output_item.added` + `function_call_arguments.delta` 分片）。
- finish_reason → 收尾 items（`output_item.done`），最后 `response.completed`（含 usage）。
- 上游断流无 finish → 仍发 `response.completed`（或 `response.incomplete`）保证客户端不挂。

## 7. 错误转换

- 上游非 2xx JSON error → 下游 Responses 错误形状：`{"error":{message,type,param,code}}`（保留 status）。
- 流中 `error` 事件 → 下游 `error` SSE 事件（不伪造 response.completed）。

## 8. 实施阶段

1. **Phase 1**：`DownstreamApi::OpenAIResponses` + 请求转换（input→messages、tools 扁平→嵌套）+ 非流式响应（chat→response 对象）。单测。
2. **Phase 2**：流式状态机（chat chunks → responses 事件）。用 DeepSeek /responses 实测事件对齐。
3. **Phase 3**：`previous_response_id` 状态 + `response_id` 合成 + 转录。Codex CLI 实测。
4. **Phase 4**：内置工具（web_search 等）在 responses 下的形态 + 错误转换完善。

## 9. 参考

- codex 调研：OpenAI Responses create / streaming / function-calling / responses-vs-chat-completions
- 本仓库旧计划：docs/api-conversion-layer-plan.md（Responses→Chat，架构可复用）
- DeepSeek /responses 实测（2026-08-06）：事件序列、function_call 流式、web_search_call
