# 设计：API 格式重写层 — Anthropic Messages（下游）⇄ Chat Completions（上游）

> 状态：设计 v2。实证来源：本仓库 `logs/` 真实交换、DeepSeek 官方三套 API 实测（2026-08-05）、
> CLIProxyAPI（router-for-me/CLIProxyAPI）转换实现研究、Anthropic Messages 官方文档（claude-api skill）、
> OpenAI / DeepSeek / xAI Grok 官方文档（codex 查阅，2026-08-05）。

## 0. 目标

为 `codex-provider-proxy` 增加可按 **供应商 + 模型** 配置的 API 格式重写层。第一个实现：

```text
下游客户端（Anthropic Messages API，如 Claude Code）──► 本代理 ──► 上游（OpenAI Chat Completions 兼容）
  POST /v1/messages?beta=true                 POST <base_url>/chat/completions
```

同时保留现有透传路径（未配置转换的 provider 行为不变）与现有 `[[rewrite.model_mappings]]`
字段级改写（两者叠加：先模型映射、后格式转换）。

**为什么是这个方向**：DeepSeek 官方已验证 `https://api.deepseek.com/anthropic`（Messages 兼容层）
+ `https://api.deepseek.com/chat/completions` 双端点并存；配置 `base_url = ".../anthropic"` 的
provider 可直接用官方 Messages 兼容层，第三方转发商只有 Chat Completions 时则需要本转换层。
方向可逆（未来加 Chat→Messages 反向转换只需注册新转换器）。

## 1. 实证基础（已确认事实）

### 1.1 下游侧：Claude Code 的真实 Messages 请求（logs/claude-exchanges 实证）

```json
{
  "model": "claude-opus-4-6",
  "max_tokens": 64000,
  "stream": true,
  "thinking": {"type": "adaptive"},
  "output_config": {"effort": "high"},
  "context_management": {...},
  "metadata": {"client_app": "claude-code", ...},
  "system": [
    {"type": "text", "text": "x-anthropic-billing-header: cc_version=...;", ...},
    {"type": "text", "text": "You are Claude Code...", "cache_control": {"type": "ephemeral"}},
    ...
  ],
  "messages": [
    {"role": "user", "content": [{"type": "text", "text": "...", "cache_control": {...}}, ...]},
    {"role": "assistant", "content": [
      {"type": "thinking", "signature": "EogCCkY...", "thinking": "..."},
      {"type": "tool_use", "id": "tooluse_...", "name": "Agent", "input": {...}},
      ...
    ]},
    {"role": "user", "content": [
      {"type": "tool_result", "tool_use_id": "tooluse_...", "content": "..."},
      ...
    ]}
  ],
  "tools": [
    {"name": "Agent", "description": "...", "input_schema": {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": {...}, "required": [...]}, ...}
  ]
}
```

要点：
- 头部带 `anthropic-beta: claude-code-20250219,...`；`?beta=true` 查询串。
- `system` 是块数组（首块是计费 attribution 文本，**不应转发给上游**）。
- `messages` 里可能混入 `role: "system"` 的中缀消息（4.8+/Fable 5 的 mid-conversation system）。
- 工具定义是 `name/description/input_schema` 三元组，无 `type` 字段；schema 用 2020-12 draft 且带 `$schema`。
- 25 个工具中包括 `WebSearch`/`WebFetch`（Claude Code 把 web search 定义为普通自定义工具+客户端执行，
  而非 Anthropic server tool——但 Anthropic API 的 server tool（`web_search_20260209`）也被 Claude Code 使用；
  两种都要能处理）。

### 1.2 下游侧：Claude Messages 流式事件序列（官方格式，claude-api skill）

```
event: message_start        data: {"type":"message_start","message":{id,type:"message",role:"assistant",model,content:[],usage:{...}}}
event: content_block_start  data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}
event: content_block_delta  data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"..."}}
event: content_block_delta  data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"..."}}
event: content_block_stop   data: {"type":"content_block_stop","index":0}
event: message_delta        data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":N}}
event: message_stop         data: {"type":"message_stop"}
```

### 1.3 上游侧：DeepSeek 官方实测（2026-08-05，provider ds，deepseek-v4-pro/flash）

| 维度 | 实测结果 |
|---|---|
| 模型 | `deepseek-v4-pro`、`deepseek-v4-flash`（无 `deepseek-ai/` 前缀；该前缀是转发商写法） |
| chat 端点 | `POST https://api.deepseek.com/chat/completions`，`Authorization: Bearer` |
| anthropic 端点 | `POST https://api.deepseek.com/anthropic/v1/messages`，`x-api-key` + `anthropic-version: 2023-06-01`（version/beta 头被忽略） |
| responses 端点 | `POST https://api.deepseek.com/responses` 存在（Codex 集成，v4-flash 可用；v4-pro "early August 2026"） |
| 非流式响应 | `choices[0].message` 含 `content`、**`reasoning_content`**（完整推理文本）、`tool_calls`；`content` 在只有 tool_calls 时为 `""`（空串非 null）；`finish_reason: stop/tool_calls/length/content_filter/insufficient_system_resource` |
| usage | `prompt_tokens`/`completion_tokens`/`total_tokens`、`prompt_tokens_details.cached_tokens`、**`completion_tokens_details.reasoning_tokens`**、`prompt_cache_hit_tokens`/`prompt_cache_miss_tokens`（DeepSeek 特有；`prompt_tokens = hit + miss`） |
| 流式 chunk | 首 chunk：`{"delta":{"role":"assistant","content":null,"reasoning_content":""}}`；推理 chunk：`{"delta":{"content":null,"reasoning_content":"We"}}`；**`content` 显式 `null`**；中间 chunk `usage: null`；末 chunk `choices:[]` + 完整 usage（`stream_options.include_usage` 时）；`[DONE]` 收尾 |
| 工具 | `tools` 仅接受 `type:"function"`（≤128 个）；**`{"type":"web_search"}` → 400** `unknown variant 'web_search', expected 'function'`；`function.name` ≤64 字符（字母数字下划线连字符）；无 `parallel_tool_calls` 文档支持 |
| 多轮工具循环 | assistant 消息 `content:""` + `tool_calls[]` 回传，`role:"tool"` 消息跟随，正常接受；**`reasoning_content` 必须原样回传，遗漏会 400**（文档明确：工具调用轮次必须带回推理内容） |
| 思考参数 | 顶层 `thinking: {"type":"enabled"/"disabled"}`（默认 enabled）+ `reasoning_effort: low\|high\|max`（默认 high；`medium`/`xhigh` 被映射为 `high`；Pro 实际支持 high/max，low 按 high 处理）。**不是** OpenAI 官方那套 `reasoning_effort` 全量枚举 |
| 参数 | **`max_tokens`**（文档明确不用 max_completion_tokens）；`response_format` 仅 `text`/`json_object`（**无 json_schema**）；`stop` ≤16 序列；`temperature` 0-2；frequency/presence penalty 已弃用 |
| anthropic 端点输出 | 标准 Anthropic shape：`content` 含 `thinking`（signature = message id 副本）、`text`、`tool_use`（**id 前缀 `call_00_`**，非 `toolu_`）；流式含 `ping` 事件、`message_start.usage` 先填 input_tokens、`content_block_delta.thinking_delta` |
| anthropic 端点兼容矩阵 | `cache_control`/`disable_parallel_tool_use`/`thinking.budget_tokens`/`metadata`（除 user_id）均**忽略**；`image`/`document` 内容块**不支持**；`tool_result.is_error` 忽略；`output_config.effort` 是思考强度入口；模型名可用 `claude-*` 自动映射（opus→v4-pro、sonnet/haiku→v4-flash） |

**含义**：DeepSeek 官方 anthropic 端点已证明"Claude Code 能接受：thinking 块 + 假 signature + `call_00_` 前缀
tool id"。转换层输出按该形状即可被真实 Claude Code 消费。

### 1.4 上游侧：Grok（logs/grok-exchanges 实证 + xAI 官方文档）

- xAI 官方把 `/v1/chat/completions` 标为 **legacy/deprecated**（新能力在 Responses API）；但仍是可用契约，本转换层只面向它。
- 请求：**`max_tokens`**（官方明确"不要写 max_completion_tokens"）、`temperature`、`stream_options.include_usage`、40 个 function 工具、`tool_choice: "auto"`。
- 工具全是标准 function（`search_web`/`search_x` 是普通 function，Grok CLI 客户端执行）；官方 function 上限 **200 个**；`strict` 隐式始终开启。
- 官方文档：**Chat Completions 不返回 reasoning_content**（"No reasoning content returned"）；但 logs 实测（otokapi 中转）流式 `delta.reasoning_content` 存在——转换层必须**容忍两态**（有/无 reasoning_content 都要工作）。
- 官方文档：流式 tool_calls "整体在单个 chunk 返回"；otokapi 实测逐 token 分片（含 index/id/type/function）——同样按分片假设实现（分片假设兼容整块，反之不成立）。
- 流式：末 chunk `choices:[]` + usage（含 `completion_tokens_details.reasoning_tokens`、`prompt_tokens_details.cached_tokens`、Grok 特有 `num_sources_used`/`cost_in_usd_ticks`）；`[DONE]` 收尾；xAI 官方流式每 chunk 带 usage（不依赖 include_usage）。
- `web_search`/`x_search`/`code_interpreter` 是 **Responses API 的原生工具**，Chat Completions 无；Grok 推理模型 `presence_penalty`/`frequency_penalty`/`stop` 不可用（带会报错）。
- 推理：Grok 4.5 `reasoning.effort: low|medium|high`（Responses 命名；SDK 层叫 reasoning_effort），**不能关闭推理**，默认 high。

### 1.5 上游侧：volcengine（logs/china-exchanges 实证）

- 工具数组混用 `function`、`custom`（codex 的 apply_patch/js_repl，带 `format: {type:"grammar",...}`）和
  **`{"type":"web_search","external_web_access":true}`**（火山原生搜索工具）。
- 实测 400：`unknown tool type: custom`——**非 function 工具类型会被拒绝**，转换层必须把
  Anthropic 工具映射为 `function` 类型（保留参数 schema），绝不能原样透传自定义类型。
- 支持 tool_choice 字符串和对象。

### 1.6 OpenAI 官方 Chat Completions（codex 查阅，2026-08-05）

- `tools[]` 联合类型只有 **`function`** 和 **`custom`**——没有 `web_search` 工具项；原生搜索通过
  独立参数 **`web_search_options`**（`search_context_size: low|medium|high`、`user_location`）启用，
  不是 tools 数组里的类型（Responses API 才有 `{"type":"web_search"}`）。
- `messages[].role` 含 `developer`（o1 及更新模型应替代 system）。
- **`max_completion_tokens` 是推荐字段**（含 reasoning tokens）；`max_tokens` 已弃用、与 o 系列不兼容。
- `reasoning_effort: none|minimal|low|medium|high|xhigh|max`。
- `response_format: text|json_object|json_schema`（json_schema 带 name/description/schema/strict）。
- usage：`prompt_tokens_details{cached_tokens,audio_tokens}`、`completion_tokens_details{reasoning_tokens,audio_tokens,accepted_prediction_tokens,rejected_prediction_tokens}`。
- 流式：首 chunk role+content:""；中间 delta；`stream_options.include_usage` 时 `[DONE]` 前有 usage chunk（choices:[]）。
- `parallel_tool_calls` 默认 true；`tool_choice` 四态 + 强制函数对象。

### 1.7 CLIProxyAPI 借鉴（研究摘要，完整报告见 agent 输出）

- 注册表 + `(from,to)` 双索引；无转换器时透传（只改 model）。
- 四个函数签名：请求转换 / 流式响应（逐 chunk 状态机）/ 非流式响应 / 错误转换。
- 流式状态机必要字段：MessageStarted、Text/ThinkingBlockStarted、ContentBlocksStopped、MessageDeltaSent、
  MessageStopSent、SawToolCall、NextContentBlockIndex。
- `message_delta` 延后到 usage 出现才发；强制上游 `stream_options.include_usage`；上游断流（无 `[DONE]`）时合成 `[DONE]` 收尾。
- 先 peek 首 chunk 再定 SSE 响应头（流式错误以 JSON 错误响应下发而非 200+error 事件）。
- 请求侧：`tool_result` 消息先于当前消息发射（OpenAI 要求 tool 消息紧跟 assistant tool_calls）；
  assistant 消息合并 content+reasoning+tool_calls 为一条，content 空写 `""`。
- 错误：状态码透传 + 上游 `error.type` 原样复用（两协议命名兼容）+ 状态码兜底映射表（529→overloaded_error）。
- 安全：thinking/tool_use 只允许映射自 assistant 角色消息（防注入）。
- 名称卫生：工具名大小写映射（Claude Code 严格匹配）、tool id 消毒、schema 归一化（补 `properties:{}`）。
- **web_search 是其空白地带**（无任何处理）——本设计要专门解决。

## 2. 架构

### 2.1 新 workspace crate：`crates/api-conversion/`

保持纯转换逻辑，不依赖 axum/reqwest/proxy 运行时（沿用 docs/api-conversion-layer-plan.md 的原则）。

```text
crates/api-conversion/
  Cargo.toml            # 依赖：serde, serde_json, bytes, futures-util, http（对齐工作区版本）
  src/
    lib.rs              # 公开 API：convert_request / convert_response_non_streaming / convert_response_stream
    dialect.rs          # DownstreamApi / UpstreamApi / ConversionKind 枚举
    error.rs            # ConversionError + Anthropic 风格错误 JSON 渲染
    sse.rs              # 增量 SSE 解析（边界查找）+ 事件字节组装
    capabilities.rs     # 按 供应商+模型 的能力表（搜索工具策略、max_tokens 字段名、reasoning_effort 支持等）
    messages/           # Anthropic Messages 侧最小模型（请求/响应/事件/块）
    chat/               # Chat Completions 侧最小模型（请求/响应/chunk）
    messages_to_chat/   # 请求映射 messages.rs + 响应映射（非流式/流式状态机）chat_response.rs
```

### 2.2 代理侧挂载点（crates/proxy）

```text
现有：incoming → strip base path → 选 provider → rewrite(model_mappings) → 发送上游 → 流回 downstream
新增：
  请求侧（send_upstream_request 之前，rewrite.rs 之后）：
    provider 声明 upstream_api=chat_completions 且请求路径为 /v1/messages
      → buffer body → messages_to_chat::request() → 改目标路径为 /chat/completions
      → 重写 Authorization（走现有 provider 机制）→ 发送
  响应侧（response_stream_and_capture 之后、Body::from_stream 之前）：
    请求已转换 → 响应流经过 chat→messages 转换器（非流式：整体 JSON；流式：SSE 状态机）
    → 修正 content-type（application/json / text/event-stream）与 content-length
```

- 现有 exchange log / statistics capture 记录**原始上游字节**（诊断价值高）；downstream 收转换后字节。
  statistics 的 token 提取已支持 Chat Completions usage 形状，无需改动。
- 透传路径（provider 未声明转换）完全不变。
- 透明重试：每次重试重新执行请求转换（与现有 rewrite 语义一致）。

## 3. 配置设计（按供应商 + 模型）

在 `[providers.X]` 增加方言声明（向后兼容，缺省 = 纯透传，现有配置零改动）：

```toml
[providers.ds]
base_url = "https://api.deepseek.com/"
api_key = "..."
# 新增：上游方言。缺省 "passthrough"。可选 "openai_chat_completions"。
upstream_api = "openai_chat_completions"
# 新增：接受哪些下游方言，缺省 = 与 upstream_api 相同（即透传）。
# 设置为 ["anthropic_messages"] 时，/v1/messages 请求被转换为 /chat/completions。
accept_downstream_apis = ["anthropic_messages"]

# 新增：按模型覆盖能力（provider 级缺省值 + 模型级覆盖）
[providers.ds.models."deepseek-v4-pro"]
max_tokens_field = "max_tokens"        # "max_tokens"（DeepSeek/Grok）| "max_completion_tokens"（OpenAI 官方新模型）
thinking_param = "thinking"            # DeepSeek 用顶层 thinking:{type}；OpenAI/Grok 无此参数 → "none"
reasoning_effort = { enabled = true, levels = ["low","high","max"], default = "high" } # 上游接受的 effort 枚举
server_tools = "drop"                  # server tool 策略：drop | function | passthrough（见 §6）
image_input = false                    # 上游是否支持图像输入（false 时 image 块转占位文本或报错）
parallel_tool_calls = true             # 是否发送 parallel_tool_calls 参数
response_format = "json_object"        # 上游 response_format 支持："text" | "json_object" | "json_schema"
stop_field = "stop"                    # 上游 stop 字段名
```

模型匹配规则沿用现有 `model_mappings` 的经验：模型级覆盖 > provider 级缺省；模型名匹配取
**转换后（model_mappings 生效后）的模型名**。未命中的模型用 provider 级缺省。

Rust 侧新增（config.rs）：

```rust
pub struct Provider {
    pub base_url: Url,
    pub api_key: String,
    pub authorization_header: Option<String>,
    pub upstream_api: UpstreamApi,            // 新
    pub accept_downstream_apis: Vec<DownstreamApi>, // 新
    pub model_capabilities: HashMap<String, ModelCapabilities>, // 新
}

pub enum UpstreamApi { Passthrough, OpenAiChatCompletions }
pub enum DownstreamApi { AnthropicMessages }   // 未来可扩展 OpenAIResponses 等

pub struct ModelCapabilities {
    pub max_tokens_field: MaxTokensField,     // MaxTokens | MaxCompletionTokens
    pub thinking_param: ThinkingParam,        // TopLevelThinking | None
    pub reasoning_effort: Option<ReasoningEffortConfig>, // 枚举 + 默认值
    pub server_tools: ServerToolPolicy,       // Drop | MapToFunction | Passthrough
    pub image_input: bool,
    pub parallel_tool_calls: bool,
    pub response_format: ResponseFormatCap,   // Text | JsonObject | JsonSchema
}
```

## 4. 请求转换：Messages → Chat Completions

### 4.1 顶层字段

| Messages 请求字段 | Chat Completions 字段 | 规则 |
|---|---|---|
| `model` | `model` | model_mappings 已先行改写；此处原样透传 |
| `max_tokens` | `max_tokens` 或 `max_completion_tokens` | 按 `max_tokens_field` 能力配置（DeepSeek/Grok 用 max_tokens；OpenAI 官方新模型用 max_completion_tokens） |
| `stream` | `stream` | 布尔透传；流式时强制附加 `stream_options: {"include_usage": true}`（OpenAI/DeepSeek 支持；xAI 官方流式自带 usage，带上也无害——可配置） |
| `temperature` | `temperature` | 透传；`top_p`/`top_k` 不映射 |
| `stop_sequences` | `stop` | 1 个→字符串，多个→数组；上游不支持 stop 时（Grok 推理模型）剥离 |
| `thinking` + `output_config.effort` | `thinking` 参数 + `reasoning_effort` | 见 §4.4 |
| `tool_choice` | `tool_choice` | `{"type":"auto"}→"auto"`、`{"type":"any"}→"required"`、`{"type":"tool","name":N}→{"type":"function","function":{"name":N}}`、`{"type":"none"}→"none"` |
| `output_config.format`（structured outputs） | `response_format` | `{"type":"json_schema","schema":S}` → `{"type":"json_schema","json_schema":{...}}`（OpenAI）；上游只支持 json_object（DeepSeek）时降级为 `{"type":"json_object"}` 或报 400（按 `response_format` 能力配置） |
| `parallel_tool_use` | `parallel_tool_calls` | 按能力配置（DeepSeek 文档未列此字段——默认不上送；OpenAI/Grok 支持） |
| `metadata` | （丢弃） | 上游无对应字段 |
| `user` | `user` | 透传 |
| `cache_control` / `context_management` / `service_tier` | （丢弃） | 上游无对应；**不得把 `cache_control` 块漏给上游**（会被拒绝） |

### 4.2 system 与消息数组展开

规则（实证 + CLIProxyAPI 校准）：

1. 顶层 `system`（字符串或块数组）→ **合并为一条 `role:"system"` 消息**放在 messages 首位；
   - 丢弃空文本块；
   - 丢弃 `x-anthropic-billing-header:` 前缀块（Claude Code 计费 attribution）；
   - 剥离 `cache_control` 字段。
2. `messages[]` 逐条：
   - `role:"user"` 字符串 content → 原样；块数组 → 逐块映射（见下表），text 块合并为 content 字符串；
   - **`role:"system"` 中缀消息** → 包成 `<system-reminder>...</system-reminder>` 的 user 消息
     （CLIProxyAPI 方案：保留指令可见性，不丢指令）；
   - `role:"assistant"` → **合并为单条 assistant 消息**：text 块→`content`，thinking 块→`reasoning_content`
     （仅当有 GPT 兼容签名或上游接受），tool_use 块→`tool_calls[]`；
     `content` 为空时写 **`""`**（空串，不是 null——DeepSeek 实证）；
     **多个 tool_use 按顺序映射**（OpenAI tool_calls 数组保序）；
   - `role:"user"` 中的 `tool_result` 块 → **独立的 `role:"tool"` 消息**，且**先于**本 user 消息发射
     （OpenAI 要求 tool 消息紧跟 assistant tool_calls）；多个 tool_result → 多条 tool 消息
     （OpenAI 无并行结果合并需求，一条 result 一条 tool 消息最稳）；
     `is_error: true` → content 前加 `[error] ` 前缀或原样（选：原样透传 content，多数上游不检查）。
3. content 块映射表：

| Messages 块 | Chat 产物 |
|---|---|
| `text` | `content` 文本 |
| `image` | `{"type":"image_url","image_url":{"url":"data:<media_type>;base64,<data>"}}`（`image_input` 能力开关） |
| `thinking` | `reasoning_content`（仅 assistant 角色；见 §4.4 签名门控） |
| `redacted_thinking` | 丢弃 |
| `tool_use` | `tool_calls[]`（仅 assistant 角色，防注入） |
| `tool_result` | `role:"tool"` 消息 |
| `document`（PDF） | 丢弃或报 400（按 image_input 同级能力，默认报 400 提示不支持） |
| `server_tool_use` / `web_search_tool_result` 等 | 丢弃（转换层不支持服务端工具回放） |

### 4.3 工具定义

| Messages 工具 | Chat 产物 |
|---|---|
| 自定义工具（name/description/input_schema） | `{"type":"function","function":{"name","description","parameters": input_schema}}`；**`parameters` 若缺省 `properties` 则补 `{"properties":{}}`**（部分上游拒绝无 properties 的 object schema）；剥掉 `$schema`（Chat 上游一般接受，但为兼容剥掉 2020-12 的 `$schema` 更稳——可配置）；`strict: true` → 透传 |
| server tool（`web_search_20260209`/`web_fetch_20260209`/`code_execution_*`/`tool_search_*`） | 按 `server_tools` 策略（§6） |

### 4.4 reasoning / effort 映射

**请求侧**（下游 `thinking` + `output_config.effort` → 上游思考控制），按能力配置分两类：

1. 上游有 `thinking` 参数（DeepSeek）：
   - `thinking: {type: adaptive}` 或 `{type: enabled, budget_tokens}` → `thinking: {"type":"enabled"}`；
     `{type: disabled}` → `{"type":"disabled"}`（DeepSeek 默认 enabled）；
   - `output_config.effort` → `reasoning_effort`，**钳制到上游枚举**：DeepSeek 只接受
     `low|high|max`（medium/xhigh 官方映射为 high；Pro 的 low 按 high 处理）——转换层把
     Claude 的 medium/xhigh/max 按能力表 `levels` 就近映射（medium→high、xhigh→high、max→max）；
   - `budget_tokens`（旧模型）→ 阈值映射（CLIProxyAPI：512–1024→low、1025–8192→medium、
     8193–24576→high、>24576→xhigh，再钳制到上游枚举）。
2. 上游无 `thinking` 参数（OpenAI/Grok）：`effort` → `reasoning_effort` 原样/钳制；
   缺省值按能力配置（DeepSeek 默认 high；OpenAI 模型默认值各异——缺省不写，让上游用默认）。

**多轮回放（关键，DeepSeek 实证）**：下游 Claude Code 会把历史 assistant 消息（含 thinking 块）
原样回传。转换层必须把 assistant 历史消息里的 `thinking` 块映射为 `reasoning_content` **一并回传**——
DeepSeek 文档明确：携带 `tools` 的请求里，工具调用轮次的 reasoning_content 缺失会 **400**。
丢弃 thinking 块只在"上游不接受回传推理内容"（Grok Chat 官方不返回，但对回传容忍度未知）时配置为
可选；缺省 = 回传。

**响应侧**：上游 `reasoning_content`（非流式 message 或流式 delta）→ 下游 `thinking` 块；
thinking 块 `signature` 用合成值（DeepSeek 官方用 message id 副本，Claude Code 接受——选
message id 副本，零额外状态）。Grok Chat 官方不返回 reasoning_content（otokapi 中转有）——
转换器无 reasoning_content 时正常工作（无 thinking 块即可）。

## 5. 响应转换：Chat Completions → Messages

### 5.1 非流式

| Chat 响应字段 | Messages 字段 |
|---|---|
| `choices[0].message.content` | `content[]` 中的 `text` 块 |
| `choices[0].message.reasoning_content` | `content[]` 中**前置**的 `thinking` 块 |
| `choices[0].message.tool_calls[]` | `content[]` 中的 `tool_use` 块（id/name/input；arguments 解析为 JSON object） |
| `choices[0].finish_reason` | `stop_reason`（映射表见 5.3） |
| `id` | 合成 `msg_<...>`（上游 id 消毒后复用，DeepSeek 实测上游 id 是 uuid 非 `msg_` 前缀——**必须加前缀** `msg_`） |
| `model` | `model` 原样 |
| `usage.prompt_tokens` | `usage.input_tokens` |
| `usage.completion_tokens` | `usage.output_tokens` |
| `usage.prompt_tokens_details.cached_tokens` | `usage.cache_read_input_tokens` |
| `usage.completion_tokens_details.reasoning_tokens` | `usage.output_tokens_details.thinking_tokens` |

Anthropic 语义：`input_tokens + cache_read_input_tokens ≈ prompt_tokens`（cache 不计入 input）。
CLIProxyAPI 取 `input = prompt - cached`；本设计取**照实映射**（input=prompt_tokens，cache_read=cached_tokens），
与 DeepSeek 官方 anthropic 端点行为一致（实测其 message_start 里 input_tokens 与 cache_read 同时非零）。
⏳ 与 Claude Code 的计费显示兼容性可在集成测试验证。

`stop_reason` 映射：
`stop→end_turn`、`length→max_tokens`、`tool_calls→tool_use`、`content_filter→end_turn`、`function_call→tool_use`（旧式）、缺省→`end_turn`。

### 5.2 流式（SSE 状态机）

输入：上游 SSE 行（`data: {...}` 逐 chunk）。输出：0..N 个 Messages SSE 事件。

状态机（每 chunk 调用，跨 chunk 保持状态）：

```text
首 chunk（含 choices[0].delta，无论有无 role——DeepSeek 首 chunk 有 role，部分上游没有）：
  → 发 message_start（id=msg_<上游id>，model，usage 全 0 占位）
    ⚠️ DeepSeek 首 chunk 的 delta.content 是显式 null、usage 是显式 null —— 必须容忍

delta.reasoning_content（非空片段）：
  → 若当前是 text 块：先 content_block_stop
  → 若 thinking 块未开：content_block_start(index, {type:"thinking", thinking:"", signature:""})
  → content_block_delta(index, {type:"thinking_delta", thinking:<片段>})

delta.content（非空片段）：
  → 若当前是 thinking 块：先 content_block_stop
  → 若 text 块未开：content_block_start(index, {type:"text", text:""})
  → content_block_delta(index, {type:"text_delta", text:<片段>})

delta.tool_calls[]（按 index 累积；id/name/arguments 可能分多个 chunk 到达——DeepSeek 一次给全，
  部分上游分片）：
  → id 只接受非空字符串；name 只在块未开始前记录；arguments 逐 chunk 拼接
  → id+name 齐备时：content_block_start(index, {type:"tool_use", id, name, input:{}})
  → 后续 arguments 增量：content_block_delta(index, {type:"input_json_delta", partial_json:<累积>})
    （⚠️ input_json_delta 的 partial_json 是"到目前为止的累积字符串"，不是增量——查 Anthropic 规范确认 ⏳）

finish_reason 或 [DONE]：
  → 若已发出 tool_use 块但 finish_reason 缺失 → 按 tool_calls 处理
  → finish_reason="tool_calls" 但从未发 tool_use 块 → 降级 stop（防"宣布 tool_use 但零块"）
  → 收尾所有打开的块（thinking/text/tool_use 各 content_block_stop；tool_use 停时补
    content_block_delta(input_json_delta, 完整累积 arguments)）
  → message_delta（delta: {stop_reason}, usage: {output_tokens}）——usage 从末 chunk 提取；
    上游没给 usage 时补 0 值
  → message_stop（恰好一次，恒最后）

[数据质量]：usage 只在末 chunk（choices:[] 时）出现——首 chunk 到末 chunk 之间都是 null/缺失。
```

**流式收尾的健壮性**（CLIProxyAPI 实证）：
- 上游断流无 `[DONE]`：转换器必须仍发 message_delta + message_stop（流结束即收尾）。
- 上游 `[DONE]` 后可能尾随非规范 chunk（Grok 实测 `[DONE]` 是最后一个，但部分上游会追加）→ 忽略。

### 5.3 错误响应转换

- 上游非 2xx JSON：`{"error":{"message","type","code","param"}}` → 下游
  `{"type":"error","error":{"type":<上游type或状态码映射>,"message":<上游message>}}`，
  **HTTP 状态码透传**。
- 状态码兜底映射（纯文本错误体时）：401→authentication_error、403→permission_error、404→not_found_error、
  413→request_too_large、429→rate_limit_error、5xx→api_error、529→overloaded_error、其余→invalid_request_error。
- **流式中途错误**（上游 200 后出现非 SSE JSON）：已发 200 时追加 `event: error\ndata: {...}\n\n` 事件后关闭
  （Anthropic 流式错误事件规范）；若尚未发任何事件（peek 阶段）则改为整包 JSON 错误响应。
- 转换层自身的 400（下游字段无法表示）：Anthropic 风格 `invalid_request_error`，message 指明
  字段与原因（如 `image input is not supported by provider ds model deepseek-v4-pro`）。

## 6. 搜索工具设计（重点）

**问题**：Claude Code 会发送搜索类工具，但各家 Chat Completions 上游的"搜索"形态完全不同：

| 上游 | 搜索形态 | 实证 |
|---|---|---|
| Anthropic 官方 | server tool：`{"type":"web_search_20260209","name":"web_search"}` + `web_fetch_20260209`；服务端执行，响应 `web_search_tool_result` 块 | skill 文档 |
| Claude Code 客户端 | WebSearch/WebFetch 声明为自定义工具形态，但**搜索在 model provider 服务端执行**（Anthropic/DeepSeek anthropic 端点识别工具名并返回 `server_tool_use`+`web_search_tool_result` 块） | logs/claude-exchanges 实证 |
| DeepSeek | **chat completions 无搜索**（`{"type":"web_search"}` → 400）；**anthropic 端点有服务端搜索**（返回百度百科等中文源）；Responses 端点有原生 web_search | 本次实测 + logs 实证 + 官方文档 |
| OpenAI 官方 Chat Completions | `tools[]` 无 web_search 类型（只有 function/custom）；原生搜索是独立参数 **`web_search_options`**（`search_context_size`） | codex 查阅 |
| volcengine（火山） | Chat API 原生工具类型 `{"type":"web_search"}`（+ max_keyword/limit/sources/user_location）；**仅 Responses API 支持**，且 PDF 文档无 `external_web_access` 字段（logs 里出现过的该形态来自更新文档）；web_search 结果以事件流/注解返回非 tool_calls | 官方 PDF（subagent 解析 2175 页）+ logs 实证 |
| Grok | Chat Completions **无搜索**（`web_search`/`x_search` 只在 Responses API）；`search_web`/`search_x` 是**普通 function 工具**（Grok CLI 客户端执行） | logs 实证 + 官方文档 |

**设计决策**：`server_tools` 策略按 供应商+模型 配置，三种取值：

1. **`drop`（缺省）**：删除 `web_search_*`/`web_fetch_*`/`code_execution_*`/`tool_search_*` 等
   server tool 定义，不上游。效果：模型无法调用搜索，工具循环安全收敛（无工具 = 无调用 =
   无断链）。对 DeepSeek/Grok（无原生搜索）是唯一安全默认。**同时把删除行为记入日志**
   （warn 级，标明 provider/model/tool 名），并可配置为静默。
2. **`function`**：转成 function 工具——
   - `web_search_20260209` → `{"type":"function","function":{"name":"web_search","description":<原描述或生成>,"parameters":{"type":"object","properties":{"query":{"type":"string","description":"The search query"}},"required":["query"]}}}`；
   - `web_fetch_20260209` / Claude Code 的 `WebFetch` → 同理（url 参数）；
   - **注意**：Chat Completions 上游没有服务端搜索。`map_to_function` 后模型会调用
     `web_search` function，但上游不执行搜索——实测 Claude Code 收到 `tool_use(web_search)`
     后尝试客户端执行，但其搜索通道依赖 Anthropic 凭据，在 DeepSeek 通道下返回空结果。
     因此对无搜索的 chat 上游，`drop` 比 `map_to_function` 更诚实（模型不调用 = 无假结果）。
   - 对 `code_execution_*`：**不提供 function 化**（无对应客户端工具语义），一律 drop 并 warn。
3. **`passthrough`**：原样发送（适用于工具形状恰好兼容的上游）。
   **实测限制（2026-08-06）**：火山方舟原生 `web_search` 工具形状是
   `{"type":"web_search"}`（无 function 包裹），Anthropic 的
   `{"type":"web_search_20260209","name":"web_search"}` 原样透传会被拒
   （400 `missing tools.function`）；glm 的是 `{"type":"web_search","web_search":{...}}`
   （服务端执行，响应顶层 `web_search` 数组）。两者都需要**专门的 provider 工具映射**
   （Anthropic server tool → provider 特有形状），列为 ServerToolPolicy 的未来扩展；
   当前对这些上游用 `drop` 最稳。

**OpenAI 官方上游的附加选项**（可选扩展，Phase 4 之后）：下游 server tool 存在时，若上游是
OpenAI 官方且支持 `web_search_options`，可把"存在 web_search 工具"翻译为请求级
`web_search_options: {"search_context_size": <按配置>}` 而非 tools 项——需要 Claude Code 侧
配合理解 `web_search_tool_result` 块回放，列为远期。

**验证计划**：真实 Claude Code 会话 + DeepSeek/Grok 上游，分别验证 drop（搜索不可用但会话正常）
与 function（客户端执行搜索）两种路径；volcengine 走 passthrough。

### 6.5 其他服务端工具盘点（2026-08-06 调研结论）

除 web_search/web_fetch/code_interpreter 外，各家 chat completions 的其他服务端工具可映射性：

| 提供方 | 工具 | Chat 可用 | 映射 Anthropic server tool | 结论 |
|---|---|---|---|---|
| GLM | retrieval（知识库） | ✅ | ❌ Anthropic 无知识库检索 server tool | 不可映射；只能代理侧自定义 function |
| GLM | mcp | ✅ | ⚠️ 与 Anthropic MCP Connector（beta mcp-client-2025-11-20）字段同构 | **但 Claude Code 实际不用 MCP Connector**——把 MCP 工具作为 custom 工具（`mcp__server__tool` 前缀）透传，已覆盖 |
| 火山 | web_search/image_process/knowledge_search/doubao_app/mcp | ❌ **官方明确仅 Responses API** | ❌ 协议层不兼容 | 不可映射（除非加 Responses 后端） |
| 千问 | MCP/image_search/图像视频生成/Omni 音频 | ❌ 仅 Responses 或独立 API | ❌ | 不可映射 |
| kimi | Formula（web_search/fetch/code_runner/quickjs/memory 等 12 个） | ✅ | ⚠️ 语义对应但**代理需执行 fibers**（客户端执行模式） | Claude Code 客户端执行不了 kimi fibers → 实际不可用；`$web_search` builtin 多一次回声回路 |

**结论**：无可新增映射。Claude Code 的 MCP 工具（`mcp__*`）按 custom function 透传已天然支持；各家"服务端工具"要么协议不兼容（火山/千问仅 Responses），要么需代理端执行（kimi fibers）。

## 7. 与其他功能的交互

- **model_mappings**：先执行（body 里 model 已改写），转换层读改写后的 model 选能力配置。
- **透明重试**：每次重试重新做请求转换（复用现有"每重试重新 resolve provider + 重放 body"语义）；
  若重试选中的 provider 不支持转换 → 返回转换错误（同现有 rewrite 语义）。
- **reject_messages_count_tokens**：仍拦截 `count_tokens`（下游路径判断不变）。
- **drop_responses_slow_down_errors**：只作用于 `*/responses` SSE，与 messages 转换互不干扰。
- **convert_429_to_503**：作用于下游状态码，转换后仍生效。
- **exchange log**：新增元数据字段 `conversion`（如 `messages_to_chat_completions`）与
  `upstream_api`/`downstream_api`；请求体日志保留**下游原始字节**，另存转换后请求体文件
  （`*.converted_request_body.bin`）；响应体日志保留**上游原始字节**，另存转换后响应
  （`*.converted_response_body.bin`）。`reconstruct_responses` 对转换请求重建下游形态。
- **statistics**：token 提取读上游原始 usage（已支持 Chat Completions 形状），无需改。

## 8. 测试策略

### 8.1 转换 crate 单元测试
- 全字段映射表逐条（请求方向 + 响应方向非流式）。
- 消息展开：system 合并/billing 块剥离/中缀 system→system-reminder/tool_result 提前/
  assistant 合并与 content=""。
- 工具转换：schema 补 properties、$schema 剥离、server tool 三种策略。
- 流式状态机：DeepSeek 真实 chunk 序列回放（首 chunk content=null、usage=null、末 chunk choices=[]）
  产出期望事件序列；断流收尾；[DONE] 后尾随 chunk 忽略；finish_reason=tool_calls 但零工具块降级。
- 错误转换：OpenAI error JSON → Anthropic error JSON；状态码映射表。

### 8.2 代理集成测试（现有 spawn_test_server 模式）
- 配置 provider upstream_api=chat_completions + mock 上游：下游 /v1/messages 请求在上游以
  /chat/completions 到达且 body 正确；响应以 Messages 形状返回。
- 未配置 provider 纯透传回归。
- 重试路径重转换。
- 流式端到端（mock 上游发 DeepSeek 形状 chunk）。

### 8.3 真实上游验证（用户环境）
- DeepSeek 官方 chat completions：Claude Code → 代理 → DeepSeek，完整 agent 循环
  （思考→工具→结果→回答）跑通；搜索工具 drop 路径正常。
- Grok（otokapi）与 volcengine：同上 + passthrough 搜索路径。

## 9. 实施阶段

1. **Phase 1**：crate 骨架 + 配置解析（upstream_api/accept_downstream_apis/models 能力表）+ 请求侧
   非流式文本转换 + 响应侧非流式 JSON 转换（含错误映射）。单测 + mock 集成测试。
2. **Phase 2**：流式状态机（text/thinking/tool_calls 全形态），DeepSeek 真实 chunk 序列测试。
3. **Phase 3**：多轮工具循环完整验证（真实 DeepSeek）；工具定义/schema 归一化；能力配置细化。
4. **Phase 4**：搜索工具策略实现 + 真实 Claude Code 验证（drop → function → passthrough）。
5. **Phase 5**：exchange log 元数据与转换体文件；README/config.example 更新。

## 10. 待确认事项（实现前需实测/验证）

- Claude Code 对"web_search 作为普通 function 工具回传 tool_use"的接受度（集成实测，§6 首选路径）。
- `input_json_delta.partial_json` 的语义（累积 vs 增量）——Anthropic 官方规范确认。
- DeepSeek `parallel_tool_calls` 是否实际接受（文档未列——默认不上送，实测确认）。
- Grok（otokapi 中转）对 `reasoning_content` 回传的接受度（官方 Chat 不返回推理内容，中转商有——
  回传是否 400 需实测）。
- Anthropic `usage` 的 `input_tokens` 与 `cache_read_input_tokens` 语义在 Claude Code 计费显示中的
  兼容性（照实映射 vs CLIProxyAPI 的 input=prompt-cached）——集成测试验证。

## 11. 参考资料

- 本仓库 `docs/api-conversion-layer-plan.md`（旧计划：Responses→Chat，架构原则可复用）
- CLIProxyAPI：https://github.com/router-for-me/CLIProxyAPI（`internal/translator/openai/claude/` 为主方向转换）
- OpenAI Chat Completions 参考：https://platform.openai.com/docs/api-reference/chat/create
- DeepSeek 文档：https://api-docs.deepseek.com/
- xAI Grok 文档：https://docs.x.ai/developers/（Chat Completions 已标 legacy）
- Anthropic Messages：https://platform.claude.com/docs/en/api/messages
- 本仓库实证日志：`logs/claude-exchanges`、`logs/grok-exchanges`、`logs/china-exchanges`、`logs/exchanges`
