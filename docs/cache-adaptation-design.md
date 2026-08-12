# 通用缓存适配机制设计

状态：**已实现**（2026-08-08 完成 §4 实现计划第 1-8 步；第 10 步真实回归部分完成）
调研报告：`/tmp/codex_relay/answer_cache_{protocols,ark,glm_kimi,minimax_mimo,ds_grok,qwen}.md`

## 1. 调研结论：三协议 × 各家缓存能力矩阵

### 1.1 协议层官方机制

| 维度 | Anthropic Messages | OpenAI Responses | OpenAI Chat |
|---|---|---|---|
| 显式块级断点 | `cache_control: {"type":"ephemeral"}`（system/text/image/tool_use/tool_result） | `prompt_cache_breakpoint: {"mode":"explicit"}`（input_text/image/file） | `prompt_cache_breakpoint`（content part） |
| 顶层参数 | 顶层 `cache_control`（自动断点放最后可缓存块） | `prompt_cache_key` + `prompt_cache_options{ mode: implicit\|explicit, ttl: 30m }` | 同 OpenAI（prompt_cache_key / prompt_cache_options） |
| 自动缓存 | 需 cache_control | 默认 implicit（1024+ tokens 精确前缀） | 1024+ tokens 精确前缀 |
| TTL | 5m（默认）/ 1h | 30m（唯一值） | 30m |
| 命中报告 | `cache_read_input_tokens` / `cache_creation_input_tokens` | `input_tokens_details.cached_tokens` / `cache_write_tokens` | `prompt_tokens_details.cached_tokens` / `cache_write_tokens` |
| 隔离 | 组织/workspace | 组织 + `prompt_cache_key` 路由 | 同 OpenAI |

### 1.2 各家 provider

| Provider | 块级 cache_control | prompt_cache_key | 自动缓存 | 显式缓存机制 | 命中报告（chat 上游） |
|---|---|---|---|---|---|
| Ark | ❌（文档无） | ❌ | ✅ 隐式（1024+） | Responses: `caching{type,prefix}` + store + previous_response_id + expire_at(7d)；Context API: mode/ttl/context_id | `prompt_tokens_details.cached_tokens` |
| GLM | ❌ | ❌ | ✅ 隐式（内容相似度） | 无 | `prompt_tokens_details.cached_tokens` |
| Kimi | ❌ | ✅ Chat 顶层（路由提示） | ✅ 前缀（>256） | 无（自动 + key 路由） | **`usage.cached_tokens`（顶层，非 details 内）** |
| MiniMax | ✅ anthropic 端点（5m/4 断点） | ✅ responses 端点 | ✅ 自动（512+） | 透传 cache_control / prompt_cache_key | 按协议原生 |
| MiMo | ❌（未定义参数会被过滤/报错） | ❌ | ✅ 自动 | 无 | 按协议原生 |
| DeepSeek | ❌（Ignored） | ❌（responses 明确不支持） | ✅ KV 前缀单元 | 无（`user_id` 做 KV 隔离） | `prompt_cache_hit_tokens`/`prompt_cache_miss_tokens` |
| Qwen | ✅ Chat/DashScope/Anthropic（块级 ephemeral，5m） | ❌（responses 用 header `x-dashscope-session-cache` + previous_response_id） | ✅ 隐式（256+） | cache_control 透传 | `prompt_tokens_details.cached_tokens` |
| Grok | ❌（legacy 已废弃） | ✅ Chat（→`x-grok-conv-id`）/Responses（sticky routing） | ✅ 自动 | key 路由 | `prompt_tokens_details.cached_tokens` |

### 1.3 关键约束（来自文档，实测/文档矛盾以文档为准）

- **DeepSeek**：`cache_control` 在 anthropic 兼容端点显式标注 Ignored；`prompt_cache_key/prompt_cache_retention` 在 responses 兼容页明确不支持（静默忽略）。`user_id`（chat）/`metadata.user_id`（anthropic）做 KVCache 隔离。
- **Ark**：Responses 显式前缀缓存创建需 `stream=false`、input≥256 tokens、`instructions` 空、`thinking` 与后续一致、`tools` 仅首轮；Web Search/Knowledge Search/MCP/豆包助手场景 `caching` 无效；低延迟 `service_tier=fast` 仅隐式。
- **MiMo**：未定义请求参数可能被过滤或报错——不能透传 prompt_cache_key。
- **Qwen**：Responses 缓存是 header `x-dashscope-session-cache: enable` + `previous_response_id`（Session 缓存），不是请求体参数；`prompt_cache_key` 响应里为 null 表示不支持。

## 2. 现有实现差距（对照调研）

| # | 差距 | 现状 | 需要 |
|---|---|---|---|
| 1 | ChatUsage 提取不全 | 只读 `prompt_tokens_details.cached_tokens` | 补 DeepSeek `prompt_cache_hit_tokens`、Kimi 顶层 `cached_tokens`、`cache_write_tokens` |
| 2 | 缓存能力无配置 | ModelCapabilities 无缓存字段 | 新增缓存能力配置（见 §3.2） |
| 3 | 块级 cache_control 无条件透传 | IrContent extra 保留，to_responses/to_chat 原样带出 | 按 provider 能力剥离（GLM/MiMo/DeepSeek/Grok/Ark）或转 prompt_cache_breakpoint（OpenAI 官方协议） |
| 4 | 顶层 cache_control 无条件写回 | apply_context 一律 out.entry("cache_control") | 按 provider 能力（MiniMax/Qwen 支持，其余剥离） |
| 5 | prompt_cache_key 无条件映射 | apply_context 对 responses 目标一律写 prompt_cache_key | 按 provider 能力（Kimi/Grok/MiniMax 支持，DeepSeek/MiMo/Qwen 剥离）；Chat 目标也需支持（Kimi/Grok 的 chat 端点） |
| 6 | Ark 显式缓存未实现 | 无 | `caching` + previous_response_id 状态链（结合 ResponseStateStore） |
| 7 | prompt_cache_options 未建模 | 无 | 透传/剥离按能力 |

## 3. 设计

### 3.1 IR 层：缓存语义完整进 IR（parse 不丢，已有基础）

`RequestIr.context.cache` 已建模 `key / retention / control`；块级 `cache_control` 保留在 `IrContent.extra`（parse 不丢）✅。
需补：
- `prompt_cache_options` 进 `CacheContext`（新增 `options: Option<Value>`）
- 解析时从 `prompt_cache_options` / `prompt_cache_retention` / 顶层 `cache_control` / 块级 extra 全部保留

### 3.2 能力配置：`ModelCapabilities` 新增缓存能力

```rust
/// Provider cache capabilities; drives stripping/passing cache controls.
pub struct CacheCapabilities {
    /// Supports Anthropic-style block-level `cache_control` (ephemeral).
    /// MiniMax(anthropic), Qwen; false strips block cache_control.
    pub block_cache_control: bool,           // default false
    /// Supports the top-level `prompt_cache_key` (routing/identity hint).
    /// Kimi/Grok/MiniMax(responses). false strips it from chat/responses.
    pub prompt_cache_key: bool,              // default false
    /// Supports `prompt_cache_options` { mode, ttl }. OpenAI-protocol only.
    pub prompt_cache_options: bool,          // default false
    /// Report cached tokens at `usage.cached_tokens` (Kimi) instead of
    /// `prompt_tokens_details.cached_tokens`.
    pub cached_tokens_top_level: bool,       // default false
    /// Report cache via `prompt_cache_hit_tokens`/`prompt_cache_miss_tokens`
    /// (DeepSeek) instead of `prompt_tokens_details.cached_tokens`.
    pub prompt_cache_hit_tokens: bool,       // default false
    /// Ark Responses explicit caching via `caching` + `previous_response_id`.
    pub ark_explicit_cache: bool,            // default false
}
```

默认值取向：**false（保守）**——不支持的字段绝不透传（避免 400/静默忽略），支持的 provider 显式开启。

各家配置：
- MiniMax: `block_cache_control=true`（anthropic 端点）, `prompt_cache_key=true`（responses 端点）——注：cache_control 仅 anthropic 端点生效，chat 上游仍剥离
- Qwen: `block_cache_control=true`（chat/dashscope/anthropic 三端点）
- Kimi: `prompt_cache_key=true`, `cached_tokens_top_level=true`
- Grok: `prompt_cache_key=true`（chat→x-grok-conv-id header 映射）
- DeepSeek: `prompt_cache_hit_tokens=true`（chat 上游报告）
- Ark: `ark_explicit_cache=true`（responses 端点）

### 3.3 render 层：按目标协议 × provider 能力转换

**块级 cache_control**（IrContent.extra 里）：
- 目标 Messages 且 `block_cache_control` → 保留
- 目标 Chat/Responses 且 `block_cache_control` → 保留（Qwen 的 chat 接受）
- 目标 Chat/Responses 且 !`block_cache_control` → 剥离块级 cache_control（deepseek/kimi/mimo/glm/grok/ark）
- OpenAI 官方协议（透传 passthrough）→ 可转 `prompt_cache_breakpoint`（先不实现，无需求）

**顶层 cache_control**：
- `block_cache_control` → 保留（Messages 目标原生；chat 目标 Qwen/MiniMax 接受）
- !`block_cache_control` → 剥离

**prompt_cache_key**（identity/key 语义）：
- Chat/Responses 目标且 `prompt_cache_key` → 写 `prompt_cache_key`（chat 与 responses 同字段）
- Chat/Responses 目标且 !`prompt_cache_key` → 剥离（避免 MiMo 报错/DeepSeek 静默）
- Messages 目标 → 现有 metadata.user_id 映射（隔离语义）保持不变
- Grok chat 目标 → 额外映射 header `x-grok-conv-id`（proxy 层加 header）

**prompt_cache_options**：
- `prompt_cache_options` 能力 → 透传
- 否则剥离

**Ark 显式缓存**（`ark_explicit_cache`，responses 上游）：
- 首轮：注入 `caching: {"type":"enabled","prefix":true}` + `store: true`（若 input ≥256 且非工具场景）
- 续轮：注入 `caching: {"type":"enabled"}` + `previous_response_id`（现有 ResponseStateStore 转录机制复用）
- 约束：`instructions` 非空时跳过（文档）；`thinking` 需一致（现有 thinking 映射保证）；前缀缓存创建时 stream=false 冲突——流式请求跳过 prefix 缓存，退隐式

### 3.4 响应侧：缓存命中报告转换

`ChatUsage` 扩展：
```rust
pub struct ChatUsage {
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,        // prompt_tokens_details.cached_tokens
    pub cached_tokens_top_level: Option<u64>, // usage.cached_tokens (Kimi)
    pub prompt_cache_hit: Option<u64>,     // prompt_cache_hit_tokens (DeepSeek)
    pub cache_write_tokens: Option<u64>,   // cache_write_tokens (OpenAI GPT-5.6+)
    pub reasoning_tokens: Option<u64>,
}
```
提取：`extract_usage` 读全部候选字段（cached_tokens = details.cached_tokens OR top-level cached_tokens OR prompt_cache_hit，按出现者取）。

输出映射（现有已做，补缺口）：
- chat→messages: cached → `cache_read_input_tokens`（已有）✅
- chat→responses: cached → `input_tokens_details.cached_tokens`（已有）✅
- 补：Kimi 顶层 cached_tokens 提取后进同一 cached 管道

### 3.5 配置样例

```toml
[providers.kimi.capabilities]
prompt_cache_key = true
cached_tokens_top_level = true

[providers.minimax.capabilities]
block_cache_control = true      # anthropic 上游
prompt_cache_key = true         # responses 上游（按上游协议条件生效）

[providers.qwen.capabilities]
block_cache_control = true

[providers.ds.capabilities]
prompt_cache_hit_tokens = true

[providers.grok.capabilities]
prompt_cache_key = true

[providers.ark.capabilities]
ark_explicit_cache = true
```

## 4. 实现计划（按依赖顺序）

1. **CacheCapabilities 结构 + ModelCapabilities 字段**（dialect.rs）——默认全 false
2. **CacheContext 补 prompt_cache_options**（request_ir.rs parse_context）
3. **ChatUsage 扩展 + extract_usage 多候选提取**（chat.rs）
4. **render 层块级 cache_control 剥离**（IrContent to_chat/to_responses 按 caps；需要把 caps 传进 IrContent 渲染——在 render_messages/render_chat_base/render_responses 的消息循环里处理 extra 剥离）
5. **render 层顶层 cache_control / prompt_cache_key / prompt_cache_options 按能力**（apply_context 带 caps）
6. **Kimi 顶层 cached_tokens 报告**（chat.rs 提取后自动进现有管道）
7. **Grok header x-grok-conv-id**（proxy 层，caps.prompt_cache_key 且目标 grok chat 时加 header）
8. **Ark 显式缓存**（proxy 层 + responses.rs，结合 ResponseStateStore）
9. **测试**：每家一个配置示例 + 转换断言（块级剥离/保留、key 透传/剥离、usage 提取）
10. **真实 provider 回归**：Kimi（cached_tokens 报告）、Qwen（cache_control 保留）、MiniMax（cache_control 透传）、DeepSeek（prompt_cache_hit_tokens）、Ark（显式缓存链）

## 5. 明确不做（无需求/文档不支持）

- OpenAI 官方 `prompt_cache_breakpoint` 转换（暂无官方协议上游）
- Qwen Responses header 会话缓存（`x-dashscope-session-cache`，与 previous_response_id 相关——需后续专项）
- Ark Context API（context_id 状态管理，超出当前范围）
- 自动缓存策略生成（如"为无显式缓存的 provider 生成稳定前缀"）——仅剥离/透传，不发明
