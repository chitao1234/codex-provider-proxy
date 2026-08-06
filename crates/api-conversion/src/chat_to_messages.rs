//! OpenAI Chat Completions response conversion to Anthropic Messages.

use serde_json::{json, Map, Value};

use crate::chat::{
    extract_usage, first_choice, first_choice_finish_reason, first_choice_message, upstream_id,
    upstream_model, ChatUsage,
};
use crate::conversion_utils::format_url_citations;
use crate::error::ConversionError;

/// Convert a Chat Completions response body into an Anthropic Messages response body.
pub fn convert_chat_response(body: &Value) -> Result<Value, ConversionError> {
    if first_choice(body).is_none() {
        return Err(ConversionError::invalid(
            "upstream chat response missing choices",
        ));
    }
    let message = first_choice_message(body)
        .ok_or_else(|| ConversionError::invalid("upstream chat response missing message"))?;

    let message_id = synth_message_id(upstream_id(body));
    let model = upstream_model(body).unwrap_or_default().to_string();

    let mut content: Vec<Value> = Vec::new();
    if let Some(reasoning) = message
        .get("reasoning_content")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
    {
        content.push(json!({
            "type": "thinking",
            "thinking": reasoning,
            "signature": message_id,
        }));
    }
    if let Some(text) = message
        .get("content")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
    {
        content.push(json!({"type": "text", "text": text}));
    }
    // MiMo returns search results in message.annotations (url_citation items) instead
    // of tool_calls. Anthropic has no citation block type, so append a text block
    // listing the sources so the downstream client can see them.
    if let Some(sources) = format_url_citations(message.get("annotations")) {
        content.push(json!({"type": "text", "text": sources}));
    }
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for call in tool_calls {
            let Some(call) = call.as_object() else {
                continue;
            };
            let function = call.get("function").and_then(Value::as_object);
            let name = function
                .and_then(|function| function.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let id = call
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .unwrap_or_default();
            if name.is_empty() {
                continue;
            }
            let input = function
                .and_then(|function| function.get("arguments"))
                .and_then(Value::as_str)
                .and_then(|arguments| serde_json::from_str::<Value>(arguments).ok())
                .filter(Value::is_object)
                .unwrap_or_else(|| json!({}));
            content.push(json!({"type": "tool_use", "id": id, "name": name, "input": input}));
        }
    }

    let finish_reason = first_choice_finish_reason(body);
    let saw_tool_call = !content.is_empty()
        && content
            .iter()
            .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"));
    let stop_reason = map_finish_reason(finish_reason, saw_tool_call);

    let usage = extract_usage(body);
    let usage_value = messages_usage(usage);

    Ok(json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": usage_value,
    }))
}

/// Build the Anthropic usage object from Chat usage (present fields only).
fn messages_usage(usage: ChatUsage) -> Value {
    let mut usage_out = Map::new();
    if let Some(input) = usage.prompt_tokens {
        usage_out.insert("input_tokens".to_string(), json!(input));
    }
    if let Some(output) = usage.completion_tokens {
        usage_out.insert("output_tokens".to_string(), json!(output));
    }
    if let Some(cached) = usage.cached_tokens {
        usage_out.insert("cache_read_input_tokens".to_string(), json!(cached));
    }
    if let Some(reasoning) = usage.reasoning_tokens {
        usage_out.insert(
            "output_tokens_details".to_string(),
            json!({"thinking_tokens": reasoning}),
        );
    }
    Value::Object(usage_out)
}

/// `msg_<upstream id>`; upstream ids already prefixed with `msg_` pass through.
fn synth_message_id(upstream_id: Option<&str>) -> String {
    match upstream_id {
        Some(id) if id.starts_with("msg_") => id.to_string(),
        Some(id) => format!("msg_{id}"),
        None => String::new(),
    }
}

fn map_finish_reason(finish_reason: Option<&str>, saw_tool_call: bool) -> &'static str {
    match finish_reason {
        Some("stop") => "end_turn",
        Some("length") => "max_tokens",
        Some("tool_calls") | Some("function_call") => {
            if saw_tool_call {
                "tool_use"
            } else {
                // Never announce tool_use without any announced tool_use block.
                "end_turn"
            }
        }
        Some("content_filter") => "end_turn",
        _ => "end_turn",
    }
}

// ---------------------------------------------------------------------------
// Streaming state machine
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::dialect::{MaxTokensField, ModelCapabilities, ServerToolPolicy, ThinkingParam};
    use crate::request_ir::{Protocol, RequestIr};

    use super::*;

    fn caps() -> ModelCapabilities {
        ModelCapabilities::default()
    }

    fn render_messages_as_chat(
        body: &Value,
        caps: &ModelCapabilities,
    ) -> Result<(Value, crate::dialect::RequestConversionReport), ConversionError> {
        RequestIr::parse(Protocol::AnthropicMessages, body)?.render_chat_with_capabilities(caps)
    }

    #[test]
    fn clamps_max_tokens_to_upstream_cap() {
        let mut caps = caps();
        caps.max_tokens_cap = Some(393216);
        let body = json!({"model": "m", "max_tokens": 1000000, "messages": []});
        let (out, _) = render_messages_as_chat(&body, &caps).unwrap();
        assert_eq!(out["max_tokens"], 393216);

        let body = json!({"model": "m", "max_tokens": 64000, "messages": []});
        let (out, _) = render_messages_as_chat(&body, &caps).unwrap();
        assert_eq!(out["max_tokens"], 64000);
    }

    #[test]
    fn converts_basic_request() {
        let body = json!({
            "model": "deepseek-v4-pro",
            "max_tokens": 64000,
            "stream": true,
            "temperature": 0.5,
            "stop_sequences": ["END"],
            "system": "You are helpful",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let (out, report) = render_messages_as_chat(&body, &caps()).unwrap();
        assert_eq!(out["model"], "deepseek-v4-pro");
        assert_eq!(out["max_tokens"], 64000);
        assert_eq!(out["stream"], true);
        assert_eq!(out["stream_options"]["include_usage"], true);
        assert_eq!(out["stop"], "END");
        assert_eq!(out["messages"][0]["role"], "system");
        assert_eq!(out["messages"][1]["role"], "user");
        assert!(report.dropped_server_tools.is_empty());
    }

    #[test]
    fn minimax_adaptive_thinking_and_reasoning_split() {
        let mut caps = caps();
        caps.thinking_param = ThinkingParam::Adaptive;
        caps.reasoning_split = Some(true);
        let body = json!({
            "model": "MiniMax-M3",
            "max_tokens": 1000,
            "thinking": {"type": "adaptive"},
            "messages": [{"role": "user", "content": "hi"}]
        });
        let (out, _) = render_messages_as_chat(&body, &caps).unwrap();
        assert_eq!(out["thinking"]["type"], "adaptive");
        assert_eq!(out["reasoning_split"], true);

        // disabled thinking maps through unchanged.
        let body = json!({
            "model": "MiniMax-M3",
            "max_tokens": 1000,
            "thinking": {"type": "disabled"},
            "messages": [{"role": "user", "content": "hi"}]
        });
        let (out, _) = render_messages_as_chat(&body, &caps).unwrap();
        assert_eq!(out["thinking"]["type"], "disabled");
    }

    #[test]
    fn drops_stop_sequences_when_upstream_rejects_stop() {
        let mut no_stop_caps = caps();
        no_stop_caps.accept_stop = false;
        let body = json!({
            "model": "grok-4.5",
            "stop_sequences": ["END"],
            "messages": [{"role": "user", "content": "hi"}]
        });
        let (out, _) = render_messages_as_chat(&body, &no_stop_caps).unwrap();
        assert!(out.get("stop").is_none(), "stop must be dropped");

        let (out, _) = render_messages_as_chat(&body, &caps()).unwrap();
        assert_eq!(out["stop"], "END");
    }

    #[test]
    fn maps_max_tokens_field_per_capability() {
        let mut caps = caps();
        caps.max_tokens_field = MaxTokensField::MaxCompletionTokens;
        let body = json!({"model": "gpt-5.5", "max_tokens": 1000, "messages": []});
        let (out, _) = render_messages_as_chat(&body, &caps).unwrap();
        assert_eq!(out["max_completion_tokens"], 1000);
        assert!(out.get("max_tokens").is_none());
    }

    #[test]
    fn strips_cache_control_and_unknown_anthropic_fields() {
        let body = json!({
            "model": "deepseek-v4-pro",
            "max_tokens": 100,
            "context_management": {"edits": []},
            "metadata": {"client_app": "claude-code"},
            "cache_control": {"type": "ephemeral"},
            "service_tier": "standard",
            "system": [{"type": "text", "text": "x-anthropic-billing-header: cc_version=2;", "cache_control": {"type": "ephemeral"}}, {"type": "text", "text": "real system"}],
            "messages": [{"role": "user", "content": [{"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}]}]
        });
        let (out, _) = render_messages_as_chat(&body, &caps()).unwrap();
        assert!(out.get("context_management").is_none());
        assert!(out.get("metadata").is_none());
        assert!(out.get("cache_control").is_none());
        assert!(out.get("service_tier").is_none());
        assert_eq!(out["messages"][0]["content"], "real system");
        assert_eq!(out["messages"][1]["content"], "hi");
    }

    #[test]
    fn preserves_unknown_provider_parameters() {
        let body = json!({
            "model": "custom-model",
            "max_tokens": 100,
            "messages": [{"role": "user", "content": "hi"}],
            "provider_budget": {"tokens": 123},
            "vendor_sampling_mode": "adaptive"
        });
        let (out, _) = render_messages_as_chat(&body, &caps()).unwrap();
        assert_eq!(out["provider_budget"]["tokens"], 123);
        assert_eq!(out["vendor_sampling_mode"], "adaptive");
    }

    #[test]
    fn converts_tool_loop_messages() {
        let body = json!({
            "model": "deepseek-v4-pro",
            "max_tokens": 100,
            "messages": [
                {"role": "user", "content": "weather?"},
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "let me call the tool", "signature": "sig1"},
                    {"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {"city": "Paris"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_1", "content": "sunny"},
                    {"type": "text", "text": "now summarize"}
                ]}
            ]
        });
        let (out, _) = render_messages_as_chat(&body, &caps()).unwrap();
        let messages = out["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["content"], "");
        assert_eq!(messages[1]["reasoning_content"], "let me call the tool");
        assert_eq!(messages[1]["tool_calls"][0]["id"], "toolu_1");
        assert_eq!(
            messages[1]["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
        assert_eq!(
            messages[1]["tool_calls"][0]["function"]["arguments"],
            "{\"city\":\"Paris\"}"
        );
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["tool_call_id"], "toolu_1");
        assert_eq!(messages[2]["content"], "sunny");
        assert_eq!(messages[3]["role"], "user");
        assert_eq!(messages[3]["content"], "now summarize");
    }

    #[test]
    fn mid_conversation_system_becomes_system_reminder() {
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "system", "content": "Terse mode"},
                {"role": "assistant", "content": "ok"}
            ]
        });
        let (out, _) = render_messages_as_chat(&body, &caps()).unwrap();
        let messages = out["messages"].as_array().unwrap();
        assert_eq!(messages[1]["role"], "user");
        assert!(messages[1]["content"]
            .as_str()
            .unwrap()
            .contains("<system-reminder>"));
    }

    #[test]
    fn converts_tools_and_drops_server_tools_by_default() {
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"name": "Read", "description": "read a file", "input_schema": {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}},
                {"type": "web_search_20260209", "name": "web_search"},
                {"type": "code_execution_20260120", "name": "code_execution"}
            ]
        });
        let (out, report) = render_messages_as_chat(&body, &caps()).unwrap();
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["function"]["name"], "Read");
        assert!(tools[0]["function"]["parameters"].get("$schema").is_none());
        assert!(tools[0]["function"]["parameters"]
            .get("properties")
            .is_some());
        assert_eq!(
            report.dropped_server_tools,
            vec!["web_search", "code_execution"]
        );
    }

    #[test]
    fn maps_server_tools_to_provider_native_shape() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        caps.search_tool_template = Some(json!({
            "type": "web_search",
            "web_search": {"search_result": true, "search_query": "{search_query}"}
        }));
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"type": "web_search_20260209", "name": "web_search"},
                {"type": "web_fetch_20260209", "name": "web_fetch"},
                {"type": "code_execution_20260120", "name": "code_execution"}
            ]
        });
        let (out, report) = render_messages_as_chat(&body, &caps).unwrap();
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "web_search");
        assert_eq!(tools[0]["web_search"]["search_result"], true);
        // {search_query} is replaced with the last user message text so the provider
        // actually performs a search (glm requires a real query).
        assert_eq!(tools[0]["web_search"]["search_query"], "hi");
        assert_eq!(report.mapped_server_tools, vec!["web_search"]);
        assert_eq!(
            report.dropped_server_tools,
            vec!["web_fetch", "code_execution"]
        );
    }

    #[test]
    fn always_enable_tools_forces_params_without_client_tools() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        caps.search_request_params = Some(json!({"enable_search": true}));
        caps.code_interpreter_request_params = Some(json!({"enable_code_interpreter": true}));
        caps.always_enable_tools = vec!["web_search".to_string(), "code_execution".to_string()];
        // Client declares no server tools at all.
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"name": "Read", "description": "read", "input_schema": {"type": "object"}}]
        });
        let (out, report) = render_messages_as_chat(&body, &caps).unwrap();
        // Search and code interpreter params are force-merged.
        assert_eq!(out["enable_search"], true);
        assert_eq!(out["enable_code_interpreter"], true);
        // Tools array still only has the client's own function.
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "Read");
        assert!(report
            .mapped_server_tools
            .contains(&"web_search".to_string()));
        assert!(report
            .mapped_server_tools
            .contains(&"code_execution".to_string()));
    }

    #[test]
    fn provider_native_code_interpreter_params_enable_qwen() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        caps.code_interpreter_request_params = Some(json!({
            "enable_code_interpreter": true,
            "enable_thinking": true
        }));
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "stream": false,
            "messages": [{"role": "user", "content": "12 的 3 次方是多少？"}],
            "tools": [
                {"type": "code_execution_20260120", "name": "code_execution"},
                {"name": "Read", "description": "read", "input_schema": {"type": "object"}}
            ]
        });
        let (out, report) = render_messages_as_chat(&body, &caps).unwrap();
        assert_eq!(out["enable_code_interpreter"], true);
        assert_eq!(out["enable_thinking"], true);
        // qwen code interpreter requires streaming; forced on.
        assert_eq!(out["stream"], true);
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "Read");
        assert_eq!(report.mapped_server_tools, vec!["code_execution"]);
    }

    #[test]
    fn provider_native_fetch_params_enable_qwen_web_extractor() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        caps.fetch_request_params = Some(json!({
            "enable_search": true,
            "search_options": {"search_strategy": "agent_max"},
            "enable_thinking": true
        }));
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "总结 https://example.com/a 的内容"}],
            "tools": [
                {"type": "web_fetch_20260209", "name": "web_fetch"},
                {"name": "Read", "description": "read", "input_schema": {"type": "object"}}
            ]
        });
        let (out, report) = render_messages_as_chat(&body, &caps).unwrap();
        // agent_max lets the model scrape URLs from the prompt.
        assert_eq!(out["enable_search"], true);
        assert_eq!(out["search_options"]["search_strategy"], "agent_max");
        assert_eq!(out["enable_thinking"], true);
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "Read");
        assert_eq!(report.mapped_server_tools, vec!["web_fetch"]);
    }

    #[test]
    fn provider_native_with_request_params_enables_qwen_search() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        caps.search_request_params = Some(json!({
            "enable_search": true,
            "search_options": {"forced_search": true}
        }));
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "今天的 AI 新闻"}],
            "tools": [
                {"type": "web_search_20260209", "name": "web_search"},
                {"type": "web_fetch_20260209", "name": "web_fetch"}
            ]
        });
        let (out, report) = render_messages_as_chat(&body, &caps).unwrap();
        // Search is a top-level request parameter, not a tool.
        assert_eq!(out["enable_search"], true);
        assert_eq!(out["search_options"]["forced_search"], true);
        assert!(out.get("tools").is_none());
        assert_eq!(report.mapped_server_tools, vec!["web_search"]);
        assert_eq!(report.dropped_server_tools, vec!["web_fetch"]);
    }

    #[test]
    fn provider_native_without_template_drops_search_tools() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::ProviderNative;
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"type": "web_search_20260209", "name": "web_search"},
                {"name": "Read", "description": "read", "input_schema": {"type": "object"}}
            ]
        });
        let (out, report) = render_messages_as_chat(&body, &caps).unwrap();
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "Read");
        assert_eq!(report.dropped_server_tools, vec!["web_search"]);
    }

    #[test]
    fn maps_server_tools_to_functions_when_configured() {
        let mut caps = caps();
        caps.server_tools = ServerToolPolicy::MapToFunction;
        let body = json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"type": "web_search_20260209", "name": "web_search"},
                {"type": "web_fetch_20260209", "name": "web_fetch"},
                {"type": "code_execution_20260120", "name": "code_execution"}
            ]
        });
        let (out, report) = render_messages_as_chat(&body, &caps).unwrap();
        let tools = out["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0]["function"]["name"], "web_search");
        assert_eq!(tools[0]["function"]["parameters"]["required"][0], "query");
        assert_eq!(tools[1]["function"]["name"], "web_fetch");
        assert_eq!(report.mapped_server_tools, vec!["web_search", "web_fetch"]);
        assert_eq!(report.dropped_server_tools, vec!["code_execution"]);
    }

    #[test]
    fn converts_tool_choice() {
        let cases = [
            (json!({"type": "auto"}), json!("auto")),
            (json!({"type": "any"}), json!("required")),
            (json!({"type": "none"}), json!("none")),
            (
                json!({"type": "tool", "name": "f"}),
                json!({"type": "function", "function": {"name": "f"}}),
            ),
        ];
        for (choice, expected) in cases {
            let body =
                json!({"model": "m", "max_tokens": 1, "messages": [], "tool_choice": choice});
            let (out, _) = render_messages_as_chat(&body, &caps()).unwrap();
            assert_eq!(out["tool_choice"], expected);
        }
    }

    #[test]
    fn converts_thinking_to_qwen_enable_thinking() {
        let mut caps = caps();
        caps.thinking_param = ThinkingParam::EnableThinking;
        caps.reasoning_effort = None;
        let body = json!({
            "model": "qwen3.7-plus",
            "max_tokens": 1,
            "thinking": {"type": "adaptive"},
            "messages": []
        });
        let (out, _) = render_messages_as_chat(&body, &caps).unwrap();
        assert_eq!(out["enable_thinking"], true);
        assert!(out.get("thinking").is_none());

        let body = json!({
            "model": "qwen3.7-plus",
            "max_tokens": 1,
            "thinking": {"type": "disabled"},
            "messages": []
        });
        let (out, _) = render_messages_as_chat(&body, &caps).unwrap();
        assert_eq!(out["enable_thinking"], false);
    }

    #[test]
    fn converts_effort_and_thinking_for_deepseek() {
        let mut caps = caps();
        caps.reasoning_effort = Some(crate::dialect::ReasoningEffortConfig {
            enabled: true,
            levels: vec!["low".into(), "high".into(), "max".into()],
            default: Some("high".into()),
        });
        let body = json!({
            "model": "deepseek-v4-pro",
            "max_tokens": 1,
            "thinking": {"type": "adaptive"},
            "output_config": {"effort": "xhigh"},
            "messages": []
        });
        let (out, _) = render_messages_as_chat(&body, &caps).unwrap();
        assert_eq!(out["thinking"]["type"], "enabled");
        assert_eq!(out["reasoning_effort"], "high");
    }

    #[test]
    fn disabled_thinking_maps_for_deepseek() {
        let mut caps = caps();
        caps.reasoning_effort = Some(crate::dialect::ReasoningEffortConfig {
            enabled: false,
            levels: vec!["low".into(), "high".into(), "max".into()],
            default: None,
        });
        let body = json!({
            "model": "deepseek-v4-pro",
            "max_tokens": 1,
            "thinking": {"type": "disabled"},
            "messages": []
        });
        let (out, _) = render_messages_as_chat(&body, &caps).unwrap();
        assert_eq!(out["thinking"]["type"], "disabled");
        assert!(out.get("reasoning_effort").is_none());
    }

    #[test]
    fn rejects_image_input_when_unsupported() {
        let body = json!({
            "model": "m",
            "max_tokens": 1,
            "messages": [{"role": "user", "content": [{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aaa"}}]}]
        });
        let err = render_messages_as_chat(&body, &caps()).unwrap_err();
        assert!(err.to_string().contains("image"));
    }

    #[test]
    fn converts_image_when_supported() {
        let mut caps = caps();
        caps.image_input = true;
        let body = json!({
            "model": "m",
            "max_tokens": 1,
            "messages": [{"role": "user", "content": [{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aaa"}}, {"type": "text", "text": "what is this"}]}]
        });
        let (out, _) = render_messages_as_chat(&body, &caps).unwrap();
        let content = out["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "what is this");
        assert_eq!(content[1]["type"], "image_url");
        assert!(content[1]["image_url"]["url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,"));
    }

    #[test]
    fn converts_mimo_annotations_to_sources_text_block() {
        let body = json!({
            "id": "mimo-1",
            "model": "mimo-v2.5-pro",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "巴黎是法国首都。",
                    "annotations": [
                        {"type": "url_citation", "url": "https://example.com/paris", "title": "Paris Guide", "summary": "..."},
                        {"type": "url_citation", "url": "https://example.com/france", "title": "", "summary": ""}
                    ]
                },
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5}
        });
        let out = convert_chat_response(&body).unwrap();
        let blocks = out["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[0]["text"], "巴黎是法国首都。");
        assert_eq!(blocks[1]["type"], "text");
        let sources = blocks[1]["text"].as_str().unwrap();
        assert!(sources.contains("Sources:"));
        assert!(sources.contains("Paris Guide: https://example.com/paris"));
        assert!(sources.contains("https://example.com/france"));
    }

    #[test]
    fn converts_non_streaming_response_with_reasoning_and_tools() {
        let body = json!({
            "id": "ff11019d-aa8d-4251-8ff0-d7644de0987e",
            "model": "deepseek-v4-pro",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!",
                    "reasoning_content": "thinking text",
                    "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"city\": \"Paris\"}"}}]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 72, "prompt_tokens_details": {"cached_tokens": 2}, "completion_tokens_details": {"reasoning_tokens": 69}}
        });
        let out = convert_chat_response(&body).unwrap();
        assert_eq!(out["id"], "msg_ff11019d-aa8d-4251-8ff0-d7644de0987e");
        assert_eq!(out["type"], "message");
        assert_eq!(out["stop_reason"], "tool_use");
        assert_eq!(out["content"][0]["type"], "thinking");
        assert_eq!(
            out["content"][0]["signature"],
            "msg_ff11019d-aa8d-4251-8ff0-d7644de0987e"
        );
        assert_eq!(out["content"][1]["type"], "text");
        assert_eq!(out["content"][2]["type"], "tool_use");
        assert_eq!(out["content"][2]["input"]["city"], "Paris");
        assert_eq!(out["usage"]["input_tokens"], 10);
        assert_eq!(out["usage"]["output_tokens"], 72);
        assert_eq!(out["usage"]["cache_read_input_tokens"], 2);
        assert_eq!(out["usage"]["output_tokens_details"]["thinking_tokens"], 69);
    }

    #[test]
    fn downgrades_tool_calls_finish_without_tool_blocks() {
        let body = json!({
            "id": "x",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "done"}, "finish_reason": "tool_calls"}]
        });
        let out = convert_chat_response(&body).unwrap();
        assert_eq!(out["stop_reason"], "end_turn");
    }
}
