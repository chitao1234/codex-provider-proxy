//! Render the internal Chat Completions-shaped request into another request dialect.
//!
//! The existing request converters normalize Messages/Responses input into a stable chat
//! request shape. These renderers provide the remaining cross-protocol request edges without
//! coupling the proxy to protocol-specific JSON trees.

use serde_json::{json, Map, Value};

use crate::error::ConversionError;

pub fn convert_chat_request_to_responses(body: &Value) -> Result<Value, ConversionError> {
    let Some(request) = body.as_object() else {
        return Err(ConversionError::invalid(
            "request body is not a JSON object",
        ));
    };
    let mut out = Map::new();
    copy(request, &mut out, "model");
    copy(request, &mut out, "stream");
    copy(request, &mut out, "temperature");
    copy(request, &mut out, "top_p");
    copy(request, &mut out, "parallel_tool_calls");
    if let Some(value) = request
        .get("max_tokens")
        .or_else(|| request.get("max_completion_tokens"))
    {
        out.insert("max_output_tokens".to_owned(), value.clone());
    }
    if let Some(value) = request.get("reasoning_effort").and_then(Value::as_str) {
        out.insert("reasoning".to_owned(), json!({"effort": value}));
    }
    if let Some(format) = request.get("response_format") {
        out.insert("text".to_owned(), json!({"format": format}));
    }
    if let Some(choice) = request.get("tool_choice") {
        out.insert(
            "tool_choice".to_owned(),
            render_responses_tool_choice(choice),
        );
    }
    let (instructions, input) = render_responses_input(request.get("messages"));
    if let Some(instructions) = instructions {
        out.insert("instructions".to_owned(), Value::String(instructions));
    }
    out.insert("input".to_owned(), Value::Array(input));
    if let Some(tools) = render_responses_tools(request.get("tools")) {
        out.insert("tools".to_owned(), Value::Array(tools));
    }
    Ok(Value::Object(out))
}

pub fn convert_chat_request_to_messages(body: &Value) -> Result<Value, ConversionError> {
    let Some(request) = body.as_object() else {
        return Err(ConversionError::invalid(
            "request body is not a JSON object",
        ));
    };
    let mut out = Map::new();
    copy(request, &mut out, "model");
    copy(request, &mut out, "stream");
    copy(request, &mut out, "temperature");
    copy(request, &mut out, "top_p");
    if let Some(value) = request
        .get("max_tokens")
        .or_else(|| request.get("max_completion_tokens"))
    {
        out.insert("max_tokens".to_owned(), value.clone());
    }
    if let Some(value) = request.get("reasoning_effort").and_then(Value::as_str) {
        out.insert("output_config".to_owned(), json!({"effort": value}));
    }
    if let Some(thinking) = request.get("thinking") {
        out.insert("thinking".to_owned(), thinking.clone());
    }
    let (system, messages) = render_messages_input(request.get("messages"));
    if !system.is_empty() {
        out.insert("system".to_owned(), Value::String(system));
    }
    out.insert("messages".to_owned(), Value::Array(messages));
    if let Some(tools) = render_messages_tools(request.get("tools")) {
        out.insert("tools".to_owned(), Value::Array(tools));
    }
    if let Some(choice) = request.get("tool_choice") {
        out.insert(
            "tool_choice".to_owned(),
            render_messages_tool_choice(choice),
        );
    }
    Ok(Value::Object(out))
}

fn copy(request: &Map<String, Value>, out: &mut Map<String, Value>, key: &str) {
    if let Some(value) = request.get(key).filter(|value| !value.is_null()) {
        out.insert(key.to_owned(), value.clone());
    }
}

fn render_responses_input(messages: Option<&Value>) -> (Option<String>, Vec<Value>) {
    let mut instructions = Vec::new();
    let mut input = Vec::new();
    let Some(messages) = messages.and_then(Value::as_array) else {
        return (None, input);
    };
    for message in messages {
        let Some(object) = message.as_object() else {
            continue;
        };
        let role = object.get("role").and_then(Value::as_str).unwrap_or("user");
        let content = object.get("content").cloned().unwrap_or(Value::Null);
        if role == "system" {
            if let Some(text) = content.as_str().filter(|text| !text.is_empty()) {
                instructions.push(text.to_owned());
            }
            continue;
        }
        if role != "tool" && !content.is_null() && (!content.as_str().is_some_and(str::is_empty)) {
            input.push(json!({"type": "message", "role": role, "content": content}));
        }
        if role == "assistant" {
            if let Some(tool_calls) = object.get("tool_calls").and_then(Value::as_array) {
                for call in tool_calls {
                    let Some(function) = call.get("function").and_then(Value::as_object) else {
                        continue;
                    };
                    input.push(json!({
                        "type": "function_call",
                        "call_id": call.get("id").and_then(Value::as_str).unwrap_or_default(),
                        "name": function.get("name").and_then(Value::as_str).unwrap_or_default(),
                        "arguments": function.get("arguments").and_then(Value::as_str).unwrap_or("{}"),
                    }));
                }
            }
        }
        if role == "tool" {
            input.push(json!({
                "type": "function_call_output",
                "call_id": object.get("tool_call_id").and_then(Value::as_str).unwrap_or_default(),
                "output": content,
            }));
        }
    }
    let instructions = (!instructions.is_empty()).then(|| instructions.join("\n\n"));
    (instructions, input)
}

fn render_messages_input(messages: Option<&Value>) -> (String, Vec<Value>) {
    let mut system = Vec::new();
    let mut output = Vec::new();
    let Some(messages) = messages.and_then(Value::as_array) else {
        return (String::new(), output);
    };
    for message in messages {
        let Some(object) = message.as_object() else {
            continue;
        };
        let role = object.get("role").and_then(Value::as_str).unwrap_or("user");
        let content = object.get("content").cloned().unwrap_or(Value::Null);
        if role == "system" {
            if let Some(text) = content.as_str().filter(|text| !text.is_empty()) {
                system.push(text.to_owned());
            }
            continue;
        }
        if role == "tool" {
            output.push(json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": object.get("tool_call_id").and_then(Value::as_str).unwrap_or_default(),
                    "content": content,
                }],
            }));
            continue;
        }
        let mut blocks = Vec::new();
        if let Some(text) = content.as_str().filter(|text| !text.is_empty()) {
            blocks.push(json!({"type": "text", "text": text}));
        }
        if role == "assistant" {
            if let Some(tool_calls) = object.get("tool_calls").and_then(Value::as_array) {
                for call in tool_calls {
                    let Some(function) = call.get("function").and_then(Value::as_object) else {
                        continue;
                    };
                    let arguments = function
                        .get("arguments")
                        .and_then(Value::as_str)
                        .and_then(|arguments| serde_json::from_str(arguments).ok())
                        .unwrap_or_else(|| json!({}));
                    blocks.push(json!({
                        "type": "tool_use",
                        "id": call.get("id").and_then(Value::as_str).unwrap_or_default(),
                        "name": function.get("name").and_then(Value::as_str).unwrap_or_default(),
                        "input": arguments,
                    }));
                }
            }
        }
        output.push(json!({"role": role, "content": blocks}));
    }
    (system.join("\n\n"), output)
}

fn render_responses_tools(tools: Option<&Value>) -> Option<Vec<Value>> {
    let tools = tools?.as_array()?;
    let rendered = tools
        .iter()
        .filter_map(|tool| {
            let function = tool.get("function")?.as_object()?;
            Some(json!({
                "type": "function",
                "name": function.get("name").and_then(Value::as_str).unwrap_or_default(),
                "description": function.get("description").and_then(Value::as_str).unwrap_or_default(),
                "parameters": function.get("parameters").cloned().unwrap_or_else(|| json!({"type":"object"})),
            }))
        })
        .collect::<Vec<_>>();
    (!rendered.is_empty()).then_some(rendered)
}

fn render_messages_tools(tools: Option<&Value>) -> Option<Vec<Value>> {
    let tools = tools?.as_array()?;
    let rendered = tools
        .iter()
        .filter_map(|tool| {
            let object = tool.as_object()?;
            let function = object.get("function").and_then(Value::as_object);
            let source = function.unwrap_or(object);
            let name = source.get("name")?.as_str()?;
            Some(json!({
                "name": name,
                "description": source.get("description").and_then(Value::as_str).unwrap_or_default(),
                "input_schema": source.get("input_schema").or_else(|| source.get("parameters")).cloned().unwrap_or_else(|| json!({"type":"object"})),
            }))
        })
        .collect::<Vec<_>>();
    (!rendered.is_empty()).then_some(rendered)
}

fn render_responses_tool_choice(choice: &Value) -> Value {
    match choice {
        Value::String(value) => Value::String(value.clone()),
        Value::Object(object) if object.get("type").and_then(Value::as_str) == Some("function") => {
            json!({"type":"function", "name": object.get("function").and_then(Value::as_object).and_then(|f| f.get("name")).and_then(Value::as_str).unwrap_or_default()})
        }
        _ => json!("auto"),
    }
}

fn render_messages_tool_choice(choice: &Value) -> Value {
    match choice {
        Value::String(value) => match value.as_str() {
            "required" => json!({"type":"any"}),
            other => json!({"type": other}),
        },
        Value::Object(object) => object
            .get("function")
            .and_then(Value::as_object)
            .and_then(|function| function.get("name"))
            .and_then(Value::as_str)
            .map(|name| json!({"type":"tool", "name": name}))
            .unwrap_or_else(|| json!({"type":"auto"})),
        _ => json!({"type":"auto"}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_chat_request_as_responses_request() {
        let chat = json!({
            "model": "m",
            "max_tokens": 100,
            "stream": true,
            "messages": [
                {"role": "system", "content": "be concise"},
                {"role": "user", "content": "hello"},
                {"role": "assistant", "tool_calls": [{"id": "call_1", "function": {"name": "lookup", "arguments": "{}"}}]},
                {"role": "tool", "tool_call_id": "call_1", "content": "done"}
            ],
            "tools": [{"type": "function", "function": {"name": "lookup", "description": "look up", "parameters": {"type": "object"}}}]
        });
        let responses = convert_chat_request_to_responses(&chat).unwrap();
        assert_eq!(responses["instructions"], "be concise");
        assert_eq!(responses["max_output_tokens"], 100);
        assert_eq!(responses["input"][0]["type"], "message");
        assert_eq!(responses["input"][1]["type"], "function_call");
        assert_eq!(responses["input"][2]["type"], "function_call_output");
        assert_eq!(responses["tools"][0]["type"], "function");
    }

    #[test]
    fn renders_chat_request_as_messages_request() {
        let chat = json!({
            "model": "m",
            "max_completion_tokens": 100,
            "messages": [
                {"role": "system", "content": "be concise"},
                {"role": "user", "content": "hello"},
                {"role": "assistant", "tool_calls": [{"id": "call_1", "function": {"name": "lookup", "arguments": "{\"q\":\"x\"}"}}]},
                {"role": "tool", "tool_call_id": "call_1", "content": "done"}
            ],
            "tools": [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}]
        });
        let messages = convert_chat_request_to_messages(&chat).unwrap();
        assert_eq!(messages["system"], "be concise");
        assert_eq!(messages["max_tokens"], 100);
        assert_eq!(messages["messages"][0]["role"], "user");
        assert_eq!(messages["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(messages["messages"][2]["content"][0]["type"], "tool_result");
        assert_eq!(messages["tools"][0]["input_schema"]["type"], "object");
    }
}
