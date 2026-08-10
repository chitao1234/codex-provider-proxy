use serde_json::{json, Map, Value};

pub fn apply_ark(request: &mut Map<String, Value>) {
    let effort = request
        .get("reasoning")
        .and_then(Value::as_object)
        .and_then(|reasoning| reasoning.get("effort"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    if let Some(effort) = effort {
        request.insert(
            "thinking".to_owned(),
            json!({"type": ark_thinking_type(&effort)}),
        );
        request.remove("reasoning");
    }
    request.remove("metadata");
    request.remove("user");
    if request
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .is_some_and(|key| key.len() > 64)
    {
        request.remove("prompt_cache_key");
    }
    if let Some(input) = request.get_mut("input").and_then(Value::as_array_mut) {
        for item in input {
            let Some(content) = item.get_mut("content").and_then(Value::as_array_mut) else {
                continue;
            };
            for block in content {
                if let Some(object) = block.as_object_mut() {
                    object.remove("cache_control");
                }
            }
        }
    }
    normalize_web_search_tools(request);
}

fn ark_thinking_type(effort: &str) -> &'static str {
    if matches!(effort, "none" | "minimal") {
        "disabled"
    } else {
        "enabled"
    }
}

fn normalize_web_search_tools(request: &mut Map<String, Value>) {
    let Some(tools) = request.get_mut("tools").and_then(Value::as_array_mut) else {
        return;
    };
    for tool in tools {
        let Some(object) = tool.as_object_mut() else {
            continue;
        };
        let kind = object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if kind != "web_search" && kind != "web_search_preview" && !kind.starts_with("web_search_")
        {
            continue;
        }
        if let Some(nested) = object
            .remove("web_search")
            .and_then(|value| value.as_object().cloned())
        {
            for (key, value) in nested {
                object.entry(key).or_insert(value);
            }
        }
        if let Some(context) = object.remove("search_context_size") {
            let max_keyword = match context.as_str() {
                Some("low") => 1,
                Some("medium") => 3,
                _ => 5,
            };
            object.insert("max_keyword".to_owned(), Value::from(max_keyword));
        }
        if kind != "web_search" {
            object.insert("type".to_owned(), Value::String("web_search".to_owned()));
        }
    }
}
