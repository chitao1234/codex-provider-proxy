use serde_json::{json, Value};

pub fn normalize_input_schema(schema: &Value) -> Value {
    let Some(object) = schema.as_object() else {
        return json!({"type": "object", "properties": {}});
    };
    let mut normalized = object.clone();
    normalized.remove("$schema");
    if normalized.get("type").and_then(Value::as_str) == Some("object")
        && !normalized.contains_key("properties")
    {
        normalized.insert("properties".to_owned(), json!({}));
    }
    Value::Object(normalized)
}

pub fn render_search_tool_template(template: &Value, query: Option<&str>) -> Value {
    let Some(object) = template.as_object() else {
        return template.clone();
    };
    let mut rendered = object.clone();
    let replacement = query.filter(|query| !query.is_empty()).unwrap_or("search");
    for value in rendered.values_mut() {
        substitute_search_query(value, replacement);
        if let Value::Object(nested) = value {
            for nested_value in nested.values_mut() {
                substitute_search_query(nested_value, replacement);
            }
        }
    }
    Value::Object(rendered)
}

fn substitute_search_query(value: &mut Value, replacement: &str) {
    if let Value::String(text) = value {
        if text.contains("{search_query}") {
            *text = text.replace("{search_query}", replacement);
        }
    }
}

pub fn format_url_citations(annotations: Option<&Value>) -> Option<String> {
    let Value::Array(items) = annotations? else {
        return None;
    };
    let citations: Vec<String> = items
        .iter()
        .filter_map(|item| {
            let object = item.as_object()?;
            if object.get("type").and_then(Value::as_str) != Some("url_citation") {
                return None;
            }
            let url = object
                .get("url")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let title = object
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or_default();
            Some(if title.is_empty() {
                url.to_owned()
            } else {
                format!("{title}: {url}")
            })
        })
        .collect();
    (!citations.is_empty()).then(|| format!("Sources:\n{}", citations.join("\n")))
}
