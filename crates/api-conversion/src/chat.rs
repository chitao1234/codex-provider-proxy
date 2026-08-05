//! OpenAI Chat Completions response/chunk structure helpers.

use serde_json::Value;

/// Usage fields extracted from a Chat response or final stream chunk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChatUsage {
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
}

impl ChatUsage {
    pub fn is_empty(&self) -> bool {
        self.prompt_tokens.is_none()
            && self.completion_tokens.is_none()
            && self.cached_tokens.is_none()
            && self.reasoning_tokens.is_none()
    }
}

/// Extract usage from a chat.completion / chat.completion.chunk body.
pub fn extract_usage(body: &Value) -> ChatUsage {
    let usage = body.get("usage").and_then(Value::as_object);
    let u64_field = |field: &str| {
        usage
            .and_then(|usage| usage.get(field))
            .and_then(Value::as_u64)
    };
    let details = |container: &str| usage.and_then(|usage| usage.get(container)).and_then(Value::as_object);
    let detail_field = |details: Option<&serde_json::Map<String, Value>>, field: &str| {
        details
            .and_then(|details| details.get(field))
            .and_then(Value::as_u64)
    };
    ChatUsage {
        prompt_tokens: u64_field("prompt_tokens"),
        completion_tokens: u64_field("completion_tokens"),
        cached_tokens: detail_field(details("prompt_tokens_details"), "cached_tokens"),
        reasoning_tokens: detail_field(details("completion_tokens_details"), "reasoning_tokens"),
    }
}

/// The first choice of a chat completion body, if any.
pub fn first_choice(body: &Value) -> Option<&Value> {
    body.get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
}

/// `choices[0].message` if present.
pub fn first_choice_message(body: &Value) -> Option<&Value> {
    first_choice(body).and_then(|choice| choice.get("message"))
}

/// `choices[0].delta` if present.
pub fn first_choice_delta(body: &Value) -> Option<&Value> {
    first_choice(body).and_then(|choice| choice.get("delta"))
}

/// `choices[0].finish_reason` string, if any.
pub fn first_choice_finish_reason(body: &Value) -> Option<&str> {
    first_choice(body)
        .and_then(|choice| choice.get("finish_reason"))
        .and_then(Value::as_str)
}

/// Whether the chunk is the final usage-only chunk (`choices: []` with usage).
pub fn is_usage_only_chunk(body: &Value) -> bool {
    body.get("choices")
        .and_then(Value::as_array)
        .is_some_and(|choices| choices.is_empty())
}

/// The upstream message id from a completion/chunk body.
pub fn upstream_id(body: &Value) -> Option<&str> {
    body.get("id").and_then(Value::as_str)
}

/// The upstream model name.
pub fn upstream_model(body: &Value) -> Option<&str> {
    body.get("model").and_then(Value::as_str)
}

/// A non-empty string field of a chunk delta (DeepSeek sends explicit `null` for content).
pub fn delta_string<'a>(delta: &'a Value, field: &str) -> Option<&'a str> {
    delta
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn extracts_usage_from_deepseek_shape() {
        let body = json!({
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 72,
                "total_tokens": 82,
                "prompt_tokens_details": {"cached_tokens": 0},
                "completion_tokens_details": {"reasoning_tokens": 69}
            }
        });
        let usage = extract_usage(&body);
        assert_eq!(usage.prompt_tokens, Some(10));
        assert_eq!(usage.completion_tokens, Some(72));
        assert_eq!(usage.cached_tokens, Some(0));
        assert_eq!(usage.reasoning_tokens, Some(69));
    }

    #[test]
    fn extracts_usage_from_grok_shape() {
        let body = json!({
            "usage": {
                "prompt_tokens": 9458,
                "completion_tokens": 18,
                "prompt_tokens_details": {"text_tokens": 9458, "cached_tokens": 128},
                "completion_tokens_details": {"reasoning_tokens": 43}
            }
        });
        let usage = extract_usage(&body);
        assert_eq!(usage.cached_tokens, Some(128));
        assert_eq!(usage.reasoning_tokens, Some(43));
    }

    #[test]
    fn identifies_usage_only_chunk_and_deltas() {
        assert!(is_usage_only_chunk(&json!({"choices": [], "usage": {"prompt_tokens": 1}})));
        assert!(!is_usage_only_chunk(&json!({"choices": [{"index": 0}]})));
        let delta = json!({"content": null, "reasoning_content": "We"});
        assert_eq!(delta_string(&delta, "content"), None);
        assert_eq!(delta_string(&delta, "reasoning_content"), Some("We"));
    }
}
