//! OpenAI Chat Completions response/chunk structure helpers.

use serde_json::Value;

/// Usage fields extracted from a Chat response or final stream chunk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChatUsage {
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    /// `usage.prompt_tokens_details.cached_tokens` (standard OpenAI), or the
    /// provider-specific equivalents normalized here: Kimi's top-level
    /// `usage.cached_tokens`, DeepSeek's `usage.prompt_cache_hit_tokens`.
    pub cached_tokens: Option<u64>,
    /// `usage.prompt_tokens_details.cache_write_tokens` (GPT-5.6+).
    pub cache_write_tokens: Option<u64>,
    /// `usage.prompt_cache_miss_tokens` (DeepSeek).
    pub prompt_cache_miss_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
}

impl ChatUsage {
    pub fn is_empty(&self) -> bool {
        self.prompt_tokens.is_none()
            && self.completion_tokens.is_none()
            && self.cached_tokens.is_none()
            && self.cache_write_tokens.is_none()
            && self.prompt_cache_miss_tokens.is_none()
            && self.reasoning_tokens.is_none()
    }
}

/// Extract usage from a chat.completion / chat.completion.chunk body.
///
/// Cached tokens are reported under three shapes across providers: standard
/// `prompt_tokens_details.cached_tokens`, Kimi's top-level `cached_tokens`, and
/// DeepSeek's `prompt_cache_hit_tokens`; all normalize into `cached_tokens`.
pub fn extract_usage(body: &Value) -> ChatUsage {
    let usage = body.get("usage").and_then(Value::as_object);
    let u64_field = |field: &str| {
        usage
            .and_then(|usage| usage.get(field))
            .and_then(Value::as_u64)
    };
    let details = |container: &str| {
        usage
            .and_then(|usage| usage.get(container))
            .and_then(Value::as_object)
    };
    let detail_field = |details: Option<&serde_json::Map<String, Value>>, field: &str| {
        details
            .and_then(|details| details.get(field))
            .and_then(Value::as_u64)
    };
    let cached = detail_field(details("prompt_tokens_details"), "cached_tokens")
        .or_else(|| u64_field("cached_tokens"))
        .or_else(|| u64_field("prompt_cache_hit_tokens"));
    ChatUsage {
        prompt_tokens: u64_field("prompt_tokens"),
        completion_tokens: u64_field("completion_tokens"),
        cached_tokens: cached,
        cache_write_tokens: detail_field(details("prompt_tokens_details"), "cache_write_tokens"),
        prompt_cache_miss_tokens: u64_field("prompt_cache_miss_tokens"),
        reasoning_tokens: detail_field(details("completion_tokens_details"), "reasoning_tokens"),
    }
}

/// The first choice of a chat completion body, if any.
pub fn first_choice(body: &Value) -> Option<&Value> {
    body.get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
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
    fn extracts_kimi_top_level_cached_tokens() {
        let body = json!({
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "cached_tokens": 8}
        });
        let usage = extract_usage(&body);
        assert_eq!(usage.cached_tokens, Some(8));
    }

    #[test]
    fn extracts_deepseek_cache_hit_tokens() {
        let body = json!({
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 5,
                "prompt_cache_hit_tokens": 60,
                "prompt_cache_miss_tokens": 40
            }
        });
        let usage = extract_usage(&body);
        assert_eq!(usage.cached_tokens, Some(60));
        assert_eq!(usage.prompt_cache_miss_tokens, Some(40));
        assert_eq!(usage.prompt_tokens, Some(100));
    }

    #[test]
    fn extracts_cache_write_tokens() {
        let body = json!({
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 5,
                "prompt_tokens_details": {"cached_tokens": 50, "cache_write_tokens": 30}
            }
        });
        let usage = extract_usage(&body);
        assert_eq!(usage.cached_tokens, Some(50));
        assert_eq!(usage.cache_write_tokens, Some(30));
    }

    #[test]
    fn identifies_usage_only_chunk() {
        assert!(is_usage_only_chunk(
            &json!({"choices": [], "usage": {"prompt_tokens": 1}})
        ));
        assert!(!is_usage_only_chunk(&json!({"choices": [{"index": 0}]})));
    }
}
