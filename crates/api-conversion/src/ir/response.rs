use serde_json::Value;

use super::{
    content::ContentItem, extension::Extension, state::ConversationState, stop_reason::StopReason,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub cache_write_input_tokens: Option<u64>,
    pub cache_miss_input_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub extensions: Vec<Extension>,
}

impl Usage {
    /// Fill a missing `total_tokens`. The total is at least the reported input or
    /// output count: a missing counterpart must not collapse it (an upstream that
    /// reports only input still consumed at least that many tokens).
    pub fn normalize(&mut self) {
        if self.total_tokens.is_none() {
            self.total_tokens = Some(
                self.input_tokens
                    .unwrap_or(0)
                    .saturating_add(self.output_tokens.unwrap_or(0)),
            );
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResponseIr {
    pub response_id: Option<String>,
    pub model: Option<String>,
    pub output: Vec<ContentItem>,
    pub usage: Usage,
    pub stop_reason: Option<StopReason>,
    pub original_stop_reason: Option<String>,
    pub state: ConversationState,
    pub extensions: Vec<Extension>,
    pub error: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_fills_total_from_either_token_count() {
        // total_tokens must be at least the reported input or output count; a
        // missing counterpart must not collapse the total to zero.
        let mut usage = Usage {
            input_tokens: Some(100),
            output_tokens: None,
            ..Usage::default()
        };
        usage.normalize();
        assert_eq!(usage.total_tokens, Some(100), "input-only fallback");

        let mut usage = Usage {
            input_tokens: None,
            output_tokens: Some(5),
            ..Usage::default()
        };
        usage.normalize();
        assert_eq!(usage.total_tokens, Some(5), "output-only fallback");

        let mut usage = Usage {
            input_tokens: Some(100),
            output_tokens: Some(5),
            ..Usage::default()
        };
        usage.normalize();
        assert_eq!(usage.total_tokens, Some(105));

        let mut usage = Usage::default();
        usage.normalize();
        assert_eq!(usage.total_tokens, Some(0), "empty fallback is zero");
    }
}
