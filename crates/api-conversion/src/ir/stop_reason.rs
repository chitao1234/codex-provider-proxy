use std::fmt;

/// Protocol-neutral completion reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    Natural,
    ToolUse,
    MaxTokens,
    ContentFilter,
    Cancelled,
    Error,
    Unknown(String),
}

impl StopReason {
    pub fn is_successful(&self) -> bool {
        !matches!(self, Self::Cancelled | Self::Error)
    }
}

impl fmt::Display for StopReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Natural => formatter.write_str("natural"),
            Self::ToolUse => formatter.write_str("tool_use"),
            Self::MaxTokens => formatter.write_str("max_tokens"),
            Self::ContentFilter => formatter.write_str("content_filter"),
            Self::Cancelled => formatter.write_str("cancelled"),
            Self::Error => formatter.write_str("error"),
            Self::Unknown(value) => formatter.write_str(value),
        }
    }
}
