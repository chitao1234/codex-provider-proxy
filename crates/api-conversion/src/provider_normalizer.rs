use serde_json::Value;

use crate::{
    dialect::ModelCapabilities, error::ConversionError, ir::RequestIr, protocol::Protocol,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestNormalizerKind {
    ArkResponses,
}

impl RequestNormalizerKind {
    pub fn target(self) -> Protocol {
        match self {
            Self::ArkResponses => Protocol::OpenAiResponses,
        }
    }

    pub fn normalize(
        self,
        semantic: &RequestIr,
        capabilities: &ModelCapabilities,
        target: Protocol,
        rendered: Value,
    ) -> Result<Value, ConversionError> {
        if target != self.target() {
            return Err(ConversionError::unsupported(
                "request normalizer",
                "provider normalizer is attached to a different target protocol",
            ));
        }
        Ok(match self {
            Self::ArkResponses => crate::protocols::openai_responses::normalizer::normalize_ark(
                semantic,
                capabilities,
                rendered,
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ark_normalizer_rejects_non_responses_targets() {
        let result = RequestNormalizerKind::ArkResponses.normalize(
            &RequestIr::default(),
            &ModelCapabilities::default(),
            Protocol::OpenAiChatCompletions,
            serde_json::json!({}),
        );

        assert!(result.is_err());
    }
}
