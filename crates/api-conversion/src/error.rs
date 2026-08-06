//! Conversion errors and Anthropic-style error payload rendering.

use std::fmt;

use serde_json::{json, Value};

/// A request that cannot be represented for the selected upstream conversion.
///
/// Rendered downstream as an Anthropic `invalid_request_error` with HTTP 400.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversionError {
    /// A known downstream field cannot be represented for this provider/model.
    UnsupportedFeature { field: String, reason: String },
    /// The request is malformed in a way that blocks conversion.
    InvalidRequest { message: String },
}

impl fmt::Display for ConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedFeature { field, reason } => {
                write!(formatter, "cannot convert `{field}`: {reason}")
            }
            Self::InvalidRequest { message } => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for ConversionError {}

impl ConversionError {
    pub fn unsupported(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::UnsupportedFeature {
            field: field.into(),
            reason: reason.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            message: message.into(),
        }
    }

    /// Anthropic Messages error envelope:
    /// `{"type":"error","error":{"type":"invalid_request_error","message":...}}`
    pub fn to_anthropic_error_body(&self) -> Value {
        json!({
            "type": "error",
            "error": {
                "type": "invalid_request_error",
                "message": self.to_string(),
            }
        })
    }

    /// OpenAI Responses error envelope: `{"error":{"message":...,"type":...,"param":null,"code":...}}`
    pub fn to_openai_error_body(&self) -> Value {
        json!({
            "error": {
                "message": self.to_string(),
                "type": "invalid_request_error",
                "param": null,
                "code": "invalid_request_error",
            }
        })
    }

    /// Anthropic error type name for an upstream HTTP status when no JSON error type is available.
    pub fn anthropic_error_type_from_status(status: http::StatusCode) -> &'static str {
        match status.as_u16() {
            401 => "authentication_error",
            403 => "permission_error",
            404 => "not_found_error",
            413 => "request_too_large",
            429 => "rate_limit_error",
            500..=599 => "api_error",
            _ => "invalid_request_error",
        }
    }
}

/// Map an upstream OpenAI-style error body to the Anthropic error envelope.
///
/// Upstream JSON of the shape `{"error": {"message", "type", "code", "param"}}` keeps its
/// `type` when it matches Anthropic naming; the HTTP status is preserved by the caller.
pub fn convert_chat_error_body(status: http::StatusCode, body: &Value) -> Value {
    let error = body.get("error").and_then(Value::as_object);
    let upstream_type = error
        .and_then(|error| error.get("type"))
        .and_then(Value::as_str)
        .filter(|error_type| {
            matches!(
                *error_type,
                "invalid_request_error"
                    | "bad_request_error"
                    | "authentication_error"
                    | "permission_error"
                    | "not_found_error"
                    | "request_too_large"
                    | "rate_limit_error"
                    | "api_error"
                    | "overloaded_error"
                    | "billing_error"
            )
        });
    let message = error
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            error
                .and_then(|error| error.get("code"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| format!("upstream request failed with {}", status.as_u16()));

    json!({
        "type": "error",
        "error": {
            "type": upstream_type.unwrap_or_else(|| ConversionError::anthropic_error_type_from_status(status)),
            "message": message,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_unsupported_feature_error_body() {
        let err = ConversionError::unsupported(
            "tools[0].type",
            "provider ds does not accept non-function tool types",
        );
        let body = err.to_anthropic_error_body();
        assert_eq!(body["type"], "error");
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("tools[0].type"));
    }

    #[test]
    fn preserves_upstream_error_type_when_compatible() {
        let body = convert_chat_error_body(
            http::StatusCode::BAD_REQUEST,
            &json!({"error": {"message": "unknown tool type", "type": "invalid_request_error", "param": "tools[0].type"}}),
        );
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["message"], "unknown tool type");
    }

    #[test]
    fn maps_status_when_upstream_type_is_foreign() {
        let body = convert_chat_error_body(
            http::StatusCode::TOO_MANY_REQUESTS,
            &json!({"error": {"message": "rate limited", "type": "my_provider_rate_limit"}}),
        );
        assert_eq!(body["error"]["type"], "rate_limit_error");
    }

    #[test]
    fn preserves_minimax_bad_request_error_type() {
        let body = convert_chat_error_body(
            http::StatusCode::BAD_REQUEST,
            &json!({"error": {"type": "bad_request_error", "message": "invalid params (2013)"}}),
        );
        assert_eq!(body["error"]["type"], "bad_request_error");
        assert_eq!(body["error"]["message"], "invalid params (2013)");
    }

    #[test]
    fn falls_back_to_code_then_status() {
        let body = convert_chat_error_body(
            http::StatusCode::UNAUTHORIZED,
            &json!({"error": {"code": "auth_failed"}}),
        );
        assert_eq!(body["error"]["type"], "authentication_error");
        assert_eq!(body["error"]["message"], "auth_failed");

        let body = convert_chat_error_body(http::StatusCode::BAD_GATEWAY, &json!({"error": {}}));
        assert_eq!(body["error"]["type"], "api_error");
    }
}
