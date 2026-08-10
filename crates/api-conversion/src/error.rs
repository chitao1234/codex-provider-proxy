//! Conversion errors and Anthropic-style error payload rendering.

use std::fmt;

use serde_json::{json, Map, Value};

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

/// Protocol-neutral representation of a non-success API response.
///
/// Standard error semantics are separated from opaque fields so provider-specific
/// diagnostics survive a cross-protocol envelope conversion.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiErrorIr {
    status: http::StatusCode,
    message: String,
    kind: Option<String>,
    code: Option<Value>,
    param: Option<Value>,
    envelope_extensions: Map<String, Value>,
    error_extensions: Map<String, Value>,
}

impl ApiErrorIr {
    pub fn parse(status: http::StatusCode, body: &Value) -> Self {
        let body_object = body.as_object();
        let error_object = body_object
            .and_then(|object| object.get("error"))
            .and_then(Value::as_object)
            .or(body_object);
        let message = error_object
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                error_object
                    .and_then(|error| error.get("code"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| format!("upstream request failed with {}", status.as_u16()));
        let kind = error_object
            .and_then(|error| error.get("type"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let code = error_object.and_then(|error| error.get("code")).cloned();
        let param = error_object.and_then(|error| error.get("param")).cloned();
        let mut envelope_extensions = body_object.cloned().unwrap_or_default();
        envelope_extensions.remove("error");
        envelope_extensions.remove("message");
        envelope_extensions.remove("code");
        envelope_extensions.remove("param");
        if envelope_extensions.get("type").and_then(Value::as_str) == Some("error") {
            envelope_extensions.remove("type");
        }
        let mut error_extensions = error_object.cloned().unwrap_or_default();
        for field in ["message", "type", "code", "param"] {
            error_extensions.remove(field);
        }
        Self {
            status,
            message,
            kind,
            code,
            param,
            envelope_extensions,
            error_extensions,
        }
    }

    pub fn render_anthropic(&self) -> Value {
        let kind = self
            .kind
            .as_deref()
            .filter(|kind| is_anthropic_error_type(kind))
            .unwrap_or_else(|| ConversionError::anthropic_error_type_from_status(self.status));
        let mut error = self.error_extensions.clone();
        error.insert("type".to_owned(), Value::String(kind.to_owned()));
        error.insert("message".to_owned(), Value::String(self.message.clone()));
        if let Some(code) = &self.code {
            error.insert("code".to_owned(), code.clone());
        }
        if let Some(param) = &self.param {
            error.insert("param".to_owned(), param.clone());
        }
        let mut body = self.envelope_extensions.clone();
        body.insert("type".to_owned(), Value::String("error".to_owned()));
        body.insert("error".to_owned(), Value::Object(error));
        Value::Object(body)
    }

    pub fn render_openai(&self) -> Value {
        let mut error = self.error_extensions.clone();
        error.insert("message".to_owned(), Value::String(self.message.clone()));
        error.insert(
            "type".to_owned(),
            Value::String(
                self.kind
                    .clone()
                    .unwrap_or_else(|| openai_error_type_from_status(self.status).to_owned()),
            ),
        );
        error.insert(
            "param".to_owned(),
            self.param.clone().unwrap_or(Value::Null),
        );
        error.insert("code".to_owned(), self.code.clone().unwrap_or(Value::Null));
        let mut body = self.envelope_extensions.clone();
        body.insert("error".to_owned(), Value::Object(error));
        Value::Object(body)
    }
}

fn is_anthropic_error_type(kind: &str) -> bool {
    matches!(
        kind,
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
}

fn openai_error_type_from_status(status: http::StatusCode) -> &'static str {
    match status.as_u16() {
        429 => "rate_limit_error",
        500..=599 => "server_error",
        _ => "invalid_request_error",
    }
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
        let body = ApiErrorIr::parse(
            http::StatusCode::BAD_REQUEST,
            &json!({"error": {"message": "unknown tool type", "type": "invalid_request_error", "param": "tools[0].type"}}),
        )
        .render_anthropic();
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["message"], "unknown tool type");
    }

    #[test]
    fn maps_status_when_upstream_type_is_foreign() {
        let body = ApiErrorIr::parse(
            http::StatusCode::TOO_MANY_REQUESTS,
            &json!({"error": {"message": "rate limited", "type": "my_provider_rate_limit"}}),
        )
        .render_anthropic();
        assert_eq!(body["error"]["type"], "rate_limit_error");
    }

    #[test]
    fn preserves_minimax_bad_request_error_type() {
        let body = ApiErrorIr::parse(
            http::StatusCode::BAD_REQUEST,
            &json!({"error": {"type": "bad_request_error", "message": "invalid params (2013)"}}),
        )
        .render_anthropic();
        assert_eq!(body["error"]["type"], "bad_request_error");
        assert_eq!(body["error"]["message"], "invalid params (2013)");
    }

    #[test]
    fn falls_back_to_code_then_status() {
        let body = ApiErrorIr::parse(
            http::StatusCode::UNAUTHORIZED,
            &json!({"error": {"code": "auth_failed"}}),
        )
        .render_anthropic();
        assert_eq!(body["error"]["type"], "authentication_error");
        assert_eq!(body["error"]["message"], "auth_failed");

        let body = ApiErrorIr::parse(http::StatusCode::BAD_GATEWAY, &json!({"error": {}}))
            .render_anthropic();
        assert_eq!(body["error"]["type"], "api_error");
    }

    #[test]
    fn converts_anthropic_error_to_openai_without_losing_extensions() {
        let body = ApiErrorIr::parse(
            http::StatusCode::TOO_MANY_REQUESTS,
            &json!({
                "type": "error",
                "request_id": "req_1",
                "error": {
                    "type": "rate_limit_error",
                    "message": "slow down",
                    "retry_after": 2
                }
            }),
        )
        .render_openai();
        assert_eq!(body["request_id"], "req_1");
        assert_eq!(body["error"]["type"], "rate_limit_error");
        assert_eq!(body["error"]["message"], "slow down");
        assert_eq!(body["error"]["retry_after"], 2);
        assert!(body["error"]["code"].is_null());
        assert!(body["error"]["param"].is_null());
    }
}
