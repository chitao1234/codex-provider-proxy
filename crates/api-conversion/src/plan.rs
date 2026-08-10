use crate::{
    dialect::{DownstreamApi, UpstreamApi},
    protocol::Protocol,
    provider_normalizer::RequestNormalizerKind,
    request::{RequestParserKind, RequestRendererKind},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestPipeline {
    pub source: Protocol,
    pub target: Protocol,
    pub parser: RequestParserKind,
    pub renderer: RequestRendererKind,
    pub normalizer: Option<RequestNormalizerKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestAction {
    Passthrough,
    Transform(RequestPipeline),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuationPolicy {
    None,
    ResponsesState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamPolicy {
    Passthrough,
    Translate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionPolicy {
    PreserveScopedOpaque,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponsePipeline {
    pub source: Protocol,
    pub target: Protocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConversionPlan {
    pub downstream: DownstreamApi,
    pub upstream: UpstreamApi,
    pub source: Protocol,
    pub target: Protocol,
    pub request: RequestAction,
    pub response: ResponsePipeline,
    pub continuation: ContinuationPolicy,
    pub stream: StreamPolicy,
    pub extensions: ExtensionPolicy,
}

impl ConversionPlan {
    pub fn new(
        downstream: DownstreamApi,
        upstream: UpstreamApi,
        source: Protocol,
        target: Protocol,
        normalizer: Option<RequestNormalizerKind>,
    ) -> Self {
        let request = if source == target && normalizer.is_none() {
            RequestAction::Passthrough
        } else {
            RequestAction::Transform(RequestPipeline {
                source,
                target,
                parser: RequestParserKind::for_protocol(source),
                renderer: RequestRendererKind::for_protocol(target),
                normalizer,
            })
        };
        Self {
            downstream,
            upstream,
            source,
            target,
            request,
            response: ResponsePipeline {
                source: target,
                target: source,
            },
            continuation: if source == Protocol::OpenAiResponses {
                ContinuationPolicy::ResponsesState
            } else {
                ContinuationPolicy::None
            },
            stream: if source == target {
                StreamPolicy::Passthrough
            } else {
                StreamPolicy::Translate
            },
            extensions: ExtensionPolicy::PreserveScopedOpaque,
        }
    }
}

impl ResponsePipeline {
    pub fn parser(self) -> crate::protocols::ResponseParserKind {
        crate::protocols::ResponseParserKind::for_protocol(self.source)
    }

    pub fn renderer(
        self,
    ) -> Result<crate::protocols::ResponseRendererKind, crate::error::ConversionError> {
        crate::protocols::ResponseRendererKind::for_protocol(self.target)
    }

    pub fn stream_parser(self) -> crate::protocols::StreamParserKind {
        crate::protocols::StreamParserKind::for_protocol(self.source)
    }

    pub fn stream_renderer(
        self,
    ) -> Result<crate::protocols::StreamRendererKind, crate::error::ConversionError> {
        crate::protocols::StreamRendererKind::for_protocol(self.target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_derives_all_execution_policies_from_protocols() {
        let plan = ConversionPlan::new(
            DownstreamApi::OpenAiResponses,
            UpstreamApi::AnthropicMessages,
            Protocol::OpenAiResponses,
            Protocol::AnthropicMessages,
            None,
        );

        assert_eq!(plan.source, Protocol::OpenAiResponses);
        assert_eq!(plan.target, Protocol::AnthropicMessages);
        assert_eq!(
            plan.request,
            RequestAction::Transform(RequestPipeline {
                source: Protocol::OpenAiResponses,
                target: Protocol::AnthropicMessages,
                parser: RequestParserKind::OpenAiResponses,
                renderer: RequestRendererKind::AnthropicMessages,
                normalizer: None,
            })
        );
        assert_eq!(
            plan.response,
            ResponsePipeline {
                source: Protocol::AnthropicMessages,
                target: Protocol::OpenAiResponses,
            }
        );
        assert_eq!(plan.continuation, ContinuationPolicy::ResponsesState);
        assert_eq!(plan.stream, StreamPolicy::Translate);
        assert_eq!(plan.extensions, ExtensionPolicy::PreserveScopedOpaque);
    }

    #[test]
    fn same_protocol_plan_is_transparent_without_a_normalizer() {
        let plan = ConversionPlan::new(
            DownstreamApi::AnthropicMessages,
            UpstreamApi::AnthropicMessages,
            Protocol::AnthropicMessages,
            Protocol::AnthropicMessages,
            None,
        );

        assert_eq!(plan.request, RequestAction::Passthrough);
        assert_eq!(plan.stream, StreamPolicy::Passthrough);
    }
}
