use serde_json::Value;

/// Semantic scope of an extension. Scope prevents unrelated provider fields from being merged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionScope {
    Root,
    Request,
    Response,
    Message,
    ContentItem,
    ToolCall,
    ToolDefinition,
    Usage,
    State,
    Provider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayPolicy {
    NeverReplay,
    ReplayAsSameProtocolItem,
    ReplayAsTargetOpaque,
    ReplayIfSupported,
    ProviderOnly,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Extension {
    pub namespace: String,
    pub key: String,
    pub value: Value,
    pub scope: ExtensionScope,
    pub source_protocol: Option<String>,
    pub provider: Option<String>,
    pub replay: ReplayPolicy,
}

impl Extension {
    pub fn new(namespace: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
            value,
            scope: ExtensionScope::Root,
            source_protocol: None,
            provider: None,
            replay: ReplayPolicy::NeverReplay,
        }
    }

    pub fn response_opaque(
        namespace: impl Into<String>,
        key: impl Into<String>,
        value: Value,
    ) -> Self {
        Self::scoped_opaque(namespace, key, value, ExtensionScope::Response)
    }

    pub fn scoped_opaque(
        namespace: impl Into<String>,
        key: impl Into<String>,
        value: Value,
        scope: ExtensionScope,
    ) -> Self {
        let namespace = namespace.into();
        let mut extension = Self::new(namespace.clone(), key, value);
        extension.scope = scope;
        extension.source_protocol = Some(namespace);
        extension.replay = ReplayPolicy::ReplayAsTargetOpaque;
        extension
    }

    pub fn is_replayable_as_opaque(&self) -> bool {
        matches!(
            self.replay,
            ReplayPolicy::ReplayAsTargetOpaque | ReplayPolicy::ReplayIfSupported
        )
    }

    pub fn is_replayable_in(&self, scope: ExtensionScope) -> bool {
        self.scope == scope && self.is_replayable_as_opaque()
    }
}
