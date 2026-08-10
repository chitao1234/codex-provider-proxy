use std::collections::BTreeMap;

use serde_json::Value;

use super::{cache::CacheIdentity, extension::Extension, identity::Identity};

/// Immutable state carried between requests in a stateful protocol conversation.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConversationState {
    pub logical_conversation_id: Option<String>,
    pub conversation: Option<Value>,
    pub source_response_id: Option<String>,
    pub target_response_ids: BTreeMap<String, String>,
    pub identity: Identity,
    pub cache: CacheIdentity,
    pub safety_identifier: Option<String>,
    pub metadata: Option<Value>,
    pub store: Option<Value>,
    pub include: Option<Value>,
    pub truncation: Option<Value>,
    pub extensions: Vec<Extension>,
}

impl ConversationState {
    pub fn can_continue(&self) -> bool {
        self.source_response_id.is_some() || !self.target_response_ids.is_empty()
    }
}
