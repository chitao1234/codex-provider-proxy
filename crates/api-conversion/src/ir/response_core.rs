use std::collections::BTreeMap;

use super::{
    content::ContentItem,
    response::{ResponseIr, Usage},
    state::ConversationState,
    streaming::{CanonicalResponseEvent, CoreUpdate, ItemKind, ResponseSnapshot},
    tools::ToolCall,
};

#[derive(Debug, Clone, Default)]
pub struct ResponseAccumulator {
    response: ResponseIr,
    item_kinds: BTreeMap<String, ItemKind>,
    item_indexes: BTreeMap<String, usize>,
    terminal: bool,
    failed: bool,
}

impl ResponseAccumulator {
    pub fn from_response(response: ResponseIr) -> Self {
        Self {
            response,
            terminal: true,
            failed: false,
            ..Self::default()
        }
    }

    pub fn apply(&mut self, event: CanonicalResponseEvent) -> CoreUpdate {
        let mut newly_completed = false;
        match &event {
            CanonicalResponseEvent::StreamStarted { response_id, model } => {
                self.response.response_id.clone_from(response_id);
                self.response.model.clone_from(model);
                self.response
                    .state
                    .source_response_id
                    .clone_from(response_id);
            }
            CanonicalResponseEvent::ItemStarted { item_id, item } => {
                if !self.item_indexes.contains_key(item_id) {
                    let index = self.response.output.len();
                    self.item_indexes.insert(item_id.clone(), index);
                    self.item_kinds.insert(item_id.clone(), *item);
                    self.response.output.push(empty_item(*item, item_id));
                }
            }
            CanonicalResponseEvent::ItemMetadataUpdated { item_id, metadata } => {
                if let Some(item) = self.item_mut(item_id) {
                    item_extensions_mut(item).extend(metadata.clone());
                } else {
                    self.response.extensions.extend(metadata.clone());
                }
            }
            CanonicalResponseEvent::ContentDelta { item_id, content } => {
                self.ensure_item(item_id, ItemKind::Message);
                if let Some(item) = self.item_mut(item_id) {
                    append_content(item, content.clone());
                }
            }
            CanonicalResponseEvent::ToolCallStarted { item_id, call } => {
                self.ensure_item(item_id, ItemKind::ToolCall);
                if let Some(ContentItem::ToolUse { call: current, .. }) = self.item_mut(item_id) {
                    *current = call.clone();
                }
            }
            CanonicalResponseEvent::ToolArgumentsDelta { item_id, arguments } => {
                self.ensure_item(item_id, ItemKind::ToolCall);
                if let Some(ContentItem::ToolUse { call, .. }) = self.item_mut(item_id) {
                    call.arguments.push_str(arguments);
                }
            }
            CanonicalResponseEvent::UsageUpdated(usage) => {
                merge_usage(&mut self.response.usage, usage)
            }
            CanonicalResponseEvent::ItemCompleted { .. } => {}
            CanonicalResponseEvent::ResponseCompleted {
                stop_reason,
                original_reason,
            } => {
                if !self.failed {
                    self.response.stop_reason = Some(stop_reason.clone());
                    self.response
                        .original_stop_reason
                        .clone_from(original_reason);
                }
                self.terminal = true;
                newly_completed = true;
            }
            CanonicalResponseEvent::StreamError { message, code } => {
                self.failed = true;
                self.terminal = true;
                self.response.error = Some(serde_json::json!({"message": message, "code": code}));
                self.response.stop_reason = Some(super::stop_reason::StopReason::Error);
            }
            CanonicalResponseEvent::ProtocolExtension(extension) => {
                self.response.extensions.push(extension.clone())
            }
        }
        CoreUpdate {
            event,
            snapshot: self.response.clone(),
            newly_completed,
        }
    }

    pub fn snapshot(&self) -> ResponseSnapshot {
        ResponseSnapshot {
            response: self.response.clone(),
            terminal: self.terminal,
            failed: self.failed,
        }
    }

    pub fn response(&self) -> &ResponseIr {
        &self.response
    }
    pub fn continuation_state(&self) -> Option<ConversationState> {
        if self.terminal
            && !self.failed
            && self
                .response
                .stop_reason
                .as_ref()
                .is_some_and(|reason| reason.is_successful())
        {
            Some(self.response.state.clone())
        } else {
            None
        }
    }
    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    fn ensure_item(&mut self, item_id: &str, kind: ItemKind) {
        if !self.item_indexes.contains_key(item_id) {
            let index = self.response.output.len();
            self.item_indexes.insert(item_id.to_owned(), index);
            self.item_kinds.insert(item_id.to_owned(), kind);
            self.response.output.push(empty_item(kind, item_id));
        }
    }

    fn item_mut(&mut self, item_id: &str) -> Option<&mut ContentItem> {
        self.item_indexes
            .get(item_id)
            .and_then(|index| self.response.output.get_mut(*index))
    }
}

fn empty_item(kind: ItemKind, item_id: &str) -> ContentItem {
    match kind {
        ItemKind::Reasoning => ContentItem::Reasoning {
            id: Some(item_id.to_owned()),
            content: Vec::new(),
            extensions: Vec::new(),
        },
        ItemKind::ToolCall => ContentItem::ToolUse {
            id: Some(item_id.to_owned()),
            call: ToolCall {
                id: Some(item_id.to_owned()),
                name: String::new(),
                arguments: String::new(),
                status: None,
                metadata: BTreeMap::new(),
                function_metadata: BTreeMap::new(),
                extensions: Vec::new(),
            },
        },
        ItemKind::ServerTool => ContentItem::ServerTool {
            id: Some(item_id.to_owned()),
            kind: String::new(),
            payload: serde_json::Value::Null,
            extensions: Vec::new(),
        },
        ItemKind::ToolResult => ContentItem::ToolResult {
            result: super::tools::ToolResult {
                call_id: Some(item_id.to_owned()),
                content: Vec::new(),
                is_error: false,
                extensions: Vec::new(),
            },
        },
        ItemKind::Unknown => ContentItem::Unknown {
            id: Some(item_id.to_owned()),
            kind: String::new(),
            payload: serde_json::Value::Null,
            extensions: Vec::new(),
        },
        ItemKind::Message => ContentItem::Message {
            role: "assistant".to_owned(),
            content: Vec::new(),
            extensions: Vec::new(),
        },
        ItemKind::Refusal => ContentItem::Refusal {
            id: Some(item_id.to_owned()),
            content: Vec::new(),
            extensions: Vec::new(),
        },
    }
}

fn item_extensions_mut(item: &mut ContentItem) -> &mut Vec<super::extension::Extension> {
    match item {
        ContentItem::Message { extensions, .. }
        | ContentItem::Refusal { extensions, .. }
        | ContentItem::Reasoning { extensions, .. }
        | ContentItem::ServerTool { extensions, .. }
        | ContentItem::Unknown { extensions, .. } => extensions,
        ContentItem::ToolUse { call, .. } => &mut call.extensions,
        ContentItem::ToolResult { result } => &mut result.extensions,
    }
}

fn append_content(item: &mut ContentItem, content: super::content::ContentPart) {
    match item {
        ContentItem::Message { content: parts, .. }
        | ContentItem::Refusal { content: parts, .. }
        | ContentItem::Reasoning { content: parts, .. } => parts.push(content),
        ContentItem::ToolResult { result } => result.content.push(content),
        ContentItem::ToolUse { .. }
        | ContentItem::ServerTool { .. }
        | ContentItem::Unknown { .. } => {}
    }
}

fn merge_usage(current: &mut Usage, update: &Usage) {
    current.input_tokens = max_option(current.input_tokens, update.input_tokens);
    current.output_tokens = max_option(current.output_tokens, update.output_tokens);
    current.total_tokens = max_option(current.total_tokens, update.total_tokens);
    current.cached_input_tokens =
        max_option(current.cached_input_tokens, update.cached_input_tokens);
}

fn max_option(current: Option<u64>, update: Option<u64>) -> Option<u64> {
    match (current, update) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}
