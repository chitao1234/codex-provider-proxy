//! Server-side state for the Responses API `previous_response_id` continuation.
//!
//! A Responses client can continue a conversation by passing `previous_response_id`.
//! While translation is enabled the proxy owns that identifier boundary, so it stores
//! protocol-neutral request/response history and materializes the complete conversation
//! for whichever upstream protocol handles the next turn.
//!
//! Entries remember the provider that produced them for diagnostics, but continuation remains
//! intentionally portable across provider or model changes. Entries expire after a TTL to bound
//! memory.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use codex_provider_proxy_api_conversion::ir::RequestIr;

/// Default time-to-live for stored response transcripts.
pub const DEFAULT_RESPONSE_STATE_TTL_SECS: u64 = 3600;
/// Default cap on stored transcripts (oldest evicted first).
pub const DEFAULT_RESPONSE_STATE_MAX_ENTRIES: usize = 1024;

/// Protocol-neutral transcript stored for one synthesized Responses response.
#[derive(Debug, Clone, PartialEq)]
pub struct ResponseState {
    pub response_id: String,
    pub provider_name: String,
    pub model: String,
    /// The complete protocol-neutral request transcript, ready to render for a continuation.
    pub request_ir: RequestIr,
    /// Reasoning from the assistant turn that produced this response.
    pub last_reasoning: Option<String>,
    pub created_at_unix: u64,
    pub expires_at_unix: u64,
}

impl ResponseState {
    pub fn new(
        response_id: String,
        provider_name: String,
        model: String,
        request_ir: RequestIr,
        last_reasoning: Option<String>,
        now_unix: u64,
        ttl_secs: u64,
    ) -> Self {
        Self {
            response_id,
            provider_name,
            model,
            request_ir,
            last_reasoning,
            created_at_unix: now_unix,
            expires_at_unix: now_unix.saturating_add(ttl_secs),
        }
    }
}

/// Thread-safe store of response transcripts with capacity eviction.
#[derive(Clone)]
pub struct ResponseStateStore {
    inner: Arc<Mutex<StoreInner>>,
}

struct StoreInner {
    states: HashMap<String, ResponseState>,
    max_entries: usize,
}

impl ResponseStateStore {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner {
                states: HashMap::new(),
                max_entries,
            })),
        }
    }

    /// Store a transcript for `response_id` (replacing any existing entry).
    pub fn put(&self, state: ResponseState) {
        let mut inner = self.inner.lock().expect("response state mutex poisoned");
        let now = now_unix();
        // Drop expired entries opportunistically, then evict oldest if over capacity.
        inner
            .states
            .retain(|_, existing| existing.expires_at_unix > now);
        inner.states.insert(state.response_id.clone(), state);
        if inner.states.len() > inner.max_entries {
            let mut oldest: Vec<(u64, String)> = inner
                .states
                .iter()
                .map(|(id, state)| (state.created_at_unix, id.clone()))
                .collect();
            oldest.sort_unstable();
            let over = inner.states.len() - inner.max_entries;
            for (_, id) in oldest.into_iter().take(over) {
                inner.states.remove(&id);
            }
        }
    }

    /// Look up the transcript stored for `response_id`; expired entries are removed.
    pub fn get(&self, response_id: &str) -> Option<ResponseState> {
        let mut inner = self.inner.lock().expect("response state mutex poisoned");
        let now = now_unix();
        if inner
            .states
            .get(response_id)
            .is_some_and(|state| state.expires_at_unix <= now)
        {
            inner.states.remove(response_id);
            return None;
        }
        inner.states.get(response_id).cloned()
    }

    /// Number of stored transcripts (test helper).
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("response state mutex poisoned")
            .states
            .len()
    }
}

pub(crate) fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_provider_proxy_api_conversion::{
        protocol::Protocol,
        request_adapter::{RequestAdapter, RequestAdapterKind},
    };
    use serde_json::json;

    fn request(body: serde_json::Value) -> RequestIr {
        RequestAdapterKind::for_protocol(Protocol::OpenAiChatCompletions)
            .parse_v2(&body)
            .unwrap()
    }

    fn state(id: &str, created: u64) -> ResponseState {
        ResponseState::new(
            id.to_string(),
            "ds".to_string(),
            "deepseek-v4-pro".to_string(),
            request(serde_json::json!({"messages": []})),
            None,
            created,
            100,
        )
    }

    #[test]
    fn put_get_round_trips() {
        let store = ResponseStateStore::new(4);
        let state = ResponseState::new(
            "resp_1".to_string(),
            "ds".to_string(),
            "m".to_string(),
            request(json!({"messages": [{"role": "user", "content": "hi"}]})),
            Some("think".to_string()),
            now_unix(),
            100,
        );
        store.put(state.clone());
        let got = store.get("resp_1").expect("stored state retrievable");
        assert_eq!(got.response_id, "resp_1");
        assert_eq!(got.request_ir, state.request_ir);
        assert_eq!(got.last_reasoning, state.last_reasoning);
        assert_eq!(store.get("resp_missing"), None);
    }

    #[test]
    fn expired_entry_is_removed() {
        let store = ResponseStateStore::new(4);
        let state = ResponseState::new(
            "resp_old".to_string(),
            "ds".to_string(),
            "m".to_string(),
            request(serde_json::json!({"messages": []})),
            None,
            now_unix().saturating_sub(100), // created long ago, already expired
            10,
        );
        store.put(state);
        assert_eq!(store.get("resp_old"), None);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn evicts_oldest_over_capacity() {
        let store = ResponseStateStore::new(2);
        let now = now_unix();
        store.put(state("resp_1", now));
        store.put(state("resp_2", now + 1));
        store.put(state("resp_3", now + 2));
        assert_eq!(store.get("resp_1"), None, "oldest evicted");
        assert!(store.get("resp_2").is_some());
        assert!(store.get("resp_3").is_some());
    }
}
