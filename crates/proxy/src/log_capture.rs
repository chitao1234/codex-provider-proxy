use std::sync::{Arc, Mutex};

use bytes::Bytes;

#[derive(Debug, Default)]
pub struct Capture {
    bytes: Vec<u8>,
    truncated: bool,
    max_bytes: usize,
}

impl Capture {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            truncated: false,
            max_bytes,
        }
    }

    pub fn push_chunk(&mut self, chunk: &Bytes) {
        if self.truncated || self.max_bytes == 0 {
            return;
        }
        let remaining = self.max_bytes.saturating_sub(self.bytes.len());
        let take = remaining.min(chunk.len());
        self.bytes.extend_from_slice(&chunk[..take]);
        if take < chunk.len() {
            self.truncated = true;
        }
    }

    pub fn summary(&self) -> CaptureSummary {
        CaptureSummary {
            bytes: self.bytes.clone(),
            truncated: self.truncated,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CaptureSummary {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

impl CaptureSummary {
    pub fn as_lossy_utf8(&self) -> String {
        String::from_utf8_lossy(&self.bytes).to_string()
    }
}

pub type SharedCapture = Arc<Mutex<Capture>>;
