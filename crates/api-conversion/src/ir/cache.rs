use serde_json::Value;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CacheIdentity {
    pub key: Option<String>,
    pub options: Option<Value>,
    pub retention: Option<Value>,
    pub control: Option<Value>,
}

impl CacheIdentity {
    pub fn is_empty(&self) -> bool {
        self.key.is_none()
            && self.options.is_none()
            && self.retention.is_none()
            && self.control.is_none()
    }
}
