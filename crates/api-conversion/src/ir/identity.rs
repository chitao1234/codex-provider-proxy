#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Identity {
    pub conversation_id: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub response_id: Option<String>,
}

impl Identity {
    pub fn is_empty(&self) -> bool {
        self.conversation_id.is_none()
            && self.user_id.is_none()
            && self.session_id.is_none()
            && self.response_id.is_none()
    }
}
