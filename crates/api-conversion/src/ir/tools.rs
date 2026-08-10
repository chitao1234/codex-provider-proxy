use std::collections::BTreeMap;

use serde_json::Value;

use super::extension::Extension;

#[derive(Debug, Clone, PartialEq)]
pub struct ToolDefinition {
    pub kind: String,
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Option<Value>,
    pub metadata: BTreeMap<String, Value>,
    pub extensions: Vec<Extension>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub id: Option<String>,
    pub name: String,
    pub arguments: String,
    pub status: Option<String>,
    pub metadata: BTreeMap<String, Value>,
    pub extensions: Vec<Extension>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolResult {
    pub call_id: Option<String>,
    pub content: Vec<super::content::ContentPart>,
    pub is_error: bool,
    pub extensions: Vec<Extension>,
}
