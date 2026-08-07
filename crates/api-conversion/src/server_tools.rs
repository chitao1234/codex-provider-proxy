//! Shared recognition of provider-side tools declared by Messages clients.

/// Prefixes of versioned Anthropic server-side tool types.
pub const TYPE_PREFIXES: &[&str] = &[
    "web_search_",
    "web_fetch_",
    "code_execution_",
    "tool_search_",
    "computer_",
    "memory_",
    "bash_",
    "text_editor_",
];

/// Whether a tool name is conventionally executed by a Messages provider.
pub fn is_known_name(name: &str) -> bool {
    matches!(
        name,
        "web_search"
            | "web_fetch"
            | "WebSearch"
            | "WebFetch"
            | "code_execution"
            | "bash"
            | "str_replace_editor"
            | "str_replace_based_edit_tool"
            | "memory"
            | "tool_search_tool_regex"
            | "tool_search_tool_bm25"
    )
}
