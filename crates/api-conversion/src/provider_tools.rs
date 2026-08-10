use serde_json::{Map, Value};

use crate::{
    dialect::{ModelCapabilities, RequestConversionReport, ServerToolPolicy},
    ir::ToolDefinition,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolTarget {
    AnthropicMessages,
    OpenAiChatCompletions,
    OpenAiResponses,
}

pub fn render(
    tools: &[ToolDefinition],
    target: ToolTarget,
    capabilities: &ModelCapabilities,
    render_plain: impl Fn(&ToolDefinition) -> Value,
    render_mapped: impl Fn(&ToolDefinition, &MappedFunctionTool) -> Value,
    last_user_query: Option<&str>,
) -> (Vec<Value>, RequestConversionReport) {
    let mut report = RequestConversionReport::default();
    let mut rendered = Vec::new();
    for tool in tools {
        if is_freeform_tool(tool) && target == ToolTarget::OpenAiResponses {
            rendered.push(opaque_value(tool));
            continue;
        }
        if !is_server_tool(tool) {
            rendered.push(render_plain(tool));
            continue;
        }
        let reported_name = if tool.name == "tool" {
            tool.kind.clone()
        } else {
            tool.name.clone()
        };
        match capabilities.server_tools {
            ServerToolPolicy::Drop => report.dropped_server_tools.push(reported_name),
            ServerToolPolicy::MapToFunction => {
                if matches!(short_name(tool), Some("web_search" | "web_fetch")) {
                    let mapped = mapped_function(tool);
                    report.mapped_server_tools.push(mapped.name.clone());
                    rendered.push(render_mapped(tool, &mapped));
                } else {
                    report.dropped_server_tools.push(reported_name);
                }
            }
            ServerToolPolicy::Passthrough => rendered.push(opaque_value(tool)),
            ServerToolPolicy::ProviderNative => match short_name(tool) {
                Some("web_search") => {
                    if let Some(template) = &capabilities.search_tool_template {
                        rendered.push(crate::conversion_utils::render_search_tool_template(
                            template,
                            last_user_query,
                        ));
                        report.mapped_server_tools.push("web_search".to_owned());
                    } else if capabilities.search_request_params.is_some() {
                        report.mapped_server_tools.push("web_search".to_owned());
                    } else {
                        report.dropped_server_tools.push(reported_name);
                    }
                }
                Some("web_fetch") if capabilities.fetch_request_params.is_some() => {
                    report.mapped_server_tools.push("web_fetch".to_owned());
                }
                Some("code_execution")
                    if capabilities.code_interpreter_request_params.is_some() =>
                {
                    report.mapped_server_tools.push("code_execution".to_owned());
                }
                _ => report.dropped_server_tools.push(reported_name),
            },
        }
    }
    (rendered, report)
}

pub fn merge_native_params(
    output: &mut Map<String, Value>,
    capabilities: &ModelCapabilities,
    report: &mut RequestConversionReport,
    last_user_query: Option<&str>,
) {
    for (tool_name, params) in [
        ("web_search", capabilities.search_request_params.as_ref()),
        ("web_fetch", capabilities.fetch_request_params.as_ref()),
        (
            "code_execution",
            capabilities.code_interpreter_request_params.as_ref(),
        ),
    ] {
        if report
            .mapped_server_tools
            .iter()
            .any(|mapped| mapped == tool_name)
        {
            merge_params(output, tool_name, params, last_user_query);
        }
    }
    for tool_name in &capabilities.always_enable_tools {
        let normalized = normalize_name(tool_name);
        if !report
            .mapped_server_tools
            .iter()
            .any(|mapped| mapped == normalized)
        {
            report.mapped_server_tools.push(normalized.to_owned());
        }
        let params = match normalized {
            "web_search" => capabilities.search_request_params.as_ref(),
            "web_fetch" => capabilities.fetch_request_params.as_ref(),
            "code_execution" => capabilities.code_interpreter_request_params.as_ref(),
            _ => None,
        };
        merge_params(output, normalized, params, last_user_query);
    }
    if report
        .mapped_server_tools
        .iter()
        .any(|name| name == "code_execution")
    {
        output.insert("stream".to_owned(), Value::Bool(true));
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MappedFunctionTool {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

fn mapped_function(tool: &ToolDefinition) -> MappedFunctionTool {
    let name = short_name(tool)
        .map(str::to_owned)
        .unwrap_or_else(|| tool.name.replace('-', "_"));
    match short_name(tool) {
        Some("web_search") => MappedFunctionTool {
            name,
            description: "Search the web for current information. Call this when the user asks about current events, recent information, or anything that may have changed since the model's knowledge cutoff.".to_owned(),
            parameters: serde_json::json!({"type":"object","properties":{"query":{"type":"string","description":"The search query"}},"required":["query"]}),
        },
        Some("web_fetch") => MappedFunctionTool {
            name,
            description: "Fetch the content of a specific URL. Call this when you need the full content of a page the user referenced.".to_owned(),
            parameters: serde_json::json!({"type":"object","properties":{"url":{"type":"string","description":"The URL to fetch"}},"required":["url"]}),
        },
        _ => MappedFunctionTool {
            name,
            description: tool.description.clone().unwrap_or_default(),
            parameters: tool
                .input_schema
                .clone()
                .unwrap_or_else(|| serde_json::json!({"type":"object","properties":{}})),
        },
    }
}

fn merge_params(
    output: &mut Map<String, Value>,
    tool_name: &str,
    params: Option<&Value>,
    last_user_query: Option<&str>,
) {
    let Some(params) = params else {
        return;
    };
    let rendered = if tool_name == "web_search" {
        crate::conversion_utils::render_search_tool_template(params, last_user_query)
    } else {
        params.clone()
    };
    if let Value::Object(rendered) = rendered {
        output.extend(rendered);
    }
}

fn opaque_value(tool: &ToolDefinition) -> Value {
    Value::Object(tool.metadata.clone().into_iter().collect())
}

fn is_server_tool(tool: &ToolDefinition) -> bool {
    if !matches!(tool.kind.as_str(), "function" | "custom" | "") {
        return true;
    }
    is_freeform_tool(tool) || crate::server_tools::is_known_name(&tool.name)
}

fn is_freeform_tool(tool: &ToolDefinition) -> bool {
    tool.kind == "custom" && tool.input_schema.is_none() && tool.metadata.contains_key("format")
}

fn short_name(tool: &ToolDefinition) -> Option<&str> {
    if crate::server_tools::is_known_name(&tool.name) {
        return Some(normalize_name(&tool.name));
    }
    let kind = tool.kind.as_str();
    match kind {
        "web_search" | "web_search_preview" => return Some("web_search"),
        "web_fetch" | "web_fetch_preview" => return Some("web_fetch"),
        "code_interpreter" => return Some("code_execution"),
        _ if crate::server_tools::is_known_name(kind) => return Some(normalize_name(kind)),
        _ => {}
    }
    crate::server_tools::TYPE_PREFIXES
        .iter()
        .find(|prefix| kind.starts_with(**prefix))
        .map(|prefix| prefix.trim_end_matches('_'))
}

fn normalize_name(name: &str) -> &str {
    match name {
        "WebSearch" => "web_search",
        "WebFetch" => "web_fetch",
        "CodeExecution" => "code_execution",
        other => other,
    }
}
