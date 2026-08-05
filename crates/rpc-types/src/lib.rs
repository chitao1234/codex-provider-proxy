use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetRouteRequest {
    pub pid: u32,
    pub provider: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetDefaultProviderRequest {
    pub provider: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRouteResponse {
    pub removed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRoutesResponse {
    pub routes: Vec<RouteEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteEntry {
    pub pid: u32,
    pub provider: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvidersResponse {
    pub default_provider: String,
    pub providers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsResponse {
    pub enabled: bool,
    pub generated_unix_ms: u64,
    pub window: StatisticsWindow,
    pub summary: StatisticsSummary,
    pub tokens: TokenUsageSummary,
    pub providers: Vec<StatisticsBreakdown>,
    pub models: Vec<StatisticsBreakdown>,
    pub client_ips: Vec<StatisticsBreakdown>,
    pub pids: Vec<StatisticsBreakdown>,
    pub route_pids: Vec<StatisticsBreakdown>,
    pub status_codes: Vec<StatisticsBreakdown>,
    pub methods: Vec<StatisticsBreakdown>,
    pub paths: Vec<StatisticsBreakdown>,
    pub hourly: Vec<HourlyStatistics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsWindow {
    pub hours: Option<u32>,
    pub from_unix_ms: Option<u64>,
    pub to_unix_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatisticsSummary {
    pub requests: u64,
    pub successful_requests: u64,
    pub error_requests: u64,
    pub error_rate: f64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub attempts: u64,
    pub average_attempts: f64,
    pub upstream_latency_ms: LatencySummary,
    pub total_duration_ms: LatencySummary,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencySummary {
    pub count: u64,
    pub average: f64,
    pub min: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenUsageSummary {
    pub responses_with_usage: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub cached_tokens: u64,
    pub cache_creation_tokens: u64,
    pub reasoning_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsBreakdown {
    pub key: String,
    pub requests: u64,
    pub successful_requests: u64,
    pub error_requests: u64,
    pub error_rate: f64,
    pub average_response_time_ms: f64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub cached_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyStatistics {
    pub bucket_start_unix_ms: u64,
    pub requests: u64,
    pub error_requests: u64,
    pub error_rate: f64,
    pub average_response_time_ms: f64,
    pub total_tokens: u64,
}
