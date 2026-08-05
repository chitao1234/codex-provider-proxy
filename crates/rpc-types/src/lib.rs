use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetRouteRequest {
    pub pid: u32,
    pub provider: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetDefaultProviderRequest {
    pub provider: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteRouteResponse {
    pub removed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListRoutesResponse {
    pub routes: Vec<RouteEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteEntry {
    pub pid: u32,
    pub provider: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvidersResponse {
    pub default_provider: String,
    pub providers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatisticsWindow {
    pub hours: Option<u32>,
    pub from_unix_ms: Option<u64>,
    pub to_unix_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LatencySummary {
    pub count: u64,
    pub average: f64,
    pub min: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsageSummary {
    pub responses_with_usage: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub cached_tokens: u64,
    pub cache_creation_tokens: u64,
    pub reasoning_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HourlyStatistics {
    pub bucket_start_unix_ms: u64,
    pub requests: u64,
    pub error_requests: u64,
    pub error_rate: f64,
    pub average_response_time_ms: f64,
    pub total_tokens: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_route_request_has_stable_json_shape() {
        let request = SetRouteRequest {
            pid: 42,
            provider: "provider_b".to_string(),
        };

        let json = serde_json::to_value(&request).expect("serialize request");

        assert_eq!(
            json,
            serde_json::json!({"pid": 42, "provider": "provider_b"})
        );
        assert_eq!(
            serde_json::from_value::<SetRouteRequest>(json).expect("deserialize request"),
            request
        );
    }

    #[test]
    fn statistics_response_round_trips() {
        let response = StatisticsResponse {
            enabled: true,
            generated_unix_ms: 1_700_000_000_000,
            window: StatisticsWindow {
                hours: Some(24),
                from_unix_ms: Some(1_699_913_600_000),
                to_unix_ms: 1_700_000_000_000,
            },
            summary: StatisticsSummary {
                requests: 2,
                successful_requests: 1,
                error_requests: 1,
                error_rate: 0.5,
                ..StatisticsSummary::default()
            },
            tokens: TokenUsageSummary {
                total_tokens: 123,
                ..TokenUsageSummary::default()
            },
            providers: vec![StatisticsBreakdown {
                key: "provider_a".to_string(),
                requests: 2,
                successful_requests: 1,
                error_requests: 1,
                error_rate: 0.5,
                average_response_time_ms: 12.5,
                request_bytes: 10,
                response_bytes: 20,
                input_tokens: 100,
                output_tokens: 23,
                total_tokens: 123,
                cached_tokens: 40,
            }],
            models: Vec::new(),
            client_ips: Vec::new(),
            pids: Vec::new(),
            route_pids: Vec::new(),
            status_codes: Vec::new(),
            methods: Vec::new(),
            paths: Vec::new(),
            hourly: vec![HourlyStatistics {
                bucket_start_unix_ms: 1_699_996_400_000,
                requests: 2,
                error_requests: 1,
                error_rate: 0.5,
                average_response_time_ms: 12.5,
                total_tokens: 123,
            }],
        };

        let json = serde_json::to_string(&response).expect("serialize statistics");
        let decoded: StatisticsResponse =
            serde_json::from_str(&json).expect("deserialize statistics");

        assert_eq!(decoded, response);
    }
}
