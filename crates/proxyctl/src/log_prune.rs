use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use chrono::{Local, LocalResult, NaiveDateTime, TimeZone};

const LOCAL_DATETIME_FORMATS: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
];

#[derive(Debug, Clone)]
pub struct PruneGroup {
    pub stem: String,
    pub started_unix_ms: u128,
    pub files: Vec<PathBuf>,
    pub total_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct PrunePlan {
    pub groups: Vec<PruneGroup>,
    pub total_files: usize,
    pub total_bytes: u64,
}

impl PrunePlan {
    pub fn exchange_count(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.total_files == 0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PruneOutcome {
    pub pruned_exchanges: usize,
    pub pruned_files: usize,
    pub pruned_bytes: u64,
}

pub fn parse_cutoff_local_datetime(local_datetime: &str) -> Result<u128> {
    let naive = LOCAL_DATETIME_FORMATS
        .iter()
        .find_map(|fmt| NaiveDateTime::parse_from_str(local_datetime, fmt).ok())
        .ok_or_else(|| {
            anyhow!(
                "invalid datetime format: expected local datetime like YYYY-MM-DDTHH:MM:SS[.fraction], got {local_datetime}"
            )
        })?;

    let local = match Local.from_local_datetime(&naive) {
        LocalResult::Single(dt) => dt,
        LocalResult::Ambiguous(earlier, later) => {
            anyhow::bail!(
                "ambiguous local datetime due to DST transition: {local_datetime} (could be {} or {})",
                earlier.to_rfc3339(),
                later.to_rfc3339()
            );
        }
        LocalResult::None => {
            anyhow::bail!(
                "invalid local datetime in current timezone (possibly skipped by DST): {local_datetime}"
            );
        }
    };

    let unix_ms = local.timestamp_millis();
    if unix_ms < 0 {
        anyhow::bail!(
            "datetimes before 1970-01-01T00:00:00 in local timezone are not supported: {local_datetime}"
        );
    }

    Ok(unix_ms as u128)
}

pub fn build_prune_plan(dir: &Path, cutoff_unix_ms: u128) -> Result<PrunePlan> {
    let entries = fs::read_dir(dir).with_context(|| format!("read dir {}", dir.display()))?;
    let mut by_stem: BTreeMap<String, PruneGroup> = BTreeMap::new();

    for entry in entries {
        let entry = entry.with_context(|| format!("read entry in {}", dir.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("read file type for {}", entry.path().display()))?;
        if !file_type.is_file() {
            continue;
        }

        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some((stem, started_unix_ms)) = parse_exchange_stem_and_started_ms(file_name) else {
            continue;
        };
        if started_unix_ms >= cutoff_unix_ms {
            continue;
        }

        let size_bytes = entry
            .metadata()
            .with_context(|| format!("read metadata for {}", path.display()))?
            .len();

        let group = by_stem.entry(stem.clone()).or_insert_with(|| PruneGroup {
            stem,
            started_unix_ms,
            files: Vec::new(),
            total_bytes: 0,
        });
        group.files.push(path);
        group.total_bytes = group.total_bytes.saturating_add(size_bytes);
    }

    let mut groups: Vec<PruneGroup> = by_stem.into_values().collect();
    for group in &mut groups {
        group.files.sort();
    }
    groups.sort_by(|a, b| {
        a.started_unix_ms
            .cmp(&b.started_unix_ms)
            .then_with(|| a.stem.cmp(&b.stem))
    });

    let total_files = groups.iter().map(|g| g.files.len()).sum();
    let total_bytes = groups.iter().map(|g| g.total_bytes).sum();

    Ok(PrunePlan {
        groups,
        total_files,
        total_bytes,
    })
}

pub fn execute_prune(plan: &PrunePlan) -> Result<PruneOutcome> {
    let mut pruned_files = 0usize;
    let mut pruned_bytes = 0u64;
    let mut pruned_exchanges = 0usize;

    for group in &plan.groups {
        for path in &group.files {
            fs::remove_file(path).with_context(|| format!("remove {}", path.display()))?;
            pruned_files = pruned_files.saturating_add(1);
        }
        pruned_bytes = pruned_bytes.saturating_add(group.total_bytes);
        pruned_exchanges = pruned_exchanges.saturating_add(1);
    }

    Ok(PruneOutcome {
        pruned_exchanges,
        pruned_files,
        pruned_bytes,
    })
}

fn parse_exchange_stem_and_started_ms(file_name: &str) -> Option<(String, u128)> {
    let stem = file_name.split('.').next()?;
    let (ts, req_id) = stem.split_once("_req_")?;
    if ts.is_empty() || req_id.is_empty() {
        return None;
    }
    if !ts.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let started_unix_ms = ts.parse::<u128>().ok()?;
    Some((stem.to_string(), started_unix_ms))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use chrono::{Local, NaiveDateTime, TimeZone};

    use super::*;

    #[test]
    fn parse_cutoff_local_datetime_parses_fractional_seconds() {
        let input = "2026-01-15T12:34:56.789";
        let unix_ms = parse_cutoff_local_datetime(input).expect("parse datetime");
        let naive =
            NaiveDateTime::parse_from_str(input, "%Y-%m-%dT%H:%M:%S%.f").expect("parse naive");
        let expected = Local
            .from_local_datetime(&naive)
            .single()
            .expect("expected non-ambiguous local datetime")
            .timestamp_millis() as u128;
        assert_eq!(unix_ms, expected);
    }

    #[test]
    fn parse_cutoff_local_datetime_rejects_date_only_input() {
        let err = parse_cutoff_local_datetime("2025-02-01")
            .expect_err("expected invalid format error")
            .to_string();
        assert!(
            err.contains("invalid datetime format"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn build_prune_plan_only_selects_older_exchange_stems() {
        let dir = create_temp_dir("prune-plan-selects-older");

        let old_stem = "1700000000000_req_1";
        let new_stem = "1760000000000_req_2";
        write_bytes(&dir.join(format!("{old_stem}.meta.json")), 7);
        write_bytes(&dir.join(format!("{old_stem}.response_body.bin")), 9);
        write_bytes(&dir.join(format!("{new_stem}.meta.json")), 11);
        write_bytes(&dir.join("random.txt"), 13);

        let cutoff = parse_cutoff_local_datetime("2024-06-01T00:00:00").expect("parse cutoff");
        let plan = build_prune_plan(&dir, cutoff).expect("build plan");

        assert_eq!(plan.exchange_count(), 1);
        assert_eq!(plan.total_files, 2);
        assert_eq!(plan.total_bytes, 16);
        assert_eq!(plan.groups[0].stem, old_stem);
        assert_eq!(plan.groups[0].files.len(), 2);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn execute_prune_removes_planned_files() {
        let dir = create_temp_dir("prune-execute-removes");
        let stem = "1700000000000_req_9";
        let old_meta = dir.join(format!("{stem}.meta.json"));
        let old_body = dir.join(format!("{stem}.response_body.bin"));
        write_bytes(&old_meta, 3);
        write_bytes(&old_body, 4);

        let cutoff = parse_cutoff_local_datetime("2024-06-01T00:00:00").expect("parse cutoff");
        let plan = build_prune_plan(&dir, cutoff).expect("build plan");
        let out = execute_prune(&plan).expect("execute prune");

        assert_eq!(out.pruned_exchanges, 1);
        assert_eq!(out.pruned_files, 2);
        assert_eq!(out.pruned_bytes, 7);
        assert!(!old_meta.exists());
        assert!(!old_body.exists());

        let _ = fs::remove_dir_all(&dir);
    }

    fn create_temp_dir(prefix: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_nanos();
        dir.push(format!("{prefix}-{}-{unique}", std::process::id()));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn write_bytes(path: &Path, len: usize) {
        fs::write(path, vec![b'x'; len]).expect("write test file");
    }
}
