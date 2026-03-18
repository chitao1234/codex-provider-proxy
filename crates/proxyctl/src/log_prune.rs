use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

const MILLIS_PER_DAY: u128 = 86_400_000;

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

pub fn parse_cutoff_date_utc(date: &str) -> Result<u128> {
    let parts: Vec<&str> = date.split('-').collect();
    if parts.len() != 3 {
        anyhow::bail!("invalid date format: expected YYYY-MM-DD, got {date}");
    }

    let year: i32 = parts[0]
        .parse()
        .with_context(|| format!("invalid year in date: {date}"))?;
    let month: u32 = parts[1]
        .parse()
        .with_context(|| format!("invalid month in date: {date}"))?;
    let day: u32 = parts[2]
        .parse()
        .with_context(|| format!("invalid day in date: {date}"))?;

    if !(1..=12).contains(&month) {
        anyhow::bail!("month out of range in date: {date}");
    }

    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        anyhow::bail!("day out of range in date: {date}");
    }

    let days_since_epoch = days_from_civil(year, month, day);
    if days_since_epoch < 0 {
        anyhow::bail!("dates before 1970-01-01 are not supported: {date}");
    }

    Ok((days_since_epoch as u128).saturating_mul(MILLIS_PER_DAY))
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

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let mut y = i64::from(year);
    let m = i64::from(month);
    let d = i64::from(day);
    y -= if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn parse_cutoff_date_parses_epoch_day() {
        let unix_ms = parse_cutoff_date_utc("1970-01-01").expect("parse date");
        assert_eq!(unix_ms, 0);
    }

    #[test]
    fn parse_cutoff_date_rejects_invalid_day() {
        let err = parse_cutoff_date_utc("2025-02-29")
            .expect_err("expected invalid day error")
            .to_string();
        assert!(err.contains("day out of range"), "unexpected error: {err}");
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

        let cutoff = parse_cutoff_date_utc("2024-06-01").expect("parse cutoff");
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

        let cutoff = parse_cutoff_date_utc("2024-06-01").expect("parse cutoff");
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
