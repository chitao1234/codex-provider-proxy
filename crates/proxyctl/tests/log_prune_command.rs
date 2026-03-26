use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const CUTOFF_LOCAL_DATETIME: &str = "2025-01-01T00:00:00";

#[test]
fn prune_logs_requires_yes_for_destructive_action() {
    let dir = create_temp_dir("proxyctl-prune-requires-yes");
    let (old_meta, old_body, _new_meta) = seed_log_files(&dir);

    let out = run_prune_command_with_input(
        &dir,
        &["--before-local-datetime", CUTOFF_LOCAL_DATETIME],
        "\n",
    );

    assert!(
        out.status.success(),
        "expected zero exit when confirmation is declined; stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("WARNING: prune-logs permanently deletes files"),
        "expected destructive warning in stderr; stderr={stderr}"
    );
    assert!(old_meta.exists(), "old meta should not be deleted");
    assert!(old_body.exists(), "old body should not be deleted");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn prune_logs_dry_run_does_not_delete_files() {
    let dir = create_temp_dir("proxyctl-prune-dry-run");
    let (old_meta, old_body, _new_meta) = seed_log_files(&dir);

    let out = run_prune_command(
        &dir,
        &[
            "--before-local-datetime",
            CUTOFF_LOCAL_DATETIME,
            "--dry-run",
        ],
    );

    assert!(
        out.status.success(),
        "expected zero exit for --dry-run; stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("dry-run: no files deleted"),
        "expected dry-run summary in stdout; stdout={stdout}"
    );
    assert!(old_meta.exists(), "old meta should remain on dry-run");
    assert!(old_body.exists(), "old body should remain on dry-run");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn prune_logs_with_yes_deletes_only_older_files() {
    let dir = create_temp_dir("proxyctl-prune-with-yes");
    let (old_meta, old_body, new_meta) = seed_log_files(&dir);

    let out = run_prune_command(
        &dir,
        &["--before-local-datetime", CUTOFF_LOCAL_DATETIME, "-y"],
    );

    assert!(
        out.status.success(),
        "expected zero exit with -y; stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!old_meta.exists(), "old meta should be deleted");
    assert!(!old_body.exists(), "old body should be deleted");
    assert!(new_meta.exists(), "newer file should be kept");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn prune_logs_without_yes_can_delete_after_interactive_confirmation() {
    let dir = create_temp_dir("proxyctl-prune-interactive-confirm");
    let (old_meta, old_body, new_meta) = seed_log_files(&dir);

    let out = run_prune_command_with_input(
        &dir,
        &["--before-local-datetime", CUTOFF_LOCAL_DATETIME],
        "yes\n",
    );

    assert!(
        out.status.success(),
        "expected zero exit after interactive confirmation; stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!old_meta.exists(), "old meta should be deleted");
    assert!(!old_body.exists(), "old body should be deleted");
    assert!(new_meta.exists(), "newer file should be kept");

    let _ = fs::remove_dir_all(&dir);
}

fn run_prune_command(log_dir: &Path, extra_args: &[&str]) -> std::process::Output {
    let bin = env!("CARGO_BIN_EXE_codex-provider-proxyctl");
    let dir_arg = log_dir.to_string_lossy().to_string();

    let mut cmd = Command::new(bin);
    cmd.current_dir(log_dir)
        .arg("prune-logs")
        .arg("--dir")
        .arg(dir_arg)
        .args(extra_args);
    cmd.output().expect("run codex-provider-proxyctl")
}

fn run_prune_command_with_input(
    log_dir: &Path,
    extra_args: &[&str],
    input: &str,
) -> std::process::Output {
    let bin = env!("CARGO_BIN_EXE_codex-provider-proxyctl");
    let dir_arg = log_dir.to_string_lossy().to_string();

    let mut cmd = Command::new(bin);
    cmd.current_dir(log_dir)
        .arg("prune-logs")
        .arg("--dir")
        .arg(dir_arg)
        .args(extra_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn codex-provider-proxyctl");
    {
        let stdin = child.stdin.as_mut().expect("child stdin");
        stdin.write_all(input.as_bytes()).expect("write stdin");
    }
    child.wait_with_output().expect("wait for command output")
}

fn seed_log_files(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let old_stem = "1700000000000_req_1";
    let new_stem = "1740000000000_req_2";

    let old_meta = dir.join(format!("{old_stem}.meta.json"));
    let old_body = dir.join(format!("{old_stem}.response_body.bin"));
    let new_meta = dir.join(format!("{new_stem}.meta.json"));

    write_bytes(&old_meta, 5);
    write_bytes(&old_body, 7);
    write_bytes(&new_meta, 11);
    write_bytes(&dir.join("notes.txt"), 13);

    (old_meta, old_body, new_meta)
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
    fs::write(path, vec![b'x'; len]).expect("write file");
}
