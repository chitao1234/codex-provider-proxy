#![cfg(target_os = "windows")]

use std::{process::Command, time::Duration};

use regex::Regex;

#[test]
fn process_scan_can_match_child_by_unique_cmdline_token() {
    let token = format!("codex-proxyctl-scan-token-{}", std::process::id());

    let mut temp_dir = std::env::temp_dir();
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos();
    temp_dir.push(format!(
        "codex-proxyctl-process-scan-cwd-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir_all(&temp_dir).expect("create temp cwd dir");

    let child_bin = env!("CARGO_BIN_EXE_process_scan_test_target");
    let mut child = Command::new(child_bin)
        .current_dir(&temp_dir)
        // Ensure the process stays up long enough for scanning to observe it.
        .args(["--sleep-ms", "5000", &token])
        .spawn()
        .expect("spawn process_scan_test_target");

    // On Windows, process enumeration is racey; retry briefly.
    let re = Regex::new(&regex::escape(&token)).expect("compile regex");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);

    let mut found = None;
    while std::time::Instant::now() < deadline {
        let (matches, _stats) =
            codex_provider_proxyctl::process_scan::find_processes_by_cmdline_regex(&re, None)
                .expect("scan processes");
        if let Some(m) = matches.iter().find(|p| p.pid == child.id()) {
            found = Some(m.clone());
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let _ = child.kill();
    let _ = child.wait();

    let found = found.expect("expected scanner to find spawned child process");
    assert!(
        found.cmdline.contains(&token),
        "expected scanned cmdline to contain the token; cmdline={}",
        found.cmdline
    );

    let expected = std::fs::canonicalize(&temp_dir).expect("canonicalize expected temp_dir");
    let got = std::fs::canonicalize(&found.cwd)
        .unwrap_or_else(|_| std::path::PathBuf::from(found.cwd.clone()));
    assert_eq!(
        got,
        expected,
        "expected scanned cwd to match child process cwd; got={} expected={}",
        got.display(),
        expected.display()
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}
