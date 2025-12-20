use regex::Regex;

#[test]
#[cfg(any(target_os = "linux", target_os = "windows"))]
fn process_scan_finds_own_pid_by_exe_name() {
    let my_pid = std::process::id();
    let exe = std::env::current_exe().expect("current_exe");
    let exe_name = exe
        .file_name()
        .expect("exe file_name")
        .to_string_lossy()
        .to_string();

    // Match on just the executable name (portable across Linux/Windows implementations).
    // Case-insensitive is helpful on Windows paths.
    let pattern = format!("(?i){}", regex::escape(&exe_name));
    let re = Regex::new(&pattern).expect("compile regex");

    let (matches, _stats) =
        codex_provider_proxyctl::process_scan::find_processes_by_cmdline_regex(&re, None)
            .expect("scan processes");

    assert!(
        matches.iter().any(|p| p.pid == my_pid),
        "expected scanner to find self pid={my_pid} by exe_name={exe_name}; got pids={:?}",
        matches.iter().map(|p| p.pid).collect::<Vec<_>>()
    );
}
