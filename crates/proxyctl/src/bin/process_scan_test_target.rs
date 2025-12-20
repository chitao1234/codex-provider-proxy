use std::time::Duration;

fn main() {
    // Small helper binary used by integration tests to validate process scanning.
    // It just sleeps long enough for the parent test to scan and match our cmdline.
    let mut sleep_ms: u64 = 3_000;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--sleep-ms" {
            if let Some(v) = args.next() {
                sleep_ms = v.parse().unwrap_or(sleep_ms);
            }
        }
    }

    std::thread::sleep(Duration::from_millis(sleep_ms));
}
