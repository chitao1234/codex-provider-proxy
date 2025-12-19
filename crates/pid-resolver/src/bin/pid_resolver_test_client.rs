use std::{env, net::TcpStream, thread, time::Duration};

fn main() {
    let mut addr: Option<String> = None;
    let mut sleep_ms: u64 = 10_000;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                addr = args.next();
            }
            "--sleep-ms" => {
                if let Some(v) = args.next() {
                    sleep_ms = v.parse().unwrap_or(sleep_ms);
                }
            }
            _ => {}
        }
    }

    let Some(addr) = addr else {
        eprintln!("usage: pid_resolver_test_client --addr 127.0.0.1:PORT [--sleep-ms N]");
        std::process::exit(2);
    };

    let _stream = TcpStream::connect(addr).unwrap();
    thread::sleep(Duration::from_millis(sleep_ms));
}
