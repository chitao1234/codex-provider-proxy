#[cfg(target_os = "linux")]
mod linux_only {
    use std::{net::SocketAddr, process::Command, time::Duration};

    use pid_resolver::platform::LinuxPidResolver;
    use pid_resolver::PidResolver;
    use tokio::net::TcpListener;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn resolves_peer_pid_for_loopback_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local: SocketAddr = listener.local_addr().unwrap();

        let client_bin = env!("CARGO_BIN_EXE_pid_resolver_test_client");
        let mut child = Command::new(client_bin)
            .args(["--addr", &local.to_string()])
            .spawn()
            .unwrap();

        let accept = timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("accept timed out")
            .expect("accept failed");

        let peer = accept.0.peer_addr().unwrap();
        let expected_pid = child.id() as u32;

        let resolver = LinuxPidResolver::default();
        let mut resolved = None;
        for _ in 0..40 {
            resolved = resolver.pid_for_peer(local, peer).await.unwrap();
            if resolved == Some(expected_pid) {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }

        let _ = child.kill();
        let _ = child.wait();

        assert_eq!(
            resolved,
            Some(expected_pid),
            "expected peer PID to resolve to the spawned client process"
        );
    }
}

#[cfg(target_os = "windows")]
mod windows_only {
    use std::{net::SocketAddr, process::Command, time::Duration};

    use pid_resolver::platform::WindowsPidResolver;
    use pid_resolver::PidResolver;
    use tokio::net::TcpListener;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn resolves_peer_pid_for_loopback_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local: SocketAddr = listener.local_addr().unwrap();

        let client_bin = env!("CARGO_BIN_EXE_pid_resolver_test_client");
        let mut child = Command::new(client_bin)
            .args(["--addr", &local.to_string()])
            .spawn()
            .unwrap();

        let accept = timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("accept timed out")
            .expect("accept failed");

        let peer = accept.0.peer_addr().unwrap();
        let expected_pid = child.id();

        let resolver = WindowsPidResolver::default();
        let mut resolved = None;
        for _ in 0..80 {
            resolved = resolver.pid_for_peer(local, peer).await.unwrap();
            if resolved == Some(expected_pid) {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }

        let _ = child.kill();
        let _ = child.wait();

        assert_eq!(
            resolved,
            Some(expected_pid),
            "expected peer PID to resolve to the spawned client process"
        );
    }
}
