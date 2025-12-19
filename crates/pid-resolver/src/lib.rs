use std::net::SocketAddr;

use anyhow::Result;
use async_trait::async_trait;

pub mod platform;

#[async_trait]
pub trait PidResolver: Send + Sync + 'static {
    async fn pid_for_peer(&self, local: SocketAddr, peer: SocketAddr) -> Result<Option<u32>>;

    /// Returns the parent PID (PPID) for `pid` if it can be determined.
    ///
    /// On Linux this is typically read from `/proc/<pid>/stat`. On unsupported
    /// platforms (or when the process no longer exists), this may return `Ok(None)`.
    async fn parent_pid(&self, _pid: u32) -> Result<Option<u32>> {
        Ok(None)
    }
}
