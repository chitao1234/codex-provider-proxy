use std::sync::Arc;

use crate::PidResolver;

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub use linux::LinuxPidResolver;

#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "windows")]
pub use windows::WindowsPidResolver;

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
use std::net::SocketAddr;

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
use anyhow::{anyhow, Result};

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub struct UnsupportedPidResolver;

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
#[async_trait::async_trait]
impl PidResolver for UnsupportedPidResolver {
    async fn pid_for_peer(&self, _local: SocketAddr, _peer: SocketAddr) -> Result<Option<u32>> {
        Err(anyhow!(
            "pid resolution is only supported on Linux and Windows for now"
        ))
    }
}

pub fn default_pid_resolver() -> Arc<dyn PidResolver> {
    #[cfg(target_os = "linux")]
    {
        Arc::new(LinuxPidResolver::default())
    }

    #[cfg(target_os = "windows")]
    {
        Arc::new(WindowsPidResolver::default())
    }

    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    {
        Arc::new(UnsupportedPidResolver)
    }
}
