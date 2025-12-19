use std::sync::Arc;

use crate::PidResolver;

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub use linux::LinuxPidResolver;

#[cfg(not(target_os = "linux"))]
use std::net::SocketAddr;

#[cfg(not(target_os = "linux"))]
use anyhow::{anyhow, Result};

#[cfg(not(target_os = "linux"))]
pub struct UnsupportedPidResolver;

#[cfg(not(target_os = "linux"))]
#[async_trait::async_trait]
impl PidResolver for UnsupportedPidResolver {
    async fn pid_for_peer(&self, _local: SocketAddr, _peer: SocketAddr) -> Result<Option<u32>> {
        Err(anyhow!("pid resolution is only supported on Linux for now"))
    }
}

pub fn default_pid_resolver() -> Arc<dyn PidResolver> {
    #[cfg(target_os = "linux")]
    {
        Arc::new(LinuxPidResolver::default())
    }

    #[cfg(not(target_os = "linux"))]
    {
        Arc::new(UnsupportedPidResolver)
    }
}
