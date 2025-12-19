use std::net::SocketAddr;

use anyhow::Result;
use async_trait::async_trait;

pub mod platform;

#[async_trait]
pub trait PidResolver: Send + Sync + 'static {
    async fn pid_for_peer(&self, local: SocketAddr, peer: SocketAddr) -> Result<Option<u32>>;
}
