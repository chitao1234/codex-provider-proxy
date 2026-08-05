use std::{
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;

use crate::PidResolver;

#[derive(Clone)]
struct TtlCache<K, V> {
    entries: Arc<DashMap<K, CacheEntry<V>>>,
}

#[derive(Clone, Copy)]
struct CacheEntry<V> {
    value: V,
    inserted_at: Instant,
}

impl<K, V> Default for TtlCache<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
        }
    }
}

impl<K, V> TtlCache<K, V>
where
    K: Clone + Eq + Hash,
    V: Copy,
{
    fn get(&self, key: &K, ttl: Duration) -> Option<V> {
        let entry = self.entries.get(key)?;
        if entry.inserted_at.elapsed() <= ttl {
            return Some(entry.value);
        }
        drop(entry);
        self.entries.remove(key);
        None
    }

    fn insert(&self, key: K, value: V) {
        self.entries.insert(
            key,
            CacheEntry {
                value,
                inserted_at: Instant::now(),
            },
        );
    }

    fn prune_expired_if_over(&self, max_entries: usize, ttl: Duration) {
        if self.entries.len() <= max_entries {
            return;
        }
        self.entries
            .retain(|_, entry| entry.inserted_at.elapsed() <= ttl);
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_cache_clones_share_fresh_entries() {
        let cache = TtlCache::default();
        let cloned = cache.clone();

        cache.insert(1, 2);

        assert_eq!(cloned.get(&1, Duration::from_secs(1)), Some(2));
    }

    #[test]
    fn ttl_cache_removes_expired_entries() {
        let cache = TtlCache::default();
        cache.entries.insert(
            1,
            CacheEntry {
                value: 2,
                inserted_at: Instant::now() - Duration::from_secs(2),
            },
        );

        assert_eq!(cache.get(&1, Duration::from_secs(1)), None);
        assert!(cache.entries.is_empty());
    }
}
