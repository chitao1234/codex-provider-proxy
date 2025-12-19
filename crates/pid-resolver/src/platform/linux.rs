use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use tokio::task::spawn_blocking;
use tracing::debug;

use crate::PidResolver;

const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(5);
// These are best-effort caps to avoid unbounded growth. We prune expired entries
// opportunistically when we notice the maps getting large.
const MAX_CONN_CACHE_ENTRIES: usize = 16_384;
const MAX_PPID_CACHE_ENTRIES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct ConnectionKey {
    local: SocketAddr,
    peer: SocketAddr,
}

#[derive(Clone)]
pub struct LinuxPidResolver {
    conn_cache: DashMap<ConnectionKey, (u32, Instant)>,
    ppid_cache: DashMap<u32, (Option<u32>, Instant)>,
    cache_ttl: Duration,
}

impl Default for LinuxPidResolver {
    fn default() -> Self {
        Self {
            conn_cache: DashMap::new(),
            ppid_cache: DashMap::new(),
            cache_ttl: DEFAULT_CACHE_TTL,
        }
    }
}

#[async_trait]
impl PidResolver for LinuxPidResolver {
    async fn pid_for_peer(&self, local: SocketAddr, peer: SocketAddr) -> Result<Option<u32>> {
        let this = self.clone();
        spawn_blocking(move || this.pid_for_peer_blocking(local, peer)).await?
    }

    async fn parent_pid(&self, pid: u32) -> Result<Option<u32>> {
        let this = self.clone();
        spawn_blocking(move || this.parent_pid_blocking(pid)).await?
    }
}

impl LinuxPidResolver {
    fn pid_for_peer_blocking(&self, local: SocketAddr, peer: SocketAddr) -> Result<Option<u32>> {
        // Cache keyed by (server_local, client_peer) since caller passes (listen_addr, peer_addr).
        // This is safer than caching by inode: socket inodes can be reused after close, while
        // 4-tuples typically don't get reused quickly (TIME_WAIT).
        let key = ConnectionKey { local, peer };
        if let Some(entry) = self.conn_cache.get(&key) {
            let (pid, at) = *entry.value();
            if at.elapsed() <= self.cache_ttl {
                return Ok(Some(pid));
            }
        }
        // If it was expired, drop it so the map doesn't grow unboundedly with cold entries.
        self.conn_cache.remove(&key);

        // Important: when we accept a TCP connection, `/proc/net/tcp*` will contain two entries:
        // - server socket: local=server_addr, remote=client_addr (inode owned by the server process)
        // - client socket: local=client_addr, remote=server_addr (inode owned by the client process)
        //
        // We want the PID of the *other end* (the peer/client), so we must look up the inode for the
        // client socket, which corresponds to (local=peer, remote=local).
        let inode = match inode_for_connection(peer, local)? {
            Some(inode) => inode,
            None => {
                debug!(
                    local = %local,
                    peer = %peer,
                    "pid resolver: no inode found for connection in /proc/net/tcp*"
                );
                return Ok(None);
            }
        };

        let pid = match pid_for_inode(inode)? {
            Some(pid) => pid,
            None => {
                debug!(
                    local = %local,
                    peer = %peer,
                    inode,
                    "pid resolver: inode found but no owning pid discovered under /proc/*/fd"
                );
                return Ok(None);
            }
        };

        self.conn_cache.insert(key, (pid, Instant::now()));
        self.maybe_prune_conn_cache();
        Ok(Some(pid))
    }

    fn parent_pid_blocking(&self, pid: u32) -> Result<Option<u32>> {
        if let Some(entry) = self.ppid_cache.get(&pid) {
            let (ppid, at) = entry.value();
            if at.elapsed() <= self.cache_ttl {
                return Ok(*ppid);
            }
        }
        // Expired; remove eagerly.
        self.ppid_cache.remove(&pid);

        let stat_path = format!("/proc/{pid}/stat");
        let stat = match fs::read_to_string(&stat_path) {
            Ok(s) => s,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                self.ppid_cache.insert(pid, (None, Instant::now()));
                self.maybe_prune_ppid_cache();
                return Ok(None);
            }
            Err(err) => return Err(err).with_context(|| format!("read {stat_path}")),
        };

        let ppid = parse_ppid_from_proc_stat(&stat).with_context(|| {
            format!("parse parent pid (ppid) from /proc stat for pid {pid}: {stat:?}")
        })?;

        let ppid = if ppid == 0 { None } else { Some(ppid) };
        self.ppid_cache.insert(pid, (ppid, Instant::now()));
        self.maybe_prune_ppid_cache();
        Ok(ppid)
    }

    fn maybe_prune_conn_cache(&self) {
        if self.conn_cache.len() <= MAX_CONN_CACHE_ENTRIES {
            return;
        }
        let mut expired: Vec<ConnectionKey> = Vec::new();
        for entry in self.conn_cache.iter() {
            if entry.value().1.elapsed() > self.cache_ttl {
                expired.push(*entry.key());
            }
        }
        for key in expired {
            self.conn_cache.remove(&key);
        }
    }

    fn maybe_prune_ppid_cache(&self) {
        if self.ppid_cache.len() <= MAX_PPID_CACHE_ENTRIES {
            return;
        }
        let mut expired: Vec<u32> = Vec::new();
        for entry in self.ppid_cache.iter() {
            if entry.value().1.elapsed() > self.cache_ttl {
                expired.push(*entry.key());
            }
        }
        for pid in expired {
            self.ppid_cache.remove(&pid);
        }
    }
}

fn parse_ppid_from_proc_stat(stat: &str) -> Option<u32> {
    // `/proc/<pid>/stat` format (simplified):
    // pid (comm) state ppid ...
    //
    // `comm` can contain spaces, so we locate the final `)` and split after it.
    let (_, after_comm) = stat.rsplit_once(')')?;
    let mut fields = after_comm.trim_start().split_whitespace();
    let _state = fields.next()?;
    let ppid = fields.next()?;
    ppid.parse::<u32>().ok()
}

fn inode_for_connection(local: SocketAddr, peer: SocketAddr) -> Result<Option<u64>> {
    match (local.ip(), peer.ip()) {
        (IpAddr::V4(_), IpAddr::V4(_)) => inode_for_connection_v4(local, peer),
        (IpAddr::V6(_), IpAddr::V6(_)) => inode_for_connection_v6(local, peer),
        _ => {
            debug!(
                local = %local,
                peer = %peer,
                "pid resolver: ip family mismatch (v4/v6); cannot map to /proc/net/tcp or tcp6"
            );
            Ok(None)
        }
    }
}

fn inode_for_connection_v4(local: SocketAddr, peer: SocketAddr) -> Result<Option<u64>> {
    let local_key = encode_proc_net_tcp_v4(local.ip(), local.port());
    let peer_key = encode_proc_net_tcp_v4(peer.ip(), peer.port());

    let tcp = fs::read_to_string("/proc/net/tcp").context("read /proc/net/tcp")?;
    let mut scanned = 0usize;
    for line in tcp.lines().skip(1) {
        scanned += 1;
        let mut parts = line.split_whitespace();
        let _sl = parts.next();
        let local_addr = parts.next();
        let rem_addr = parts.next();
        let _st = parts.next();
        let _tx_rx = parts.next();
        let _tr_tm = parts.next();
        let _retrnsmt = parts.next();
        let _uid = parts.next();
        let _timeout = parts.next();
        let inode = parts.next();

        let (Some(local_addr), Some(rem_addr), Some(inode)) = (local_addr, rem_addr, inode) else {
            continue;
        };

        if local_addr == local_key && rem_addr == peer_key {
            let inode = inode
                .parse::<u64>()
                .with_context(|| format!("parse inode from /proc/net/tcp line: {line}"))?;
            debug!(
                local = %local,
                peer = %peer,
                inode,
                scanned_lines = scanned,
                "pid resolver: matched connection in /proc/net/tcp"
            );
            return Ok(Some(inode));
        }
    }

    debug!(
        local = %local,
        peer = %peer,
        scanned_lines = scanned,
        "pid resolver: no matching connection found in /proc/net/tcp"
    );
    Ok(None)
}

fn inode_for_connection_v6(local: SocketAddr, peer: SocketAddr) -> Result<Option<u64>> {
    let local_key = encode_proc_net_tcp_v6(local.ip(), local.port());
    let peer_key = encode_proc_net_tcp_v6(peer.ip(), peer.port());

    let tcp6 = fs::read_to_string("/proc/net/tcp6").context("read /proc/net/tcp6")?;
    let mut scanned = 0usize;
    for line in tcp6.lines().skip(1) {
        scanned += 1;
        let mut parts = line.split_whitespace();
        let _sl = parts.next();
        let local_addr = parts.next();
        let rem_addr = parts.next();
        let _st = parts.next();
        let _tx_rx = parts.next();
        let _tr_tm = parts.next();
        let _retrnsmt = parts.next();
        let _uid = parts.next();
        let _timeout = parts.next();
        let inode = parts.next();

        let (Some(local_addr), Some(rem_addr), Some(inode)) = (local_addr, rem_addr, inode) else {
            continue;
        };

        if local_addr == local_key && rem_addr == peer_key {
            let inode = inode
                .parse::<u64>()
                .with_context(|| format!("parse inode from /proc/net/tcp6 line: {line}"))?;
            debug!(
                local = %local,
                peer = %peer,
                inode,
                scanned_lines = scanned,
                "pid resolver: matched connection in /proc/net/tcp6"
            );
            return Ok(Some(inode));
        }
    }

    debug!(
        local = %local,
        peer = %peer,
        scanned_lines = scanned,
        "pid resolver: no matching connection found in /proc/net/tcp6"
    );
    Ok(None)
}

fn encode_proc_net_tcp_v4(ip: IpAddr, port: u16) -> String {
    let ip = match ip {
        IpAddr::V4(v4) => v4,
        IpAddr::V6(_) => Ipv4Addr::UNSPECIFIED,
    };
    let octets = ip.octets();
    format!(
        "{:02X}{:02X}{:02X}{:02X}:{:04X}",
        octets[3], octets[2], octets[1], octets[0], port
    )
}

fn encode_proc_net_tcp_v6(ip: IpAddr, port: u16) -> String {
    let bytes = match ip {
        IpAddr::V6(v6) => v6.octets(),
        IpAddr::V4(v4) => {
            // IPv4 isn't expected here, but keep this deterministic.
            let mut b = [0u8; 16];
            b[12..16].copy_from_slice(&v4.octets());
            b
        }
    };

    // /proc/net/tcp6 prints the IPv6 address as 4 32-bit words, each in little-endian byte order.
    // Example: ::1 => ...01000000
    let mut hex = String::with_capacity(32 + 1 + 4);
    for word in bytes.chunks_exact(4) {
        hex.push_str(&format!(
            "{:02X}{:02X}{:02X}{:02X}",
            word[3], word[2], word[1], word[0]
        ));
    }
    hex.push(':');
    hex.push_str(&format!("{:04X}", port));
    hex
}

fn pid_for_inode(inode: u64) -> Result<Option<u32>> {
    let proc = fs::read_dir("/proc").context("read /proc")?;
    let mut scanned_pids = 0usize;
    let mut scanned_fds = 0usize;
    for entry in proc {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let file_name = entry.file_name();
        let Some(pid) = file_name.to_string_lossy().parse::<u32>().ok() else {
            continue;
        };
        scanned_pids += 1;

        let mut fd_dir = PathBuf::from("/proc");
        fd_dir.push(pid.to_string());
        fd_dir.push("fd");
        let fds = match fs::read_dir(&fd_dir) {
            Ok(fds) => fds,
            Err(_) => continue,
        };

        for fd in fds {
            let fd = match fd {
                Ok(fd) => fd,
                Err(_) => continue,
            };
            scanned_fds += 1;
            let target = match fs::read_link(fd.path()) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let Some(target_str) = target.to_str() else {
                continue;
            };
            if let Some(found_inode) = parse_socket_inode(target_str) {
                if found_inode == inode {
                    debug!(pid, inode, "resolved pid for socket inode");
                    return Ok(Some(pid));
                }
            }
        }
    }
    debug!(
        inode,
        scanned_pids,
        scanned_fds,
        "pid resolver: no pid found owning socket inode"
    );
    Ok(None)
}

fn parse_socket_inode(link_target: &str) -> Option<u64> {
    // symlink target is usually: "socket:[123456]"
    let link_target = link_target.strip_prefix("socket:[")?;
    let link_target = link_target.strip_suffix(']')?;
    link_target.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv6Addr;

    #[test]
    fn encodes_ipv4_as_proc_net_tcp_key() {
        let key = encode_proc_net_tcp_v4(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        assert_eq!(key, "0100007F:1F90");
    }

    #[test]
    fn encodes_ipv6_as_proc_net_tcp6_key() {
        let key = encode_proc_net_tcp_v6(IpAddr::V6(Ipv6Addr::LOCALHOST), 8080);
        assert_eq!(key, "00000000000000000000000001000000:1F90");
    }

    #[test]
    fn parses_socket_inode() {
        assert_eq!(parse_socket_inode("socket:[12345]"), Some(12345));
        assert_eq!(parse_socket_inode("anon_inode:[eventfd]"), None);
    }

    #[test]
    fn parses_ppid_from_proc_stat_with_spaces_in_comm() {
        let stat = "12345 (some process name) S 4242 1 2 3 4 5";
        assert_eq!(parse_ppid_from_proc_stat(stat), Some(4242));
    }

    #[test]
    fn parses_ppid_from_proc_stat_with_parens_in_comm() {
        let stat = "12345 (weird)name) R 99 1 2 3 4";
        assert_eq!(parse_ppid_from_proc_stat(stat), Some(99));
    }
}
