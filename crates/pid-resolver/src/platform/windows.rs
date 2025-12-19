use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use tokio::task::spawn_blocking;

use crate::PidResolver;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct ConnectionKey {
    local: SocketAddr,
    peer: SocketAddr,
}

#[derive(Clone)]
pub struct WindowsPidResolver {
    conn_cache: DashMap<ConnectionKey, (u32, Instant)>,
    ppid_cache: DashMap<u32, (Option<u32>, Instant)>,
    cache_ttl: Duration,
}

impl Default for WindowsPidResolver {
    fn default() -> Self {
        Self {
            conn_cache: DashMap::new(),
            ppid_cache: DashMap::new(),
            cache_ttl: Duration::from_secs(5),
        }
    }
}

#[async_trait]
impl PidResolver for WindowsPidResolver {
    async fn pid_for_peer(&self, local: SocketAddr, peer: SocketAddr) -> Result<Option<u32>> {
        let this = self.clone();
        spawn_blocking(move || this.pid_for_peer_blocking(local, peer)).await?
    }

    async fn parent_pid(&self, pid: u32) -> Result<Option<u32>> {
        let this = self.clone();
        spawn_blocking(move || this.parent_pid_blocking(pid)).await?
    }
}

impl WindowsPidResolver {
    fn pid_for_peer_blocking(&self, local: SocketAddr, peer: SocketAddr) -> Result<Option<u32>> {
        // Cache keyed by (server_local, client_peer) since caller passes (listen_addr, peer_addr).
        let key = ConnectionKey { local, peer };
        if let Some(entry) = self.conn_cache.get(&key) {
            let (pid, at) = *entry.value();
            if at.elapsed() <= self.cache_ttl {
                return Ok(Some(pid));
            }
        }

        // Match the client-owned socket entry: local=peer, remote=local.
        let pid = match (local.ip(), peer.ip()) {
            (IpAddr::V4(_), IpAddr::V4(_)) => pid_for_connection_v4(peer, local)
                .context("resolve peer pid via GetExtendedTcpTable (v4)")?,
            (IpAddr::V6(_), IpAddr::V6(_)) => pid_for_connection_v6(peer, local)
                .context("resolve peer pid via GetExtendedTcpTable (v6)")?,
            _ => None,
        };

        if let Some(pid) = pid {
            self.conn_cache.insert(key, (pid, Instant::now()));
        }
        Ok(pid)
    }

    fn parent_pid_blocking(&self, pid: u32) -> Result<Option<u32>> {
        if let Some(entry) = self.ppid_cache.get(&pid) {
            let (ppid, at) = entry.value();
            if at.elapsed() <= self.cache_ttl {
                return Ok(*ppid);
            }
        }

        let ppid = process_parent_pid(pid).context("resolve parent pid via Toolhelp snapshot")?;
        let ppid = if ppid == 0 { None } else { Some(ppid) };
        self.ppid_cache.insert(pid, (ppid, Instant::now()));
        Ok(ppid)
    }
}

fn port_from_network_dword(dw_port: u32) -> u16 {
    u16::from_be(dw_port as u16)
}

fn ipv4_from_network_dword(dw_addr: u32) -> Ipv4Addr {
    // The docs specify the address is in network byte order.
    Ipv4Addr::from(u32::from_be(dw_addr))
}

fn pid_for_connection_v4(local: SocketAddr, peer: SocketAddr) -> Result<Option<u32>> {
    use std::ptr;

    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetExtendedTcpTable, MIB_TCPTABLE_OWNER_PID, TCP_TABLE_OWNER_PID_ALL,
    };
    use windows_sys::Win32::Networking::WinSock::AF_INET;

    let IpAddr::V4(local_ip) = local.ip() else {
        return Ok(None);
    };
    let IpAddr::V4(peer_ip) = peer.ip() else {
        return Ok(None);
    };

    let local_ip = local_ip;
    let local_port = local.port();
    let peer_ip = peer_ip;
    let peer_port = peer.port();

    let mut size: u32 = 0;
    // First call to get required size.
    let rc = unsafe {
        GetExtendedTcpTable(
            ptr::null_mut(),
            &mut size,
            0,
            AF_INET as u32,
            TCP_TABLE_OWNER_PID_ALL,
            0,
        )
    };

    // ERROR_INSUFFICIENT_BUFFER (122) is expected here.
    if rc != 0 && rc != 122 {
        return Err(anyhow!(
            "GetExtendedTcpTable(size probe) failed with error {rc}"
        ));
    }
    if size == 0 {
        return Ok(None);
    }

    let mut buf = vec![0u8; size as usize];
    let rc = unsafe {
        GetExtendedTcpTable(
            buf.as_mut_ptr().cast(),
            &mut size,
            0,
            AF_INET as u32,
            TCP_TABLE_OWNER_PID_ALL,
            0,
        )
    };
    if rc != 0 {
        return Err(anyhow!("GetExtendedTcpTable failed with error {rc}"));
    }

    let table = buf.as_ptr().cast::<MIB_TCPTABLE_OWNER_PID>();
    let num_entries = unsafe { (*table).dwNumEntries as usize };
    if num_entries == 0 {
        return Ok(None);
    }

    // Safety: buffer is owned and large enough for dwNumEntries rows.
    let first_row = unsafe { (*table).table.as_ptr() };
    let rows = unsafe { std::slice::from_raw_parts(first_row, num_entries) };

    for row in rows {
        let row_local_ip = ipv4_from_network_dword(row.dwLocalAddr);
        let row_local_port = port_from_network_dword(row.dwLocalPort);
        let row_remote_ip = ipv4_from_network_dword(row.dwRemoteAddr);
        let row_remote_port = port_from_network_dword(row.dwRemotePort);

        if row_local_ip == local_ip
            && row_local_port == local_port
            && row_remote_ip == peer_ip
            && row_remote_port == peer_port
        {
            return Ok(Some(row.dwOwningPid));
        }
    }

    Ok(None)
}

fn pid_for_connection_v6(local: SocketAddr, peer: SocketAddr) -> Result<Option<u32>> {
    use std::ptr;

    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetExtendedTcpTable, MIB_TCP6TABLE_OWNER_PID, TCP_TABLE_OWNER_PID_ALL,
    };
    use windows_sys::Win32::Networking::WinSock::AF_INET6;

    let IpAddr::V6(local_ip) = local.ip() else {
        return Ok(None);
    };
    let IpAddr::V6(peer_ip) = peer.ip() else {
        return Ok(None);
    };

    let local_ip = local_ip;
    let local_port = local.port();
    let peer_ip = peer_ip;
    let peer_port = peer.port();

    let mut size: u32 = 0;
    let rc = unsafe {
        GetExtendedTcpTable(
            ptr::null_mut(),
            &mut size,
            0,
            AF_INET6 as u32,
            TCP_TABLE_OWNER_PID_ALL,
            0,
        )
    };
    if rc != 0 && rc != 122 {
        return Err(anyhow!(
            "GetExtendedTcpTable(size probe, v6) failed with error {rc}"
        ));
    }
    if size == 0 {
        return Ok(None);
    }

    let mut buf = vec![0u8; size as usize];
    let rc = unsafe {
        GetExtendedTcpTable(
            buf.as_mut_ptr().cast(),
            &mut size,
            0,
            AF_INET6 as u32,
            TCP_TABLE_OWNER_PID_ALL,
            0,
        )
    };
    if rc != 0 {
        return Err(anyhow!("GetExtendedTcpTable(v6) failed with error {rc}"));
    }

    let table = buf.as_ptr().cast::<MIB_TCP6TABLE_OWNER_PID>();
    let num_entries = unsafe { (*table).dwNumEntries as usize };
    if num_entries == 0 {
        return Ok(None);
    }

    let first_row = unsafe { (*table).table.as_ptr() };
    let rows = unsafe { std::slice::from_raw_parts(first_row, num_entries) };

    for row in rows {
        let row_local_ip = Ipv6Addr::from(row.ucLocalAddr);
        let row_local_port = port_from_network_dword(row.dwLocalPort);
        let row_remote_ip = Ipv6Addr::from(row.ucRemoteAddr);
        let row_remote_port = port_from_network_dword(row.dwRemotePort);

        if row_local_ip == local_ip
            && row_local_port == local_port
            && row_remote_ip == peer_ip
            && row_remote_port == peer_port
        {
            return Ok(Some(row.dwOwningPid));
        }
    }

    Ok(None)
}

fn process_parent_pid(pid: u32) -> Result<u32> {
    use std::mem::{size_of, zeroed};

    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, PROCESSENTRY32W,
        TH32CS_SNAPPROCESS,
    };

    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(anyhow!("CreateToolhelp32Snapshot failed"));
    }

    let mut entry: PROCESSENTRY32W = unsafe { zeroed() };
    entry.dwSize = size_of::<PROCESSENTRY32W>() as u32;

    let mut found: Option<u32> = None;
    let mut ok = unsafe { Process32FirstW(snapshot, &mut entry) };
    while ok != 0 {
        if entry.th32ProcessID == pid {
            found = Some(entry.th32ParentProcessID);
            break;
        }
        ok = unsafe { Process32NextW(snapshot, &mut entry) };
    }

    unsafe { CloseHandle(snapshot) };

    Ok(found.unwrap_or(0))
}
