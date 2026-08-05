use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use tokio::task::spawn_blocking;

use crate::{platform::TtlCache, PidResolver};

const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(5);
// Best-effort caps to avoid unbounded growth (TTL alone doesn't evict).
const MAX_CONN_CACHE_ENTRIES: usize = 16_384;
const MAX_PPID_CACHE_ENTRIES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct ConnectionKey {
    local: SocketAddr,
    peer: SocketAddr,
}

#[derive(Clone)]
pub struct WindowsPidResolver {
    conn_cache: TtlCache<ConnectionKey, u32>,
    ppid_cache: TtlCache<u32, Option<u32>>,
    cache_ttl: Duration,
}

impl Default for WindowsPidResolver {
    fn default() -> Self {
        Self {
            conn_cache: TtlCache::default(),
            ppid_cache: TtlCache::default(),
            cache_ttl: DEFAULT_CACHE_TTL,
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
        if let Some(pid) = self.conn_cache.get(&key, self.cache_ttl) {
            return Ok(Some(pid));
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
            self.conn_cache.insert(key, pid);
            self.conn_cache
                .prune_expired_if_over(MAX_CONN_CACHE_ENTRIES, self.cache_ttl);
        }
        Ok(pid)
    }

    fn parent_pid_blocking(&self, pid: u32) -> Result<Option<u32>> {
        if let Some(ppid) = self.ppid_cache.get(&pid, self.cache_ttl) {
            return Ok(ppid);
        }

        let ppid = process_parent_pid(pid).context("resolve parent pid via Toolhelp snapshot")?;
        self.ppid_cache.insert(pid, ppid);
        self.ppid_cache
            .prune_expired_if_over(MAX_PPID_CACHE_ENTRIES, self.cache_ttl);
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

struct AlignedBuffer {
    words: Vec<usize>,
    len_bytes: usize,
}

impl AlignedBuffer {
    fn as_ptr(&self) -> *const u8 {
        self.words.as_ptr().cast()
    }

    fn as_mut_ptr(&mut self) -> *mut std::ffi::c_void {
        self.words.as_mut_ptr().cast()
    }
}

fn query_tcp_table(address_family: u32, context: &str) -> Result<Option<AlignedBuffer>> {
    use std::{mem::size_of, ptr};

    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetExtendedTcpTable, TCP_TABLE_OWNER_PID_ALL,
    };

    const ERROR_INSUFFICIENT_BUFFER: u32 = 122;

    let mut size = 0u32;
    let rc = unsafe {
        GetExtendedTcpTable(
            ptr::null_mut(),
            &mut size,
            0,
            address_family,
            TCP_TABLE_OWNER_PID_ALL,
            0,
        )
    };
    if rc != 0 && rc != ERROR_INSUFFICIENT_BUFFER {
        return Err(anyhow!(
            "GetExtendedTcpTable size probe ({context}) failed with error {rc}"
        ));
    }

    while size != 0 {
        let requested_bytes = size as usize;
        let word_count = requested_bytes.div_ceil(size_of::<usize>());
        let mut buffer = AlignedBuffer {
            words: vec![0usize; word_count],
            len_bytes: requested_bytes,
        };
        let rc = unsafe {
            GetExtendedTcpTable(
                buffer.as_mut_ptr(),
                &mut size,
                0,
                address_family,
                TCP_TABLE_OWNER_PID_ALL,
                0,
            )
        };
        if rc == ERROR_INSUFFICIENT_BUFFER {
            continue;
        }
        if rc != 0 {
            return Err(anyhow!(
                "GetExtendedTcpTable ({context}) failed with error {rc}"
            ));
        }
        if size as usize > buffer.words.len() * size_of::<usize>() {
            return Err(anyhow!(
                "GetExtendedTcpTable ({context}) returned size {size} larger than its buffer"
            ));
        }
        buffer.len_bytes = size as usize;
        return Ok(Some(buffer));
    }

    Ok(None)
}

fn validate_table_rows(
    buffer_len: usize,
    rows_offset: usize,
    row_size: usize,
    row_count: usize,
    context: &str,
) -> Result<()> {
    let rows_bytes = row_size
        .checked_mul(row_count)
        .ok_or_else(|| anyhow!("{context} row byte count overflow"))?;
    let required_bytes = rows_offset
        .checked_add(rows_bytes)
        .ok_or_else(|| anyhow!("{context} table byte count overflow"))?;
    if required_bytes > buffer_len {
        return Err(anyhow!(
            "{context} table is truncated: needs {required_bytes} bytes, has {buffer_len}"
        ));
    }
    Ok(())
}

fn pid_for_connection_v4(local: SocketAddr, peer: SocketAddr) -> Result<Option<u32>> {
    use std::mem::{offset_of, size_of};

    use windows_sys::Win32::NetworkManagement::IpHelper::{
        MIB_TCPROW_OWNER_PID, MIB_TCPTABLE_OWNER_PID,
    };
    use windows_sys::Win32::Networking::WinSock::AF_INET;

    let IpAddr::V4(local_ip) = local.ip() else {
        return Ok(None);
    };
    let IpAddr::V4(peer_ip) = peer.ip() else {
        return Ok(None);
    };

    let local_port = local.port();
    let peer_port = peer.port();

    let Some(buffer) = query_tcp_table(AF_INET as u32, "IPv4")? else {
        return Ok(None);
    };
    if buffer.len_bytes < size_of::<u32>() {
        return Err(anyhow!("IPv4 TCP table is missing its entry count"));
    }
    let table = buffer.as_ptr().cast::<MIB_TCPTABLE_OWNER_PID>();
    let num_entries = unsafe { (*table).dwNumEntries as usize };
    let rows_offset = offset_of!(MIB_TCPTABLE_OWNER_PID, table);
    validate_table_rows(
        buffer.len_bytes,
        rows_offset,
        size_of::<MIB_TCPROW_OWNER_PID>(),
        num_entries,
        "IPv4 TCP",
    )?;
    let first_row = unsafe {
        buffer
            .as_ptr()
            .add(rows_offset)
            .cast::<MIB_TCPROW_OWNER_PID>()
    };
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
    use std::mem::{offset_of, size_of};

    use windows_sys::Win32::NetworkManagement::IpHelper::{
        MIB_TCP6ROW_OWNER_PID, MIB_TCP6TABLE_OWNER_PID,
    };
    use windows_sys::Win32::Networking::WinSock::AF_INET6;

    let IpAddr::V6(local_ip) = local.ip() else {
        return Ok(None);
    };
    let IpAddr::V6(peer_ip) = peer.ip() else {
        return Ok(None);
    };

    let local_port = local.port();
    let peer_port = peer.port();

    let Some(buffer) = query_tcp_table(AF_INET6 as u32, "IPv6")? else {
        return Ok(None);
    };
    if buffer.len_bytes < size_of::<u32>() {
        return Err(anyhow!("IPv6 TCP table is missing its entry count"));
    }
    let table = buffer.as_ptr().cast::<MIB_TCP6TABLE_OWNER_PID>();
    let num_entries = unsafe { (*table).dwNumEntries as usize };
    let rows_offset = offset_of!(MIB_TCP6TABLE_OWNER_PID, table);
    validate_table_rows(
        buffer.len_bytes,
        rows_offset,
        size_of::<MIB_TCP6ROW_OWNER_PID>(),
        num_entries,
        "IPv6 TCP",
    )?;
    let first_row = unsafe {
        buffer
            .as_ptr()
            .add(rows_offset)
            .cast::<MIB_TCP6ROW_OWNER_PID>()
    };
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

fn process_parent_pid(pid: u32) -> Result<Option<u32>> {
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

    Ok(found)
}

#[cfg(test)]
mod tests {
    use std::mem::align_of;

    use windows_sys::Win32::NetworkManagement::IpHelper::{
        MIB_TCP6TABLE_OWNER_PID, MIB_TCPTABLE_OWNER_PID,
    };

    use super::*;

    #[test]
    fn tcp_table_buffer_is_aligned_for_windows_table_types() {
        let buffer = AlignedBuffer {
            words: vec![0usize; 4],
            len_bytes: 4 * size_of::<usize>(),
        };
        let address = buffer.as_ptr() as usize;

        assert_eq!(address % align_of::<MIB_TCPTABLE_OWNER_PID>(), 0);
        assert_eq!(address % align_of::<MIB_TCP6TABLE_OWNER_PID>(), 0);
    }

    #[test]
    fn validates_complete_table_rows() {
        validate_table_rows(44, 4, 20, 2, "test").expect("complete rows");
    }

    #[test]
    fn rejects_truncated_table_rows() {
        let error =
            validate_table_rows(43, 4, 20, 2, "test").expect_err("truncated rows should fail");

        assert!(error.to_string().contains("table is truncated"));
    }
}
