use anyhow::Result;
use regex::Regex;

#[cfg(target_os = "linux")]
use anyhow::Context;
#[cfg(target_os = "linux")]
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub pid: u32,
    pub cwd: String,
    pub cmdline: String,
}

#[derive(Debug, Default, Clone)]
pub struct ProcessScanStats {
    pub pids_seen: usize,
    pub matched: usize,
    pub unreadable_cmdline: usize,
    pub unreadable_cwd: usize,
}

#[cfg(target_os = "linux")]
fn list_proc_pids() -> Result<Vec<u32>> {
    let mut pids = Vec::new();
    for entry in std::fs::read_dir("/proc").context("read /proc")? {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let file_name = entry.file_name();
        let s = file_name.to_string_lossy();
        if let Ok(pid) = s.parse::<u32>() {
            pids.push(pid);
        }
    }
    pids.sort_unstable();
    Ok(pids)
}

#[cfg(target_os = "linux")]
fn parse_cmdline_bytes(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }

    let parts: Vec<String> = bytes
        .split(|b| *b == b'\0')
        .filter(|p| !p.is_empty())
        .map(|p| String::from_utf8_lossy(p).to_string())
        .collect();

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}

#[cfg(target_os = "linux")]
fn read_proc_cmdline(pid: u32) -> Result<Option<String>> {
    let path = format!("/proc/{pid}/cmdline");
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(err) => {
            if err.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(err).with_context(|| format!("read {path}"));
        }
    };
    Ok(parse_cmdline_bytes(&bytes))
}

#[cfg(target_os = "linux")]
fn read_proc_cwd(pid: u32) -> Result<Option<PathBuf>> {
    let path = format!("/proc/{pid}/cwd");
    let p = match std::fs::read_link(&path) {
        Ok(p) => p,
        Err(err) => {
            if err.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(err).with_context(|| format!("readlink {path}"));
        }
    };
    Ok(Some(p))
}

#[cfg(target_os = "linux")]
pub fn find_processes_by_cmdline_regex(
    re: &Regex,
    limit: Option<usize>,
) -> Result<(Vec<ProcessInfo>, ProcessScanStats)> {
    let mut out = Vec::new();
    let mut stats = ProcessScanStats::default();

    for pid in list_proc_pids()? {
        stats.pids_seen += 1;
        if let Some(limit) = limit {
            if out.len() >= limit {
                break;
            }
        }

        let cmdline = match read_proc_cmdline(pid) {
            Ok(Some(cmdline)) => cmdline,
            Ok(None) => {
                stats.unreadable_cmdline += 1;
                continue;
            }
            Err(_) => {
                stats.unreadable_cmdline += 1;
                continue;
            }
        };

        if !re.is_match(&cmdline) {
            continue;
        }

        let cwd = match read_proc_cwd(pid) {
            Ok(Some(p)) => p.to_string_lossy().to_string(),
            Ok(None) => {
                stats.unreadable_cwd += 1;
                "<unknown>".to_string()
            }
            Err(_) => {
                stats.unreadable_cwd += 1;
                "<unknown>".to_string()
            }
        };

        out.push(ProcessInfo { pid, cwd, cmdline });
        stats.matched += 1;
    }

    Ok((out, stats))
}

#[cfg(target_os = "windows")]
pub fn find_processes_by_cmdline_regex(
    re: &Regex,
    limit: Option<usize>,
) -> Result<(Vec<ProcessInfo>, ProcessScanStats)> {
    use std::mem::{size_of, zeroed, MaybeUninit};

    use windows_sys::Wdk::System::Threading::{
        NtQueryInformationProcess, ProcessBasicInformation, ProcessCommandLineInformation,
        ProcessWow64Information,
    };
    use windows_sys::Win32::Foundation::UNICODE_STRING;
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::Debug::ReadProcessMemory;
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, PROCESSENTRY32W,
        TH32CS_SNAPPROCESS,
    };
    use windows_sys::Win32::System::Threading::{
        OpenProcess, QueryFullProcessImageNameW, PROCESS_BASIC_INFORMATION, PROCESS_NAME_WIN32,
        PROCESS_QUERY_INFORMATION, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_VM_READ,
    };

    fn utf16_cstr_to_string(buf: &[u16]) -> String {
        let nul = buf.iter().position(|c| *c == 0).unwrap_or(buf.len());
        String::from_utf16_lossy(&buf[..nul])
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct PebPartial {
        reserved1: [u8; 2],
        being_debugged: u8,
        reserved2: [u8; 1],
        reserved3: [*mut core::ffi::c_void; 2],
        ldr: *mut core::ffi::c_void,
        process_parameters: *mut core::ffi::c_void,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Peb32Partial {
        inherited_address_space: u8,
        read_image_file_exec_options: u8,
        being_debugged: u8,
        bit_field: u8,
        mutant: u32,
        image_base_address: u32,
        ldr: u32,
        process_parameters: u32,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CurDir {
        dos_path: UNICODE_STRING,
        handle: windows_sys::Win32::Foundation::HANDLE,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct UnicodeString32 {
        length: u16,
        maximum_length: u16,
        buffer: u32,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CurDir32 {
        dos_path: UnicodeString32,
        handle: u32,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct RtlUserProcessParametersPartial {
        maximum_length: u32,
        length: u32,
        flags: u32,
        debug_flags: u32,
        console_handle: windows_sys::Win32::Foundation::HANDLE,
        console_flags: u32,
        standard_input: windows_sys::Win32::Foundation::HANDLE,
        standard_output: windows_sys::Win32::Foundation::HANDLE,
        standard_error: windows_sys::Win32::Foundation::HANDLE,
        current_directory: CurDir,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct RtlUserProcessParameters32Partial {
        maximum_length: u32,
        length: u32,
        flags: u32,
        debug_flags: u32,
        console_handle: u32,
        console_flags: u32,
        standard_input: u32,
        standard_output: u32,
        standard_error: u32,
        current_directory: CurDir32,
    }

    fn read_process_struct<T: Copy>(
        handle: windows_sys::Win32::Foundation::HANDLE,
        addr: *const core::ffi::c_void,
    ) -> Option<T> {
        if addr.is_null() {
            return None;
        }
        let mut out = MaybeUninit::<T>::uninit();
        let mut bytes_read: usize = 0;
        let ok = unsafe {
            ReadProcessMemory(
                handle,
                addr,
                out.as_mut_ptr().cast(),
                size_of::<T>(),
                &mut bytes_read,
            )
        };
        if ok == 0 || bytes_read != size_of::<T>() {
            return None;
        }
        Some(unsafe { out.assume_init() })
    }

    fn read_process_bytes(
        handle: windows_sys::Win32::Foundation::HANDLE,
        addr: *const core::ffi::c_void,
        size: usize,
    ) -> Option<Vec<u8>> {
        if addr.is_null() || size == 0 {
            return None;
        }
        let mut buf = vec![0u8; size];
        let mut bytes_read: usize = 0;
        let ok = unsafe {
            ReadProcessMemory(handle, addr, buf.as_mut_ptr().cast(), size, &mut bytes_read)
        };
        if ok == 0 || bytes_read != size {
            return None;
        }
        Some(buf)
    }

    fn decode_utf16_le_bytes(bytes: &[u8]) -> Option<String> {
        if bytes.is_empty() || (bytes.len() % 2) != 0 {
            return None;
        }
        let utf16: Vec<u16> = bytes
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        Some(
            String::from_utf16_lossy(&utf16)
                .trim_end_matches('\u{0}')
                .to_string(),
        )
    }

    fn read_process_unicode_string(
        handle: windows_sys::Win32::Foundation::HANDLE,
        buffer: *const core::ffi::c_void,
        len_bytes: usize,
    ) -> Option<String> {
        // Defensive cap: cwd should never be huge; cap to 1 MiB of UTF-16 bytes.
        if len_bytes == 0 || (len_bytes % 2) != 0 || len_bytes > 1024 * 1024 {
            return None;
        }
        let bytes = read_process_bytes(handle, buffer, len_bytes)?;
        decode_utf16_le_bytes(&bytes)
    }

    fn query_process_cwd_via_peb(handle: windows_sys::Win32::Foundation::HANDLE) -> Option<String> {
        // First, check whether this is a WOW64 process. If so, we need to walk the 32-bit PEB.
        let wow64_peb_addr = {
            let mut out: usize = 0;
            let mut ret_len: u32 = 0;
            let status = unsafe {
                NtQueryInformationProcess(
                    handle,
                    ProcessWow64Information,
                    (&mut out as *mut usize).cast(),
                    size_of::<usize>() as u32,
                    &mut ret_len,
                )
            };
            if status == 0 && out != 0 {
                Some(out)
            } else {
                None
            }
        };

        if let Some(peb32_addr) = wow64_peb_addr {
            let peb32: Peb32Partial =
                read_process_struct(handle, peb32_addr as *const core::ffi::c_void)?;
            let params_addr = peb32.process_parameters as usize;
            if params_addr == 0 {
                return None;
            }

            let params: RtlUserProcessParameters32Partial =
                read_process_struct(handle, params_addr as *const core::ffi::c_void)?;
            let ustr = params.current_directory.dos_path;
            let len_bytes = ustr.length as usize;
            let buf_addr = ustr.buffer as usize;
            if buf_addr == 0 {
                return None;
            }

            return read_process_unicode_string(
                handle,
                buf_addr as *const core::ffi::c_void,
                len_bytes,
            );
        }

        // 64-bit PEB path.
        let mut pbasicinfo = MaybeUninit::<PROCESS_BASIC_INFORMATION>::uninit();
        let mut ret_len: u32 = 0;
        let status = unsafe {
            NtQueryInformationProcess(
                handle,
                ProcessBasicInformation,
                pbasicinfo.as_mut_ptr().cast(),
                size_of::<PROCESS_BASIC_INFORMATION>() as u32,
                &mut ret_len,
            )
        };
        if status != 0 {
            return None;
        }
        let pbasicinfo = unsafe { pbasicinfo.assume_init() };
        let peb_addr = pbasicinfo.PebBaseAddress.cast::<core::ffi::c_void>();
        let peb: PebPartial = read_process_struct(handle, peb_addr)?;
        if peb.process_parameters.is_null() {
            return None;
        }
        let params: RtlUserProcessParametersPartial =
            read_process_struct(handle, peb.process_parameters.cast())?;
        let ustr = params.current_directory.dos_path;

        let len_bytes = ustr.Length as usize;
        let buf_ptr = ustr.Buffer.cast::<core::ffi::c_void>();
        read_process_unicode_string(handle, buf_ptr, len_bytes)
    }

    fn query_process_image_path(handle: windows_sys::Win32::Foundation::HANDLE) -> Option<String> {
        // Best-effort: `QueryFullProcessImageNameW` returns a Win32 path (e.g. `C:\...`) rather than
        // a device path (e.g. `\Device\HarddiskVolume...`).
        let mut buf: Vec<u16> = vec![0u16; 32_768];
        let mut len = buf.len() as u32;
        let ok = unsafe {
            QueryFullProcessImageNameW(handle, PROCESS_NAME_WIN32, buf.as_mut_ptr(), &mut len)
        };
        if ok == 0 || len == 0 {
            return None;
        }
        Some(String::from_utf16_lossy(&buf[..(len as usize)]))
    }

    fn query_process_command_line(
        handle: windows_sys::Win32::Foundation::HANDLE,
    ) -> Option<String> {
        // Uses `NtQueryInformationProcess(ProcessCommandLineInformation)` which returns a
        // `UNICODE_STRING` plus the command line buffer in our output buffer.
        let mut needed: u32 = 0;
        let _ = unsafe {
            NtQueryInformationProcess(
                handle,
                ProcessCommandLineInformation,
                std::ptr::null_mut(),
                0,
                &mut needed,
            )
        };
        if needed == 0 {
            return None;
        }

        let mut buf = vec![0u8; needed as usize];
        let status = unsafe {
            NtQueryInformationProcess(
                handle,
                ProcessCommandLineInformation,
                buf.as_mut_ptr().cast(),
                needed,
                &mut needed,
            )
        };
        if status != 0 {
            return None;
        }

        if buf.len() < size_of::<UNICODE_STRING>() {
            return None;
        }

        // `buf` is `Vec<u8>` (alignment 1), so reading a `UNICODE_STRING` from it must use an
        // unaligned load.
        let ustr: UNICODE_STRING =
            unsafe { std::ptr::read_unaligned(buf.as_ptr().cast::<UNICODE_STRING>()) };
        let len_bytes = ustr.Length as usize;
        if len_bytes == 0 || (len_bytes % 2) != 0 {
            return None;
        }

        // `ustr.Buffer` usually points within `buf`, but validate before dereferencing.
        let buf_start = buf.as_ptr() as usize;
        let buf_end = buf_start + buf.len();
        let mut ptr = ustr.Buffer as usize;

        if ptr < buf_start || ptr + len_bytes > buf_end {
            // Some implementations/versions appear to place the string directly after the struct.
            let fallback_ptr = buf_start + size_of::<UNICODE_STRING>();
            if fallback_ptr + len_bytes > buf_end {
                return None;
            }
            ptr = fallback_ptr;
        }

        // Avoid creating a potentially-unaligned `&[u16]` by decoding from raw bytes instead.
        let offset = ptr.checked_sub(buf_start)?;
        let bytes = buf.get(offset..offset + len_bytes)?;
        let utf16: Vec<u16> = bytes
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        let s = String::from_utf16_lossy(&utf16);
        Some(s.trim_end_matches('\u{0}').to_string())
    }

    fn open_process_for_query(pid: u32) -> Option<windows_sys::Win32::Foundation::HANDLE> {
        // Prefer limited info (more likely to succeed). Retry with full query access for APIs that
        // might require it on some Windows versions/configurations.
        let handle =
            unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_VM_READ, 0, pid) };
        if !handle.is_null() {
            return Some(handle);
        }
        let handle = unsafe { OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, 0, pid) };
        if !handle.is_null() {
            return Some(handle);
        }

        // Fall back to query-only, so we can at least read the cmdline/image path even if VM reads
        // are denied.
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if !handle.is_null() {
            return Some(handle);
        }
        let handle = unsafe { OpenProcess(PROCESS_QUERY_INFORMATION, 0, pid) };
        if handle.is_null() {
            None
        } else {
            Some(handle)
        }
    }

    let mut out = Vec::new();
    let mut stats = ProcessScanStats::default();

    // Enumerate processes via Toolhelp (Win32 APIs).
    #[derive(Debug)]
    struct ProcStub {
        pid: u32,
        exe_name: String,
    }

    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        anyhow::bail!("CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS) failed");
    }

    let mut stubs: Vec<ProcStub> = Vec::new();
    let mut entry: PROCESSENTRY32W = unsafe { zeroed() };
    entry.dwSize = size_of::<PROCESSENTRY32W>() as u32;

    let mut ok = unsafe { Process32FirstW(snapshot, &mut entry) };
    while ok != 0 {
        let pid = entry.th32ProcessID;
        let exe_name = utf16_cstr_to_string(&entry.szExeFile);
        stubs.push(ProcStub { pid, exe_name });
        ok = unsafe { Process32NextW(snapshot, &mut entry) };
    }

    unsafe { CloseHandle(snapshot) };

    stubs.sort_by_key(|p| p.pid);

    for p in stubs {
        stats.pids_seen += 1;
        if let Some(limit) = limit {
            if out.len() >= limit {
                break;
            }
        }

        let mut cmdline = None;
        let handle = open_process_for_query(p.pid);
        if let Some(handle) = handle {
            cmdline =
                query_process_command_line(handle).or_else(|| query_process_image_path(handle));
            unsafe { CloseHandle(handle) };
        }
        let cmdline = cmdline.or_else(|| {
            if p.exe_name.is_empty() {
                None
            } else {
                Some(p.exe_name.clone())
            }
        });
        let Some(cmdline) = cmdline else {
            stats.unreadable_cmdline += 1;
            continue;
        };

        if !re.is_match(&cmdline) {
            continue;
        }

        let cwd = match open_process_for_query(p.pid) {
            Some(handle) => {
                let cwd = query_process_cwd_via_peb(handle);
                unsafe { CloseHandle(handle) };
                cwd
            }
            None => None,
        };

        let cwd = match cwd {
            Some(cwd) => cwd,
            None => {
                if p.pid == std::process::id() {
                    std::env::current_dir()
                        .ok()
                        .map(|p| p.to_string_lossy().to_string())
                        .unwrap_or_else(|| {
                            stats.unreadable_cwd += 1;
                            "<unknown>".to_string()
                        })
                } else {
                    stats.unreadable_cwd += 1;
                    "<unknown>".to_string()
                }
            }
        };

        out.push(ProcessInfo {
            pid: p.pid,
            cwd,
            cmdline,
        });
        stats.matched += 1;
    }

    Ok((out, stats))
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub fn find_processes_by_cmdline_regex(
    _re: &Regex,
    _limit: Option<usize>,
) -> Result<(Vec<ProcessInfo>, ProcessScanStats)> {
    anyhow::bail!("process scanning is only supported on Linux and Windows today")
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::parse_cmdline_bytes;

    #[test]
    fn parse_cmdline_splits_on_nul() {
        let bytes = b"/bin/bash\0-c\0echo hi\0";
        let s = parse_cmdline_bytes(bytes).unwrap();
        assert_eq!(s, "/bin/bash -c echo hi");
    }

    #[test]
    fn parse_cmdline_empty_is_none() {
        assert!(parse_cmdline_bytes(b"").is_none());
        assert!(parse_cmdline_bytes(b"\0\0").is_none());
    }
}
