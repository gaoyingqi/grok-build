//! Dedicated-thread reader for the ACP stdio transport's standard input.
//!
//! Every ACP client (VS Code extension, grok-desktop, the leader bridge) drives
//! the agent over a **persistent, bidirectional** newline-delimited JSON-RPC
//! stream on stdio: it writes requests on the child's stdin and reads responses
//! on stdout, keeping **stdin open for the whole session**.
//!
//! # Why not `tokio::io::stdin()`
//!
//! `tokio::io::stdin()` is not truly asynchronous. Tokio services it with a
//! blocking `std::io` read on an internal pool thread, and that read **cannot be
//! cancelled**. For interactive / persistent uses the
//! [`tokio::io::Stdin`](https://docs.rs/tokio/latest/tokio/io/struct.Stdin.html)
//! docs recommend "spawn a thread dedicated to user input and use blocking IO
//! directly in that thread". [`spawn_stdin_line_reader`] does exactly that.
//!
//! # Why the reader takes *exclusive* ownership of stdin (Windows)
//!
//! `std::io::Stdin` is a process-global handle guarded by a re-entrant mutex
//! (the `StdinLock`). A blocking read **holds that lock for the entire duration
//! of the read** — and for the persistent stdio transport the reader is almost
//! always parked in a read, waiting for the client's next line. If *any other*
//! code in the process then calls `std::io::stdin()` (e.g. a stray interactive
//! prompt reached only on a particular platform), it blocks on the lock until
//! the reader's in-flight read returns — which only happens at **EOF**, i.e.
//! when the client closes stdin. For a persistent ACP client that never closes
//! stdin mid-session this is a hard hang: the agent freezes part-way through a
//! request (observed on **Windows** during `session/new`) and only unblocks when
//! the transport is torn down. macOS/Linux don't reach the offending stray read,
//! so they were unaffected — but the hazard is real on any platform.
//!
//! To make the transport robust, on Windows the reader thread takes a **private
//! duplicate** of the real stdin handle and then points the process's standard
//! input at **`NUL`**. The reader keeps reading the client's bytes through its
//! private handle, while every *other* `std::io::stdin()` read in the process
//! observes immediate EOF instead of deadlocking on the lock. This mirrors what
//! already makes leader mode safe (the agent subprocess is spawned with
//! `stdin = NUL`, so its stray reads EOF instantly). Unix keeps reading
//! `std::io::stdin()` directly — it has no second stdin reader on these paths
//! and the extra FFI/`dup` would add risk for no benefit.
//!
//! # Escaped-slash normalization (acp 0.6 wire workaround)
//!
//! Every line is forwarded through `normalize_json_line` — see the
//! crate-private `normalize` module for the contract and its scope.

use std::io::{self, BufRead};

use tokio::sync::mpsc;

use crate::normalize::normalize_json_line;

/// Channel depth for buffered stdin lines. Small: the reader thread blocks on a
/// full channel, applying natural backpressure to a flooding peer rather than
/// growing memory without bound.
const STDIN_LINE_CHANNEL_DEPTH: usize = 64;

/// Spawn a dedicated OS thread that reads newline-delimited lines from the
/// process's standard input with **synchronous, blocking** `std::io` and yields
/// each line (its trailing `\n` included, like `read_line`/`read_until`) on the
/// returned channel. A final line without a trailing newline is still delivered
/// before the channel closes.
///
/// This compatibility API keeps the historical `Receiver<Vec<u8>>` shape and
/// therefore treats a reader error as channel termination. New ACP runtimes
/// must use [`spawn_stdin_line_reader_with_errors`] so EOF and I/O failure stay
/// distinct. The reader is the **sole** stdin consumer in agent-stdio and
/// leader-bridge paths; on Windows it also redirects process stdin to `NUL` so
/// stray readers cannot deadlock (see the [module docs](self)).
pub fn spawn_stdin_line_reader() -> mpsc::Receiver<Vec<u8>> {
    spawn_stdin_line_reader_internal(drop_reader_error)
}

/// Spawn the synchronous stdin reader while preserving fatal read errors.
///
/// `None` from the returned receiver means normal EOF (or receiver drop), while
/// `Some(Err(_))` is a fatal reader error. Error messages are sanitized before
/// they enter the channel; callers can inspect only the stable [`io::ErrorKind`]
/// and must not log the underlying OS text.
pub fn spawn_stdin_line_reader_with_errors() -> mpsc::Receiver<io::Result<Vec<u8>>> {
    spawn_stdin_line_reader_internal(keep_reader_result)
}

fn keep_reader_result(result: io::Result<Vec<u8>>) -> Option<io::Result<Vec<u8>>> {
    Some(result)
}

fn drop_reader_error(result: io::Result<Vec<u8>>) -> Option<Vec<u8>> {
    result.ok()
}

fn spawn_stdin_line_reader_internal<T, F>(map: F) -> mpsc::Receiver<T>
where
    T: Send + 'static,
    F: Fn(io::Result<Vec<u8>>) -> Option<T> + Clone + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<T>(STDIN_LINE_CHANNEL_DEPTH);

    // Windows 上先隔离全局 stdin；Unix 直接锁定标准输入，保证只有一个阻塞 reader。
    #[cfg(windows)]
    let private_stdin: Option<std::fs::File> = isolate_process_stdin();

    let thread_tx = tx.clone();
    let thread_map = map.clone();
    let spawn_result = std::thread::Builder::new()
        .name("acp-stdin".to_string())
        .spawn(move || {
            #[cfg(windows)]
            if let Some(file) = private_stdin {
                forward_lines_with_map(
                    std::io::BufReader::new(file),
                    &thread_tx,
                    &thread_map,
                    crate::line_reader::MAX_LINE_SIZE,
                );
                return;
            }
            let stdin = std::io::stdin();
            forward_lines_with_map(
                stdin.lock(),
                &thread_tx,
                &thread_map,
                crate::line_reader::MAX_LINE_SIZE,
            );
        });

    if let Err(error) = spawn_result {
        // 线程创建失败也必须可观察，不能退化为正常 EOF。
        let stable_error = io::Error::new(error.kind(), "ACP stdin reader unavailable");
        if let Some(event) = map(Err(stable_error)) {
            let _ = tx.blocking_send(event);
        }
    }
    rx
}

/// 供单元测试复用的错误感知转发器；Ok(0) 只表示正常 EOF。
#[cfg(test)]
fn forward_lines<R: BufRead>(reader: R, tx: &mpsc::Sender<io::Result<Vec<u8>>>) {
    forward_lines_with_map(
        reader,
        tx,
        keep_reader_result,
        crate::line_reader::MAX_LINE_SIZE,
    );
}

fn forward_lines_with_map<R, T, F>(
    mut reader: R,
    tx: &mpsc::Sender<T>,
    map: F,
    max_line_size: usize,
) where
    R: BufRead,
    T: Send + 'static,
    F: Fn(io::Result<Vec<u8>>) -> Option<T>,
{
    let mut line = Vec::new();
    loop {
        line.clear();
        match read_line_capped_with_limit(&mut reader, &mut line, max_line_size) {
            Ok(0) => break,
            Ok(_) => {
                let normalized = normalize_json_line(std::mem::take(&mut line));
                let Some(event) = map(Ok(normalized)) else {
                    break;
                };
                // 通道满时阻塞当前 OS reader，向上游施加有界反压。
                if tx.blocking_send(event).is_err() {
                    break;
                }
            }
            Err(error) => {
                let stable_error = sanitize_reader_error(error);
                if let Some(event) = map(Err(stable_error)) {
                    let _ = tx.blocking_send(event);
                }
                break;
            }
        }
    }
}

/// 逐块读取一行，硬限制累计字节数，避免无换行输入触发无界分配。
fn read_line_capped_with_limit<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
    max_line_size: usize,
) -> io::Result<usize> {
    line.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(line.len());
        }

        let (count, complete) = match available.iter().position(|&byte| byte == b'\n') {
            Some(position) => (position + 1, true),
            None => (available.len(), false),
        };
        let next_len = line.len().checked_add(count).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "ACP stdin line length overflow")
        })?;
        if next_len > max_line_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ACP stdin line exceeds maximum length",
            ));
        }
        line.extend_from_slice(&available[..count]);
        reader.consume(count);
        if complete {
            return Ok(line.len());
        }
    }
}

fn sanitize_reader_error(error: io::Error) -> io::Error {
    io::Error::new(error.kind(), "ACP stdin read failed")
}

/// Duplicate the real stdin handle for private use and repoint the process's
/// `STD_INPUT_HANDLE` at `NUL`, returning the duplicate as an owned [`File`].
///
/// Returns `None` (caller falls back to `std::io::stdin()`) when there is no
/// stdin handle or duplication fails. Win32 declarations are inlined to avoid a
/// `windows`/`windows-sys` dependency, matching the pager's console setup.
///
/// [`File`]: std::fs::File
#[cfg(windows)]
fn isolate_process_stdin() -> Option<std::fs::File> {
    use std::os::windows::io::FromRawHandle as _;

    // Win32 constants (inlined to avoid a dependency).
    const STD_INPUT_HANDLE: u32 = 0xFFFF_FFF6; // (DWORD)-10
    const DUPLICATE_SAME_ACCESS: u32 = 0x0000_0002;
    const GENERIC_READ: u32 = 0x8000_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const OPEN_EXISTING: u32 = 0x0000_0003;
    const INVALID_HANDLE: *mut core::ffi::c_void = -1_isize as *mut core::ffi::c_void;

    unsafe extern "system" {
        fn GetStdHandle(nStdHandle: u32) -> *mut core::ffi::c_void;
        fn SetStdHandle(nStdHandle: u32, hHandle: *mut core::ffi::c_void) -> i32;
        fn GetCurrentProcess() -> *mut core::ffi::c_void;
        fn DuplicateHandle(
            hSourceProcessHandle: *mut core::ffi::c_void,
            hSourceHandle: *mut core::ffi::c_void,
            hTargetProcessHandle: *mut core::ffi::c_void,
            lpTargetHandle: *mut *mut core::ffi::c_void,
            dwDesiredAccess: u32,
            bInheritHandle: i32,
            dwOptions: u32,
        ) -> i32;
        fn CreateFileW(
            lpFileName: *const u16,
            dwDesiredAccess: u32,
            dwShareMode: u32,
            lpSecurityAttributes: *mut core::ffi::c_void,
            dwCreationDisposition: u32,
            dwFlagsAndAttributes: u32,
            hTemplateFile: *mut core::ffi::c_void,
        ) -> *mut core::ffi::c_void;
    }

    // SAFETY: standard Win32 console/file calls; every return value is checked
    // before use and the duplicated handle is wrapped in an owning `File`.
    unsafe {
        let current = GetStdHandle(STD_INPUT_HANDLE);
        if current.is_null() || current == INVALID_HANDLE {
            return None;
        }

        let process = GetCurrentProcess();
        let mut duplicate: *mut core::ffi::c_void = std::ptr::null_mut();
        if DuplicateHandle(
            process,
            current,
            process,
            &mut duplicate,
            0,
            0, // not inheritable
            DUPLICATE_SAME_ACCESS,
        ) == 0
        {
            return None;
        }

        // Repoint the process's std input at NUL so stray `std::io::stdin()`
        // reads observe EOF instead of blocking on the held `StdinLock`. If NUL
        // can't be opened we still return the duplicate so the reader works;
        // we just forgo the stray-read isolation.
        let nul: Vec<u16> = "NUL\0".encode_utf16().collect();
        let nul_handle = CreateFileW(
            nul.as_ptr(),
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null_mut(),
            OPEN_EXISTING,
            0,
            std::ptr::null_mut(),
        );
        if nul_handle != INVALID_HANDLE && !nul_handle.is_null() {
            SetStdHandle(STD_INPUT_HANDLE, nul_handle);
        }

        Some(std::fs::File::from_raw_handle(duplicate as _))
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor, Read};

    use super::*;

    /// 先提供一行，再注入错误，验证错误不会伪装成 channel EOF。
    struct ErrorAfterLine {
        state: u8,
    }

    impl Read for ErrorAfterLine {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::Other, "raw reader secret"))
        }
    }

    impl BufRead for ErrorAfterLine {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            if self.state == 0 {
                Ok(b"ok\n")
            } else {
                Err(io::Error::new(io::ErrorKind::Other, "raw reader secret"))
            }
        }

        fn consume(&mut self, amount: usize) {
            if amount > 0 {
                self.state = 1;
            }
        }
    }

    #[test]
    fn bufread_error_is_forwarded_separately_from_eof() {
        let (tx, mut rx) = mpsc::channel(4);
        forward_lines(ErrorAfterLine { state: 0 }, &tx);

        let first = rx
            .blocking_recv()
            .expect("reader should forward the complete line")
            .expect("the complete line should be successful");
        assert_eq!(first, b"ok\n");

        let error = rx
            .blocking_recv()
            .expect("reader error must be observable before channel close")
            .expect_err("reader error must not be converted to EOF");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "ACP stdin read failed");
    }

    #[test]
    fn unterminated_line_is_rejected_at_small_test_limit_without_large_buffer() {
        let mut reader = Cursor::new(b"123456789".to_vec());
        let mut line = Vec::new();
        let error = read_line_capped_with_limit(&mut reader, &mut line, 8)
            .expect_err("an unterminated line over the limit must fail");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            line.len() <= 8,
            "test reader must never retain bytes over the cap"
        );
    }
}
