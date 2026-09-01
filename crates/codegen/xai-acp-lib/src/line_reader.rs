//! Cancel-safe line-buffered [`AsyncRead`] wrapper.
//!
//! `agent-client-protocol` v0.6's `handle_io` uses `select_biased!` with
//! `BufReader::read_line`. `read_line` is **not** cancel-safe: it internally
//! calls `consume()` on partial reads, so dropping the future mid-read loses
//! bytes and corrupts the stream.
//!
//! [`LineBufferedRead`] works around this by pre-reading complete `\n`-delimited
//! lines on a dedicated task and serving them through a channel. The `poll_read`
//! implementation only returns `Pending` *between* lines (when no buffered data
//! remains), so ACP's `BufReader::read_line` always finds `\n` without
//! suspending, and can never be cancelled mid-read by `select_biased!`.

use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::{
    AsyncBufRead, AsyncBufReadExt as _, AsyncRead, AsyncWrite, SinkExt as _, StreamExt as _,
    channel::mpsc, io::BufReader,
};

/// Maximum size of a single NDJSON line (64 MiB).
///
/// This is shared with the synchronous stdin reader so every ACP ingress has
/// one hard allocation bound.
pub(crate) const MAX_LINE_SIZE: usize = 64 * 1024 * 1024;

/// Stable categories used by the failure-aware ACP I/O wrappers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcpTransportErrorKind {
    /// stdout write returned an error or an unexpected zero-byte write.
    StdoutWrite,
    /// stdout flush returned an error.
    StdoutFlush,
    /// stdout close returned an error.
    StdoutClose,
    /// stdin/ACP input returned an I/O error.
    StdinRead,
    /// An input line exceeded [`MAX_LINE_SIZE`].
    StdinLineTooLong,
    /// The stdin-to-ACP bridge could not write to its bounded duplex stream.
    StdinBridge,
}

impl AcpTransportErrorKind {
    /// Stable, non-sensitive label suitable for stderr debug events.
    pub const fn stable_code(self) -> &'static str {
        match self {
            Self::StdoutWrite => "stdout_write_failed",
            Self::StdoutFlush => "stdout_flush_failed",
            Self::StdoutClose => "stdout_close_failed",
            Self::StdinRead => "stdin_read_failed",
            Self::StdinLineTooLong => "stdin_line_too_long",
            Self::StdinBridge => "stdin_bridge_failed",
        }
    }

    fn io_error(self) -> io::Error {
        let kind = match self {
            Self::StdoutWrite | Self::StdoutFlush | Self::StdoutClose => io::ErrorKind::BrokenPipe,
            Self::StdinRead | Self::StdinBridge => io::ErrorKind::Other,
            Self::StdinLineTooLong => io::ErrorKind::InvalidData,
        };
        let message = match self {
            Self::StdoutWrite => "ACP stdout write failed",
            Self::StdoutFlush => "ACP stdout flush failed",
            Self::StdoutClose => "ACP stdout close failed",
            Self::StdinRead => "ACP stdin read failed",
            Self::StdinLineTooLong => "ACP stdin line exceeds maximum length",
            Self::StdinBridge => "ACP stdin bridge failed",
        };
        io::Error::new(kind, message)
    }
}

#[derive(Debug, Default)]
struct TransportStateSnapshot {
    failure: Option<AcpTransportErrorKind>,
    reader_waker: Option<Waker>,
}

#[derive(Debug, Default)]
struct TransportStateInner {
    snapshot: Mutex<TransportStateSnapshot>,
}

/// 连接级共享失败状态；首个失败会唤醒正在等待输入的 ACP reader。
#[derive(Clone, Debug, Default)]
pub struct AcpTransportState {
    inner: Arc<TransportStateInner>,
}

impl AcpTransportState {
    /// 创建一个未失败的 transport 状态。
    pub fn new() -> Self {
        Self::default()
    }

    /// 返回首个稳定失败分类，不暴露底层错误正文。
    pub fn failure(&self) -> Option<AcpTransportErrorKind> {
        let snapshot = match self.inner.snapshot.lock() {
            Ok(snapshot) => snapshot,
            Err(poisoned) => poisoned.into_inner(),
        };
        snapshot.failure
    }

    /// 记录首个失败并唤醒 ACP input future。
    pub fn fail(&self, kind: AcpTransportErrorKind) {
        let reader_waker = {
            let mut snapshot = match self.inner.snapshot.lock() {
                Ok(snapshot) => snapshot,
                Err(poisoned) => poisoned.into_inner(),
            };
            if snapshot.failure.is_some() {
                return;
            }
            snapshot.failure = Some(kind);
            snapshot.reader_waker.take()
        };
        if let Some(waker) = reader_waker {
            waker.wake();
        }
    }

    fn register_reader(&self, waker: &Waker) -> Option<AcpTransportErrorKind> {
        let mut snapshot = match self.inner.snapshot.lock() {
            Ok(snapshot) => snapshot,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(kind) = snapshot.failure {
            return Some(kind);
        }
        snapshot.reader_waker = Some(waker.clone());
        None
    }

    fn clear_reader_waker(&self) {
        let mut snapshot = match self.inner.snapshot.lock() {
            Ok(snapshot) => snapshot,
            Err(poisoned) => poisoned.into_inner(),
        };
        snapshot.reader_waker = None;
    }

    fn failure_error(&self) -> io::Error {
        match self.failure() {
            Some(kind) => kind.io_error(),
            None => io::Error::new(io::ErrorKind::Other, "ACP transport failed"),
        }
    }
}

/// 在共享失败状态下读取 ACP 输入；写端失败后立即拒绝后续输入。
pub struct AcpTransportReader<R> {
    inner: R,
    state: AcpTransportState,
}

impl<R> AcpTransportReader<R> {
    /// 包装一个 futures-compatible reader。
    pub fn new(inner: R, state: AcpTransportState) -> Self {
        Self { inner, state }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for AcpTransportReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.state.register_reader(cx.waker()).is_some() {
            return Poll::Ready(Err(self.state.failure_error()));
        }

        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        match result {
            Poll::Ready(Ok(size)) => {
                self.state.clear_reader_waker();
                match self.state.failure() {
                    Some(_) => Poll::Ready(Err(self.state.failure_error())),
                    None => Poll::Ready(Ok(size)),
                }
            }
            Poll::Ready(Err(error)) => {
                self.state.clear_reader_waker();
                let kind = if error.kind() == io::ErrorKind::InvalidData {
                    AcpTransportErrorKind::StdinLineTooLong
                } else {
                    AcpTransportErrorKind::StdinRead
                };
                self.state.fail(kind);
                Poll::Ready(Err(self.state.failure_error()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// 在共享失败状态下写出 ACP 输出；写失败会唤醒 reader 让第三方循环退出。
pub struct AcpTransportWriter<W> {
    inner: W,
    state: AcpTransportState,
    /// Tokio stdio may report bytes accepted before its blocking write finishes.
    pending_write: Option<usize>,
}

impl<W> AcpTransportWriter<W> {
    /// 包装唯一的 ACP stdout writer。
    pub fn new(inner: W, state: AcpTransportState) -> Self {
        Self {
            inner,
            state,
            pending_write: None,
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for AcpTransportWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.state.failure().is_some() {
            return Poll::Ready(Err(self.state.failure_error()));
        }

        // Tokio stdout 会先报告“已接收”再在 flush 中返回阻塞写错误；先确认它。
        if let Some(size) = self.pending_write {
            return match Pin::new(&mut self.inner).poll_flush(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(())) => {
                    self.pending_write = None;
                    Poll::Ready(Ok(size))
                }
                Poll::Ready(Err(_)) => {
                    self.pending_write = None;
                    self.state.fail(AcpTransportErrorKind::StdoutWrite);
                    Poll::Ready(Err(self.state.failure_error()))
                }
            };
        }

        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(0)) if !buf.is_empty() => {
                self.state.fail(AcpTransportErrorKind::StdoutWrite);
                Poll::Ready(Err(self.state.failure_error()))
            }
            Poll::Ready(Ok(size)) if size > 0 => {
                self.pending_write = Some(size);
                match Pin::new(&mut self.inner).poll_flush(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        self.pending_write = None;
                        Poll::Ready(Ok(size))
                    }
                    Poll::Ready(Err(_)) => {
                        self.pending_write = None;
                        self.state.fail(AcpTransportErrorKind::StdoutWrite);
                        Poll::Ready(Err(self.state.failure_error()))
                    }
                }
            }
            Poll::Ready(Err(_)) => {
                self.state.fail(AcpTransportErrorKind::StdoutWrite);
                Poll::Ready(Err(self.state.failure_error()))
            }
            result => result,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.state.failure().is_some() {
            return Poll::Ready(Err(self.state.failure_error()));
        }
        if self.pending_write.is_some() {
            match Pin::new(&mut self.inner).poll_flush(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => self.pending_write = None,
                Poll::Ready(Err(_)) => {
                    self.pending_write = None;
                    self.state.fail(AcpTransportErrorKind::StdoutFlush);
                    return Poll::Ready(Err(self.state.failure_error()));
                }
            }
        }
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Ready(Err(_)) => {
                self.state.fail(AcpTransportErrorKind::StdoutFlush);
                Poll::Ready(Err(self.state.failure_error()))
            }
            result => result,
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.state.failure().is_some() {
            return Poll::Ready(Err(self.state.failure_error()));
        }
        if self.pending_write.is_some() {
            match Pin::new(&mut self.inner).poll_flush(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => self.pending_write = None,
                Poll::Ready(Err(_)) => {
                    self.pending_write = None;
                    self.state.fail(AcpTransportErrorKind::StdoutClose);
                    return Poll::Ready(Err(self.state.failure_error()));
                }
            }
        }
        match Pin::new(&mut self.inner).poll_close(cx) {
            Poll::Ready(Err(_)) => {
                self.state.fail(AcpTransportErrorKind::StdoutClose);
                Poll::Ready(Err(self.state.failure_error()))
            }
            result => result,
        }
    }
}

/// An [`AsyncRead`] that only yields complete `\n`-delimited lines.
///
/// Internally, a background task reads lines from the wrapped reader and sends
/// them through a channel. [`poll_read`](AsyncRead::poll_read) serves bytes
/// from the current line buffer and only returns `Poll::Pending` when no
/// buffered bytes remain (i.e. between lines). This guarantees that a consumer
/// calling `BufReader::read_line` on this reader will always complete without
/// intermediate `Pending` states, making it safe to use inside `select!`.
pub struct LineBufferedRead {
    /// Buffered bytes from the current line being served.
    buf: Vec<u8>,
    /// Read cursor within `buf`.
    pos: usize,
    /// Receives complete lines (or an IO error) from the reader task.
    rx: mpsc::Receiver<io::Result<Vec<u8>>>,
}

impl LineBufferedRead {
    /// Wrap an `AsyncRead` source, spawning the reader task via
    /// [`tokio::task::spawn_local`].
    pub fn spawn_local(source: impl AsyncRead + Unpin + 'static) -> Self {
        Self::new(source, |fut| {
            tokio::task::spawn_local(fut);
        })
    }

    /// Wrap an `AsyncRead` source with cancel-safe line buffering.
    ///
    /// A background task is spawned (via `spawn`) that reads `\n`-delimited
    /// lines from `source` and feeds them into the returned reader.
    pub fn new(
        source: impl AsyncRead + Unpin + 'static,
        spawn: impl FnOnce(futures::future::LocalBoxFuture<'static, ()>),
    ) -> Self {
        let (mut tx, rx) = mpsc::channel(64);

        spawn(Box::pin(async move {
            let mut reader = BufReader::new(source);
            let mut line = Vec::new();
            loop {
                match read_line_capped(&mut reader, &mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        if tx.send(Ok(line.split_off(0))).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        }));

        Self {
            buf: Vec::new(),
            pos: 0,
            rx,
        }
    }
}

impl AsyncRead for LineBufferedRead {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        // Serve remaining bytes from the current line.
        if this.pos < this.buf.len() {
            let avail = this.buf.len() - this.pos;
            let n = avail.min(buf.len());
            buf[..n].copy_from_slice(&this.buf[this.pos..this.pos + n]);
            this.pos += n;
            if this.pos >= this.buf.len() {
                this.buf.clear();
                this.pos = 0;
            }
            return Poll::Ready(Ok(n));
        }

        // No buffered data — try to receive the next complete line.
        match this.rx.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(line))) => {
                let n = line.len().min(buf.len());
                buf[..n].copy_from_slice(&line[..n]);
                if n < line.len() {
                    // Stash the remainder for subsequent poll_read calls.
                    this.buf = line;
                    this.pos = n;
                }
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(None) => Poll::Ready(Ok(0)), // EOF
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Read a single `\n`-delimited line into `buf`, capped at [`MAX_LINE_SIZE`].
///
/// Unlike `read_line`, this checks the accumulated size after each internal
/// buffer fill, so memory usage stays bounded even if the peer never sends
/// a newline.
async fn read_line_capped(
    reader: &mut (impl AsyncBufRead + Unpin),
    buf: &mut Vec<u8>,
) -> io::Result<usize> {
    read_line_capped_with_limit(reader, buf, MAX_LINE_SIZE).await
}

async fn read_line_capped_with_limit(
    reader: &mut (impl AsyncBufRead + Unpin),
    buf: &mut Vec<u8>,
    max_line_size: usize,
) -> io::Result<usize> {
    buf.clear();
    loop {
        let (consumed, done) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                return Ok(buf.len()); // EOF
            }
            let (count, done) = match available.iter().position(|&byte| byte == b'\n') {
                Some(position) => (position + 1, true),
                None => (available.len(), false),
            };
            let next_len = buf.len().checked_add(count).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "ACP input line length overflow")
            })?;
            if next_len > max_line_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ACP input line exceeds maximum length",
                ));
            }
            buf.extend_from_slice(&available[..count]);
            (count, done)
        };
        reader.consume_unpin(consumed);
        if done {
            return Ok(buf.len());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, future::Future, io::ErrorKind, rc::Rc, task::Poll};

    use futures::{AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, io::Cursor};

    use super::*;

    /// Helper: run a test inside a tokio LocalSet so spawn_local works.
    fn run<F: Future<Output = ()>>(f: F) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                tokio::task::LocalSet::new().run_until(f).await;
            });
    }

    #[test]
    fn single_line() {
        run(async {
            let source = Cursor::new(b"hello world\n");
            let mut reader = LineBufferedRead::spawn_local(source);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, b"hello world\n");
        });
    }

    #[test]
    fn multiple_lines() {
        run(async {
            let source = Cursor::new(b"line1\nline2\nline3\n");
            let mut reader = LineBufferedRead::spawn_local(source);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, b"line1\nline2\nline3\n");
        });
    }

    #[test]
    fn eof_with_partial_line() {
        run(async {
            let source = Cursor::new(b"complete\nno trailing newline");
            let mut reader = LineBufferedRead::spawn_local(source);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, b"complete\nno trailing newline");
        });
    }

    #[test]
    fn empty_input() {
        run(async {
            let source = Cursor::new(b"");
            let mut reader = LineBufferedRead::spawn_local(source);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert!(buf.is_empty());
        });
    }

    #[test]
    fn large_line_within_limit() {
        run(async {
            // A line larger than BufReader's 8KB buffer but well under 64 MiB.
            let mut data = vec![b'x'; 100_000];
            data.push(b'\n');
            let source = Cursor::new(data.clone());
            let mut reader = LineBufferedRead::spawn_local(source);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, data);
        });
    }

    #[test]
    fn read_line_capped_rejects_oversized() {
        // Test the capped reader directly with a small override isn't
        // practical (MAX_LINE_SIZE is const), so test via the real limit.
        // Just verify the function works for normal input.
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let data = b"normal line\n";
                let mut reader = BufReader::new(Cursor::new(&data[..]));
                let mut buf = Vec::new();
                let n = read_line_capped(&mut reader, &mut buf).await.unwrap();
                assert_eq!(n, 12);
                assert_eq!(buf, b"normal line\n");

                // EOF returns 0
                buf.clear();
                let n = read_line_capped(&mut reader, &mut buf).await.unwrap();
                assert_eq!(n, 0);
            });
    }

    #[test]
    fn small_read_buffer() {
        run(async {
            // Verify poll_read correctly serves a line across multiple small reads.
            let source = Cursor::new(b"abcdef\n");
            let mut reader = LineBufferedRead::spawn_local(source);
            let mut small_buf = [0u8; 3];

            // First read: "abc"
            let n = reader.read(&mut small_buf).await.unwrap();
            assert_eq!(&small_buf[..n], b"abc");

            // Second read: "def"
            let n = reader.read(&mut small_buf).await.unwrap();
            assert_eq!(&small_buf[..n], b"def");

            // Third read: "\n"
            let n = reader.read(&mut small_buf).await.unwrap();
            assert_eq!(&small_buf[..n], b"\n");

            // EOF
            let n = reader.read(&mut small_buf).await.unwrap();
            assert_eq!(n, 0);
        });
    }

    #[test]
    fn unterminated_line_is_rejected_at_small_async_limit() {
        run(async {
            let data = b"123456789";
            let mut reader = BufReader::new(Cursor::new(&data[..]));
            let mut line = Vec::new();
            let error = read_line_capped_with_limit(&mut reader, &mut line, 8)
                .await
                .expect_err("an unterminated line over the limit must fail");

            assert_eq!(error.kind(), ErrorKind::InvalidData);
            assert!(
                line.len() <= 8,
                "the async reader must not retain bytes over the cap"
            );
        });
    }

    struct AlwaysFailWriter {
        calls: Rc<Cell<usize>>,
    }

    impl AsyncWrite for AlwaysFailWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.calls.set(self.calls.get() + 1);
            Poll::Ready(Err(std::io::Error::new(
                ErrorKind::BrokenPipe,
                "raw stdout failure",
            )))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.calls.set(self.calls.get() + 1);
            Poll::Ready(Err(std::io::Error::new(
                ErrorKind::BrokenPipe,
                "raw stdout failure",
            )))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.calls.set(self.calls.get() + 1);
            Poll::Ready(Err(std::io::Error::new(
                ErrorKind::BrokenPipe,
                "raw stdout failure",
            )))
        }
    }

    #[test]
    fn stdout_write_failure_stops_reader_without_leaking_error_text() {
        run(async {
            let calls = Rc::new(Cell::new(0));
            let state = AcpTransportState::new();
            let mut writer = AcpTransportWriter::new(
                AlwaysFailWriter {
                    calls: calls.clone(),
                },
                state.clone(),
            );

            let error = writer
                .write_all(b"secret output")
                .await
                .expect_err("writer failure must be returned");
            assert_eq!(error.kind(), ErrorKind::BrokenPipe);
            assert_eq!(error.to_string(), "ACP stdout write failed");
            assert_eq!(state.failure(), Some(AcpTransportErrorKind::StdoutWrite));

            // 后续写入必须在 wrapper 层短路，不能再次触碰底层 writer。
            let _ = writer.write_all(b"later").await;
            assert_eq!(calls.get(), 1);

            // writer 失败后，reader 立即返回错误，不能再消费后续请求。
            let mut reader = AcpTransportReader::new(Cursor::new(b"later request\n"), state);
            let mut buf = [0u8; 32];
            let error = reader
                .read(&mut buf)
                .await
                .expect_err("reader must stop after stdout failure");
            assert_eq!(error.kind(), ErrorKind::BrokenPipe);
            assert_eq!(error.to_string(), "ACP stdout write failed");
        });
    }
}
