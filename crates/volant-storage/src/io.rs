//! Pluggable I/O backends for segment append / fsync.
//!
//! Default path uses [`StdIoBackend`] (portable `pwrite` / seek+write).
//! Optional Linux `io_uring` backend is gated behind the `io-uring` feature.

use std::fs::File;

use volant_core::{Error, Result};

/// Which I/O backend the storage engine should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IoBackendKind {
    /// Portable std / pread-pwrite style I/O (always available).
    #[default]
    Std,
    /// Linux `io_uring` (requires `io-uring` feature; falls back to Std otherwise).
    IoUring,
}

/// Low-level file write / sync operations used by the append path.
///
/// `Send + Sync` so backends can live behind `Arc` (e.g. shared broker state
/// and `tokio::spawn` connection handlers).
pub trait IoBackend: Send + Sync {
    /// Write the entire `buf` at absolute `offset` (does not change the file cursor
    /// in a way callers rely on; implementations may seek).
    fn write_all_at(&mut self, file: &File, offset: u64, buf: &[u8]) -> Result<()>;

    /// Durable sync of file data + metadata (`fsync` / `File::sync_all`).
    fn fsync(&mut self, file: &File) -> Result<()>;

    /// Human-readable backend name (for logs / diagnostics).
    fn name(&self) -> &'static str {
        "unknown"
    }
}

/// Portable backend using `write_at` on Unix and seek+write elsewhere.
#[derive(Debug, Default)]
pub struct StdIoBackend;

impl IoBackend for StdIoBackend {
    fn write_all_at(&mut self, file: &File, offset: u64, buf: &[u8]) -> Result<()> {
        write_all_at_std(file, offset, buf)
    }

    fn fsync(&mut self, file: &File) -> Result<()> {
        file.sync_all().map_err(Error::from)
    }

    fn name(&self) -> &'static str {
        "std"
    }
}

fn write_all_at_std(file: &File, offset: u64, buf: &[u8]) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        let mut written = 0usize;
        while written < buf.len() {
            match file.write_at(&buf[written..], offset + written as u64) {
                Ok(0) => {
                    return Err(Error::Storage(
                        "write_at returned 0 bytes before completing".into(),
                    ));
                }
                Ok(n) => written += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Error::from(e)),
            }
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};
        // Windows / others: seek + write via cloned handle.
        let mut f = file.try_clone().map_err(Error::from)?;
        f.seek(SeekFrom::Start(offset)).map_err(Error::from)?;
        f.write_all(buf).map_err(Error::from)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// io_uring feature gate
// ---------------------------------------------------------------------------

#[cfg(all(feature = "io-uring", not(target_os = "linux")))]
compile_error!(
    "feature \"io-uring\" is only supported on Linux (target_os = \"linux\")"
);

/// Linux `io_uring` backend with synchronous submit+wait (Phase 5).
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub struct UringIoBackend {
    ring: io_uring::IoUring,
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl std::fmt::Debug for UringIoBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringIoBackend").finish_non_exhaustive()
    }
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl UringIoBackend {
    /// Create a ring with a small fixed SQ/CQ depth.
    pub fn new() -> Result<Self> {
        let ring = io_uring::IoUring::new(64).map_err(|e| {
            Error::Storage(format!("io_uring init failed: {e}"))
        })?;
        Ok(Self { ring })
    }
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl IoBackend for UringIoBackend {
    fn write_all_at(&mut self, file: &File, offset: u64, buf: &[u8]) -> Result<()> {
        use io_uring::{opcode, types};
        use std::os::unix::io::AsRawFd;

        if buf.is_empty() {
            return Ok(());
        }

        let fd = types::Fd(file.as_raw_fd());
        // Single-shot write; for large buffers kernel may short-write — loop.
        let mut done = 0usize;
        while done < buf.len() {
            let slice = &buf[done..];
            let write_e = opcode::Write::new(fd, slice.as_ptr(), slice.len() as u32)
                .offset(offset + done as u64)
                .build()
                .user_data(0x57); // 'W'

            // SAFETY: slice lives until wait completes below (sync submit+wait).
            unsafe {
                self.ring
                    .submission()
                    .push(&write_e)
                    .map_err(|e| Error::Storage(format!("io_uring sq push: {e}")))?;
            }
            self.ring
                .submit_and_wait(1)
                .map_err(|e| Error::Storage(format!("io_uring submit: {e}")))?;

            let cqe = self
                .ring
                .completion()
                .next()
                .ok_or_else(|| Error::Storage("io_uring: no CQE after wait".into()))?;
            let result = cqe.result();
            if result < 0 {
                return Err(Error::Storage(format!(
                    "io_uring write failed: errno {}",
                    -result
                )));
            }
            if result == 0 {
                return Err(Error::Storage(
                    "io_uring write returned 0 bytes".into(),
                ));
            }
            done += result as usize;
        }
        Ok(())
    }

    fn fsync(&mut self, file: &File) -> Result<()> {
        use io_uring::{opcode, types};
        use std::os::unix::io::AsRawFd;

        let fd = types::Fd(file.as_raw_fd());
        let sync_e = opcode::Fsync::new(fd).build().user_data(0x46); // 'F'

        unsafe {
            self.ring
                .submission()
                .push(&sync_e)
                .map_err(|e| Error::Storage(format!("io_uring sq push: {e}")))?;
        }
        self.ring
            .submit_and_wait(1)
            .map_err(|e| Error::Storage(format!("io_uring submit: {e}")))?;

        let cqe = self
            .ring
            .completion()
            .next()
            .ok_or_else(|| Error::Storage("io_uring: no CQE after fsync wait".into()))?;
        let result = cqe.result();
        if result < 0 {
            return Err(Error::Storage(format!(
                "io_uring fsync failed: errno {}",
                -result
            )));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "io_uring"
    }
}

/// Build an I/O backend for the given kind.
///
/// When `IoUring` is requested but the `io-uring` feature is off (or the
/// platform is not Linux), falls back to [`StdIoBackend`].
pub fn create_io_backend(kind: IoBackendKind) -> Result<Box<dyn IoBackend>> {
    match kind {
        IoBackendKind::Std => Ok(Box::new(StdIoBackend)),
        IoBackendKind::IoUring => {
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            {
                return Ok(Box::new(UringIoBackend::new()?));
            }
            #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
            {
                tracing::warn!(
                    "IoBackendKind::IoUring requested but unavailable; using StdIoBackend"
                );
                Ok(Box::new(StdIoBackend))
            }
        }
    }
}

/// Alignment required for `O_DIRECT` writes (4 KiB).
pub const DIRECT_IO_ALIGN: usize = 4096;

/// Whether the `direct-io` feature is compiled in.
pub fn direct_io_feature_enabled() -> bool {
    cfg!(feature = "direct-io")
}

/// Whether the `io-uring` feature is compiled in (Linux only is usable).
pub fn io_uring_feature_enabled() -> bool {
    cfg!(all(feature = "io-uring", target_os = "linux"))
}

/// Open options helper: apply `O_DIRECT` when feature + config request it.
///
/// Returns `true` if the flag was applied. On non-Linux or without the feature,
/// returns `false` (safe fallback — caller opens normally).
pub fn apply_direct_io_flag(opts: &mut std::fs::OpenOptions, want_direct: bool) -> bool {
    if !want_direct {
        return false;
    }
    if !cfg!(feature = "direct-io") {
        tracing::debug!("direct_io requested but `direct-io` feature disabled; ignoring");
        return false;
    }
    #[cfg(all(feature = "direct-io", target_os = "linux"))]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // O_DIRECT = 0x4000 on Linux (libc::O_DIRECT).
        opts.custom_flags(libc::O_DIRECT);
        return true;
    }
    #[cfg(not(all(feature = "direct-io", target_os = "linux")))]
    {
        let _ = opts;
        tracing::debug!("direct_io not supported on this platform; using buffered I/O");
        false
    }
}

/// Round `n` up to a multiple of `align` (align must be non-zero power of two preferred).
pub fn align_up(n: usize, align: usize) -> usize {
    if align == 0 {
        return n;
    }
    n.div_ceil(align) * align
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_file() -> (std::path::PathBuf, File) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "volant-io-{}-{}-{}",
            std::process::id(),
            n,
            nanos
        ));
        let f = File::create(&path).unwrap();
        (path, f)
    }

    #[test]
    fn std_backend_write_all_at_roundtrip() {
        let (path, file) = tmp_file();
        let mut backend = StdIoBackend;
        backend.write_all_at(&file, 0, b"hello").unwrap();
        backend.write_all_at(&file, 5, b" world").unwrap();
        backend.fsync(&file).unwrap();
        drop(file);

        let mut f = File::open(&path).unwrap();
        let mut buf = String::new();
        f.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "hello world");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn create_backend_std() {
        let b = create_io_backend(IoBackendKind::Std).unwrap();
        assert_eq!(b.name(), "std");
    }

    #[test]
    fn create_backend_uring_falls_back_without_feature() {
        // On macOS / without feature this must not fail.
        let b = create_io_backend(IoBackendKind::IoUring).unwrap();
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        assert_eq!(b.name(), "io_uring");
        #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
        assert_eq!(b.name(), "std");
    }

    #[test]
    fn align_up_works() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
    }

    #[test]
    fn apply_direct_io_flag_noop_without_request() {
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true).write(true).create(true);
        assert!(!apply_direct_io_flag(&mut opts, false));
    }
}
