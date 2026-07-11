//! Buffer pool / slab for encode scratch buffers.
//!
//! Blocks are fixed-capacity `Vec<u8>` (power-of-two sizes preferred). Acquired
//! buffers return to the free list on [`PooledBuf`] drop.

use std::sync::Arc;

use parking_lot::Mutex;

/// Shared free-list of fixed-capacity byte buffers.
#[derive(Debug)]
pub struct BufferPool {
    inner: Arc<PoolInner>,
}

#[derive(Debug)]
struct PoolInner {
    free: Mutex<Vec<Vec<u8>>>,
    block_size: usize,
    /// Soft cap on free-list length (initial pre-allocation size).
    max_blocks: usize,
}

/// A buffer checked out from a [`BufferPool`].
///
/// On drop the underlying `Vec` is cleared and returned to the pool (unless the
/// capacity was grown past `2 * block_size`, in which case it is discarded).
#[derive(Debug)]
pub struct PooledBuf {
    buf: Vec<u8>,
    pool: Option<Arc<PoolInner>>,
}

impl BufferPool {
    /// Create a pool pre-populated with `blocks` buffers of `block_size` capacity.
    ///
    /// `block_size` of 0 is treated as 64 KiB. Prefer power-of-two sizes for
    /// direct-I/O alignment (e.g. 64 KiB).
    pub fn with_capacity(blocks: usize, block_size: usize) -> Self {
        let block_size = if block_size == 0 {
            64 * 1024
        } else {
            block_size
        };
        let mut free = Vec::with_capacity(blocks);
        for _ in 0..blocks {
            free.push(Vec::with_capacity(block_size));
        }
        Self {
            inner: Arc::new(PoolInner {
                free: Mutex::new(free),
                block_size,
                max_blocks: blocks.max(1),
            }),
        }
    }

    /// Capacity of each pooled block in bytes.
    pub fn block_size(&self) -> usize {
        self.inner.block_size
    }

    /// Number of buffers currently on the free list.
    pub fn free_count(&self) -> usize {
        self.inner.free.lock().len()
    }

    /// Acquire a buffer. Returns to the pool when [`PooledBuf`] is dropped.
    pub fn acquire(&self) -> PooledBuf {
        let mut free = self.inner.free.lock();
        let mut buf = free.pop().unwrap_or_else(|| {
            Vec::with_capacity(self.inner.block_size)
        });
        buf.clear();
        // Ensure at least block_size capacity for typical encode use.
        if buf.capacity() < self.inner.block_size {
            buf.reserve(self.inner.block_size - buf.capacity());
        }
        PooledBuf {
            buf,
            pool: Some(Arc::clone(&self.inner)),
        }
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl PooledBuf {
    /// Mutable access to the underlying vector (for encode into).
    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    /// Immutable slice of written bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// Clear length to 0 without releasing capacity.
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// Current length in bytes.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Allocated capacity.
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Detach from the pool (buffer will not be returned on drop).
    pub fn into_vec(mut self) -> Vec<u8> {
        self.pool = None;
        std::mem::take(&mut self.buf)
    }
}

impl AsRef<[u8]> for PooledBuf {
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        let Some(pool) = self.pool.take() else {
            return;
        };
        let mut buf = std::mem::take(&mut self.buf);
        buf.clear();
        // Discard buffers that grew far beyond the slab size.
        let max_keep = pool.block_size.saturating_mul(2).max(pool.block_size);
        if buf.capacity() > max_keep {
            return;
        }
        let mut free = pool.free.lock();
        // Soft cap: avoid unbounded growth if many threads acquire at once.
        if free.len() < pool.max_blocks.saturating_mul(4).max(16) {
            free.push(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn acquire_release_returns_to_pool() {
        let pool = BufferPool::with_capacity(2, 1024);
        assert_eq!(pool.free_count(), 2);
        {
            let mut a = pool.acquire();
            assert_eq!(pool.free_count(), 1);
            a.as_mut_vec().extend_from_slice(b"hello");
            assert_eq!(a.as_slice(), b"hello");
        }
        assert_eq!(pool.free_count(), 2);
    }

    #[test]
    fn churn_no_leak() {
        let pool = BufferPool::with_capacity(4, 4096);
        for i in 0..10_000 {
            let mut b = pool.acquire();
            b.as_mut_vec().extend_from_slice(&[i as u8; 64]);
            assert!(!b.is_empty());
            drop(b);
        }
        // Free list should not be empty and not grow without bound.
        let free = pool.free_count();
        assert!(free >= 1, "expected buffers returned, free={free}");
        assert!(free <= 64, "free list unbounded: {free}");
    }

    #[test]
    fn concurrent_churn() {
        let pool = Arc::new(BufferPool::with_capacity(8, 2048));
        let mut handles = Vec::new();
        for t in 0..4 {
            let p = Arc::clone(&pool);
            handles.push(thread::spawn(move || {
                for i in 0..2000 {
                    let mut b = p.acquire();
                    let n = (t + i) as u64;
                    b.as_mut_vec().extend_from_slice(&n.to_le_bytes());
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(pool.free_count() >= 1);
    }

    #[test]
    fn into_vec_does_not_return() {
        let pool = BufferPool::with_capacity(1, 512);
        assert_eq!(pool.free_count(), 1);
        let b = pool.acquire();
        assert_eq!(pool.free_count(), 0);
        let v = b.into_vec();
        assert!(v.capacity() >= 512);
        assert_eq!(pool.free_count(), 0);
    }

    #[test]
    fn zero_block_size_defaults() {
        let pool = BufferPool::with_capacity(1, 0);
        assert_eq!(pool.block_size(), 64 * 1024);
    }
}
