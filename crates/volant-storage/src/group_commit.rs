//! Time-based group-commit coordinator (shared fsync across callers).
//!
//! Waiters register a generation ticket, then either lead (sleep remaining
//! window or until `max_records`, `flush`, notify) or wait on the condvar.
//! No background thread: empty / no waiters does not spin.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use volant_core::{Error, Result};

/// Ticket for a waiter that must observe a later flush generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommitTicket {
    wait_gen: u64,
}

struct Inner {
    flush_gen: u64,
    pending_records: u64,
    pending_waiters: u64,
    window_start: Option<Instant>,
    has_leader: bool,
    /// `(flush_gen, message)` of the last failed flush, if any.
    last_error: Option<(u64, String)>,
}

/// Shared group-commit coordinator (generation counter + condvar).
pub struct GroupCommit {
    state: Mutex<Inner>,
    cond: Condvar,
    max_ms: u64,
    max_records: u64,
    flushes: AtomicU64,
    records: AtomicU64,
}

impl std::fmt::Debug for GroupCommit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupCommit")
            .field("max_ms", &self.max_ms)
            .field("max_records", &self.max_records)
            .field("flushes", &self.flushes.load(Ordering::Relaxed))
            .field("records", &self.records.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl GroupCommit {
    /// Create a coordinator. `max_ms == 0` means group-commit is off.
    pub fn new(max_ms: u64, max_records: u64) -> Self {
        Self {
            state: Mutex::new(Inner {
                flush_gen: 0,
                pending_records: 0,
                pending_waiters: 0,
                window_start: None,
                has_leader: false,
                last_error: None,
            }),
            cond: Condvar::new(),
            max_ms,
            max_records: max_records.max(1),
            flushes: AtomicU64::new(0),
            records: AtomicU64::new(0),
        }
    }

    /// Whether the time window is enabled.
    pub fn enabled(&self) -> bool {
        self.max_ms > 0
    }

    /// Accumulate records written since the last durable flush.
    pub fn add_pending(&self, n: u64) {
        if n == 0 {
            return;
        }
        let mut g = self.state.lock();
        g.pending_records = g.pending_records.saturating_add(n);
        if g.pending_records >= self.max_records {
            self.cond.notify_all();
        }
    }

    /// Unflushed record count tracked by the coordinator.
    pub fn pending_records(&self) -> u64 {
        self.state.lock().pending_records
    }

    /// Current waiter count (test / metrics hook).
    pub fn pending_waiters(&self) -> u64 {
        self.state.lock().pending_waiters
    }

    /// Register as a waiter for the next flush generation.
    ///
    /// The first waiter opens the time window.
    pub fn register_waiter(&self) -> GroupCommitTicket {
        let mut g = self.state.lock();
        if g.pending_waiters == 0 {
            g.window_start = Some(Instant::now());
        }
        g.pending_waiters = g.pending_waiters.saturating_add(1);
        GroupCommitTicket {
            wait_gen: g.flush_gen,
        }
    }

    /// Wait until `flush_gen` advances past `ticket`, or lead the flush.
    ///
    /// `flush_fn` is invoked **without** the coordinator lock held. Followers
    /// re-check after every wake so a waiter that arrives mid-flush can become
    /// the next leader instead of blocking forever.
    pub fn wait_or_lead<F>(&self, ticket: GroupCommitTicket, flush_fn: F) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        let mut flush_fn = Some(flush_fn);
        let mut g = Some(self.state.lock());
        loop {
            let guard = g.as_mut().expect("group-commit lock");
            if let Some(err) = already_done(guard, ticket) {
                guard.pending_waiters = guard.pending_waiters.saturating_sub(1);
                return err;
            }

            if !guard.has_leader {
                if flush_fn.is_none() {
                    self.cond.wait(guard);
                    continue;
                }
                guard.has_leader = true;
                loop {
                    let guard = g.as_mut().expect("group-commit lock");
                    if let Some(err) = already_done(guard, ticket) {
                        guard.has_leader = false;
                        guard.pending_waiters = guard.pending_waiters.saturating_sub(1);
                        self.cond.notify_all();
                        return err;
                    }
                    if self.should_flush_now(guard) {
                        break;
                    }
                    let remaining = remaining_window(guard, self.max_ms);
                    if remaining.is_zero() {
                        break;
                    }
                    self.cond.wait_for(guard, remaining);
                }
                drop(g.take());
                let result = flush_fn.take().expect("leader flushes at most once")();
                let mut guard = self.state.lock();
                guard.has_leader = false;
                self.cond.notify_all();
                if let Err(e) = result {
                    guard.flush_gen = guard.flush_gen.wrapping_add(1);
                    guard.last_error = Some((guard.flush_gen, e.to_string()));
                    guard.window_start = None;
                    guard.pending_waiters = guard.pending_waiters.saturating_sub(1);
                    return Err(e);
                }
                g = Some(guard);
                continue;
            }

            self.cond.wait(guard);
        }
    }

    fn should_flush_now(&self, g: &Inner) -> bool {
        g.pending_records >= self.max_records || remaining_window(g, self.max_ms).is_zero()
    }

    /// Called after a successful `fsync`. Wakes all waiters.
    pub fn notify_flushed(&self, records: u64) {
        let mut g = self.state.lock();
        g.flush_gen = g.flush_gen.wrapping_add(1);
        let n = if records > 0 {
            records
        } else {
            g.pending_records
        };
        g.pending_records = 0;
        g.window_start = None;
        g.last_error = None;
        if self.max_ms > 0 {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            if n > 0 {
                self.records.fetch_add(n, Ordering::Relaxed);
            }
        }
        self.cond.notify_all();
    }

    /// Successful group-commit flush count (0 when disabled).
    pub fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::Relaxed)
    }

    /// Records covered by those flushes.
    pub fn records(&self) -> u64 {
        self.records.load(Ordering::Relaxed)
    }
}

fn remaining_window(g: &Inner, max_ms: u64) -> Duration {
    if max_ms == 0 {
        return Duration::ZERO;
    }
    let start = match g.window_start {
        Some(s) => s,
        None => return Duration::ZERO,
    };
    let window = Duration::from_millis(max_ms);
    window.saturating_sub(start.elapsed())
}

fn already_done(g: &Inner, ticket: GroupCommitTicket) -> Option<Result<()>> {
    if g.flush_gen > ticket.wait_gen {
        Some(flush_error_for(g, ticket))
    } else {
        None
    }
}

fn flush_error_for(g: &Inner, ticket: GroupCommitTicket) -> Result<()> {
    if let Some((gen, msg)) = &g.last_error {
        if *gen > ticket.wait_gen {
            return Err(Error::Storage(format!("group-commit flush failed: {msg}")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn no_waiters_does_not_flush() {
        let gc = GroupCommit::new(50, 64);
        assert_eq!(gc.pending_waiters(), 0);
        assert_eq!(gc.flushes(), 0);
        assert_eq!(gc.pending_records(), 0);
    }

    #[test]
    fn two_waiters_share_one_flush() {
        let gc = Arc::new(GroupCommit::new(40, 64));
        let flushes = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(2));

        let h = {
            let gc = Arc::clone(&gc);
            let flushes = Arc::clone(&flushes);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                gc.add_pending(1);
                let ticket = gc.register_waiter();
                barrier.wait();
                gc.wait_or_lead(ticket, || {
                    flushes.fetch_add(1, Ordering::SeqCst);
                    gc.notify_flushed(1);
                    Ok(())
                })
            })
        };

        gc.add_pending(1);
        let ticket = gc.register_waiter();
        barrier.wait();
        gc.wait_or_lead(ticket, || {
            flushes.fetch_add(1, Ordering::SeqCst);
            gc.notify_flushed(1);
            Ok(())
        })
        .unwrap();
        h.join().unwrap().unwrap();

        let n = flushes.load(Ordering::SeqCst);
        assert!(n >= 1 && n <= 2, "fsyncs={n}");
        assert_eq!(gc.pending_waiters(), 0);
    }
}
