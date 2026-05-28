use std::{
    future::Future,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use crate::{
    errors::Result,
    pool::{Pool, TaskSlot, TASK_SLOT_RECRUITED},
    ClientHandle,
};

/// Future that resolves to a `ClientHandle`.
pub struct GetHandle {
    pool: Pool,
    /// Tracks how many pending-open futures remain to be scanned in the
    /// current cycle.  Zero means "start a fresh cycle on next poll".
    pending_open_scan_remaining: usize,
    pending_open_scan_driver: bool,
    /// Liveness slot for the current tasks-queue entry.  Marked stale on drop
    /// so that `recruit_one_scan_driver` skips this entry if it is still in the
    /// queue when this future is cancelled.
    park_slot: Option<Arc<TaskSlot>>,
    /// Whether this future has registered an unmet-demand waiter with the pool
    /// that has not yet been released.  Ensures the pool's `waiters` counter is
    /// balanced exactly once on completion or cancellation.
    demand_registered: bool,
}

impl GetHandle {
    pub(crate) fn new(pool: &Pool) -> Self {
        Self {
            pool: pool.clone(),
            pending_open_scan_remaining: 0,
            pending_open_scan_driver: false,
            park_slot: None,
            demand_registered: false,
        }
    }
}

impl Future for GetHandle {
    type Output = Result<ClientHandle>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.pool).poll(
            cx,
            &mut this.pending_open_scan_remaining,
            &mut this.pending_open_scan_driver,
            &mut this.park_slot,
            &mut this.demand_registered,
        )
    }
}

impl Drop for GetHandle {
    fn drop(&mut self) {
        // Release this future's unmet-demand registration so the pool does not
        // keep opening connections for a waiter that has gone away.
        if self.demand_registered {
            self.pool.inner.release_waiter();
            self.demand_registered = false;
        }
        // Mark any current tasks-queue entry stale so recruit_one_scan_driver
        // skips it.  Do this before handing off the scan driver so that the
        // recruited waiter does not accidentally pop our own (now-stale) entry.
        let dropped_recruited_waiter = match self.park_slot.take() {
            Some(slot) => slot.mark_stale() == TASK_SLOT_RECRUITED,
            None => false,
        };
        if self.pending_open_scan_driver {
            self.pool.inner.handoff_pending_open_scan_driver();
        } else if dropped_recruited_waiter
            && self
                .pool
                .inner
                .pending_open_scan_needed
                .load(Ordering::Acquire)
        {
            self.pool.inner.recruit_one_scan_driver();
        }
    }
}
