use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{errors::Result, pool::Pool, ClientHandle};

/// Future that resolves to a `ClientHandle`.
pub struct GetHandle {
    pool: Pool,
    /// Tracks how many pending-open futures remain to be scanned in the
    /// current cycle.  Zero means "start a fresh cycle on next poll".
    pending_open_scan_remaining: usize,
    pending_open_scan_driver: bool,
}

impl GetHandle {
    pub(crate) fn new(pool: &Pool) -> Self {
        Self {
            pool: pool.clone(),
            pending_open_scan_remaining: 0,
            pending_open_scan_driver: false,
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
        )
    }
}

impl Drop for GetHandle {
    fn drop(&mut self) {
        if self.pending_open_scan_driver {
            self.pool.inner.handoff_pending_open_scan_driver();
        }
    }
}
