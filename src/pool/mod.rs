use std::{
    fmt,
    io::ErrorKind,
    mem,
    pin::Pin,
    sync::{
        atomic::{self, Ordering},
        Arc, Mutex, Weak,
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use futures_util::future::BoxFuture;
use log::{error, warn};

use crate::{
    errors::{Error, Result},
    types::{IntoOptions, OptionsSource},
    Client, ClientHandle,
};

pub use self::futures::GetHandle;
use futures_util::FutureExt;
use url::Url;

mod futures;

const PENDING_OPEN_POLL_BUDGET: usize = 4;

struct PendingOpenWake {
    inner: Weak<Inner>,
}

impl Wake for PendingOpenWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(inner) = self.inner.upgrade() {
            inner
                .pending_open_scan_needed
                .store(true, Ordering::Release);
            let driver_waker = inner.pending_open_scan_driver_waker.lock().unwrap().clone();
            if let Some(waker) = driver_waker {
                waker.wake();
            } else {
                // No active driver: recruit exactly one parked waiter instead of
                // waking all of them.  Only one waiter can acquire the scan-driver
                // role, so waking everyone is needless amplification.
                inner.recruit_one_scan_driver();
            }
        }
    }
}

const TASK_SLOT_LIVE: u8 = 0;
pub(crate) const TASK_SLOT_RECRUITED: u8 = 1;
const TASK_SLOT_STALE: u8 = 2;

/// A parked waiter's slot in `Inner.tasks`.  The state tracks whether the
/// owning `GetHandle` is still live, has been recruited to drive a pending-open
/// scan, or has gone stale because the future was dropped or resumed.
pub(crate) struct TaskSlot {
    state: atomic::AtomicU8,
}

impl TaskSlot {
    fn live() -> Arc<Self> {
        Arc::new(Self {
            state: atomic::AtomicU8::new(TASK_SLOT_LIVE),
        })
    }

    #[cfg(test)]
    fn stale() -> Arc<Self> {
        Arc::new(Self {
            state: atomic::AtomicU8::new(TASK_SLOT_STALE),
        })
    }

    fn is_active(&self) -> bool {
        self.state.load(Ordering::Acquire) != TASK_SLOT_STALE
    }

    fn try_recruit(&self) -> bool {
        self.state
            .compare_exchange(
                TASK_SLOT_LIVE,
                TASK_SLOT_RECRUITED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(crate) fn mark_stale(&self) -> u8 {
        self.state.swap(TASK_SLOT_STALE, Ordering::AcqRel)
    }
}

type TaskEntry = (Arc<TaskSlot>, Waker);

pub(crate) struct Inner {
    new: crossbeam::queue::ArrayQueue<BoxFuture<'static, Result<ClientHandle>>>,
    idle: crossbeam::queue::ArrayQueue<ClientHandle>,
    tasks: crossbeam::queue::SegQueue<TaskEntry>,
    ongoing: atomic::AtomicUsize,
    conn_slots: atomic::AtomicUsize,
    pending_open_scan_driver: atomic::AtomicBool,
    pending_open_scan_driver_waker: Mutex<Option<Waker>>,
    /// No longer wrapped in `Arc`: `Inner` itself is already behind `Arc<Inner>`,
    /// so the outer `Arc` was redundant.
    pending_open_scan_needed: atomic::AtomicBool,
    hosts: Vec<Url>,
    connections_num: atomic::AtomicUsize,
}

impl Inner {
    pub(crate) fn release_conn(&self) {
        if self.ongoing.load(Ordering::Acquire) == 0 {
            warn!("release_conn called when no connections are ongoing");
            return;
        }
        self.ongoing.fetch_sub(1, Ordering::AcqRel);
        self.release_conn_slot();
        self.wake_tasks();
    }

    fn conn_count(&self) -> usize {
        self.conn_slots.load(Ordering::Acquire)
    }

    fn try_reserve_conn(&self, max: usize) -> bool {
        let mut current = self.conn_count();

        while current < max {
            match self.conn_slots.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }

        false
    }

    fn release_conn_slot(&self) {
        if self
            .conn_slots
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_sub(1)
            })
            .is_err()
        {
            warn!("release_conn_slot called when no connections are reserved");
        }
    }

    pub(crate) fn release_pending_open_scan_driver(&self) {
        {
            let mut driver_waker = self.pending_open_scan_driver_waker.lock().unwrap();
            self.pending_open_scan_driver
                .store(false, Ordering::Release);
            *driver_waker = None;
        }

        if self.pending_open_scan_needed.load(Ordering::Acquire) {
            // Recruit exactly one parked waiter to take over the scan-driver
            // role.  wake_tasks (wake-all) is intentionally NOT used here: we
            // only need a single new driver, not every parked waiter.
            self.recruit_one_scan_driver();
        }
    }

    pub(crate) fn handoff_pending_open_scan_driver(&self) {
        self.pending_open_scan_needed.store(true, Ordering::Release);
        self.release_pending_open_scan_driver();
    }

    fn try_acquire_pending_open_scan_driver(&self) -> bool {
        self.pending_open_scan_driver
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn set_pending_open_scan_driver_waker(&self, waker: &Waker) {
        *self.pending_open_scan_driver_waker.lock().unwrap() = Some(waker.clone());
    }

    fn wake_tasks(&self) {
        while let Some((slot, waker)) = self.tasks.pop() {
            if slot.is_active() {
                waker.wake();
            }
            // Stale entries are silently drained.
        }
    }

    /// Wake exactly one live parked waiter and discard all stale entries
    /// encountered before it.  Used to recruit a new pending-open scan driver
    /// without amplifying wakes to every parked waiter.
    pub(crate) fn recruit_one_scan_driver(&self) {
        while let Some((slot, waker)) = self.tasks.pop() {
            if slot.try_recruit() {
                waker.wake();
                return;
            }
            // Stale entry — drain it and keep searching.
        }
    }
}

#[derive(Clone)]
pub(crate) enum PoolBinding {
    None,
    Attached(Pool),
    Detached(Pool),
}

impl From<PoolBinding> for Option<Pool> {
    fn from(binding: PoolBinding) -> Self {
        match binding {
            PoolBinding::None => None,
            PoolBinding::Attached(pool) | PoolBinding::Detached(pool) => Some(pool),
        }
    }
}

impl PoolBinding {
    pub(crate) fn take(&mut self) -> Self {
        mem::replace(self, PoolBinding::None)
    }

    fn return_conn(self, client: ClientHandle) {
        if let Some(mut pool) = self.into() {
            Pool::return_conn(&mut pool, client);
        }
    }

    pub(crate) fn is_attached(&self) -> bool {
        matches!(self, PoolBinding::Attached(_))
    }

    pub(crate) fn is_some(&self) -> bool {
        !matches!(self, PoolBinding::None)
    }

    pub(crate) fn attach(&mut self) {
        match self.take() {
            PoolBinding::Detached(pool) => *self = PoolBinding::Attached(pool),
            _ => unreachable!(),
        }
    }

    pub(crate) fn detach(&mut self) {
        match self.take() {
            PoolBinding::Attached(pool) => *self = PoolBinding::Detached(pool),
            _ => unreachable!(),
        }
    }
}

/// Asynchronous pool of Clickhouse connections.
#[derive(Clone)]
pub struct Pool {
    options: OptionsSource,
    pub(crate) inner: Arc<Inner>,
    min: usize,
    max: usize,
}

#[derive(Debug)]
pub struct PoolInfo {
    pub new_len: usize,
    pub idle_len: usize,
    pub tasks_len: usize,
    pub ongoing: usize,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let info = self.info();
        f.debug_struct("Pool")
            .field("min", &self.min)
            .field("max", &self.max)
            .field("new connections count", &info.new_len)
            .field("idle connections count", &info.idle_len)
            .field("tasks count", &info.tasks_len)
            .field("ongoing connections count", &info.ongoing)
            .finish()
    }
}

impl Pool {
    /// Constructs a new Pool.
    pub fn new<O>(options: O) -> Self
    where
        O: IntoOptions,
    {
        let options_src = options.into_options_src();

        let mut min = 5;
        let mut max = 10;
        let mut hosts = vec![];

        match options_src.get() {
            Ok(opt) => {
                min = opt.pool_min;
                max = opt.pool_max;
                hosts.push(opt.addr.clone());
                hosts.extend(opt.alt_hosts.iter().cloned());
            }
            Err(err) => error!("{}", err),
        }

        for host in &hosts {
            if host.port() == Some(8123) {
                warn!(
                    "The attempt to establish a connection through the text protocol. clickhouse-rs is for using the binary protocol."
                );
                break;
            }
        }

        let inner = Arc::new(Inner {
            new: crossbeam::queue::ArrayQueue::new(max),
            idle: crossbeam::queue::ArrayQueue::new(max),
            tasks: crossbeam::queue::SegQueue::new(),
            ongoing: atomic::AtomicUsize::new(0),
            conn_slots: atomic::AtomicUsize::new(0),
            pending_open_scan_driver: atomic::AtomicBool::new(false),
            pending_open_scan_driver_waker: Mutex::new(None),
            pending_open_scan_needed: atomic::AtomicBool::new(true),
            connections_num: atomic::AtomicUsize::new(0),
            hosts,
        });

        Self {
            options: options_src,
            inner,
            min,
            max,
        }
    }

    pub fn info(&self) -> PoolInfo {
        PoolInfo {
            new_len: self.inner.new.len(),
            idle_len: self.inner.idle.len(),
            tasks_len: self.inner.tasks.len(),
            ongoing: self.inner.ongoing.load(Ordering::Acquire),
        }
    }

    /// Returns future that resolves to `ClientHandle`.
    pub fn get_handle(&self) -> GetHandle {
        GetHandle::new(self)
    }

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        scan_remaining: &mut usize,
        scan_driver: &mut bool,
        park_slot: &mut Option<Arc<TaskSlot>>,
    ) -> Poll<Result<ClientHandle>> {
        if !*scan_driver
            && self.inner.pending_open_scan_needed.load(Ordering::Acquire)
            && self.inner.try_acquire_pending_open_scan_driver()
        {
            *scan_driver = true;
            if let Some(slot) = park_slot.take() {
                slot.mark_stale();
            }
        }
        if *scan_driver {
            self.inner.set_pending_open_scan_driver_waker(cx.waker());
        }

        let handle_futures_result = if *scan_driver {
            self.handle_futures(cx, scan_remaining)
        } else {
            Ok(())
        };

        let scan_complete = *scan_driver && *scan_remaining == 0;
        let pool_info = if handle_futures_result.is_err() {
            Some(self.info())
        } else {
            None
        };

        if let Some(client) = self.take_conn() {
            if *scan_driver {
                if scan_complete {
                    self.inner.release_pending_open_scan_driver();
                } else {
                    self.inner.handoff_pending_open_scan_driver();
                }
                *scan_driver = false;
            }
            if let Err(err) = handle_futures_result {
                warn!(
                    "Pending ClickHouse connection failed while idle connections were available; using idle connection instead. error={}; pool_info={:?}",
                    err,
                    pool_info.unwrap_or_else(|| self.info())
                );
            }
            return Poll::Ready(Ok(client));
        }

        if let Err(err) = handle_futures_result {
            if scan_complete {
                self.inner.release_pending_open_scan_driver();
                *scan_driver = false;
            }
            return Poll::Ready(Err(err));
        }

        if self.inner.try_reserve_conn(self.max) {
            match self.inner.new.push(self.new_connection()) {
                Ok(()) => {
                    self.inner
                        .pending_open_scan_needed
                        .store(true, Ordering::Release);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Err(_) => self.inner.release_conn_slot(),
            }
        }

        if scan_complete {
            if self.inner.pending_open_scan_needed.load(Ordering::Acquire) {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            // Park with a live slot so that recruit_one_scan_driver can find
            // us when the next pending-open wake arrives.
            let slot = TaskSlot::live();
            // Invalidate any previous slot before registering a new one.
            if let Some(old) = park_slot.take() {
                old.mark_stale();
            }
            self.inner.tasks.push((slot.clone(), cx.waker().clone()));
            *park_slot = Some(slot);
            self.inner.release_pending_open_scan_driver();
            *scan_driver = false;
            return Poll::Pending;
        }

        if *scan_remaining == 0 {
            self.park_pending_open_waiter(cx, scan_driver, park_slot);
        }
        Poll::Pending
    }

    fn park_pending_open_waiter(
        &self,
        cx: &mut Context<'_>,
        scan_driver: &mut bool,
        park_slot: &mut Option<Arc<TaskSlot>>,
    ) {
        // Invalidate any previous slot to avoid stale-but-alive entries in the
        // queue that could misdirect a future recruit_one_scan_driver call.
        if let Some(old) = park_slot.take() {
            old.mark_stale();
        }
        let slot = TaskSlot::live();
        self.inner.tasks.push((slot.clone(), cx.waker().clone()));
        *park_slot = Some(slot);

        if !*scan_driver
            && self.inner.pending_open_scan_needed.load(Ordering::Acquire)
            && self.inner.try_acquire_pending_open_scan_driver()
        {
            if let Some(slot) = park_slot.take() {
                slot.mark_stale();
            }
            *scan_driver = true;
            self.inner.set_pending_open_scan_driver_waker(cx.waker());
            cx.waker().wake_by_ref();
        }
    }

    fn new_connection(&self) -> BoxFuture<'static, Result<ClientHandle>> {
        let source = self.options.clone();
        let pool = Some(self.clone());

        let (max_attempts, retry_timeout) = {
            match source.get() {
                Ok(opt) => (opt.send_retries, opt.retry_timeout),
                Err(_) => (0usize, Duration::from_secs(0)),
            }
        };

        Box::pin(async move { Self::retry_open(source, pool, max_attempts, retry_timeout).await })
    }

    async fn retry_open(
        source: OptionsSource,
        pool: Option<Pool>,
        max_attempts: usize,
        retry_timeout: Duration,
    ) -> Result<ClientHandle> {
        let mut attempt = 0;

        loop {
            let result = Client::open(source.clone(), pool.clone()).await;

            match result {
                Err(Error::Io(ref err)) => {
                    if err.kind() == ErrorKind::BrokenPipe
                        || err.kind() == ErrorKind::ConnectionRefused
                    {
                        if attempt >= max_attempts {
                            error!(
                                "Failed to connect to ClickHouse after {} attempts: {}",
                                attempt, err
                            );

                            return result;
                        }

                        attempt += 1;

                        warn!(
                            "Failed to connect to ClickHouse: {}. Retrying {}/{}...",
                            err, attempt, max_attempts
                        );

                        #[cfg(feature = "async_std")]
                        {
                            use async_std::task;
                            task::sleep(retry_timeout).await;
                        }

                        #[cfg(not(feature = "async_std"))]
                        {
                            tokio::time::sleep(retry_timeout).await;
                        }
                    } else {
                        error!(
                            "Failed to connect to ClickHouse due to non-retriable IO error: {}",
                            err
                        );
                    }
                }
                Err(_) => return result,
                Ok(handle) => {
                    if attempt > 0 {
                        warn!(
                            "Successfully connected to ClickHouse after {} retry attempts",
                            attempt
                        );
                    }

                    return Ok(handle);
                }
            }
        }
    }

    fn handle_futures(&mut self, cx: &mut Context<'_>, scan_remaining: &mut usize) -> Result<()> {
        // Begin a fresh scan cycle when the previous one has completed or was
        // never started.  The cycle length is the queue depth at this moment;
        // futures added after the cycle starts are left for the next cycle.
        if *scan_remaining == 0 {
            self.inner
                .pending_open_scan_needed
                .store(false, Ordering::Release);
            *scan_remaining = self.inner.new.len();
        } else {
            *scan_remaining = (*scan_remaining).min(self.inner.new.len());
        }

        let to_poll = (*scan_remaining).min(PENDING_OPEN_POLL_BUDGET);
        let pending_waker = Waker::from(Arc::new(PendingOpenWake {
            inner: Arc::downgrade(&self.inner),
        }));
        let mut pending_cx = Context::from_waker(&pending_waker);
        let mut first_err = None;
        let mut wake_waiters = false;
        let mut state_changed = false;
        let mut polled = 0;
        let mut queue_exhausted = false;
        let cycle_remaining = *scan_remaining;

        for _ in 0..to_poll {
            let Some(mut new) = self.inner.new.pop() else {
                // Queue was drained by another driver between our cycle-start
                // snapshot and this iteration.  Treat the cycle as complete so
                // a stale scan_remaining does not trigger a self-wake on an
                // empty queue.
                queue_exhausted = true;
                break;
            };
            polled += 1;

            match new.poll_unpin(&mut pending_cx) {
                Poll::Ready(Ok(client)) => {
                    if self.inner.idle.push(client).is_err() {
                        self.inner.release_conn_slot();
                        warn!(
                            "Ready ClickHouse connection could not be added to idle pool; closing it. pool_info={:?}",
                            self.info()
                        );
                    }
                    wake_waiters = true;
                    state_changed = true;
                }
                Poll::Pending => {
                    if self.inner.new.push(new).is_err() {
                        self.inner.release_conn_slot();
                        wake_waiters = true;
                        state_changed = true;
                    }
                }
                Poll::Ready(Err(err)) => {
                    self.inner.release_conn_slot();
                    wake_waiters = true;
                    state_changed = true;
                    if first_err.is_none() {
                        first_err = Some(err);
                    } else {
                        warn!(
                            "Additional pending ClickHouse connection failed while another pending connection error is being returned. error={}",
                            err
                        );
                    }
                }
            }
        }

        let unscanned_in_cycle = polled < cycle_remaining && !queue_exhausted;

        if state_changed || queue_exhausted {
            // A connection resolved/dropped, or the queue was drained by
            // another driver: reset so the next poll starts a fresh cycle.
            // wake_tasks() (when state_changed) handles waiter continuation.
            *scan_remaining = 0;
            if queue_exhausted {
                self.inner
                    .pending_open_scan_needed
                    .store(false, Ordering::Release);
            } else if unscanned_in_cycle {
                self.inner
                    .pending_open_scan_needed
                    .store(true, Ordering::Release);
            }
        } else {
            *scan_remaining = scan_remaining.saturating_sub(polled);
            if *scan_remaining == 0
                && self
                    .inner
                    .pending_open_scan_needed
                    .swap(false, Ordering::AcqRel)
            {
                // A pending-open future woke during this scan cycle. That wake
                // may have been coalesced with our continuation wake, so run a
                // fresh bounded cycle instead of parking and missing it.
                *scan_remaining = self.inner.new.len();
            }
        }

        if wake_waiters {
            self.inner.wake_tasks();
        } else if *scan_remaining > 0 {
            // Budget exhausted mid-cycle with no state change: self-wake once
            // so we continue scanning without requiring an external event.
            cx.waker().wake_by_ref();
        }

        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn take_conn(&mut self) -> Option<ClientHandle> {
        if let Some(mut client) = self.inner.idle.pop() {
            client.pool = PoolBinding::Attached(self.clone());
            client.set_inside(false);
            client.set_used();
            self.inner.ongoing.fetch_add(1, Ordering::AcqRel);
            Some(client)
        } else {
            None
        }
    }

    fn return_conn(&mut self, mut client: ClientHandle) {
        let min = self.min;

        let is_attached = client.pool.is_attached();
        client.pool = PoolBinding::None;
        client.set_inside(true);

        if self.inner.ongoing.load(Ordering::Acquire) == 0 {
            warn!("return_conn called when no connections are ongoing");
            return;
        }

        let returned_to_idle = if self.inner.idle.len() < min
            && is_attached
            && client.inner.is_some()
        {
            match self.inner.idle.push(client) {
                Ok(()) => true,
                Err(_) => {
                    warn!(
                        "Returned ClickHouse connection could not be added to idle pool; closing it. pool_info={:?}",
                        self.info()
                    );
                    false
                }
            }
        } else {
            false
        };

        self.inner.ongoing.fetch_sub(1, Ordering::AcqRel);
        if !returned_to_idle {
            self.inner.release_conn_slot();
        }

        self.inner.wake_tasks();
    }

    pub(crate) fn get_addr(&self) -> &Url {
        let n = self.inner.hosts.len();
        let index = self.inner.connections_num.fetch_add(1, Ordering::SeqCst);
        &self.inner.hosts[index % n]
    }
}

impl Drop for ClientHandle {
    fn drop(&mut self) {
        if let (pool, Some(inner)) = (self.pool.take(), self.inner.take()) {
            if !pool.is_some() {
                return;
            }

            if !self.has_been_used() {
                // If the client was never taken from the pool, we don't need to return the connection
                warn!("Dropping a client that was not used.");
                return;
            }

            let context = self.context.clone();
            let client = Self {
                inner: Some(inner),
                pool: pool.clone(),
                context,
                used: false.into(),
            };

            pool.return_conn(client);
        }
    }
}

#[cfg(feature = "tokio_io")]
#[cfg(test)]
mod test {
    use std::{
        future::Future,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        task::{Context, Poll, Wake, Waker},
        time::{Duration, Instant},
    };

    use futures_util::future;

    use crate::{
        errors::{DriverError, Error, Result},
        io::{ClickhouseTransport, Stream},
        test_misc::DATABASE_URL,
        types::Context as ClientContext,
        Block, ClientHandle, Options,
    };

    use super::{Pool, PoolBinding, TaskSlot};
    use url::Url;

    struct CountWake {
        wakes: Arc<AtomicUsize>,
    }

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct CountingPending {
        polls: Arc<AtomicUsize>,
    }

    impl Future for CountingPending {
        type Output = Result<ClientHandle>;

        fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }
    }

    struct ReadyAfterStoredWake {
        client: Option<ClientHandle>,
        polls: usize,
        waker: Arc<Mutex<Option<Waker>>>,
    }

    impl Future for ReadyAfterStoredWake {
        type Output = Result<ClientHandle>;

        fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.polls == 0 {
                self.polls += 1;
                *self.waker.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(Ok(self.client.take().unwrap()))
            }
        }
    }

    async fn synthetic_idle_client(pool: &Pool) -> ClientHandle {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        accept.await.unwrap();

        ClientHandle {
            inner: Some(ClickhouseTransport::new(
                Stream::from(stream),
                false,
                Some(pool.clone()),
            )),
            context: ClientContext::default(),
            pool: PoolBinding::Detached(pool.clone()),
            used: AtomicBool::new(false),
        }
    }

    fn reserve_conn(pool: &Pool) {
        assert!(pool.inner.try_reserve_conn(pool.max));
    }

    fn count_waker() -> (Arc<AtomicUsize>, Waker) {
        let wakes = Arc::new(AtomicUsize::new(0));
        (wakes.clone(), Waker::from(Arc::new(CountWake { wakes })))
    }

    #[tokio::test]
    async fn test_connect() -> Result<()> {
        // pool_max(1) is required: get_handle() eagerly opens pending connections
        // up to pool_max, so the default pool would leave more than one idle handle.
        let options = Options::from_str(DATABASE_URL.as_str())
            .unwrap()
            .pool_max(1);
        let pool = Pool::new(options);
        {
            let mut c = pool.get_handle().await?;
            c.ping().await?;
        }

        let info = pool.info();
        assert_eq!(info.ongoing, 0);
        assert_eq!(info.idle_len, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_detach() -> Result<()> {
        async fn done(pool: Pool) -> Result<()> {
            let p = pool.clone();
            let mut c = p.get_handle().await?;
            c.ping().await?;
            c.pool.detach();
            Ok(())
        }

        let options = Options::from_str(DATABASE_URL.as_str())
            .unwrap()
            .pool_max(1);
        let pool = Pool::new(options);
        done(pool.clone()).await?;
        assert_eq!(pool.info().idle_len, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_many_connection() -> Result<()> {
        let options = Options::from_str(DATABASE_URL.as_str())
            .unwrap()
            .pool_min(6)
            .pool_max(12);
        let pool = Pool::new(options);

        async fn exec_query(pool: &Pool) -> Result<u32> {
            let mut c = pool.get_handle().await?;
            let block = c.query("SELECT toUInt32(1), sleep(1)").fetch_all().await?;

            let value: u32 = block.get(0, 0)?;
            Ok(value)
        }

        let expected = 22_u32;

        let start = Instant::now();

        let mut requests = Vec::new();
        for _ in 0..expected as usize {
            requests.push(exec_query(&pool))
        }

        let xs = future::join_all(requests).await;
        let mut actual: u32 = 0;

        for x in xs {
            actual += x?;
        }
        assert_eq!(actual, expected);

        let spent = start.elapsed();

        assert!(spent >= Duration::from_millis(2000));
        #[cfg(feature = "_tls")]
        assert!(spent < Duration::from_millis(5000)); // slow connect
        #[cfg(not(feature = "_tls"))]
        assert!(spent < Duration::from_millis(5000)); // slow Docker/arch emulation

        assert_eq!(pool.info().idle_len, 6);
        Ok(())
    }

    #[tokio::test]
    async fn test_wrong_insert() -> Result<()> {
        let options = Options::from_str(DATABASE_URL.as_str())
            .unwrap()
            .pool_max(1);
        let pool = Pool::new(options);
        {
            let block = Block::new();
            let mut c = pool.get_handle().await?;
            c.insert("unexisting", block).await.unwrap_err();
        }
        let info = pool.info();
        assert_eq!(info.ongoing, 0);
        assert_eq!(info.tasks_len, 0);
        assert_eq!(info.idle_len, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_wrong_execute() -> Result<()> {
        let options = Options::from_str(DATABASE_URL.as_str())
            .unwrap()
            .pool_max(1);
        let pool = Pool::new(options);
        {
            let mut c = pool.get_handle().await?;
            c.execute("DROP TABLE unexisting").await.unwrap_err();
        }
        let info = pool.info();
        assert_eq!(info.ongoing, 0);
        assert_eq!(info.tasks_len, 0);
        assert_eq!(info.idle_len, 0);
        Ok(())
    }

    #[test]
    fn test_get_addr() {
        let options =
            Options::from_str("tcp://host1:9000?alt_hosts=host2:9000,host3:9000").unwrap();
        let pool = Pool::new(options);

        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host1:9000").unwrap());
        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host2:9000").unwrap());
        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host3:9000").unwrap());
        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host1:9000").unwrap())
    }

    #[tokio::test]
    async fn test_get_handle_starts_pending_connections_up_to_pool_max() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let mut accepted = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                accepted.push(stream);
            }
        });

        let pool_max = 4;
        let options = Options::from_str(&format!("tcp://{}", addr))
            .unwrap()
            .pool_min(0)
            .pool_max(pool_max)
            .connection_timeout(Duration::from_secs(30));
        let pool = Pool::new(options);
        let mut handles: Vec<_> = (0..pool_max).map(|_| Box::pin(pool.get_handle())).collect();

        tokio::time::timeout(
            Duration::from_secs(2),
            future::poll_fn(|cx| {
                for handle in &mut handles {
                    let _ = handle.as_mut().poll(cx);
                }

                if pool.info().new_len == pool_max {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }),
        )
        .await
        .unwrap();

        assert_eq!(pool.info().new_len, pool_max);
        accept.abort();
        Ok(())
    }

    #[tokio::test]
    async fn test_get_handle_bounds_pending_open_polling_during_cold_burst() -> Result<()> {
        let pool_max = 16;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));
        let polls = Arc::new(AtomicUsize::new(0));

        for _ in 0..pool_max {
            reserve_conn(&pool);
            assert!(pool
                .inner
                .new
                .push(Box::pin(CountingPending {
                    polls: polls.clone(),
                }))
                .is_ok());
        }

        let (_, waker) = count_waker();
        let mut cx = Context::from_waker(&waker);
        let mut waiters: Vec<_> = (0..pool_max).map(|_| Box::pin(pool.get_handle())).collect();

        for waiter in &mut waiters {
            assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        }

        let poll_count = polls.load(Ordering::SeqCst);
        assert_eq!(pool.info().new_len, pool_max);
        assert!(poll_count <= pool_max * super::PENDING_OPEN_POLL_BUDGET);
        assert!(poll_count < pool_max * pool_max);
        Ok(())
    }

    #[tokio::test]
    async fn test_completed_pending_open_wakes_parked_waiter() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(0).pool_max(1));
        reserve_conn(&pool);
        assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());

        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake {
            wakes: wakes.clone(),
        }));
        let mut cx = Context::from_waker(&waker);
        let mut waiter = Box::pin(pool.get_handle());

        assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(pool.info().tasks_len, 1);
        assert_eq!(wakes.load(Ordering::SeqCst), 0);

        let _ = pool.inner.new.pop();
        let client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(future::ready(Ok(client))))
            .is_ok());

        let mut driver = pool.clone();
        let mut scan = 0usize;
        driver.handle_futures(&mut cx, &mut scan)?;

        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        let handle = match waiter.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(handle)) => handle,
            _ => panic!("waiter did not acquire the completed pending connection"),
        };
        assert_eq!(pool.info().ongoing, 1);
        drop(handle);
        assert_eq!(pool.info().ongoing, 0);
        assert_eq!(pool.inner.conn_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_pending_failure_wakes_live_waiter_after_stale_waiter() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(0).pool_max(1));
        reserve_conn(&pool);
        assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());

        let (stale_wakes, stale_waker) = count_waker();
        let mut stale_cx = Context::from_waker(&stale_waker);
        let mut stale_waiter = Box::pin(pool.get_handle());
        assert!(matches!(
            stale_waiter.as_mut().poll(&mut stale_cx),
            Poll::Pending
        ));
        drop(stale_waiter);

        let (live_wakes, live_waker) = count_waker();
        let mut live_cx = Context::from_waker(&live_waker);
        let mut live_waiter = Box::pin(pool.get_handle());
        assert!(matches!(
            live_waiter.as_mut().poll(&mut live_cx),
            Poll::Pending
        ));
        assert_eq!(pool.info().tasks_len, 2);

        assert!(pool.inner.new.pop().is_some());
        assert!(pool
            .inner
            .new
            .push(Box::pin(future::ready(Err(Error::Driver(
                DriverError::Timeout,
            )))))
            .is_ok());

        let mut driver = pool.clone();
        let mut scan = 0usize;
        assert!(driver.handle_futures(&mut live_cx, &mut scan).is_err());

        assert_eq!(stale_wakes.load(Ordering::SeqCst), 0);
        assert_eq!(live_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(pool.info().tasks_len, 0);
        assert_eq!(pool.inner.conn_count(), 0);
        drop(live_waiter);
        Ok(())
    }

    #[tokio::test]
    async fn test_pending_open_wakes_parked_waiter() -> Result<()> {
        let pool_max = 2;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));
        for _ in 0..pool_max {
            reserve_conn(&pool);
            assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());
        }

        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake {
            wakes: wakes.clone(),
        }));
        let mut cx = Context::from_waker(&waker);
        let mut waiters: Vec<_> = (0..pool_max).map(|_| Box::pin(pool.get_handle())).collect();

        for waiter in &mut waiters {
            assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        }
        assert_eq!(pool.info().tasks_len, pool_max);
        assert_eq!(wakes.load(Ordering::SeqCst), 0);

        for _ in 0..pool_max {
            assert!(pool.inner.new.pop().is_some());
        }
        for _ in 0..pool_max {
            assert!(pool
                .inner
                .new
                .push(Box::pin(future::ready(Err(Error::Driver(
                    DriverError::Timeout,
                )))))
                .is_ok());
        }

        let mut driver = pool.clone();
        let mut scan = 0usize;
        assert!(driver.handle_futures(&mut cx, &mut scan).is_err());

        assert_eq!(wakes.load(Ordering::SeqCst), pool_max);
        assert_eq!(pool.info().tasks_len, 0);
        assert_eq!(pool.info().new_len, 0);
        assert_eq!(pool.inner.conn_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_returned_to_idle_retains_conn_slot_when_pool_min_keeps_idle() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(1).pool_max(2));
        reserve_conn(&pool);
        pool.inner
            .idle
            .push(synthetic_idle_client(&pool).await)
            .unwrap();

        let handle = pool.get_handle().await?;
        assert_eq!(pool.info().idle_len, 0);
        assert_eq!(pool.info().ongoing, 1);
        assert_eq!(pool.inner.conn_count(), 1);

        drop(handle);

        assert_eq!(pool.info().ongoing, 0);
        assert_eq!(pool.info().idle_len, 1);
        assert_eq!(pool.inner.conn_count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_handle_returns_idle_when_pending_connection_fails() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(0));
        reserve_conn(&pool);
        pool.inner
            .idle
            .push(synthetic_idle_client(&pool).await)
            .unwrap();
        reserve_conn(&pool);
        assert!(pool
            .inner
            .new
            .push(Box::pin(future::ready(Err(Error::Driver(
                DriverError::Timeout,
            )))))
            .is_ok());

        let handle = pool.get_handle().await?;

        let info = pool.info();
        assert_eq!(info.new_len, 0);
        assert_eq!(info.idle_len, 0);
        assert_eq!(info.ongoing, 1);
        assert_eq!(pool.inner.conn_count(), 1);
        drop(handle);
        assert_eq!(pool.info().ongoing, 0);
        assert_eq!(pool.inner.conn_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_futures_drops_ready_connection_when_idle_full() -> Result<()> {
        let pool_max = 2;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));

        for _ in 0..pool_max {
            reserve_conn(&pool);
            pool.inner
                .idle
                .push(synthetic_idle_client(&pool).await)
                .unwrap();
        }

        pool.inner.conn_slots.fetch_add(1, Ordering::AcqRel);
        let client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(future::ready(Ok(client))))
            .is_ok());

        let (wakes, waker) = count_waker();
        let slot = TaskSlot::live();
        pool.inner.tasks.push((slot, waker.clone()));
        let mut cx = Context::from_waker(&waker);
        let mut driver = pool.clone();

        assert_eq!(pool.info().tasks_len, 1);
        let mut scan = 0usize;
        driver.handle_futures(&mut cx, &mut scan)?;

        let info = pool.info();
        assert_eq!(info.new_len, 0);
        assert_eq!(info.idle_len, pool_max);
        assert_eq!(info.tasks_len, 0);
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(pool.inner.conn_count(), pool_max);
        Ok(())
    }

    /// GRD-C001: a ready future sitting beyond the first scan budget must be
    /// observed without requiring an external event.  The waiter should be
    /// served after at most ceil(N / BUDGET) self-woken polls.
    #[tokio::test]
    async fn test_ready_future_beyond_budget_is_eventually_observed() -> Result<()> {
        let pool_max = super::PENDING_OPEN_POLL_BUDGET + 1;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));

        // First BUDGET futures are permanently pending; the last one is ready.
        for _ in 0..pool_max - 1 {
            reserve_conn(&pool);
            assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());
        }
        reserve_conn(&pool);
        let ready_client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(future::ready(Ok(ready_client))))
            .is_ok());

        let (wakes, waker) = count_waker();
        let mut cx = Context::from_waker(&waker);
        let mut waiter = Box::pin(pool.get_handle());

        // First poll: scans BUDGET futures (all pending), self-wakes once to
        // continue the scan cycle.
        assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "expected exactly one self-wake to continue the scan cycle"
        );
        assert_eq!(
            pool.info().tasks_len,
            0,
            "partial scan continuation should not also park the waiter"
        );

        // Second poll (continuation): scans the remaining 1 future which is
        // ready; the connection is placed in idle and returned to the waiter.
        let handle = match waiter.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(handle)) => handle,
            _ => panic!("expected Ready(Ok(_)) on the continuation poll"),
        };
        assert_eq!(pool.info().ongoing, 1);
        drop(handle);
        assert_eq!(pool.info().ongoing, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_ready_during_partial_scan_keeps_unscanned_futures_scannable() -> Result<()> {
        let pool_max = super::PENDING_OPEN_POLL_BUDGET + 1;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));
        let pending_polls = Arc::new(AtomicUsize::new(0));

        reserve_conn(&pool);
        let ready_client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(future::ready(Ok(ready_client))))
            .is_ok());
        for _ in 0..super::PENDING_OPEN_POLL_BUDGET {
            reserve_conn(&pool);
            assert!(pool
                .inner
                .new
                .push(Box::pin(CountingPending {
                    polls: pending_polls.clone(),
                }))
                .is_ok());
        }

        let (_, driver_waker) = count_waker();
        let mut driver_cx = Context::from_waker(&driver_waker);
        let mut driver_waiter = Box::pin(pool.get_handle());
        let handle = match driver_waiter.as_mut().poll(&mut driver_cx) {
            Poll::Ready(Ok(handle)) => handle,
            _ => panic!("driver did not return ready pending-open connection"),
        };
        assert_eq!(
            pending_polls.load(Ordering::SeqCst),
            super::PENDING_OPEN_POLL_BUDGET - 1,
            "first scan should leave one future from the cycle unscanned"
        );

        let (_, next_waker) = count_waker();
        let mut next_cx = Context::from_waker(&next_waker);
        let mut next_waiter = Box::pin(pool.get_handle());
        assert!(matches!(
            next_waiter.as_mut().poll(&mut next_cx),
            Poll::Pending
        ));
        assert_eq!(
            pending_polls.load(Ordering::SeqCst),
            (super::PENDING_OPEN_POLL_BUDGET - 1) + super::PENDING_OPEN_POLL_BUDGET,
            "a later waiter must be able to scan the unscanned pending-open futures"
        );

        drop(handle);
        assert_eq!(pool.info().ongoing, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_releasing_driver_with_scan_needed_wakes_parked_waiter() -> Result<()> {
        let pool_max = super::PENDING_OPEN_POLL_BUDGET + 1;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));

        reserve_conn(&pool);
        let first_ready_client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(future::ready(Ok(first_ready_client))))
            .is_ok());
        for _ in 0..super::PENDING_OPEN_POLL_BUDGET - 1 {
            reserve_conn(&pool);
            assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());
        }
        reserve_conn(&pool);
        let unscanned_ready_client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(future::ready(Ok(unscanned_ready_client))))
            .is_ok());

        let (_, driver_waker) = count_waker();
        let mut driver_cx = Context::from_waker(&driver_waker);
        let mut driver_pool = pool.clone();
        let mut scan_remaining = 0usize;
        assert!(pool.inner.try_acquire_pending_open_scan_driver());

        driver_pool.handle_futures(&mut driver_cx, &mut scan_remaining)?;
        assert_eq!(scan_remaining, 0);
        assert!(pool.inner.pending_open_scan_needed.load(Ordering::Acquire));
        let first_handle = driver_pool
            .take_conn()
            .expect("driver should consume the first ready connection");

        let (parked_wakes, parked_waker) = count_waker();
        let mut parked_cx = Context::from_waker(&parked_waker);
        let mut parked_waiter = Box::pin(pool.get_handle());
        assert!(matches!(
            parked_waiter.as_mut().poll(&mut parked_cx),
            Poll::Pending
        ));
        assert_eq!(parked_wakes.load(Ordering::SeqCst), 0);
        assert_eq!(pool.info().tasks_len, 1);

        pool.inner.release_pending_open_scan_driver();
        assert_eq!(
            parked_wakes.load(Ordering::SeqCst),
            1,
            "releasing a driver while more scanning is needed must recruit parked waiters"
        );

        let second_handle = match parked_waiter.as_mut().poll(&mut parked_cx) {
            Poll::Ready(Ok(handle)) => handle,
            _ => panic!("parked waiter did not scan the unobserved ready pending-open future"),
        };
        assert_eq!(pool.info().ongoing, 2);

        drop(second_handle);
        drop(first_handle);
        while pool.inner.new.pop().is_some() {
            pool.inner.release_conn_slot();
        }
        assert_eq!(pool.info().ongoing, 0);
        assert_eq!(pool.inner.conn_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_non_driver_park_rechecks_scan_needed_after_release() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(0).pool_max(1));
        reserve_conn(&pool);
        assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());
        pool.inner
            .pending_open_scan_needed
            .store(true, Ordering::Release);

        let (wakes, waker) = count_waker();
        let mut cx = Context::from_waker(&waker);
        let mut scan_driver = false;

        let mut park_slot = None;
        pool.park_pending_open_waiter(&mut cx, &mut scan_driver, &mut park_slot);

        assert!(
            scan_driver,
            "a waiter that parks after a missed release wake must recruit itself as scan driver"
        );
        assert!(pool.inner.pending_open_scan_driver.load(Ordering::Acquire));
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "recruited waiter must self-wake to drive the pending scan"
        );
        assert_eq!(pool.info().tasks_len, 1);

        pool.inner.release_pending_open_scan_driver();
        while pool.inner.new.pop().is_some() {
            pool.inner.release_conn_slot();
        }
        assert_eq!(pool.inner.conn_count(), 0);
        Ok(())
    }

    /// GRD-C001: when every pending-open future is still Pending after one
    /// full scan cycle the self-wake must stop — no unbounded busy-loop.
    /// Verifies that ceil(N/BUDGET) − 1 = 1 self-wake is produced and then
    /// the chain terminates; the scan does not spin indefinitely.
    #[tokio::test]
    async fn test_all_pending_scan_cycle_stops_self_waking() -> Result<()> {
        let pool_max = super::PENDING_OPEN_POLL_BUDGET + 1;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));

        for _ in 0..pool_max {
            reserve_conn(&pool);
            assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());
        }

        let (wakes, waker) = count_waker();
        let mut cx = Context::from_waker(&waker);
        let mut waiter = Box::pin(pool.get_handle());

        // Poll 1: scans BUDGET futures, exhausts budget with scan_remaining = 1,
        // emits exactly one self-wake to continue the cycle.
        assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "expected exactly one self-wake after the first partial scan"
        );
        assert_eq!(
            pool.info().tasks_len,
            0,
            "partial scan continuation should not also park the waiter"
        );

        // Poll 2 (driven by that self-wake): scans the remaining 1 future;
        // scan_remaining reaches 0 — no further self-wake is emitted.
        assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "self-wake chain must stop once the full scan cycle is exhausted"
        );
        assert_eq!(
            pool.info().tasks_len,
            1,
            "waiter should park after the scan cycle completes with all futures pending"
        );
        Ok(())
    }

    /// GRD-C001: stale scan counter — if the queue is drained by another
    /// driver between the cycle snapshot and a continuation poll, the empty
    /// pop must reset scan_remaining to 0 and must NOT emit a self-wake.
    #[tokio::test]
    async fn test_stale_scan_counter_does_not_busy_loop_on_empty_queue() -> Result<()> {
        let pool_max = super::PENDING_OPEN_POLL_BUDGET + 1;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));

        for _ in 0..pool_max {
            reserve_conn(&pool);
            assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());
        }

        let (wakes, waker) = count_waker();
        let mut cx = Context::from_waker(&waker);

        // Simulate a mid-cycle state: scan_remaining = 1 (continuation wake
        // was issued for 1 remaining future), but the queue has been drained
        // by another driver in the meantime.
        let mut scan_remaining: usize = 1;
        while pool.inner.new.pop().is_some() {}
        assert_eq!(pool.inner.new.len(), 0);

        let mut driver = pool.clone();
        driver.handle_futures(&mut cx, &mut scan_remaining)?;

        assert_eq!(
            scan_remaining, 0,
            "scan_remaining must be reset when the queue is empty"
        );
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            0,
            "no self-wake must be emitted when the queue is empty"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_external_wake_during_scan_cycle_revisits_scanned_futures() -> Result<()> {
        let pool_max = super::PENDING_OPEN_POLL_BUDGET + 1;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));
        let stored_waker = Arc::new(Mutex::new(None));

        reserve_conn(&pool);
        let ready_client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(ReadyAfterStoredWake {
                client: Some(ready_client),
                polls: 0,
                waker: stored_waker.clone(),
            }))
            .is_ok());
        for _ in 0..super::PENDING_OPEN_POLL_BUDGET - 1 {
            reserve_conn(&pool);
            assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());
        }
        reserve_conn(&pool);
        assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());

        let (wakes, waker) = count_waker();
        let mut cx = Context::from_waker(&waker);
        let mut waiter = Box::pin(pool.get_handle());

        assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(pool.info().tasks_len, 0);

        let pending_open_waker = stored_waker.lock().unwrap().take().unwrap();
        pending_open_waker.wake();
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            2,
            "external wake should wake the active scan driver even without parked waiters"
        );

        assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(
            pool.info().tasks_len,
            0,
            "externally woken first-slice future should schedule a bounded rescan instead of parking"
        );
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            3,
            "rescan should be explicitly scheduled after the coalesced external wake is observed"
        );

        let handle = match waiter.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(handle)) => handle,
            _ => panic!("rescan did not observe the externally woken pending-open future"),
        };
        assert_eq!(pool.info().ongoing, 1);
        drop(handle);
        assert_eq!(pool.info().ongoing, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_pending_open_wake_after_driver_drop_recruits_parked_waiter() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(0).pool_max(1));
        let stored_waker = Arc::new(Mutex::new(None));

        reserve_conn(&pool);
        let ready_client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(ReadyAfterStoredWake {
                client: Some(ready_client),
                polls: 0,
                waker: stored_waker.clone(),
            }))
            .is_ok());

        let (driver_wakes, driver_waker) = count_waker();
        let mut driver_cx = Context::from_waker(&driver_waker);
        let mut driver_waiter = Box::pin(pool.get_handle());
        assert!(matches!(
            driver_waiter.as_mut().poll(&mut driver_cx),
            Poll::Pending
        ));
        assert_eq!(driver_wakes.load(Ordering::SeqCst), 0);
        assert_eq!(pool.info().tasks_len, 1);
        drop(driver_waiter);

        let (parked_wakes, parked_waker) = count_waker();
        let mut parked_cx = Context::from_waker(&parked_waker);
        let mut parked_waiter = Box::pin(pool.get_handle());
        assert!(matches!(
            parked_waiter.as_mut().poll(&mut parked_cx),
            Poll::Pending
        ));
        assert_eq!(pool.info().tasks_len, 2);

        let pending_open_waker = stored_waker.lock().unwrap().take().unwrap();
        pending_open_waker.wake();
        assert_eq!(
            parked_wakes.load(Ordering::SeqCst),
            1,
            "pending-open wake after the original driver is gone must recruit parked waiters"
        );
        assert_eq!(pool.info().tasks_len, 0);

        let handle = match parked_waiter.as_mut().poll(&mut parked_cx) {
            Poll::Ready(Ok(handle)) => handle,
            _ => panic!("parked waiter did not observe ready pending-open future"),
        };
        assert_eq!(pool.info().ongoing, 1);
        drop(handle);
        assert_eq!(pool.info().ongoing, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_idle_return_mid_scan_hands_off_driver() -> Result<()> {
        let pending_open_count = super::PENDING_OPEN_POLL_BUDGET + 1;
        let pool_max = pending_open_count + 1;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));
        let stored_waker = Arc::new(Mutex::new(None));

        reserve_conn(&pool);
        assert!(pool
            .inner
            .idle
            .push(synthetic_idle_client(&pool).await)
            .is_ok());

        reserve_conn(&pool);
        assert!(pool
            .inner
            .new
            .push(Box::pin(ReadyAfterStoredWake {
                client: Some(synthetic_idle_client(&pool).await),
                polls: 0,
                waker: stored_waker.clone(),
            }))
            .is_ok());
        for _ in 0..super::PENDING_OPEN_POLL_BUDGET - 1 {
            reserve_conn(&pool);
            assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());
        }
        reserve_conn(&pool);
        assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());

        let (driver_wakes, driver_waker) = count_waker();
        let mut driver_cx = Context::from_waker(&driver_waker);
        let mut driver_waiter = Box::pin(pool.get_handle());
        let idle_handle = match driver_waiter.as_mut().poll(&mut driver_cx) {
            Poll::Ready(Ok(handle)) => handle,
            _ => panic!("driver did not return available idle connection"),
        };
        assert_eq!(driver_wakes.load(Ordering::SeqCst), 1);
        assert!(!pool.inner.pending_open_scan_driver.load(Ordering::Acquire));
        assert!(pool
            .inner
            .pending_open_scan_driver_waker
            .lock()
            .unwrap()
            .is_none());

        let pending_open_waker = stored_waker.lock().unwrap().take().unwrap();
        pending_open_waker.wake();
        assert_eq!(
            driver_wakes.load(Ordering::SeqCst),
            1,
            "completed driver futures retained by callers must not keep future pending-open wakes on a stale driver"
        );

        let (_, next_waker) = count_waker();
        let mut next_cx = Context::from_waker(&next_waker);
        let mut next_waiter = Box::pin(pool.get_handle());
        let handle = match next_waiter.as_mut().poll(&mut next_cx) {
            Poll::Ready(Ok(handle)) => handle,
            _ => panic!("next waiter did not observe ready pending-open future"),
        };
        assert_eq!(pool.info().ongoing, 2);
        drop(handle);
        drop(idle_handle);
        assert_eq!(pool.info().ongoing, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_only_one_waiter_drives_pending_open_scan_cycle() -> Result<()> {
        let pool_max = super::PENDING_OPEN_POLL_BUDGET + 1;
        let waiter_count = pool_max * 2;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));
        let polls = Arc::new(AtomicUsize::new(0));

        for _ in 0..pool_max {
            reserve_conn(&pool);
            assert!(pool
                .inner
                .new
                .push(Box::pin(CountingPending {
                    polls: polls.clone(),
                }))
                .is_ok());
        }

        let (wakes, waker) = count_waker();
        let mut cx = Context::from_waker(&waker);
        let mut waiters: Vec<_> = (0..waiter_count)
            .map(|_| Box::pin(pool.get_handle()))
            .collect();

        assert!(matches!(waiters[0].as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            super::PENDING_OPEN_POLL_BUDGET
        );
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(pool.info().tasks_len, 0);

        for waiter in waiters.iter_mut().skip(1) {
            assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Pending));
        }
        assert_eq!(
            polls.load(Ordering::SeqCst),
            super::PENDING_OPEN_POLL_BUDGET,
            "non-driver waiters should park instead of scanning the same pending opens"
        );
        assert_eq!(pool.info().tasks_len, waiter_count - 1);

        assert!(matches!(waiters[0].as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            pool_max,
            "the single scan driver should finish the bounded cycle once"
        );
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(pool.info().tasks_len, waiter_count);

        let mut late_waiter = Box::pin(pool.get_handle());
        assert!(matches!(late_waiter.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            pool_max,
            "late waiters should not start redundant scans after an all-pending cycle"
        );
        assert_eq!(pool.info().tasks_len, waiter_count + 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_cancelled_scan_continuation_wakes_parked_waiter() -> Result<()> {
        let pool_max = super::PENDING_OPEN_POLL_BUDGET + 1;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));

        for _ in 0..pool_max {
            reserve_conn(&pool);
            assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());
        }

        let (parked_wakes, parked_waker) = count_waker();
        let mut parked_cx = Context::from_waker(&parked_waker);
        let mut parked_waiter = Box::pin(pool.get_handle());

        assert!(matches!(
            parked_waiter.as_mut().poll(&mut parked_cx),
            Poll::Pending
        ));
        assert_eq!(pool.info().tasks_len, 0);
        assert!(matches!(
            parked_waiter.as_mut().poll(&mut parked_cx),
            Poll::Pending
        ));
        assert_eq!(pool.info().tasks_len, 1);
        let parked_wakes_before_continuation = parked_wakes.load(Ordering::SeqCst);

        while pool.inner.new.pop().is_some() {}
        for _ in 0..pool_max - 1 {
            assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());
        }
        let ready_client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(future::ready(Ok(ready_client))))
            .is_ok());
        pool.inner
            .pending_open_scan_needed
            .store(true, Ordering::Release);

        let (driver_wakes, driver_waker) = count_waker();
        let mut driver_cx = Context::from_waker(&driver_waker);
        let mut driver_waiter = Box::pin(pool.get_handle());

        assert!(matches!(
            driver_waiter.as_mut().poll(&mut driver_cx),
            Poll::Pending
        ));
        assert_eq!(driver_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(
            parked_wakes.load(Ordering::SeqCst),
            parked_wakes_before_continuation,
            "normal scan continuation should only self-wake the driving waiter"
        );
        assert_eq!(pool.info().tasks_len, 1);
        drop(driver_waiter);
        assert_eq!(
            parked_wakes.load(Ordering::SeqCst),
            parked_wakes_before_continuation + 1,
            "dropping the mid-cycle waiter must hand continuation to a parked waiter"
        );
        assert_eq!(pool.info().tasks_len, 0);

        let handle = match parked_waiter.as_mut().poll(&mut parked_cx) {
            Poll::Ready(Ok(handle)) => handle,
            _ => panic!("parked waiter did not acquire the ready pending-open connection"),
        };
        assert_eq!(pool.info().ongoing, 1);
        drop(handle);
        assert_eq!(pool.info().ongoing, 0);
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Tests for recruit-one semantics and live-aware task entries
    // -------------------------------------------------------------------------

    /// When a pending-open future wakes (PendingOpenWake) and there is no
    /// active scan-driver waker, exactly ONE parked live waiter must be
    /// recruited — not all of them.
    #[tokio::test]
    async fn test_pending_open_wake_recruits_exactly_one_live_waiter() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(0).pool_max(1));
        let stored_waker = Arc::new(Mutex::new(None));

        // The ReadyAfterStoredWake future stores the PendingOpenWake waker on
        // its first poll so that we can fire it later from test code.
        reserve_conn(&pool);
        let ready_client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(ReadyAfterStoredWake {
                client: Some(ready_client),
                polls: 0,
                waker: stored_waker.clone(),
            }))
            .is_ok());

        // Poll a temporary driver to capture the PendingOpenWake waker.
        let (_, tmp_waker) = count_waker();
        let mut tmp_cx = Context::from_waker(&tmp_waker);
        let mut tmp_waiter = Box::pin(pool.get_handle());
        assert!(matches!(
            tmp_waiter.as_mut().poll(&mut tmp_cx),
            Poll::Pending
        ));
        // Drop the driver: marks its tasks-entry stale and releases the driver
        // role via handoff.  The queue was empty at that point so nothing is
        // recruited by the handoff.
        drop(tmp_waiter);

        // Capture the PendingOpenWake waker stored by ReadyAfterStoredWake.
        let pending_open_waker = stored_waker
            .lock()
            .unwrap()
            .take()
            .expect("ReadyAfterStoredWake must store a waker on first poll");

        // Manually push three fresh live entries so we have multiple candidates.
        let (wakes_x, waker_x) = count_waker();
        let (wakes_y, waker_y) = count_waker();
        let (wakes_z, waker_z) = count_waker();
        pool.inner.tasks.push((TaskSlot::live(), waker_x));
        pool.inner.tasks.push((TaskSlot::live(), waker_y));
        pool.inner.tasks.push((TaskSlot::live(), waker_z));

        assert!(
            !pool.inner.pending_open_scan_driver.load(Ordering::Acquire),
            "no active scan driver before firing"
        );

        // Fire the PendingOpenWake: no driver waker → must call recruit_one.
        pending_open_waker.wake();

        let total = wakes_x.load(Ordering::SeqCst)
            + wakes_y.load(Ordering::SeqCst)
            + wakes_z.load(Ordering::SeqCst);
        assert_eq!(
            total, 1,
            "recruit-one: exactly one live waiter must be woken, got {}",
            total
        );
        assert_eq!(
            pool.info().tasks_len,
            2,
            "the two un-recruited entries must remain in tasks"
        );

        while pool.inner.new.pop().is_some() {
            pool.inner.release_conn_slot();
        }
        Ok(())
    }

    /// If a recruit-one wake targets a parked waiter that is cancelled before
    /// its next poll, that recruited responsibility must hand off to another
    /// live parked waiter.
    #[tokio::test]
    async fn test_cancelled_recruited_waiter_hands_off_to_next_waiter() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(0).pool_max(1));
        let stored_waker = Arc::new(Mutex::new(None));

        reserve_conn(&pool);
        let ready_client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(ReadyAfterStoredWake {
                client: Some(ready_client),
                polls: 0,
                waker: stored_waker.clone(),
            }))
            .is_ok());

        let (wakes_a, waker_a) = count_waker();
        let (wakes_b, waker_b) = count_waker();
        let mut cx_a = Context::from_waker(&waker_a);
        let mut cx_b = Context::from_waker(&waker_b);
        let mut waiter_a = Box::pin(pool.get_handle());
        let mut waiter_b = Box::pin(pool.get_handle());

        assert!(matches!(waiter_a.as_mut().poll(&mut cx_a), Poll::Pending));
        assert!(matches!(waiter_b.as_mut().poll(&mut cx_b), Poll::Pending));
        assert_eq!(pool.info().tasks_len, 2);

        let pending_open_waker = stored_waker
            .lock()
            .unwrap()
            .take()
            .expect("ReadyAfterStoredWake must store a waker on first poll");

        pending_open_waker.wake();
        assert_eq!(
            wakes_a.load(Ordering::SeqCst),
            1,
            "first parked waiter should be the recruited waiter"
        );
        assert_eq!(
            wakes_b.load(Ordering::SeqCst),
            0,
            "second parked waiter should not be woken by the initial recruit"
        );

        drop(waiter_a);
        assert_eq!(
            wakes_b.load(Ordering::SeqCst),
            1,
            "dropping the recruited waiter must recruit the next live waiter"
        );

        let handle = match waiter_b.as_mut().poll(&mut cx_b) {
            Poll::Ready(Ok(handle)) => handle,
            _ => panic!("handoff waiter did not drive the pending-open scan"),
        };
        assert_eq!(pool.info().ongoing, 1);
        drop(handle);
        assert_eq!(pool.info().ongoing, 0);
        Ok(())
    }

    /// Stale entries that precede a live entry must be skipped/drained, and
    /// the live waiter must be successfully recruited.
    #[tokio::test]
    async fn test_recruit_one_skips_stale_entries_before_live_waiter() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(0).pool_max(1));

        // Push two stale entries (alive=false) followed by one live entry
        // directly into tasks so that the test is independent of the
        // GetHandle polling path.
        let (stale_wakes_1, stale_waker_1) = count_waker();
        let (stale_wakes_2, stale_waker_2) = count_waker();
        let (live_wakes, live_waker) = count_waker();

        pool.inner.tasks.push((TaskSlot::stale(), stale_waker_1));
        pool.inner.tasks.push((TaskSlot::stale(), stale_waker_2));
        pool.inner.tasks.push((TaskSlot::live(), live_waker));

        assert_eq!(pool.info().tasks_len, 3);

        // Trigger recruit_one_scan_driver via release_pending_open_scan_driver.
        pool.inner
            .pending_open_scan_needed
            .store(true, Ordering::Release);
        assert!(pool.inner.try_acquire_pending_open_scan_driver());
        pool.inner.release_pending_open_scan_driver();

        assert_eq!(
            stale_wakes_1.load(Ordering::SeqCst),
            0,
            "stale entry 1 must not be woken"
        );
        assert_eq!(
            stale_wakes_2.load(Ordering::SeqCst),
            0,
            "stale entry 2 must not be woken"
        );
        assert_eq!(
            live_wakes.load(Ordering::SeqCst),
            1,
            "live waiter must be recruited"
        );
        assert_eq!(
            pool.info().tasks_len,
            0,
            "all entries (stale and recruited) must be drained from the queue"
        );
        Ok(())
    }

    /// Repeated pending-open wakes must each recruit at most one waiter
    /// (O(wakes) total, not O(waiters × wakes)).
    #[tokio::test]
    async fn test_repeated_pending_open_wakes_are_o_wakes_not_o_waiters_times_wakes() -> Result<()>
    {
        let pool = Pool::new(Options::default().pool_min(0).pool_max(5));

        // Push 5 live entries into tasks.
        let wakes_counters: Vec<Arc<AtomicUsize>> =
            (0..5).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        for i in 0..5 {
            pool.inner.tasks.push((
                TaskSlot::live(),
                Waker::from(Arc::new(CountWake {
                    wakes: wakes_counters[i].clone(),
                })),
            ));
        }

        // Fire the recruit-one path 3 times by acquiring/releasing the scan
        // driver with pending_open_scan_needed=true.  Each release calls
        // recruit_one_scan_driver(), which must wake exactly one waiter.
        for _ in 0..3 {
            pool.inner
                .pending_open_scan_needed
                .store(true, Ordering::Release);
            assert!(pool.inner.try_acquire_pending_open_scan_driver());
            pool.inner.release_pending_open_scan_driver();
        }

        let total: usize = wakes_counters
            .iter()
            .map(|w| w.load(Ordering::SeqCst))
            .sum();
        assert_eq!(
            total, 3,
            "O(wakes): expected 3 wakes for 3 recruit events with 5 parked waiters, got {}",
            total
        );
        assert_eq!(
            pool.info().tasks_len,
            2,
            "2 un-recruited entries must remain in tasks"
        );
        Ok(())
    }

    /// State-change (return_conn / release_conn) wake-all must still wake ALL
    /// live parked waiters, not just one.
    #[tokio::test]
    async fn test_state_change_wakes_all_live_waiters() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(0).pool_max(2));

        let (_wakes_a, waker_a) = count_waker();
        let (wakes_b, waker_b) = count_waker();
        let mut cx_a = Context::from_waker(&waker_a);
        let mut cx_b = Context::from_waker(&waker_b);

        reserve_conn(&pool);
        assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());
        reserve_conn(&pool);
        assert!(pool.inner.new.push(Box::pin(future::pending())).is_ok());

        let mut w_a = Box::pin(pool.get_handle());
        let mut w_b = Box::pin(pool.get_handle());
        // First waiter drives; drop it so only non-drivers remain parked.
        assert!(matches!(w_a.as_mut().poll(&mut cx_a), Poll::Pending));
        drop(w_a);
        assert!(matches!(w_b.as_mut().poll(&mut cx_b), Poll::Pending));

        // Place a second live waiter manually so we have two non-driver live entries.
        let (wakes_c, waker_c) = count_waker();
        pool.inner.tasks.push((TaskSlot::live(), waker_c.clone()));

        // Trigger a state change (wake-all).
        pool.inner.wake_tasks();

        // Both live waiters must be woken.
        assert_eq!(
            wakes_b.load(Ordering::SeqCst),
            1,
            "live waiter b must be woken by state-change wake_tasks"
        );
        assert_eq!(
            wakes_c.load(Ordering::SeqCst),
            1,
            "live waiter c must be woken by state-change wake_tasks"
        );
        assert_eq!(pool.info().tasks_len, 0, "all entries must be drained");

        drop(w_b);
        while pool.inner.new.pop().is_some() {
            pool.inner.release_conn_slot();
        }
        Ok(())
    }
}
