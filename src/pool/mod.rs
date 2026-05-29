use std::{
    fmt,
    io::ErrorKind,
    mem,
    pin::Pin,
    sync::{
        atomic::{self, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
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

// Per-poll work budget for existing connection-open futures.
const PENDING_OPEN_POLL_BUDGET: usize = 4;
// Cold-start backpressure cap for queued/in-flight connection opens.
const PENDING_OPEN_LIMIT: usize = 4;

fn pending_open_limit(max: usize) -> usize {
    max.min(PENDING_OPEN_LIMIT)
}

pub(crate) struct Inner {
    new: crossbeam::queue::ArrayQueue<BoxFuture<'static, Result<ClientHandle>>>,
    idle: crossbeam::queue::ArrayQueue<ClientHandle>,
    tasks: crossbeam::queue::SegQueue<Waker>,
    ongoing: atomic::AtomicUsize,
    conn_slots: atomic::AtomicUsize,
    // Counts queued opens plus opens temporarily popped for polling.
    pending_opens: atomic::AtomicUsize,
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

    fn pending_open_count(&self) -> usize {
        self.pending_opens.load(Ordering::Acquire)
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

    fn try_reserve_pending_open(&self, max: usize) -> bool {
        let mut current = self.pending_open_count();

        while current < max {
            match self.pending_opens.compare_exchange_weak(
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

    fn release_pending_open(&self) {
        if self
            .pending_opens
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_sub(1)
            })
            .is_err()
        {
            warn!("release_pending_open called when no connection opens are pending");
        }
    }

    fn wake_tasks(&self) {
        while let Some(task) = self.tasks.pop() {
            task.wake()
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
            new: crossbeam::queue::ArrayQueue::new(pending_open_limit(max).max(1)),
            idle: crossbeam::queue::ArrayQueue::new(max),
            tasks: crossbeam::queue::SegQueue::new(),
            ongoing: atomic::AtomicUsize::new(0),
            conn_slots: atomic::AtomicUsize::new(0),
            pending_opens: atomic::AtomicUsize::new(0),
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

    fn pending_open_limit(&self) -> usize {
        pending_open_limit(self.max)
    }

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<ClientHandle>> {
        let handle_futures_result = self.handle_futures(cx);
        let pool_info = if handle_futures_result.is_err() {
            Some(self.info())
        } else {
            None
        };

        if let Some(client) = self.take_conn() {
            if let Err(err) = handle_futures_result {
                warn!(
                    "Pending ClickHouse connection failed while idle connections were available; using idle connection instead. error={}; pool_info={:?}",
                    err,
                    pool_info.unwrap_or_else(|| self.info())
                );
            }
            return Poll::Ready(Ok(client));
        }

        handle_futures_result?;

        if self.inner.try_reserve_conn(self.max) {
            if self
                .inner
                .try_reserve_pending_open(self.pending_open_limit())
            {
                match self.inner.new.push(self.new_connection()) {
                    Ok(()) => {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    Err(_) => {
                        self.inner.release_pending_open();
                        self.inner.release_conn_slot();
                    }
                }
            } else {
                self.inner.release_conn_slot();
            }
        }

        self.inner.tasks.push(cx.waker().clone());
        Poll::Pending
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

    fn handle_futures(&mut self, cx: &mut Context<'_>) -> Result<()> {
        let len = self.inner.new.len().min(PENDING_OPEN_POLL_BUDGET);
        let mut first_err = None;
        let mut wake_waiters = false;

        for _ in 0..len {
            let Some(mut new) = self.inner.new.pop() else {
                break;
            };

            // The pending-open reservation moves with this future while it is polled.
            // Terminal paths release it; pending futures keep it when requeued.
            match new.poll_unpin(cx) {
                Poll::Ready(Ok(client)) => {
                    self.inner.release_pending_open();
                    if self.inner.idle.push(client).is_err() {
                        self.inner.release_conn_slot();
                        warn!(
                            "Ready ClickHouse connection could not be added to idle pool; closing it. pool_info={:?}",
                            self.info()
                        );
                    }
                    wake_waiters = true;
                }
                Poll::Pending => {
                    if self.inner.new.push(new).is_err() {
                        self.inner.release_pending_open();
                        self.inner.release_conn_slot();
                        wake_waiters = true;
                    }
                }
                Poll::Ready(Err(err)) => {
                    self.inner.release_pending_open();
                    self.inner.release_conn_slot();
                    wake_waiters = true;
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

        if wake_waiters {
            self.inner.wake_tasks();
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
            Arc,
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

    use super::{Pool, PoolBinding};
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

    fn reserve_pending_open(pool: &Pool) {
        reserve_conn(pool);
        assert!(pool
            .inner
            .try_reserve_pending_open(pool.pending_open_limit()));
    }

    fn replace_pending_open_with(
        pool: &Pool,
        new: futures_util::future::BoxFuture<'static, Result<ClientHandle>>,
    ) {
        assert!(pool.inner.new.pop().is_some());
        assert!(pool.inner.new.push(new).is_ok());
    }

    fn count_waker() -> (Arc<AtomicUsize>, Waker) {
        let wakes = Arc::new(AtomicUsize::new(0));
        (wakes.clone(), Waker::from(Arc::new(CountWake { wakes })))
    }

    #[tokio::test]
    async fn test_connect() -> Result<()> {
        let options = Options::from_str(DATABASE_URL.as_str()).unwrap();
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

        let pool = Pool::new(DATABASE_URL.as_str());
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
        assert!(spent < Duration::from_millis(2500));

        assert_eq!(pool.info().idle_len, 6);
        Ok(())
    }

    #[tokio::test]
    async fn test_wrong_insert() -> Result<()> {
        let pool = Pool::new(DATABASE_URL.as_str());
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
        let pool = Pool::new(DATABASE_URL.as_str());
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
    async fn test_get_handle_limits_pending_connections_during_cold_burst() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let mut accepted = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                accepted.push(stream);
            }
        });

        let pool_max = 16;
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

                let pending_limit = pool.pending_open_limit();
                if pool.info().new_len == pending_limit
                    && pool.inner.pending_open_count() == pending_limit
                    && pool.inner.conn_count() == pending_limit
                {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }),
        )
        .await
        .unwrap();

        let pending_limit = pool.pending_open_limit();
        assert_eq!(pool.info().new_len, pending_limit);
        assert_eq!(pool.inner.pending_open_count(), pending_limit);
        assert_eq!(pool.inner.conn_count(), pending_limit);
        assert!(pending_limit < pool_max);
        accept.abort();
        Ok(())
    }

    #[tokio::test]
    async fn test_get_handle_bounds_pending_open_polling_during_cold_burst() -> Result<()> {
        let pool_max = 16;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));
        let polls = Arc::new(AtomicUsize::new(0));
        let pending_limit = pool.pending_open_limit();

        for _ in 0..pending_limit {
            reserve_pending_open(&pool);
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
        assert_eq!(pool.info().new_len, pending_limit);
        assert_eq!(pool.inner.pending_open_count(), pending_limit);
        assert!(poll_count <= pool_max * super::PENDING_OPEN_POLL_BUDGET);
        assert!(poll_count < pool_max * pool_max);
        Ok(())
    }

    #[tokio::test]
    async fn test_completed_pending_open_wakes_parked_waiter() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(0).pool_max(1));
        reserve_pending_open(&pool);
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

        let client = synthetic_idle_client(&pool).await;
        replace_pending_open_with(&pool, Box::pin(future::ready(Ok(client))));

        let mut driver = pool.clone();
        driver.handle_futures(&mut cx)?;

        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        let handle = match waiter.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(handle)) => handle,
            _ => panic!("waiter did not acquire the completed pending connection"),
        };
        assert_eq!(pool.info().ongoing, 1);
        drop(handle);
        assert_eq!(pool.info().ongoing, 0);
        assert_eq!(pool.inner.conn_count(), 0);
        assert_eq!(pool.inner.pending_open_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_pending_failure_wakes_live_waiter_after_stale_waiter() -> Result<()> {
        let pool = Pool::new(Options::default().pool_min(0).pool_max(1));
        reserve_pending_open(&pool);
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

        replace_pending_open_with(
            &pool,
            Box::pin(future::ready(Err(Error::Driver(DriverError::Timeout)))),
        );

        let mut driver = pool.clone();
        assert!(driver.handle_futures(&mut live_cx).is_err());

        assert_eq!(stale_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(live_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(pool.info().tasks_len, 0);
        assert_eq!(pool.inner.conn_count(), 0);
        assert_eq!(pool.inner.pending_open_count(), 0);
        drop(live_waiter);
        Ok(())
    }

    #[tokio::test]
    async fn test_pending_open_wakes_parked_waiter() -> Result<()> {
        let pool_max = 2;
        let pool = Pool::new(Options::default().pool_min(0).pool_max(pool_max));
        for _ in 0..pool_max {
            reserve_pending_open(&pool);
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
            replace_pending_open_with(
                &pool,
                Box::pin(future::ready(Err(Error::Driver(DriverError::Timeout)))),
            );
        }

        let mut driver = pool.clone();
        assert!(driver.handle_futures(&mut cx).is_err());

        assert_eq!(wakes.load(Ordering::SeqCst), pool_max);
        assert_eq!(pool.info().tasks_len, 0);
        assert_eq!(pool.info().new_len, 0);
        assert_eq!(pool.inner.conn_count(), 0);
        assert_eq!(pool.inner.pending_open_count(), 0);
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
        reserve_pending_open(&pool);
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
        assert_eq!(pool.inner.pending_open_count(), 0);
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
        assert!(pool
            .inner
            .try_reserve_pending_open(pool.pending_open_limit()));
        let client = synthetic_idle_client(&pool).await;
        assert!(pool
            .inner
            .new
            .push(Box::pin(future::ready(Ok(client))))
            .is_ok());

        let (wakes, waker) = count_waker();
        pool.inner.tasks.push(waker.clone());
        let mut cx = Context::from_waker(&waker);
        let mut driver = pool.clone();

        assert_eq!(pool.info().tasks_len, 1);
        driver.handle_futures(&mut cx)?;

        let info = pool.info();
        assert_eq!(info.new_len, 0);
        assert_eq!(info.idle_len, pool_max);
        assert_eq!(info.tasks_len, 0);
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(pool.inner.conn_count(), pool_max);
        assert_eq!(pool.inner.pending_open_count(), 0);
        Ok(())
    }
}
