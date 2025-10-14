use async_trait::async_trait;
use futures::future::try_join_all;
use tokio::time::Instant;

use crate::{
    Crudable, CrudableHook, CrudableInvalidateCause, CrudableMap, CrudableSource,
    UpdateComparingParams,
    list::{CrudableSourceListExt, CrudingListParams},
};
use std::{
    collections::{HashMap, HashSet},
    num::{NonZeroU8, NonZeroU32},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

pub enum MaybeArc<T> {
    Arced(Arc<T>),
    Owned(T),
}

impl<T> MaybeArc<T>
where
    T: Clone,
{
    pub fn take_or_clone(self) -> T {
        match self {
            MaybeArc::Arced(arc_t) => (*arc_t).clone(),
            MaybeArc::Owned(t) => t,
        }
    }
}

impl<T> AsRef<T> for MaybeArc<T> {
    fn as_ref(&self) -> &T {
        match self {
            MaybeArc::Arced(arc_t) => arc_t.as_ref(),
            MaybeArc::Owned(t) => t,
        }
    }
}

type CreateUpdateHook<Handler, CRUD, Ctx, SourceHandle, Error> = dyn CrudableHook<
        In = Vec<CRUD>,
        Out = Vec<CRUD>,
        Error = Error,
        Ctx = Ctx,
        Handler = Handler,
        SourceHandle = SourceHandle,
    > + Send
    + Sync
    + 'static;
type UpdateComparingHook<Handler, CRUD, Ctx, SourceHandle, Error> = dyn CrudableHook<
        In = UpdateComparingParams<CRUD>,
        Out = Vec<CRUD>,
        Error = Error,
        Ctx = Ctx,
        Handler = Handler,
        SourceHandle = SourceHandle,
    > + Send
    + Sync
    + 'static;
type BeforeReadDeleteHook<Handler, Pkey, Ctx, SourceHandle, Error> = dyn CrudableHook<
        In = Vec<Pkey>,
        Out = Vec<Pkey>,
        Error = Error,
        Ctx = Ctx,
        Handler = Handler,
        SourceHandle = SourceHandle,
    > + Send
    + Sync
    + 'static;
type AfterReadHook<Handler, CRUD, Ctx, SourceHandle, Error> = dyn CrudableHook<
        In = Vec<MaybeArc<CRUD>>,
        Out = Vec<MaybeArc<CRUD>>,
        Error = Error,
        Ctx = Ctx,
        Handler = Handler,
        SourceHandle = SourceHandle,
    > + Send
    + Sync
    + 'static;
type BeforeDeleteResolvedHook<Handler, CRUD, Ctx, SourceHandle, Error> = dyn CrudableHook<
        In = Vec<MaybeArc<CRUD>>,
        Out = (),
        Error = Error,
        Ctx = Ctx,
        Handler = Handler,
        SourceHandle = SourceHandle,
    > + Send
    + Sync
    + 'static;
type BeforeReadListHook<Handler, Column, Ctx, SourceHandle, Error> = dyn CrudableHook<
        In = CrudingListParams<Column>,
        Out = CrudingListParams<Column>,
        Error = Error,
        Ctx = Ctx,
        Handler = Handler,
        SourceHandle = SourceHandle,
    > + Send
    + Sync
    + 'static;

#[allow(clippy::type_complexity)]
pub struct CrudableHandlerImpl<
    CRUD: Crudable,
    Map: CrudableMap<CRUD>,
    Source: CrudableSource<CRUD>,
    Ctx,
    Error: std::error::Error + From<Source::Error> + From<Arc<Source::Error>>,
    Column = (),
> {
    map: Map,
    source: Source,

    before_create: Option<Arc<CreateUpdateHook<Self, CRUD, Ctx, Source::SourceHandle, Error>>>,

    before_read:
        Option<Arc<BeforeReadDeleteHook<Self, CRUD::Pkey, Ctx, Source::SourceHandle, Error>>>,
    after_read: Option<Arc<AfterReadHook<Self, CRUD, Ctx, Source::SourceHandle, Error>>>,
    read_batcher_sender: Option<ReadBatcher<CRUD, Source>>,

    before_update: Option<Arc<CreateUpdateHook<Self, CRUD, Ctx, Source::SourceHandle, Error>>>,
    update_comparing:
        Option<Arc<UpdateComparingHook<Self, CRUD, Ctx, Source::SourceHandle, Error>>>,

    before_delete:
        Option<Arc<BeforeReadDeleteHook<Self, CRUD::Pkey, Ctx, Source::SourceHandle, Error>>>,
    before_delete_resolved:
        Option<Arc<BeforeDeleteResolvedHook<Self, CRUD, Ctx, Source::SourceHandle, Error>>>,

    before_read_list:
        Option<Arc<BeforeReadListHook<Self, Column, Ctx, Source::SourceHandle, Error>>>,
}

impl<CRUD, Map, Source, Ctx, Error, Column> Clone
    for CrudableHandlerImpl<CRUD, Map, Source, Ctx, Error, Column>
where
    CRUD: Crudable,
    Map: CrudableMap<CRUD>,
    Source: CrudableSource<CRUD>,
    Ctx: Clone,
    Error: std::error::Error + From<Source::Error> + From<Arc<Source::Error>>,
{
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
            source: self.source.clone(),
            before_create: self.before_create.clone(),
            before_read: self.before_read.clone(),
            after_read: self.after_read.clone(),
            read_batcher_sender: self.read_batcher_sender.clone(),
            before_update: self.before_update.clone(),
            update_comparing: self.update_comparing.clone(),
            before_delete: self.before_delete.clone(),
            before_delete_resolved: self.before_delete_resolved.clone(),
            before_read_list: self.before_read_list.clone(),
        }
    }
}

#[async_trait]
pub trait CrudableHandler<CRUD, Ctx, SourceHandle, Error>
where
    CRUD: Crudable,
    Ctx: Send,
    SourceHandle: Send,
    Error: Send,
{
    async fn create(
        &self,
        input: Vec<CRUD>,
        ctx: Ctx,
        source_handle: SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error>;
    async fn read(
        &self,
        input: Vec<<CRUD as Crudable>::Pkey>,
        ctx: Ctx,
        source_handle: SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error>;
    async fn update(
        &self,
        input: Vec<CRUD>,
        ctx: Ctx,
        source_handle: SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error>;
    async fn delete(
        &self,
        input: Vec<<CRUD as Crudable>::Pkey>,
        ctx: Ctx,
        source_handle: SourceHandle,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait CrudableHandlerListExt<CRUD, Ctx, SourceHandle, Error, Column>
where
    CRUD: Crudable,
    Ctx: Send,
    SourceHandle: Send,
    Error: Send,
    Column: FromStr + Send + Sync + 'static,
{
    async fn read_list(
        &self,
        params: CrudingListParams<Column>,
        ctx: Ctx,
        handle: SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error>;
}

#[async_trait]
impl<CRUD, Map, Source, Ctx, SourceHandle, Error, DbError, Column>
    CrudableHandlerListExt<CRUD, Ctx, SourceHandle, Error, Column>
    for CrudableHandlerImpl<CRUD, Map, Source, Ctx, Error, Column>
where
    CRUD: Crudable,
    Map: CrudableMap<CRUD>,
    Source: CrudableSourceListExt<CRUD, Column, Error = DbError, SourceHandle = SourceHandle>,
    Ctx: Clone + Send + 'static,
    SourceHandle: Clone + Send + 'static,
    Error: std::error::Error + From<DbError> + From<Arc<DbError>> + Send,
    DbError: Send + Sync,
    Column: std::fmt::Debug + FromStr + Send + Sync + 'static,
{
    #[tracing::instrument(skip(self, ctx, handle), err)]
    async fn read_list(
        &self,
        mut params: CrudingListParams<Column>,
        ctx: Ctx,
        handle: SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        if let Some(ref hook) = self.before_read_list {
            params = hook
                .invoke(self.clone(), params, ctx.clone(), handle.clone())
                .await?;
        }

        let ids = self.source.read_list_to_ids(params, handle.clone()).await?;

        self.read_inner(ids, ctx, handle).await
    }
}

#[async_trait]
impl<CRUD, Map, Source, Ctx, SourceHandle, Error, DbError, Column>
    CrudableHandler<CRUD, Ctx, SourceHandle, Error>
    for CrudableHandlerImpl<CRUD, Map, Source, Ctx, Error, Column>
where
    CRUD: Crudable,
    Map: CrudableMap<CRUD>,
    Source: CrudableSource<CRUD, Error = DbError, SourceHandle = SourceHandle>,
    Ctx: Clone + Send + 'static,
    SourceHandle: Clone + Send + 'static,
    Error: std::error::Error + From<DbError> + From<Arc<DbError>> + Send,
    DbError: Send + Sync,
{
    async fn create(
        &self,
        input: Vec<CRUD>,
        ctx: Ctx,
        source_handle: SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        self.create_inner(input, ctx, source_handle).await
    }

    async fn read(
        &self,
        input: Vec<<CRUD as Crudable>::Pkey>,
        ctx: Ctx,
        source_handle: SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        self.read_inner(input, ctx, source_handle).await
    }

    async fn update(
        &self,
        input: Vec<CRUD>,
        ctx: Ctx,
        source_handle: SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        self.update_inner(input, ctx, source_handle).await
    }

    async fn delete(
        &self,
        input: Vec<CRUD::Pkey>,
        ctx: Ctx,
        source_handle: Source::SourceHandle,
    ) -> Result<(), Error> {
        self.delete_inner(input, ctx, source_handle).await
    }
}

pub struct CrudableHandlerBatchReadsOpts<SourceHandle> {
    /// Total ms to wait before querying after receiving at least 1 id
    pub batch_time_ms: NonZeroU8,
    /// If number of unique ids over this threshold, will immediately execute query
    ///
    /// You can consider the hard cap as 2 times the soft cap
    pub soft_cap: NonZeroU32,
    /// Defines how many reads can happen concurrently. You can share this semaphore between
    /// different handlers to limit the application's overall read usage
    ///
    /// If this is ever dropped the batcher will panic
    pub read_semaphore: Arc<tokio::sync::Semaphore>,
    pub source_handle_for_reads: SourceHandle,
}

pub struct CrudableHandlerImplOpts<SourceHandle> {
    pub batch_reads: Option<CrudableHandlerBatchReadsOpts<SourceHandle>>,
}

struct ReadBatcher<CRUD, Source>
where
    CRUD: Crudable,
    Source: CrudableSource<CRUD>,
{
    chunk_tx: tokio::sync::mpsc::Sender<(
        tokio::sync::oneshot::Sender<BatcherResponse<CRUD::Pkey, CRUD, Source::Error>>,
        Vec<CRUD::Pkey>,
    )>,
    soft_cap: usize,
}

impl<CRUD, Source> Clone for ReadBatcher<CRUD, Source>
where
    CRUD: Crudable,
    Source: CrudableSource<CRUD>,
{
    fn clone(&self) -> Self {
        Self {
            chunk_tx: self.chunk_tx.clone(),
            soft_cap: self.soft_cap.clone(),
        }
    }
}

type BatcherResponse<Pkey, CRUD, Error> = Result<Arc<HashMap<Pkey, Arc<CRUD>>>, Arc<Error>>;

impl<CRUD, Source> ReadBatcher<CRUD, Source>
where
    CRUD: Crudable,
    Source: CrudableSource<CRUD>,
    Source::SourceHandle: Clone,
{
    fn new<Map: CrudableMap<CRUD>>(
        map: Map,
        source: Source,
        batch_opts: CrudableHandlerBatchReadsOpts<Source::SourceHandle>,
    ) -> Self {
        let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel(50);
        let batch_time_limit = Duration::from_millis(batch_opts.batch_time_ms.get() as _);
        let batch_soft_cap = batch_opts.soft_cap.get() as usize;
        let read_semaphore = batch_opts.read_semaphore;

        // reader loop
        tokio::spawn({
            async move {
                struct ReadBatcherCurrentSendInfo<CRUD, Source>
                where
                    CRUD: Crudable,
                    Source: CrudableSource<CRUD>,
                {
                    chunk: HashSet<CRUD::Pkey>,
                    requester_rx_channels: Vec<
                        tokio::sync::oneshot::Sender<
                            BatcherResponse<CRUD::Pkey, CRUD, Source::Error>,
                        >,
                    >,
                }

                let mut batch_info = None;

                let send_query = |batch_info: ReadBatcherCurrentSendInfo<CRUD, Source>| {
                    let read_semaphore = read_semaphore.clone();
                    let source = source.clone();
                    let source_handle = batch_opts.source_handle_for_reads.clone();
                    let map = map.clone();

                    tokio::spawn(async move {
                        let _permit = read_semaphore.acquire().await.unwrap();
                        let res = source
                            .read(
                                &batch_info.chunk.into_iter().collect::<Vec<_>>(),
                                source_handle,
                            )
                            .await
                            .map(|res| {
                                Arc::new(
                                    res.into_iter()
                                        .map(|crud| (crud.pkey(), Arc::new(crud)))
                                        .collect::<HashMap<_, _>>(),
                                )
                            })
                            .map_err(Arc::new);

                        if let Ok(ref res_ok) = res {
                            let values = res_ok
                                .values()
                                .map(|crud| (**crud).clone())
                                .collect::<Vec<_>>();
                            map.insert(values).await;
                        }

                        for requester_channel_tx in batch_info.requester_rx_channels {
                            // if error just drop, we don't care if no readers
                            let _ = requester_channel_tx.send(res.clone());
                        }
                    });
                };

                let mut batch_deadline = Instant::now();

                loop {
                    fn poll_chunks<'a, CRUD: Crudable, Source: CrudableSource<CRUD>>(
                        send_query: impl Fn(ReadBatcherCurrentSendInfo<CRUD, Source>) + 'a,
                        chunk_rx: &'a mut tokio::sync::mpsc::Receiver<(
                            tokio::sync::oneshot::Sender<
                                BatcherResponse<CRUD::Pkey, CRUD, Source::Error>,
                            >,
                            Vec<<CRUD as Crudable>::Pkey>,
                        )>,
                        batch_info: &'a mut Option<ReadBatcherCurrentSendInfo<CRUD, Source>>,
                        batch_deadline: &'a mut Instant,
                        batch_time_limit: Duration,
                        batch_soft_cap: usize,
                    ) -> impl Future<Output = bool> + 'a {
                        async move {
                            if let Some((requester_response_tx, chunk)) = chunk_rx.recv().await {
                                let batch_info_ref = batch_info.get_or_insert_with(|| {
                                    *batch_deadline = Instant::now() + batch_time_limit;
                                    ReadBatcherCurrentSendInfo::<CRUD, Source> {
                                        chunk: HashSet::with_capacity(batch_soft_cap * 2),
                                        requester_rx_channels: Vec::with_capacity(100),
                                    }
                                });

                                batch_info_ref.chunk.extend(chunk);
                                batch_info_ref
                                    .requester_rx_channels
                                    .push(requester_response_tx);

                                if batch_info_ref.chunk.len() >= batch_soft_cap {
                                    let batch_info = std::mem::take(batch_info).unwrap();

                                    send_query(batch_info);
                                }
                                false
                            } else {
                                true
                            }
                        }
                    }

                    let do_break = if batch_info.is_some() {
                        let now = Instant::now();

                        if batch_deadline < now {
                            let batch_info = std::mem::take(&mut batch_info).unwrap();

                            send_query(batch_info);
                            false
                        } else {
                            let alarm = tokio::time::sleep(batch_deadline - now);
                            tokio::select! {
                                do_break = poll_chunks(send_query, &mut chunk_rx, &mut batch_info, &mut batch_deadline, batch_time_limit, batch_soft_cap) => { do_break },
                                _ = alarm => {
                                    let batch_info = batch_info.take().unwrap();

                                    send_query(batch_info);
                                    false
                                }
                            }
                        }
                    } else {
                        poll_chunks(
                            send_query,
                            &mut chunk_rx,
                            &mut batch_info,
                            &mut batch_deadline,
                            batch_time_limit,
                            batch_soft_cap,
                        )
                        .await
                    };

                    if do_break {
                        if let Some(info) = batch_info.take() {
                            send_query(info);
                        }
                        break;
                    }
                }
            }
        });

        Self {
            chunk_tx,
            soft_cap: batch_opts.soft_cap.get() as _,
        }
    }

    async fn batch(&self, keys: &[CRUD::Pkey]) -> BatcherResponse<CRUD::Pkey, CRUD, Source::Error> {
        let mut responses = vec![];
        for chunk in keys.chunks(self.soft_cap) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.chunk_tx.send((tx, chunk.to_vec())).await.unwrap();
            responses.push(async move { rx.await.unwrap() });
        }

        let maps = try_join_all(responses).await?;

        let mut res = HashMap::with_capacity(keys.len());
        for map in maps {
            res.extend(map.iter().map(|(k, v)| (k.clone(), v.clone())));
        }

        Ok(Arc::new(res))
    }
}

impl<CRUD, Map, Source, Ctx, SourceHandle, Error, Column>
    CrudableHandlerImpl<CRUD, Map, Source, Ctx, Error, Column>
where
    CRUD: Crudable,
    Map: CrudableMap<CRUD>,
    Source: CrudableSource<CRUD, SourceHandle = SourceHandle>,
    Ctx: Clone + Send + 'static,
    SourceHandle: Clone + Send + 'static,
    Error: std::error::Error + From<Source::Error> + From<Arc<Source::Error>> + Send,
    Source::Error: Send,
{
    /// Create a new instance of a CrudableHandler, all hooks will be empty, you need to install
    /// them
    pub fn new(
        map: Map,
        source: Source,
        opts: CrudableHandlerImplOpts<Source::SourceHandle>,
    ) -> Self {
        let read_batcher_sender = if let Some(batch_read_opts) = opts.batch_reads {
            Some(ReadBatcher::new(
                map.clone(),
                source.clone(),
                batch_read_opts,
            ))
        } else {
            None
        };

        Self {
            map,
            source,
            read_batcher_sender,
            before_create: None,
            before_read: None,
            after_read: None,
            before_update: None,
            update_comparing: None,
            before_delete: None,
            before_delete_resolved: None,
            before_read_list: None,
        }
    }

    pub fn install_before_create(
        mut self,
        hook: Arc<CreateUpdateHook<Self, CRUD, Ctx, SourceHandle, Error>>,
    ) -> Self {
        self.before_create = Some(hook);
        self
    }
    pub fn install_before_read(
        mut self,
        hook: Arc<BeforeReadDeleteHook<Self, CRUD::Pkey, Ctx, SourceHandle, Error>>,
    ) -> Self {
        self.before_read = Some(hook);
        self
    }
    pub fn install_after_read(
        mut self,
        hook: Arc<AfterReadHook<Self, CRUD, Ctx, SourceHandle, Error>>,
    ) -> Self {
        self.after_read = Some(hook);
        self
    }
    pub fn install_before_update(
        mut self,
        hook: Arc<CreateUpdateHook<Self, CRUD, Ctx, SourceHandle, Error>>,
    ) -> Self {
        self.before_update = Some(hook);
        self
    }
    pub fn install_update_comparing(
        mut self,
        hook: Arc<UpdateComparingHook<Self, CRUD, Ctx, SourceHandle, Error>>,
    ) -> Self {
        self.update_comparing = Some(hook);
        self
    }
    pub fn install_before_delete(
        mut self,
        hook: Arc<BeforeReadDeleteHook<Self, CRUD::Pkey, Ctx, SourceHandle, Error>>,
    ) -> Self {
        self.before_delete = Some(hook);
        self
    }
    pub fn install_before_delete_resolved(
        mut self,
        hook: Arc<BeforeDeleteResolvedHook<Self, CRUD, Ctx, SourceHandle, Error>>,
    ) -> Self {
        self.before_delete_resolved = Some(hook);
        self
    }
    pub fn install_before_read_list(
        mut self,
        hook: Arc<BeforeReadListHook<Self, Column, Ctx, SourceHandle, Error>>,
    ) -> Self {
        self.before_read_list = Some(hook);
        self
    }

    #[tracing::instrument(skip_all, err)]
    async fn create_inner(
        &self,
        mut input: Vec<CRUD>,
        ctx: Ctx,
        source_handle: Source::SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        if let Some(ref hook) = self.before_create {
            input = hook
                .invoke(self.clone(), input, ctx.clone(), source_handle.clone())
                .await?;
        }

        if input.is_empty() {
            return Ok(vec![]);
        }

        input = self.source.create(input, source_handle.clone()).await?;

        if self.source.should_use_cache(source_handle.clone()).await {
            Ok(self
                .map
                .insert(input)
                .await
                .into_iter()
                .map(MaybeArc::Arced)
                .collect())
        } else {
            Ok(input.into_iter().map(MaybeArc::Owned).collect())
        }
    }

    #[tracing::instrument(skip_all, err)]
    async fn read_inner(
        &self,
        mut input: Vec<CRUD::Pkey>,
        ctx: Ctx,
        source_handle: Source::SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        if let Some(ref hook) = self.before_read {
            input = hook
                .invoke(
                    self.clone(),
                    input.clone(),
                    ctx.clone(),
                    source_handle.clone(),
                )
                .await?;
        }

        let read_source: Box<
            dyn FnOnce(
                    Vec<CRUD::Pkey>,
                ) -> Pin<
                    Box<dyn Future<Output = Result<Vec<Arc<CRUD>>, Arc<Source::Error>>> + Send>,
                > + Send,
        > = if let Some(ref batcher) = self.read_batcher_sender
            && self.source.can_use_batcher(source_handle.clone()).await
        {
            let batcher = batcher.clone();
            Box::new(move |keys: Vec<CRUD::Pkey>| {
                Box::pin(async move {
                    let keys_clone = keys.clone();
                    let items = batcher.batch(&keys).await?;

                    let mut res = Vec::with_capacity(keys_clone.len());
                    for key in keys_clone {
                        if let Some(crud) = items.get(&key) {
                            res.push(crud.clone())
                        }
                    }

                    Ok(res)
                })
            })
        } else {
            let handler = self.clone();
            let source_handle = source_handle.clone();

            Box::new(move |keys: Vec<CRUD::Pkey>| {
                Box::pin(async move {
                    Ok(handler
                        .map
                        .insert(
                            handler
                                .source
                                .read(&keys, source_handle.clone())
                                .await
                                .map_err(Arc::new)?,
                        )
                        .await)
                })
            })
        };

        let mut items = if self.source.should_use_cache(source_handle.clone()).await {
            let (mut items, missed_keys) = self.get_from_map(&input).await;

            // Inserting to cache, this takes care of invalidated cache entries.
            items.extend(
                read_source(missed_keys)
                    .await?
                    .into_iter()
                    .map(MaybeArc::Arced),
            );

            items
        } else {
            self.source
                .read(&input, source_handle.clone())
                .await?
                .into_iter()
                .map(MaybeArc::Owned)
                .collect()
        };

        if let Some(ref hook) = self.after_read {
            items = hook
                .invoke(self.clone(), items, ctx.clone(), source_handle)
                .await?;
        }

        Ok(items)
    }

    #[tracing::instrument(skip_all, err)]
    async fn update_inner(
        &self,
        mut input: Vec<CRUD>,
        ctx: Ctx,
        source_handle: Source::SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        if let Some(ref hook) = self.before_update {
            input = hook
                .invoke(self.clone(), input, ctx.clone(), source_handle.clone())
                .await?;
        }

        if input.is_empty() {
            return Ok(vec![]);
        }

        let keys = input.iter().map(CRUD::pkey).collect::<Vec<_>>();

        let current = self
            .source
            .read_for_update(&keys, source_handle.clone())
            .await?;

        if let Some(ref hook) = self.update_comparing {
            input = hook
                .invoke(
                    self.clone(),
                    UpdateComparingParams {
                        current,
                        update_payload: input,
                    },
                    ctx.clone(),
                    source_handle.clone(),
                )
                .await?;
        }

        if input.is_empty() {
            return Ok(vec![]);
        }

        input = self.source.update(input, source_handle.clone()).await?;

        if self.source.should_use_cache(source_handle.clone()).await {
            // Invalidate cache instead of updating it, only reading from the source should generate new entries in the cache
            let invalidated = input
                .iter()
                .map(|item| (item.pkey(), item.mono_field()))
                .collect::<Vec<_>>();
            self.map
                .invalidate(
                    invalidated.iter().map(|(a, b)| (a, b)),
                    CrudableInvalidateCause::Update,
                )
                .await;

            Ok(input.into_iter().map(MaybeArc::Owned).collect())
        } else {
            Ok(input.into_iter().map(MaybeArc::Owned).collect())
        }
    }

    #[tracing::instrument(skip_all, err)]
    async fn delete_inner(
        &self,
        mut input: Vec<CRUD::Pkey>,
        ctx: Ctx,
        source_handle: Source::SourceHandle,
    ) -> Result<(), Error> {
        if let Some(ref hook) = self.before_delete {
            input = hook
                .invoke(self.clone(), input, ctx.clone(), source_handle.clone())
                .await?;
        }

        let items = if self.source.should_use_cache(source_handle.clone()).await {
            let (mut items, missed_keys) = self.get_from_map(&input).await;

            items.extend(
                self.map
                    .insert(
                        self.source
                            .read(&missed_keys, source_handle.clone())
                            .await?,
                    )
                    .await
                    .into_iter()
                    .map(MaybeArc::Arced),
            );

            items
        } else {
            self.source
                .read(&input, source_handle.clone())
                .await?
                .into_iter()
                .map(MaybeArc::Owned)
                .collect()
        };

        if let Some(ref hook) = self.before_delete_resolved {
            hook.invoke(self.clone(), items, ctx.clone(), source_handle.clone())
                .await?;
        }

        let deleted = self
            .source
            .delete(&input, source_handle.clone())
            .await?
            .into_iter()
            .map(|crud| (crud.pkey(), crud.mono_field()))
            .collect::<Vec<_>>();

        if self.source.should_use_cache(source_handle.clone()).await {
            self.map
                .invalidate(
                    deleted.iter().map(|(a, b)| (a, b)),
                    CrudableInvalidateCause::Delete,
                )
                .await;
        }

        Ok(())
    }

    async fn get_from_map(&self, keys: &[CRUD::Pkey]) -> (Vec<MaybeArc<CRUD>>, Vec<CRUD::Pkey>) {
        let cached_items = self.map.get(keys).await;
        let mut res_hit = Vec::new();
        let mut res_miss = Vec::new();

        // Zip is guaranteed to be aligned because the order of items in cached_items is same as the corresponding primary keys in keys.
        for (key, cached_item) in keys.iter().zip(cached_items.iter()) {
            match cached_item {
                Some(item) => res_hit.push(MaybeArc::Arced(item.clone())),
                None => res_miss.push(key.clone()),
            }
        }

        (res_hit, res_miss)
    }
}

pub trait CrudableHandlerGetter<CRUD, Ctx, SourceHandle, Error>: Clone + Send + Sync {
    fn handler(&self) -> &dyn CrudableHandler<CRUD, Ctx, SourceHandle, Error>;
}

pub trait CrudableHandlerGetterListExt<CRUD, Ctx, SourceHandle, Error, Column>:
    Clone + Send + Sync
{
    fn handler_list(&self) -> &dyn CrudableHandlerListExt<CRUD, Ctx, SourceHandle, Error, Column>;
}
