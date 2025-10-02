use async_trait::async_trait;

use crate::{
    Crudable, CrudableHook, CrudableMap, CrudableSource, UpdateComparingParams,
    list::{CrudableSourceListExt, CrudingListParams},
};
use std::{str::FromStr, sync::Arc};

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
    Error: std::error::Error + From<Source::Error>,
    Column = (),
> {
    map: Map,
    source: Source,

    before_create: Option<Arc<CreateUpdateHook<Self, CRUD, Ctx, Source::SourceHandle, Error>>>,

    before_read:
        Option<Arc<BeforeReadDeleteHook<Self, CRUD::Pkey, Ctx, Source::SourceHandle, Error>>>,
    after_read: Option<Arc<AfterReadHook<Self, CRUD, Ctx, Source::SourceHandle, Error>>>,

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
    Error: std::error::Error + From<Source::Error>,
{
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
            source: self.source.clone(),
            before_create: self.before_create.clone(),
            before_read: self.before_read.clone(),
            after_read: self.after_read.clone(),
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
    Error: std::error::Error + From<DbError> + Send,
    DbError: Send,
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
    Error: std::error::Error + From<DbError> + Send,
    DbError: Send,
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

impl<CRUD, Map, Source, Ctx, SourceHandle, Error, Column>
    CrudableHandlerImpl<CRUD, Map, Source, Ctx, Error, Column>
where
    CRUD: Crudable,
    Map: CrudableMap<CRUD>,
    Source: CrudableSource<CRUD, SourceHandle = SourceHandle>,
    Ctx: Clone + Send + 'static,
    SourceHandle: Clone + Send + 'static,
    Error: std::error::Error + From<Source::Error> + Send,
    Source::Error: Send,
{
    /// Create a new instance of a CrudableHandler, all hooks will be empty, you need to install
    /// them
    pub fn new(map: Map, source: Source) -> Self {
        Self {
            map,
            source,
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
                .persist_to_map(input)
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

        let mut items = if self.source.should_use_cache(source_handle.clone()).await {
            let (mut items, missed_keys) = self.get_from_map(&input).await;

            // Inserting to cache, this takes care of invalidated cache entries.
            items.extend(
                self.persist_to_map(
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
            let keys: Vec<CRUD::Pkey> = input.iter().map(|item| item.pkey()).collect();
            self.invalidate_from_map(&keys).await;

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
                self.persist_to_map(
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

        self.source.delete(&input, source_handle.clone()).await?;

        if self.source.should_use_cache(source_handle.clone()).await {
            self.invalidate_from_map(&input).await;
        }

        Ok(())
    }

    async fn persist_to_map(&self, input: Vec<CRUD>) -> Vec<Arc<CRUD>> {
        self.map.insert(input).await
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

    async fn invalidate_from_map(&self, keys: &[CRUD::Pkey]) {
        self.map.invalidate(keys).await;
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
