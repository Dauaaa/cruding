use async_trait::async_trait;

use crate::{Crudable, CrudableHook, CrudableMap, CrudableSource, UpdateComparingParams};
use std::sync::Arc;

pub enum MaybeArc<T> {
    Arced(Arc<T>),
    Owned(T),
}

type CreateUpdateHook<Handler, CRUD, Ctx, Error> = dyn CrudableHook<In = Vec<CRUD>, Out = Vec<CRUD>, Error = Error, Ctx = Ctx, Handler = Handler>
    + Send
    + Sync
    + 'static;
type UpdateComparingHook<Handler, CRUD, Ctx, Error> = dyn CrudableHook<
        In = UpdateComparingParams<CRUD>,
        Out = Vec<CRUD>,
        Error = Error,
        Ctx = Ctx,
        Handler = Handler,
    > + Send
    + Sync
    + 'static;
type BeforeReadDeleteHook<Handler, Pkey, Ctx, Error> = dyn CrudableHook<In = Vec<Pkey>, Out = Vec<Pkey>, Error = Error, Ctx = Ctx, Handler = Handler>
    + Send
    + Sync
    + 'static;
type AfterReadHook<Handler, CRUD, Ctx, Error> = dyn CrudableHook<
        In = Vec<MaybeArc<CRUD>>,
        Out = Vec<MaybeArc<CRUD>>,
        Error = Error,
        Ctx = Ctx,
        Handler = Handler,
    > + Send
    + Sync
    + 'static;
type BeforeDeleteResolvedHook<Handler, CRUD, Ctx, Error> = dyn CrudableHook<In = Vec<MaybeArc<CRUD>>, Out = (), Error = Error, Ctx = Ctx, Handler = Handler>
    + Send
    + Sync
    + 'static;

pub struct CrudableHandlerImpl<
    CRUD: Crudable,
    Map: CrudableMap<CRUD>,
    Source: CrudableSource<CRUD>,
    Ctx,
    Error: From<Source::Error>,
> {
    map: Map,
    source: Source,

    before_create: Option<Box<CreateUpdateHook<Self, CRUD, Ctx, Error>>>,

    before_read: Option<Box<BeforeReadDeleteHook<Self, CRUD::Pkey, Ctx, Error>>>,
    after_read: Option<Box<AfterReadHook<Self, CRUD, Ctx, Error>>>,

    before_update: Option<Box<CreateUpdateHook<Self, CRUD, Ctx, Error>>>,
    update_comparing: Option<Box<UpdateComparingHook<Self, CRUD, Ctx, Error>>>,

    before_delete: Option<Box<BeforeReadDeleteHook<Self, CRUD::Pkey, Ctx, Error>>>,
    before_delete_resolved: Option<Box<BeforeDeleteResolvedHook<Self, CRUD, Ctx, Error>>>,
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
        ctx: &mut Ctx,
        source_handle: &mut SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error>;
    async fn read(
        &self,
        input: Vec<<CRUD as Crudable>::Pkey>,
        ctx: &mut Ctx,
        source_handle: &mut SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error>;
    async fn update(
        &self,
        input: Vec<CRUD>,
        ctx: &mut Ctx,
        source_handle: &mut SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error>;
    async fn delete(
        &self,
        input: Vec<<CRUD as Crudable>::Pkey>,
        ctx: &mut Ctx,
        source_handle: &mut SourceHandle,
    ) -> Result<(), Error>;
}

#[async_trait]
impl<CRUD, Map, Source, Ctx, SourceHandle, Error, DbError>
    CrudableHandler<CRUD, Ctx, SourceHandle, Error>
    for CrudableHandlerImpl<CRUD, Map, Source, Ctx, Error>
where
    CRUD: Crudable,
    Map: CrudableMap<CRUD>,
    Source: CrudableSource<CRUD, Error = DbError, SourceHandle = SourceHandle>,
    Ctx: Send,
    SourceHandle: Send,
    Error: From<DbError> + Send,
    DbError: Send,
{
    async fn create(
        &self,
        input: Vec<CRUD>,
        ctx: &mut Ctx,
        source_handle: &mut SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        self.create_inner(input, ctx, source_handle).await
    }

    async fn read(
        &self,
        input: Vec<<CRUD as Crudable>::Pkey>,
        ctx: &mut Ctx,
        source_handle: &mut SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        self.read_inner(input, ctx, source_handle).await
    }

    async fn update(
        &self,
        input: Vec<CRUD>,
        ctx: &mut Ctx,
        source_handle: &mut SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        self.update_inner(input, ctx, source_handle).await
    }

    async fn delete(
        &self,
        input: Vec<CRUD::Pkey>,
        ctx: &mut Ctx,
        source_handle: &mut Source::SourceHandle,
    ) -> Result<(), Error> {
        self.delete_inner(input, ctx, source_handle).await
    }
}

impl<CRUD, Map, Source, Ctx, SourceHandle, Error> CrudableHandlerImpl<CRUD, Map, Source, Ctx, Error>
where
    CRUD: Crudable,
    Map: CrudableMap<CRUD>,
    Source: CrudableSource<CRUD, SourceHandle = SourceHandle>,
    Ctx: Send,
    SourceHandle: Send,
    Error: From<Source::Error> + Send,
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
        }
    }

    pub fn install_before_create(
        mut self,
        hook: Box<CreateUpdateHook<Self, CRUD, Ctx, Error>>,
    ) -> Self {
        self.before_create = Some(hook);
        self
    }
    pub fn install_before_read(
        mut self,
        hook: Box<BeforeReadDeleteHook<Self, CRUD::Pkey, Ctx, Error>>,
    ) -> Self {
        self.before_read = Some(hook);
        self
    }
    pub fn install_after_read(mut self, hook: Box<AfterReadHook<Self, CRUD, Ctx, Error>>) -> Self {
        self.after_read = Some(hook);
        self
    }
    pub fn install_before_update(
        mut self,
        hook: Box<CreateUpdateHook<Self, CRUD, Ctx, Error>>,
    ) -> Self {
        self.before_update = Some(hook);
        self
    }
    pub fn install_update_comparing(
        mut self,
        hook: Box<UpdateComparingHook<Self, CRUD, Ctx, Error>>,
    ) -> Self {
        self.update_comparing = Some(hook);
        self
    }
    pub fn install_before_delete(
        mut self,
        hook: Box<BeforeReadDeleteHook<Self, CRUD::Pkey, Ctx, Error>>,
    ) -> Self {
        self.before_delete = Some(hook);
        self
    }
    pub fn install_before_delete_resolved(
        mut self,
        hook: Box<BeforeDeleteResolvedHook<Self, CRUD, Ctx, Error>>,
    ) -> Self {
        self.before_delete_resolved = Some(hook);
        self
    }

    #[tracing::instrument(skip_all)]
    async fn create_inner(
        &self,
        mut input: Vec<CRUD>,
        ctx: &mut Ctx,
        source_handle: &mut Source::SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        if let Some(ref hook) = self.before_create {
            input = hook.invoke(self, input, ctx).await?;
        }

        input = self
            .source
            .create(input, source_handle)
            .await
            .map_err(Into::into)?;

        if self.source.should_use_cache(source_handle) {
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

    #[tracing::instrument(skip_all)]
    async fn read_inner(
        &self,
        mut input: Vec<CRUD::Pkey>,
        ctx: &mut Ctx,
        source_handle: &mut Source::SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        if let Some(ref hook) = self.before_read {
            input = hook.invoke(self, input.clone(), ctx).await?;
        }

        let mut items = if self.source.should_use_cache(source_handle) {
            let (mut items, missed_keys) = self.get_from_map(&input).await;

            items.extend(
                self.persist_to_map(
                    self.source
                        .read(&missed_keys, source_handle)
                        .await
                        .map_err(Into::into)?,
                )
                .await
                .into_iter()
                .map(MaybeArc::Arced),
            );

            items
        } else {
            self.source
                .read(&input, source_handle)
                .await
                .map_err(Into::into)?
                .into_iter()
                .map(MaybeArc::Owned)
                .collect()
        };

        if let Some(ref hook) = self.after_read {
            items = hook.invoke(self, items, ctx).await?;
        }

        Ok(items)
    }

    #[tracing::instrument(skip_all)]
    async fn update_inner(
        &self,
        mut input: Vec<CRUD>,
        ctx: &mut Ctx,
        source_handle: &mut Source::SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        if let Some(ref hook) = self.before_update {
            input = hook.invoke(self, input, ctx).await?;
        }

        let keys = input.iter().map(CRUD::pkey).collect::<Vec<_>>();

        let current = self
            .source
            .read_for_update(&keys, source_handle)
            .await
            .map_err(Into::into)?;

        if let Some(ref hook) = self.update_comparing {
            input = hook
                .invoke(
                    self,
                    UpdateComparingParams {
                        current,
                        update_payload: input,
                    },
                    ctx,
                )
                .await?;
        }

        input = self
            .source
            .update(input, source_handle)
            .await
            .map_err(Into::into)?;

        if self.source.should_use_cache(source_handle) {
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

    #[tracing::instrument(skip_all)]
    async fn delete_inner(
        &self,
        mut input: Vec<CRUD::Pkey>,
        ctx: &mut Ctx,
        source_handle: &mut Source::SourceHandle,
    ) -> Result<(), Error> {
        if let Some(ref hook) = self.before_delete {
            input = hook.invoke(self, input, ctx).await?;
        }

        let items = if self.source.should_use_cache(source_handle) {
            let (mut items, missed_keys) = self.get_from_map(&input).await;

            items.extend(
                self.persist_to_map(
                    self.source
                        .read(&missed_keys, source_handle)
                        .await
                        .map_err(Into::into)?,
                )
                .await
                .into_iter()
                .map(MaybeArc::Arced),
            );

            items
        } else {
            self.source
                .read(&input, source_handle)
                .await
                .map_err(Into::into)?
                .into_iter()
                .map(MaybeArc::Owned)
                .collect()
        };

        if let Some(ref hook) = self.before_delete_resolved {
            hook.invoke(self, items, ctx).await?;
        }

        self.source
            .delete(&input, source_handle)
            .await
            .map_err(Into::into)?;

        if self.source.should_use_cache(source_handle) {
            self.invalidate_from_map(&input).await;
        }

        Ok(())
    }

    async fn persist_to_map(&self, input: Vec<CRUD>) -> Vec<Arc<CRUD>> {
        let mut res = Vec::with_capacity(input.len());

        for item in input {
            res.push(self.map.insert(item).await);
        }

        res
    }

    async fn get_from_map(&self, keys: &[CRUD::Pkey]) -> (Vec<MaybeArc<CRUD>>, Vec<CRUD::Pkey>) {
        let mut res_hit = Vec::with_capacity(keys.len());
        let mut res_miss = Vec::with_capacity(keys.len());

        for key in keys {
            if let Some(item) = self.map.get(key).await {
                res_hit.push(MaybeArc::Arced(item));
            } else {
                res_miss.push(key.clone());
            }
        }

        (res_hit, res_miss)
    }

    async fn invalidate_from_map(&self, keys: &[CRUD::Pkey]) {
        for key in keys {
            self.map.invalidate(key).await;
        }
    }
}

pub trait CrudableHandlerGetter<CRUD, Ctx, SourceHandle, Error>: Clone + Send + Sync {
    fn handler(&self) -> &dyn CrudableHandler<CRUD, Ctx, SourceHandle, Error>;
}

#[async_trait]
impl<CRUD, Ctx, SourceHandle, Error, CAH> CrudableHandler<CRUD, Ctx, SourceHandle, Error> for CAH
where
    CRUD: Crudable,
    Ctx: Send,
    Error: Send,
    SourceHandle: Send,
    CAH: CrudableHandlerGetter<CRUD, Ctx, SourceHandle, Error>,
{
    async fn create(
        &self,
        input: Vec<CRUD>,
        ctx: &mut Ctx,
        source_handle: &mut SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        self.handler().create(input, ctx, source_handle).await
    }

    async fn read(
        &self,
        input: Vec<<CRUD as Crudable>::Pkey>,
        ctx: &mut Ctx,
        source_handle: &mut SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        self.handler().read(input, ctx, source_handle).await
    }

    async fn update(
        &self,
        input: Vec<CRUD>,
        ctx: &mut Ctx,
        source_handle: &mut SourceHandle,
    ) -> Result<Vec<MaybeArc<CRUD>>, Error> {
        self.handler().update(input, ctx, source_handle).await
    }

    async fn delete(
        &self,
        input: Vec<<CRUD as Crudable>::Pkey>,
        ctx: &mut Ctx,
        source_handle: &mut SourceHandle,
    ) -> Result<(), Error> {
        self.handler().delete(input, ctx, source_handle).await
    }
}
