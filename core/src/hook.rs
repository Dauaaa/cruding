use std::{future::Future, marker::PhantomData, sync::Arc};

use async_trait::async_trait;

/// Hooks are called throughout the cruding handler implementation so a user may install handlers that shape
/// a specific cruding implementation.
#[async_trait]
pub trait CrudableHook {
    type In: Send;
    type Out: Send;
    type Error: Send;
    type Ctx: Send;
    type Handler: Send + Sync;
    type SourceHandle: Send;

    async fn invoke<'a>(
        &'a self,
        handler: Self::Handler,
        input: Self::In,
        ctx: Self::Ctx,
        handle: Self::SourceHandle,
    ) -> Result<Self::Out, Self::Error>;
}

pub struct HookFn<Handler, Ctx, In, Out, SourceHandle, Error, F> {
    f: F,
    _ph: PhantomData<(Handler, Ctx, In, Out, SourceHandle, Error)>,
}

impl<Handler, Ctx, In, Out, SourceHandle, Error, F>
    HookFn<Handler, Ctx, In, Out, SourceHandle, Error, F>
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _ph: PhantomData,
        }
    }
}

/// All functions with signature like
///
/// async fn(&Handler, In, &mut Ctx) -> Result<Out, Error>
///
/// Automatically implement hook
#[async_trait]
impl<Handler, Ctx, In, Out, Error, SourceHandle, F, Fut> CrudableHook
    for HookFn<Handler, Ctx, In, Out, SourceHandle, Error, F>
where
    Handler: Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
    Error: Send + Sync + 'static,
    SourceHandle: Send + Sync + 'static,
    F: for<'a, 'b> Fn(Handler, In, Ctx, SourceHandle) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Out, Error>> + Send + 'static,
{
    type In = In;
    type Out = Out;
    type Error = Error;
    type Ctx = Ctx;
    type Handler = Handler;
    type SourceHandle = SourceHandle;

    async fn invoke<'a>(
        &'a self,
        handler: Self::Handler,
        input: Self::In,
        ctx: Self::Ctx,
        handle: Self::SourceHandle,
    ) -> Result<Self::Out, Self::Error> {
        (self.f)(handler, input, ctx, handle).await
    }
}

pub fn make_crudable_hook<Handler, In, Ctx, Out, SourceHandle, Error, F, Fut>(
    f: F,
) -> Arc<
    dyn CrudableHook<
            In = In,
            Out = Out,
            Error = Error,
            Ctx = Ctx,
            SourceHandle = SourceHandle,
            Handler = Handler,
        > + Send
        + Sync,
>
where
    F: for<'a, 'b> Fn(Handler, In, Ctx, SourceHandle) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Out, Error>> + Send + 'static,
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
    Error: Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
    SourceHandle: Send + Sync + 'static,
    Handler: Send + Sync + 'static,
{
    Arc::new(HookFn::new(f))
}
