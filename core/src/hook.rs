use std::{future::Future, marker::PhantomData};

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

    async fn invoke<'a, 'b, 'c>(
        &'a self,
        handler: &'b Self::Handler,
        input: Self::In,
        ctx: &'c mut Self::Ctx,
    ) -> Result<Self::Out, Self::Error>;
}

pub struct HookFn<Handler, Ctx, In, Out, Error, F> {
    f: F,
    _ph: PhantomData<(Handler, Ctx, In, Out, Error)>,
}

impl<Handler, Ctx, In, Out, Error, F> HookFn<Handler, Ctx, In, Out, Error, F> {
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
impl<Handler, Ctx, In, Out, Error, F, Fut> CrudableHook for HookFn<Handler, Ctx, In, Out, Error, F>
where
    Handler: Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
    Error: Send + Sync + 'static,
    F: for<'a> Fn(&'a Handler, In, &'a mut Ctx) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Out, Error>> + Send + 'static,
{
    type In = In;
    type Out = Out;
    type Error = Error;
    type Ctx = Ctx;
    type Handler = Handler;

    async fn invoke(
        &self,
        handler: &Self::Handler,
        input: Self::In,
        ctx: &mut Self::Ctx,
    ) -> Result<Self::Out, Self::Error> {
        (self.f)(handler, input, ctx).await
    }
}

pub fn make_crudable_hook<Handler, In, Ctx, Out, Error, F, Fut>(
    f: F,
) -> Box<
    dyn CrudableHook<In = In, Out = Out, Error = Error, Ctx = Ctx, Handler = Handler> + Send + Sync,
>
where
    F: for<'a> Fn(&'a Handler, In, &'a mut Ctx) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Out, Error>> + Send + 'static,
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
    Error: Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
    Handler: Send + Sync + 'static,
{
    Box::new(HookFn::new(f))
}
