use std::sync::Arc;

use cruding_core::Crudable;
use serde::{Deserialize, Serialize};

pub trait CrudableAxum: Crudable + Serialize + for<'de> Deserialize<'de>
where
    <Self as Crudable>::Pkey: From<<Self as CrudableAxum>::PkeyDe>,
{
    type PkeyDe: for<'de> Deserialize<'de> + Send;
}

// Narrow “Column: FromStr + Send + Sync + 'static” into one bound
pub trait ColumnParse: std::str::FromStr + Send + Sync + 'static {}
impl<T: std::str::FromStr + Send + Sync + 'static> ColumnParse for T {}

/// Pack the per-request dyn context into one alias to shorten bounds.
pub type ReqCtx<AxumCtx, InnerCtx> = Arc<(AxumCtx, InnerCtx)>;
