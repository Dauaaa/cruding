use std::str::FromStr;

use crate::{Crudable, CrudableSource};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct CrudingListParams<Column> {
    pub filters: Vec<CrudingListFilter<Column>>,
    pub sorts: Vec<CrudingListSort<Column>>,
    pub pagination: CrudingListPagination,
}

#[derive(Debug, Clone)]
pub enum CrudingListFilterOperators {
    Eq(serde_json::Value),
    Neq(serde_json::Value),
    Gt(serde_json::Value),
    Ge(serde_json::Value),
    Lt(serde_json::Value),
    Le(serde_json::Value),
    In(Vec<serde_json::Value>),
    NotIn(Vec<serde_json::Value>),
}
#[derive(Debug, Clone)]
pub enum CrudingListSortOrder {
    Asc,
    Desc,
}
#[derive(Debug, Clone)]
pub struct CrudingListFilter<Column> {
    pub column: Column,
    pub op: CrudingListFilterOperators,
}
#[derive(Debug, Clone)]
pub struct CrudingListSort<Column> {
    pub column: Column,
    pub order: CrudingListSortOrder,
}
#[derive(Debug, Clone)]
pub struct CrudingListPagination {
    pub page: u32,
    pub size: u32,
}

#[async_trait]
pub trait CrudableSourceListExt<CRUD: Crudable, Column: FromStr + Send + Sync + 'static>:
    CrudableSource<CRUD>
{
    async fn read_list_to_ids(
        &self,
        params: CrudingListParams<Column>,
        handle: &mut Self::SourceHandle,
    ) -> Result<Vec<CRUD::Pkey>, Self::Error>;
}
