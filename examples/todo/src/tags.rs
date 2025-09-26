/// Non cruding functions for the tags domain
mod custom;

use chrono::{DateTime, Utc};
use cruding::{Crudable, axum_api::types::CrudableAxum, pg_source::PostgresCrudableTable};
use sea_orm::{ActiveModelBehavior, DeriveEntityModel, prelude::*};
use serde::{Deserialize, Serialize};

pub use custom::{
    PostgresTagsRepoImpl, TagsCounterHandler, TagsRepo, build_tags_counter_handler,
    tag_like_filter, tags_counter::Entity as TagsCounterEntity, update_counters,
};

#[derive(Debug, Clone, Serialize, Deserialize, DeriveEntityModel)]
#[sea_orm(table_name = "tags")]
pub struct Model {
    // server side fields
    #[serde(default)]
    creation_time: DateTime<Utc>,
    #[serde(default)]
    update_time: DateTime<Utc>,

    // client fields
    #[sea_orm(primary_key)]
    tag: String,
    #[sea_orm(primary_key)]
    entity_id: String,
    #[sea_orm(primary_key)]
    entity_type: String,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "custom::tags_counter::Entity",
        from = "Column::Tag",
        to = "custom::tags_counter::Column::Tag",
        on_delete = "Cascade"
    )]
    TagsCounter,
}

impl Related<custom::tags_counter::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::TagsCounter.def()
    }
}

// domain implementation

impl Model {
    pub fn initialize(&mut self, now: DateTime<Utc>) {
        self.creation_time = now;
        self.update_time = now;
    }
}

// start of cruding stuff

// Need to implement this for cruding stuff, rust should provide this implementation as a code
// action
impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl Crudable for Model {
    // This model has a composite primary key. It's important that the order is maintained!
    type Pkey = (String, String, String);
    type MonoField = DateTime<Utc>;

    fn pkey(&self) -> Self::Pkey {
        (
            self.tag.clone(),
            self.entity_id.clone(),
            self.entity_type.clone(),
        )
    }

    fn mono_field(&self) -> Self::MonoField {
        self.update_time
    }
}

/// Helper to make queries about todo
#[derive(Debug, Serialize, Deserialize)]
pub struct TagIdHelper {
    tag: String,
    entity_id: String,
    entity_type: String,
}

impl From<TagIdHelper> for (String, String, String) {
    fn from(value: TagIdHelper) -> Self {
        (value.tag, value.entity_id, value.entity_type)
    }
}

impl CrudableAxum for Model {
    type PkeyDe = TagIdHelper;
}

impl PostgresCrudableTable for Entity {
    fn get_pkey_filter(
        keys: &[<Self::Model as Crudable>::Pkey],
    ) -> impl sea_orm::sea_query::IntoCondition {
        Expr::tuple([
            Expr::column(Column::Tag),
            Expr::column(Column::EntityId),
            Expr::column(Column::EntityType),
        ])
        .is_in(keys.iter().map(|ids| {
            Expr::tuple([
                Expr::value(ids.0.clone()),
                Expr::value(ids.1.clone()),
                Expr::value(ids.2.clone()),
            ])
        }))
    }

    fn get_pkey_columns() -> Vec<Self::Column> {
        vec![Column::Tag, Column::EntityId, Column::EntityType]
    }
}
