/// Non cruding functions for the tags domain
mod custom;

use chrono::{DateTime, Utc};
use cruding::Crudable;
use sea_orm::{ActiveModelBehavior, DeriveEntityModel, prelude::*};
use serde::{Deserialize, Serialize};

pub use custom::{
    PostgresTagsRepoImpl, TagsCounterHandler, TagsRepo, build_tags_counter_handler,
    tag_like_filter, tags_counter, tags_counter::Entity as TagsCounterEntity, update_counters,
};

#[cruding_macros::crudable_seaorm(axum)]
#[derive(Debug, Clone, Serialize, Deserialize, DeriveEntityModel)]
#[sea_orm(table_name = "tags")]
pub struct Model {
    // server side fields
    #[serde(default)]
    creation_time: DateTime<Utc>,
    #[serde(default)]
    #[crudable(mono)]
    update_time: DateTime<Utc>,

    // client fields
    #[sea_orm(primary_key, auto_increment = false)]
    tag: String,
    #[sea_orm(primary_key, auto_increment = false)]
    entity_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    entity_type: String,
}

impl ActiveModelBehavior for ActiveModel {}

// not actual fk relation
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "custom::tags_counter::Entity",
        from = "Column::Tag",
        to = "custom::tags_counter::Column::Tag"
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
    pub fn tag(&self) -> &String {
        &self.tag
    }
    pub fn initialize(&mut self, now: DateTime<Utc>) {
        self.creation_time = now;
        self.update_time = now;
    }
}
