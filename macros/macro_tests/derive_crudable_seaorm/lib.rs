#![allow(dead_code)]

mod todo {
    use chrono::{DateTime, Utc};
    use cruding_macros::crudable_seaorm;
    use sea_orm::{DeriveEntityModel, prelude::*};
    use serde::{Deserialize, Serialize};

    // normal sea_orm stuff
    #[crudable_seaorm]
    #[derive(Debug, Clone, Serialize, Deserialize, DeriveEntityModel)]
    #[sea_orm(table_name = "todos")]
    pub struct Model {
        // server side fields should all have defaults and are basically ignored when deserialized
        // from a client. You know the "directionality" of the struct depending on the hook.
        //
        // For example, before_create should reset all these fields while update_comparing should
        // set these values to the current/update mono
        #[sea_orm(primary_key, auto_increment = false)]
        #[serde(default)]
        id_1: Uuid,
        #[sea_orm(primary_key, auto_increment = false)]
        #[serde(default)]
        id_2: i64,
        #[serde(default)]
        creation_time: DateTime<Utc>,
        #[serde(default)]
        #[crudable(mono)]
        update_time: DateTime<Utc>,
        #[serde(default)]
        done_time: Option<DateTime<Utc>>,

        // client fields
        pub name: String,
        pub description: String,
    }

    impl ActiveModelBehavior for ActiveModel {}

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}
}
