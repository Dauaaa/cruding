use chrono::{DateTime, Utc};
use sea_orm::{DeriveEntityModel, prelude::*};
use serde::{Deserialize, Serialize};

// normal sea_orm stuff
#[cruding_macros::crudable_seaorm(axum)]
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
    pub status: TodoStatus,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumIter, DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", enum_name = "todo_status")]
pub enum TodoStatus {
    #[sea_orm(string_value = "todo")]
    Todo,
    #[sea_orm(string_value = "in-progress")]
    InProgress,
    #[sea_orm(string_value = "done")]
    Done,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

// domain implementation

impl Model {
    pub fn initialize(&mut self, now: DateTime<Utc>) {
        self.id_1 = Uuid::new_v4();
        self.id_2 = u64::cast_signed(self.id_1.as_u64_pair().0);
        self.creation_time = now;
        self.update_time = now;
    }

    pub fn update_from_current(&mut self, current: &Self, now: DateTime<Utc>) {
        assert_eq!(self.id_1, current.id_1);
        assert_eq!(self.id_2, current.id_2);

        // persist fixed fields
        self.creation_time = current.creation_time;

        // apply business rules
        match (current.is_done(), self.is_done()) {
            (true, false) => self.done_time = Some(now),
            (false, true) => self.done_time = None,
            (true, true) | (false, false) => {}
        }

        // update mono field
        self.update_time = now;
    }

    pub fn is_done(&self) -> bool {
        matches!(self.status, TodoStatus::Done)
    }

    pub fn id_1(&self) -> Uuid {
        self.id_1
    }

    pub fn id_2(&self) -> i64 {
        self.id_2
    }
}
