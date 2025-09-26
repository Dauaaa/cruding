use sea_orm::{
    ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Schema, Statement,
    sea_query::PostgresQueryBuilder,
};

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_string())
}

async fn connect_and_prepare() -> DatabaseConnection {
    let conn = Database::connect(&db_url())
        .await
        .expect("connect postgres");

    let schema = Schema::new(DatabaseBackend::Postgres);
    let mut stmt = schema.create_table_from_entity(cruding_todo_example::tags::Entity);
    conn.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        stmt.if_not_exists().to_string(PostgresQueryBuilder),
    ))
    .await
    .ok();

    let schema = Schema::new(DatabaseBackend::Postgres);
    let mut stmt = schema.create_table_from_entity(cruding_todo_example::todo::Entity);
    conn.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        stmt.if_not_exists().to_string(PostgresQueryBuilder),
    ))
    .await
    .ok();

    conn
}

#[tokio::main]
async fn main() {
    connect_and_prepare().await;
}
