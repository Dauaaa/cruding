#![allow(dead_code)]
use axum::{
    Router,
    http::{Request, header::CONTENT_TYPE},
    response::Response,
};
use cruding::{
    handler::CrudableHandlerListExt, list::CrudingListPagination,
    pg_source::PostgresCrudableConnection,
};
use cruding_todo_example::{AppState, todo::TodoStatus};
use futures::TryStreamExt;
use sea_orm::{ConnectionTrait, Database, DatabaseConnection};
use serde::{Deserialize, Serialize};
use tower::Service;
use tracing::level_filters::LevelFilter;
use uuid::Uuid;

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_string())
}

async fn connect_and_prepare() -> DatabaseConnection {
    let conn = Database::connect(&db_url())
        .await
        .expect("connect postgres");

    conn.execute_unprepared(
        r#"
CREATE TABLE IF NOT EXISTS "tags" (
    "creation_time" timestamp with time zone NOT NULL,
    "update_time" timestamp with time zone NOT NULL,
    "tag" varchar NOT NULL,
    "entity_id" varchar NOT NULL,
    "entity_type" varchar NOT NULL,
    CONSTRAINT "pk-tags" PRIMARY KEY ("tag", "entity_id", "entity_type")
);
CREATE TABLE IF NOT EXISTS "todos" (
    "id_1" uuid NOT NULL,
    "id_2" bigint NOT NULL,
    "creation_time" timestamp with time zone NOT NULL,
    "update_time" timestamp with time zone NOT NULL,
    "done_time" timestamp with time zone,
    "name" varchar NOT NULL,
    "description" varchar NOT NULL,
    "status" text NOT NULL,
    CONSTRAINT "pk-todos" PRIMARY KEY ("id_1", "id_2")
);
CREATE TABLE IF NOT EXISTS "tags_counter" (
    "tag" varchar NOT NULL PRIMARY KEY,
    "total" bigint NOT NULL,
    "creation_time" timestamp with time zone NOT NULL,
    "update_time" timestamp with time zone NOT NULL
);
TRUNCATE TABLE tags_counter; TRUNCATE TABLE tags; TRUNCATE TABLE todos;
"#,
    )
    .await
    .unwrap();

    conn
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .pretty()
        .init();

    let db_conn = connect_and_prepare().await;

    let state = AppState::new(db_conn).await;

    let todo_router = AppState::todo_router();
    let tags_router = AppState::tags_router();

    let mut app: Router<()> = Router::new()
        .merge(todo_router)
        .merge(tags_router)
        .with_state(state.clone());

    let todos: Vec<cruding_todo_example::todo::Model> = call(
        &mut app,
        todo_create_request(&vec![
            CreateTodoRequest {
                name: "my-todo".to_string(),
                description: "important things todo, non important things to procrastinate"
                    .to_string(),
                status: TodoStatus::Todo,
            },
            CreateTodoRequest {
                name: "todo-2".to_string(),
                description: "something".to_string(),
                status: TodoStatus::Todo,
            },
        ]),
    )
    .await
    .unwrap();

    assert_eq!(todos.len(), 2);

    let todo = todos.iter().find(|x| x.name == "my-todo").unwrap();
    let other_todo = todos.iter().find(|x| x.name != "my-todo").unwrap().clone();

    assert_eq!(todo.name, "my-todo");
    assert_eq!(
        todo.description,
        "important things todo, non important things to procrastinate"
    );
    assert_eq!(todo.status, TodoStatus::Todo);

    let mut todos_from_read: Vec<cruding_todo_example::todo::Model> = call(
        &mut app,
        todo_filter_request(r#"filter[name][=]="my-todo""#),
    )
    .await
    .unwrap();

    assert_eq!(todos_from_read.len(), 1);

    let todo = todos_from_read.pop().unwrap();

    assert_eq!(todo.name, "my-todo");
    assert_eq!(
        todo.description,
        "important things todo, non important things to procrastinate"
    );
    assert_eq!(todo.status, TodoStatus::Todo);

    let mut tags: Vec<cruding_todo_example::tags::Model> = call(
        &mut app,
        tag_create_request(&vec![build_todo_tag("hehexd", &todo)]),
    )
    .await
    .unwrap();

    assert_eq!(tags.len(), 1);

    let tag = tags.pop().unwrap();

    assert_eq!(tag.tag(), "hehexd");

    let mut tag_counters = state
        .tags_counter_handler
        .read_list(
            cruding::list::CrudingListParams {
                filters: vec![],
                sorts: vec![],
                pagination: CrudingListPagination { page: 0, size: 100 },
            },
            (),
            PostgresCrudableConnection::new(
                cruding::pg_source::PostgresCrudableConnectionInner::Connection(
                    state.db_conn.clone(),
                ),
            ),
        )
        .await
        .unwrap();
    assert_eq!(tag_counters.len(), 1);

    let tag_counter = tag_counters.pop().unwrap();
    assert_eq!(tag_counter.as_ref().tag(), "hehexd");
    assert_eq!(tag_counter.as_ref().count(), 1);

    let mut tags: Vec<cruding_todo_example::tags::Model> = call(
        &mut app,
        tag_create_request(&vec![build_todo_tag("hehexd", &other_todo)]),
    )
    .await
    .unwrap();

    assert_eq!(tags.len(), 1);

    let tag = tags.pop().unwrap();

    assert_eq!(tag.tag(), "hehexd");

    let mut tag_counters = state
        .tags_counter_handler
        .read_list(
            cruding::list::CrudingListParams {
                filters: vec![],
                sorts: vec![],
                pagination: CrudingListPagination { page: 0, size: 100 },
            },
            (),
            PostgresCrudableConnection::new(
                cruding::pg_source::PostgresCrudableConnectionInner::Connection(
                    state.db_conn.clone(),
                ),
            ),
        )
        .await
        .unwrap();
    assert_eq!(tag_counters.len(), 1);

    let tag_counter = tag_counters.pop().unwrap();
    assert_eq!(tag_counter.as_ref().tag(), "hehexd");
    assert_eq!(tag_counter.as_ref().count(), 1);

    state
        .tags_repo
        .update_counter(
            &state.tags_counter_handler,
            PostgresCrudableConnection::new(
                cruding::pg_source::PostgresCrudableConnectionInner::Connection(
                    state.db_conn.clone(),
                ),
            ),
            100,
        )
        .await
        .unwrap();

    let mut tag_counters = state
        .tags_counter_handler
        .read_list(
            cruding::list::CrudingListParams {
                filters: vec![],
                sorts: vec![],
                pagination: CrudingListPagination { page: 0, size: 100 },
            },
            (),
            PostgresCrudableConnection::new(
                cruding::pg_source::PostgresCrudableConnectionInner::Connection(
                    state.db_conn.clone(),
                ),
            ),
        )
        .await
        .unwrap();
    assert_eq!(tag_counters.len(), 1);

    let tag_counter = tag_counters.pop().unwrap();
    assert_eq!(tag_counter.as_ref().tag(), "hehexd");
    assert_eq!(tag_counter.as_ref().count(), 2);

    let mut read_todos: Vec<cruding_todo_example::todo::Model> = call(
        &mut app,
        todo_read_request(vec![TodoId {
            id_1: todo.id_1(),
            id_2: todo.id_2(),
        }]),
    )
    .await
    .unwrap();

    assert_eq!(read_todos.len(), 1);

    let read_todo = read_todos.pop().unwrap();
    assert_eq!(read_todo.name, todo.name);
    assert_eq!(read_todo.description, todo.description);
    assert_eq!(read_todo.status, todo.status);

    call_no_body(
        &mut app,
        todo_delete_request(vec![TodoId {
            id_1: todo.id_1(),
            id_2: todo.id_2(),
        }]),
    )
    .await
    .unwrap();
    let read_todos: Vec<cruding_todo_example::todo::Model> = call(
        &mut app,
        todo_read_request(vec![TodoId {
            id_1: todo.id_1(),
            id_2: todo.id_2(),
        }]),
    )
    .await
    .unwrap();
    assert!(read_todos.is_empty());
}

fn build_todo_tag(
    tag: impl Into<String>,
    todo: &cruding_todo_example::todo::Model,
) -> CreateTagRequest {
    CreateTagRequest {
        tag: tag.into(),
        entity_id: format!("{}_{}", todo.id_1(), todo.id_2()),
        entity_type: "todo".to_string(),
    }
}

#[tracing::instrument(skip(app))]
async fn call<T: for<'de> Deserialize<'de>>(
    app: &mut Router<()>,
    req: Request<axum::body::Body>,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    let response = app.call(req).await?;

    tracing::info!("{}", response.status());

    Ok(serde_json::from_slice(
        &response
            .into_body()
            .into_data_stream()
            .map_ok(|x| x.to_vec())
            .try_collect::<Vec<_>>()
            .await?
            .concat(),
    )?)
}

#[tracing::instrument(skip(app))]
async fn call_no_body(
    app: &mut Router<()>,
    req: Request<axum::body::Body>,
) -> Result<Response, Box<dyn std::error::Error + Send + Sync>> {
    let response = app.call(req).await?;

    tracing::info!("{}", response.status());

    Ok(response)
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateTodoRequest {
    name: String,
    description: String,
    status: TodoStatus,
}

#[derive(Debug, Serialize, Deserialize)]
struct TodoId {
    id_1: Uuid,
    id_2: i64,
}

#[derive(Debug, Serialize)]
struct CreateTagRequest {
    tag: String,
    entity_id: String,
    entity_type: String,
}

fn todo_create_request(input: &Vec<CreateTodoRequest>) -> Request<axum::body::Body> {
    Request::builder()
        .uri("/todo/create")
        .method("POST")
        .header(CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&input).unwrap()))
        .unwrap()
}

fn todo_update_request(input: Vec<CreateTodoRequest>) -> Request<axum::body::Body> {
    Request::builder()
        .uri("/todo/update")
        .method("POST")
        .header(CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&input).unwrap()))
        .unwrap()
}

fn todo_delete_request(input: Vec<TodoId>) -> Request<axum::body::Body> {
    Request::builder()
        .uri("/todo/delete")
        .method("POST")
        .header(CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&input).unwrap()))
        .unwrap()
}

fn todo_read_request(input: Vec<TodoId>) -> Request<axum::body::Body> {
    Request::builder()
        .uri("/todo/read")
        .method("POST")
        .header(CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&input).unwrap()))
        .unwrap()
}

fn todo_filter_request(qs: &str) -> Request<axum::body::Body> {
    Request::builder()
        .uri(format!("/todo/list?{}", urlencoding::encode(qs)))
        .method("GET")
        .body(axum::body::Body::empty())
        .unwrap()
}

fn tag_create_request(input: &Vec<CreateTagRequest>) -> Request<axum::body::Body> {
    tracing::debug!("{input:?}");

    Request::builder()
        .uri("/tags/create".to_string())
        .method("POST")
        .header(CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&input).unwrap()))
        .unwrap()
}
