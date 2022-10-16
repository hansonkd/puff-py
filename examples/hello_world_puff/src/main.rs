use puff_rs::program::commands::{PytestCommand, WSGIServerCommand, DjangoManagementCommand};
use puff_rs::graphql::handlers::{handle_graphql, handle_subscriptions, playground};
use puff_rs::prelude::*;


fn main() -> ExitCode {
    let rc = RuntimeConfig::default()
        .add_env("DJANGO_SETTINGS_MODULE", "hello_world_django_app.settings")
        .set_postgres_pool_size(20)
        .set_redis(true)
        .set_pubsub(true)
        .set_gql_schema_class("hello_world_py_app.Schema");

    let router = Router::new()
            .get("/", playground("/graphql", "/subscriptions"))
            .post("/graphql", handle_graphql())
            .get("/subscriptions", handle_subscriptions());

    Program::new("puff_django_app_example")
        .about("This is my first graphql app")
        .runtime_config(rc)
        .command(DjangoManagementCommand::new())
        .command(WSGIServerCommand::new_with_router("hello_world_django_app.wsgi.application", router))
        .command(PytestCommand::new("."))
        .run()
}