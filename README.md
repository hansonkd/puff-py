# Python for ☁ Puff ☁!

`puff-py` provides support to run Python on the puff runtime.

See the official [puff repository](https://github.com/hansonkd/puff) for more details.

There is also an integrated example with Django found in this repo's `/examples` folder [which gives specific guidance on how to use Django](https://github.com/hansonkd/puff-py/tree/main/examples/hello_world_puff)

To use the example

```bash
cd ./examples/hello_world_puff
cargo build
cd hello_world_py_app
poetry install
poetry run cargo run runserver
```

You may have to adjust your `PUFF_POSTGRES_URL` and `PUFF_REDIS_URL` to your local Postgres and redis instances to get it to run.

# Using Django with Puff

Set up your project:

```bash
cargo new my_puff_proj --bin
cd my_puff_proj
cargo add puff-rs
poetry new my_puff_proj_py
cd my_puff_proj_py
poetry add puff-py
poetry add django
poetry run django-admin startproject hello_world_django_app .
```

Add Django to your Puff Program

```rust
use puff_rs::program::commands::{PytestCommand, WSGIServerCommand, DjangoManagementCommand};
use puff_rs::graphql::handlers::{handle_graphql, handle_subscriptions, playground};
use puff_rs::prelude::*;


fn main() -> ExitCode {
    let rc = RuntimeConfig::default()
        .add_env("DJANGO_SETTINGS_MODULE", "hello_world_django_app.settings")
        .set_postgres(true)
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
```

Change your settings to use Puff Database and Redis Cache

```python
DATABASES = {
    'default': {
        'ENGINE': 'puff.contrib.django.postgres',
        'NAME': 'global',
    }
}

CACHES = {
    'default': {
        'BACKEND': 'puff.contrib.django.redis.PuffRedisCache',
        'LOCATION': 'redis://global',
    }
}
```

Change `wsgi.py` to serve static files;

```python
...

from django.core.wsgi import get_wsgi_application
from django.contrib.staticfiles.handlers import StaticFilesHandler

application = StaticFilesHandler(get_wsgi_application())
```

Use Poetry and Cargo run instead of `./manage.py`

```bash
poetry run cargo run runserver
poetry run cargo run django migrate
```