# Set up database

## Install diesel_cli

Install dependencies.

```
% sudo apt install libpq-dev
% cargo install diesel_cli --no-default-features --features postgres 
```

## Start PostgreSQL on Docker.

```
$ cd quickwit-metastore
$ docker-compose up -d
```

## Setup Diesel for quickwit-metastore

Prepare `.env` file. Putting the `DATABASE_URL` environment variable in `.env` file.
E.g.
```
$ echo DATABASE_URL=postgres://quickwit-dev:quickwit-dev@localhost:5432/metastore > .env
```

Set up diesel. This will create `migrations/postgresql` and `diesel.toml`.
```
$ diesel setup --migration-dir=migrations/postgresql
```

Copy `diesel.toml` to `diesel_postgresql.toml` for PostgreSQL module.
```
$ cp diesel.toml diesel_postgresql.toml  # change print_schema tp `src/postgresql/schema.rs`
```

Then, modify `diesel_postgresql.toml` like as follows:
```
[print_schema]
file = "src/postgresql/schema.rs"
```

Generate `*.sql` files.
```
$ diesel migration generate --migration-dir=migrations/postgresql create_indexes
$ diesel migration generate --migration-dir=migrations/postgresql create_splits
```

Modify the `.sql` file in the generated `migrations` directory. 

```
$ diesel migration run --migration-dir=migrations/postgresql --config-file=diesel_postgresql.toml
```

`quickwit-metastore/src/postgresql/schema.rs` will be generated.


Unit testing

```
$ cargo test --all-features
```
