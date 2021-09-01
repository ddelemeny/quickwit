# Install diesel_cli

Install dependencies.

```
% sudo apt install libpq-dev
% cargo install diesel_cli --no-default-features --features postgres 
```

Start PostgreSQL on Docker.

```
$ cd quickwit-metastore
$ docker-compose up -d
```

Setup diesel.rs

```
$ echo DATABASE_URL=postgres://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/${PGDATABASE} > .env
$ diesel setup
$ diesel migration generate create_indexes
$ diesel migration generate create_splits
```

Update the `.sql` file in the generated `migrations` directory. 

```
$ diesel migration run
```

`quickwit-metastore/src/schema.rs` will be generated.


Unit testing

```
$ cargo test --all-features
```
