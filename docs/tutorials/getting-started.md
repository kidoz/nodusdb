# Getting started

In this tutorial you will run NodusDB on your own machine, connect to it with a
standard PostgreSQL client, create a table, query it, and confirm the data is
still there after a restart. Everything runs locally; nothing you do here
touches a network service.

Budget about fifteen minutes, most of which is the first compile.

You will need:

- A stable Rust toolchain. The repository pins one in `rust-toolchain.toml`, so
  `rustup` will pick the right version automatically.
- `psql`, or another PostgreSQL client you are comfortable with.
- A terminal in the root of a checkout of this repository.

## 1. Start the server

NodusDB ships an example configuration for exactly this purpose. Run it:

```bash
NODUS_CONFIG=nodus.toml.example cargo run --bin nodus_server
```

The first build takes a while. When the server is ready, the log ends with a
line like:

```text
INFO nodus_pgwire::server: PGWire server listening on 127.0.0.1:5432 (tls: false, max_connections: 100)
```

Leave this running and open a second terminal for the rest of the tutorial.

That example configuration gave you four things worth knowing:

- the PostgreSQL wire protocol on `127.0.0.1:5432`;
- an HTTP admin and metrics port on `127.0.0.1:8088`;
- durable storage under `./.local/nodus/data`, because `[storage] data_dir` is
  set — this is what makes step 5 work;
- a local-only `nodus` user with the password `nodus`.

On first start the server also creates a database called `default` and a schema
called `public`, which is where your table will live.

## 2. Check that it is alive

Ask the health endpoint:

```bash
curl http://127.0.0.1:8088/healthz
```

It answers `OK`. The bundled CLI asks the same question more politely:

```bash
cargo run --bin nodus_cli -- health
```

```text
Server at http://127.0.0.1:8088 is healthy.
```

You now have a running database. The next step is to talk SQL to it.

## 3. Connect with psql

```bash
psql -h 127.0.0.1 -p 5432 -U nodus -d default
```

The password is `nodus`. You are talking to NodusDB through the same wire
protocol PostgreSQL uses, which is why your existing client works unchanged.

## 4. Create a table and put data in it

Type these into the `psql` session, one at a time:

```sql
CREATE TABLE city (
    id      INT PRIMARY KEY,
    name    TEXT NOT NULL,
    country TEXT
);
```

```sql
INSERT INTO city VALUES (1, 'Berlin', 'DE'), (2, 'Lisbon', 'PT');
```

```sql
SELECT name FROM city WHERE country = 'DE';
```

That returns one row, `Berlin`. Now let the database do some work for you:

```sql
SELECT country, count(*) FROM city GROUP BY country ORDER BY country;
```

Two rows come back, one per country. You have created a table, written rows
through a primary key, filtered on a non-key column, and run an aggregate — the
ordinary shape of an OLTP workload, on a distributed storage engine.

## 5. Restart and confirm the data survived

This is the part that matters. Leave `psql` open, go back to the first terminal,
and stop the server with `Ctrl-C`. Start it again with the same command:

```bash
NODUS_CONFIG=nodus.toml.example cargo run --bin nodus_server
```

Your `psql` session was cut when the server stopped, so reconnect:

```bash
psql -h 127.0.0.1 -p 5432 -U nodus -d default
```

```sql
SELECT * FROM city ORDER BY id;
```

Both rows are still there, and so is the table definition. Your `COMMIT` was
acknowledged only after the write reached the write-ahead log on disk, so a stop
— clean or not — cannot lose it.

Had you started the server without `[storage] data_dir`, NodusDB would have run
entirely in memory and this step would have returned nothing. That is a
deliberate, explicit opt-out, described in the
[durability contract](../reference/durability-contract.md).

## Where to go next

You have a working single-node database and you know how to reach it.

- To load an existing PostgreSQL database into it, follow
  [Import a PostgreSQL dump](../how-to/import-a-postgresql-dump.md).
- To take a backup of what you just built, follow
  [Back up and restore](../how-to/back-up-and-restore.md).
- To understand what "durable" means here in precise terms, read the
  [durability contract](../reference/durability-contract.md) and then
  [how durability works](../explanation/durability.md).

When you are finished, stop the server with `Ctrl-C`. Your data stays in
`./.local/nodus/data` until you delete that directory.
