# dhttp

An independent single-node HTTP API for DuckDB with the core rqlite-style SQL endpoints and an additional file-import API. It is not affiliated with or endorsed by the DuckDB Foundation. It is **not** a replicated database and does not provide SQLite SQL compatibility.

## Run

Requires Go 1.25 or newer and a C compiler (DuckDB's Go driver uses CGO).

```sh
cd dhttp
go run . -db ./data.duckdb -listen 127.0.0.1:4001
```

The server defaults to loopback and one database connection. To require HTTP Basic authentication, set **both** `DUCKDB_HTTP_USER` and `DUCKDB_HTTP_PASSWORD`. Use TLS (or an encrypted tunnel such as Tailscale) when exposing it over a network: Basic authentication alone does not encrypt traffic. `GET /readyz` checks availability.

## SQL API

`GET /db/query?q=...`, `POST /db/query`, `POST /db/execute`, and `POST /db/request` accept the usual rqlite statement arrays. POST also accepts one SQL statement with `Content-Type: text/plain`. `/db/request` can mix reads and writes. `?transaction` wraps a request's statements in a transaction; on failure the batch is rolled back. `?associative`, `?pretty`, and `?timings` are supported.

```sh
curl -X POST localhost:4001/db/execute -H 'Content-Type: application/json' \
  -d '["CREATE TABLE events (id INTEGER, message VARCHAR)"]'

curl -X POST localhost:4001/db/execute -H 'Content-Type: application/json' \
  -d '[["INSERT INTO events VALUES (?, ?)", 1, "hello"]]'

curl -G localhost:4001/db/query --data-urlencode 'q=SELECT * FROM events'
```

Responses contain `results`, with `columns`, `types`, and `values` for queries, or `rows_affected` where provided by DuckDB for writes. Statement failures have an `error` field. Common result types are mapped to SQLite-like names, but these are **not** SQLite declared column types. DuckDB does not provide SQLite `last_insert_id` semantics or rqlite cluster/read-consistency behavior. SQL dialects differ. Query classification is not a security boundary.

### Query output formats

JSON remains the default rqlite-style response. For a **single read query**, `GET` or `POST /db/query?format=csv|ndjson|parquet` returns the query rows directly in that format (no `results` wrapper). POST supports positional and named parameters as usual. `format=json` explicitly requests the default behavior.

```sh
curl -G 'http://127.0.0.1:4001/db/query?format=csv' \
  --data-urlencode 'q=SELECT * FROM events' -o events.csv

curl -X POST 'http://127.0.0.1:4001/db/query?format=ndjson' \
  -H 'Content-Type: application/json' \
  -d '[["SELECT * FROM events WHERE id >= ?", 10]]' -o events.ndjson

curl -G 'http://127.0.0.1:4001/db/query?format=parquet' \
  --data-urlencode 'q=SELECT * FROM events' -o events.parquet
```

Non-JSON formats require exactly one query and cannot be combined with `transaction`, `associative`, `pretty`, or `timings`. Writes, mixed requests, and multi-query requests continue to use JSON. Exports are staged in a temporary file before streaming to avoid sending a partial success response if the query fails; large responses require sufficient temporary disk space.

## Import API

Upload a **raw file body** to `POST /db/import/{table}?format=...&mode=...` (not multipart). Formats: `parquet`, `csv`, `json` (auto-detect JSON arrays or NDJSON), or `ndjson` (force newline-delimited). Modes: `create` (default; fails if the table exists), `append` (inserts columns by name), or `replace` (atomically swaps the existing table). Names must be simple SQL identifiers. The upload is staged in a temporary file and removed after the import.

```sh
curl -X POST 'localhost:4001/db/import/events?format=parquet&mode=replace' \
  --data-binary @events.parquet

curl -X POST 'localhost:4001/db/import/events?format=ndjson&mode=append' \
  --data-binary @events.ndjson

curl -X POST 'localhost:4001/db/import/events?format=csv&mode=append' \
  --data-binary @events.csv
```

Schema is inferred by DuckDB when creating or replacing a table. Appends must be compatible with the existing schema. Uploads default to a 512 MiB limit; adjust with `-max-upload-mb`.

## Backup

Run this **while the server is running**. The command contacts the server rather than opening or copying its live database file:

```sh
go run . backup -url http://127.0.0.1:4001 -out ./backup-2026-10-17.duckdb
```

Or download the snapshot directly with `GET /db/backup` (also accepts `?fmt=binary`):

```sh
curl --fail -o backup.duckdb http://127.0.0.1:4001/db/backup
```

The server creates a separate DuckDB file using `COPY FROM DATABASE` and streams it to the client. The CLI writes a temporary file and **refuses to overwrite** an existing destination. If Basic authentication is configured, set `DUCKDB_HTTP_USER` and `DUCKDB_HTTP_PASSWORD` for the CLI too (or use `curl -u`). This is a DuckDB binary backup, **not** rqlite's SQLite backup format. It covers the primary DuckDB database; separately attached external databases, files, or remote data sources are not included. Restore by stopping the server, moving the original database aside, and starting it with `-db` pointing to the backup. Test restores regularly.

**Security:** SQL callers must be trusted. DuckDB SQL can read/write files, access network resources, or load extensions. Use authentication, network isolation, and an OS account with limited file access. This service does not provide per-table authorization or sandboxing.

Run tests with `go test ./...`.

## CI and releases

The `.github/workflows/test.yml` workflow runs tests and vet on macOS arm64, Linux amd64, and Linux arm64. Pushing a `v*` tag runs those checks again, builds a native CGO-enabled binary on each platform, and publishes three archives plus `SHA256SUMS` in a GitHub release.

Locally, `go run . --version` prints `dhttp dev`; tagged release binaries print their embedded tag. The release process needs hosted `macos-15` (arm64) and `ubuntu-24.04-arm` runners and `contents: write` access for its publish job.
