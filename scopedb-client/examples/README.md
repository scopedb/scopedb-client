# ScopeDB Rust SDK examples

Start with a quickstart, then choose the example for your task. Every example uses only the public `scopedb-client` API. The shared client helper uses `Client::builder(...).api_key(...)`, so copied examples do not need a direct reqwest dependency. API keys belong only in a trusted process; never return one to an untrusted client.

These examples focus on SDK integration and assume valid ScopeQL. For language syntax, use the canonical [Quickstart](https://docs.scopedb.io/guides/quickstart), [query guide](https://docs.scopedb.io/guides/query-events), and [language reference](https://docs.scopedb.io/reference/).

The commands below run from the repository root.

## Read-only discovery

These examples can run against a reachable ScopeDB endpoint without modifying data.

| Example                        | Shows                                                | Run                             |
| ------------------------------ | ---------------------------------------------------- | ------------------------------- |
| [`statement.rs`](statement.rs) | Query shorthand and object result rows               | `cargo run --example statement` |
| [`catalog.rs`](catalog.rs)     | Automatic REST catalog pagination and full resources | `cargo run --example catalog`   |
| [`table.rs`](table.rs)         | Quoted table identifiers and full table descriptions | `cargo run --example table`     |

## Before running a write example

Every write example refuses to start unless `SCOPEDB_TABLE` names an existing, disposable table. Do not point these examples at production unless the writes are intentional.

The append, stream, telemetry, and transform examples can share this schema:

```sql
CREATE TABLE public.sdk_example_events (
  id int,
  event_id string,
  occurred_at timestamp,
  name string,
  attributes object
);
```

Configuration comes from:

- `SCOPEDB_ENDPOINT` (defaults to `http://127.0.0.1:6543`)
- `SCOPEDB_API_KEY` (optional Bearer API key; preferred)
- `SCOPEDB_TOKEN` (fallback when `SCOPEDB_API_KEY` is unset or empty)
- `SCOPEDB_DATABASE` (defaults to `scopedb`)
- `SCOPEDB_SCHEMA` (defaults to `public`)
- `SCOPEDB_TABLE` (required for writes)

Set the disposable destination once before running a write example:

```sh
export SCOPEDB_TABLE=sdk_example_events
```

```powershell
$env:SCOPEDB_TABLE = "sdk_example_events"
```

## Choose a write example

| Example | Shows | Run |
| --- | --- | --- |
| [`append.rs`](append.rs) | Write NDJSON | `cargo run --example append` |
| [`append_stream.rs`](append_stream.rs) | Send rows with a stream | `cargo run --example append_stream` |
| [`bulk_append.rs`](bulk_append.rs) | Import many rows | `cargo run --example bulk_append` |
| [`telemetry.rs`](telemetry.rs) | Write logs or events | `cargo run --example telemetry` |
| [`ingest_transform.rs`](ingest_transform.rs) | Transform rows with SQL before writing | `cargo run --example ingest_transform` |

`append.rs` writes prepared NDJSON with `table.append()`. Each line is one JSON object.

`append_stream.rs` and `bulk_append.rs` create a stream with `table.append_stream().build()`. Use `send()` for one row or `send_all()` for an iterator of rows. These calls add data to the SDK's pending writes. Call `shutdown()` when finished to wait for writing to complete and close the stream. Use `flush()` if you want to wait for pending writes and then keep using the stream.

`telemetry.rs` configures `.failure_policy(AppendFailurePolicy::Continue)` so the stream keeps running after a batch fails. It uses `try_send()` to add a row without waiting. Check its return value, and read the report returned by `shutdown()` for the write results.

## Check the examples

Compile every example without running it:

```sh
cargo check --examples
```
