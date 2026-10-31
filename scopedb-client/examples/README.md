# ScopeDB Rust SDK examples

Runnable examples using the public `scopedb-client` API. For ScopeQL syntax, see the [Quickstart](https://www.scopedb.io/docs/guides/quickstart), [query guide](https://www.scopedb.io/docs/guides/query-events), and [language reference](https://www.scopedb.io/docs/reference).

Run the commands below from the repository root.

## Read-only discovery

These examples run against a reachable ScopeDB endpoint without modifying data.

| Example                        | Shows                                                | Run                             |
| ------------------------------ | ---------------------------------------------------- | ------------------------------- |
| [`statement.rs`](statement.rs) | Query shorthand and object result rows               | `cargo run --example statement` |
| [`catalog.rs`](catalog.rs)     | Declarative catalog snapshots and table definitions  | `cargo run --example catalog`   |
| [`table.rs`](table.rs)         | Quoted table identifiers and full table descriptions | `cargo run --example table`     |

## Before running a write example

Write examples require `SCOPEDB_TABLE` to name an existing disposable table.

The append, stream, telemetry, and file import examples share this schema:

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

| Example                                | Shows                               | Run                                                  |
| -------------------------------------- | ----------------------------------- | ---------------------------------------------------- |
| [`append.rs`](append.rs)               | Write NDJSON                        | `cargo run --example append`                         |
| [`append_stream.rs`](append_stream.rs) | Send rows with a stream             | `cargo run --example append_stream`                  |
| [`bulk_append.rs`](bulk_append.rs)     | Import many rows                    | `cargo run --example bulk_append`                    |
| [`telemetry.rs`](telemetry.rs)         | Write logs or events                | `cargo run --example telemetry`                      |
| [`import_ndjson.rs`](import_ndjson.rs) | Import a bounded NDJSON file stream | `cargo run --example import_ndjson -- events.ndjson` |

`append.rs` writes prepared NDJSON in one request with `table.append()`.

`append_stream.rs` and `bulk_append.rs` create a stream with `table.append_stream().build()`, use `send()` or `send_all()` to admit rows, and call `shutdown()` to wait for pending writes and close the stream.

`telemetry.rs` configures `.failure_policy(AppendFailurePolicy::Continue)` so the stream keeps running after a batch fails, uses `try_send()` for non-blocking admission, and inspects the final report returned by `shutdown()`.

`import_ndjson.rs` reads an NDJSON file one line at a time with bounded memory and settles the admitted prefix with `shutdown()` if input parsing fails.

## Check the examples

Compile every example without running it:

```sh
cargo check --examples
```
