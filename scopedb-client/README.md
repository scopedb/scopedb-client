# ScopeDB SDK for Rust

`scopedb-client` is an async Rust client for ScopeDB. It supports statements, declarative catalog reads and conditional updates, direct NDJSON table appends, and bounded concurrent streaming writes.

SDK 0.4 requires ScopeDB 0.6.0 or later and uses its native Statement, declarative Catalog, and table ingest APIs. See the [migration guide](#migrate-to-scopedb-06) when upgrading from SDK 0.3.

## Installation

From your application crate:

```sh
cargo add scopedb-client serde_json
cargo add tokio --features macros,rt-multi-thread
```

## Quickstart

Set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` using the [connection guide](https://www.scopedb.io/docs/guides/connect-to-scopedb). Save this program as `src/main.rs` and run `cargo run`:

```rust,no_run
use scopedb_client::Client;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder(std::env::var("SCOPEDB_ENDPOINT")?)
        .api_key(std::env::var("SCOPEDB_API_KEY")?)
        .build()?;

    let result = client.query("SELECT 1 AS ready").await?;
    println!("{:?}", result.into_objects()?);
    Ok(())
}
```

## Documentation

- [SDK guide](https://www.scopedb.io/docs/developer/rust)
- [API reference](https://docs.rs/scopedb-client)
- Runnable examples are documented in `examples/README.md`.
- Release history is recorded in the repository's `CHANGELOG.md`.
- [Report an issue](https://github.com/scopedb/scopedb-client/issues)

This SDK sends ScopeQL statements to ScopeDB; the language is documented separately. Use these canonical entry points:

- [Quickstart](https://www.scopedb.io/docs/guides/quickstart)
- [Query guide](https://www.scopedb.io/docs/guides/query-events)
- [Language reference](https://www.scopedb.io/docs/reference)

## Client configuration

Applications that own TLS, proxy, timeout, or pooling settings can pass a compatible HTTP client through `.http_client(...)`. The SDK-owned HTTP client disables redirects and transport retries to avoid replaying writes; custom clients must use `.redirect(reqwest::redirect::Policy::none()).retry(reqwest::retry::never())` for the same guarantee. `Client::new(endpoint, http_client)` remains available when authentication is already configured on that client. Use the reqwest version re-exported as `scopedb_client::reqwest` to avoid dependency-version mismatches.

Every table append or Catalog mutation HTTP request has a client timeout of 30 seconds by default. Its budget starts after local encoding, compression, and request preparation and covers one HTTP attempt through reading the complete response body. Configure it with `Client::builder(endpoint).write_timeout(Duration::from_secs(60))` before `build()`; it must be nonzero. `Client::new` uses the same default. Streams inherit the client timeout unless an append stream explicitly sets `attempt_timeout`. Shorter timeouts on a custom HTTP client still apply. Admission, retry backoff, and an entire `flush()`/`shutdown()` barrier are outside this budget.

Append client timeouts after an HTTP attempt begins preserve the existing error kind and source and add `Error::write_details()` with `reason: WriteErrorReason::Timeout` and `outcome: AppendState::Unknown`; `is_retryable()` is false. The structured details, source, and HTTP diagnostics remain available on subsequent errors returned by the stream and batch failure events. A timeout does not roll back a write: reconcile a client-interrupted write before replaying it. A server HTTP 408 is still a server response; an explicitly rejected append follows the existing safe retry policy. Streams retry temporary rejected batches by default. Unknown outcomes require explicit `.retry_unknown(true)`; this permits at-least-once delivery and can duplicate rows. Catalog interruptions use `catalog_commit_outcome_unknown` and are never replayed automatically.

```rust
use scopedb_client::{AppendState, WriteErrorReason};
# fn inspect(error: &scopedb_client::Error) {
if let Some(details) = error.write_details() {
    if details.reason == WriteErrorReason::Timeout {
        assert_eq!(details.outcome, AppendState::Unknown);
        assert!(!error.is_retryable());
        eprintln!("write may have committed; reconcile before replaying");
    }
}
# }
```

Statement JSON request bodies and all table append requests use zstd compression by default. The default HTTP client also negotiates compressed responses. Append limits are based on the uncompressed NDJSON body.

The runnable examples read authentication from `SCOPEDB_API_KEY`. For backward compatibility, they fall back to `SCOPEDB_TOKEN` when the API key variable is unset or empty. The builder marks the resulting authorization header as sensitive so standard header and request `Debug` formatting redacts the credential.

## Run a statement

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let result = client.query("SELECT 1 AS ready").await?;
let rows = result.into_objects()?;
println!("{rows:?}");
# Ok(())
# }
```

`raw_rows()` exposes the string-or-null wire cells without parsing. `to_values()` borrows the result while `into_values()` consumes it and moves owned string cells. `to_objects()` and `into_objects()` key values by output column name; they return an error when names are duplicated, so use the value form for intentionally duplicated columns. `first()` converts only the first row for point lookups and aggregates.

Typed values support `PartialEq`, so applications can compare cells and result rows directly. Array, object, any, and binary values retain their encoded strings and compare those strings; floating-point values follow Rust's usual equality rules.

For lifecycle control, call `client.statement(scopeql).submit()` and retain the returned handle. `snapshot()` reads the latest cached status without a network request, `refresh().await` fetches one current status, `wait()` polls to a terminal result, and `cancel()` requests cancellation. The statement ID and initial snapshot are available immediately after submission. `cancel()` returns the observed status: a pending or running response still needs `wait()`; completion retains results and failure retains the original error. Cancellation is a distinct `ErrorKind::StatementCancelled` when waiting. Terminal snapshots are cached. Dropping a query, refresh, or wait future stops local waiting and does not cancel server execution; keep a handle and call `cancel()` explicitly.

If `query()` or `execute()` fails after submission, `Error::statement_id()` identifies the existing execution. Resume it with `client.statement_handle(id).wait()`; the error does not advertise retrying the entire query as safe. Temporary errors from a handle's `refresh()` or `wait()` can still be retried on that handle. A lost submission response, HTTP 408, or server/gateway 5xx leaves submission uncertain and is not classified as retryable. Use `with_statement_id(id)` before submission when you need to look up that execution after a lost response; a known ID alone does not confirm acceptance.

## Browse the catalog

Catalog reads return complete declarative definitions and an opaque ETag. Select the smallest subtree you need. Apply replaces that entire subtree, including its children; omitted fields are cleared. Preview with `dry_run`, apply with `if_match` to reject concurrent changes, and opt into `allow_destructive` only after reviewing data loss. Mutations never retry automatically. An interrupted or malformed mutation response has code `catalog_commit_outcome_unknown`; read the current state before deciding whether to retry. Typed definitions reject unknown fields, including nested fields, to prevent silent loss during replacement. Use `raw()` to read and apply newer definitions as `serde_json::Value`; arbitrary-precision JSON numbers are retained.

```rust
use scopedb_client::CatalogOptions;

# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let resource = client.catalog().table("scopedb", "public", "events");
let snapshot = resource.get().await?;
let mut definition = snapshot.definition;
definition.comment = Some("Application events".to_string());
let preview = resource.apply(&definition, CatalogOptions {
    dry_run: true,
    if_match: Some(snapshot.etag),
    ..Default::default()
}).await?;
println!("destructive = {}", preview.destructive);
resource.apply(&definition, CatalogOptions {
    if_match: Some(preview.etag),
    ..Default::default()
}).await?;
# Ok(())
# }
```

See `examples/catalog.rs` for read-only catalog discovery.

## Writing rows

The destination table must already exist. `Table` uses `scopedb` and `public` when the database or schema is not specified.

### Write with a stream

Use `append_stream()` to send Rust values. The SDK converts them to NDJSON and writes them in batches automatically.

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let stream = client.table("events").append_stream().build()?;

stream.send(&serde_json::json!({"id": 1, "name": "first"})).await?;
stream.send_all([
    serde_json::json!({"id": 2, "name": "second"}),
    serde_json::json!({"id": 3, "name": "third"}),
]).await?;

let report = stream.shutdown().await?;
println!("Written: {} rows", report.committed_rows);
# Ok(())
# }
```

`send()` adds one row to the SDK's pending writes; `send_all()` adds rows from an iterator. These calls do not wait for the rows to be written. Call `shutdown()` when you are finished to wait for writing to complete and close the stream. If `send_all()` fails partway, its error includes the accepted count and `stats()` retains cumulative counts; still settle that accepted prefix with `shutdown()`.

To wait for pending writes while keeping the stream open, call `flush()`:

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
# let stream = client.table("events").append_stream().build()?;
let report = stream.flush().await?;
println!("Written: {} rows", report.committed_rows);
# stream.shutdown().await?;
# Ok(())
# }
```

A report covers activity since the previous barrier. With the default Stop policy, a failed `flush()` or `shutdown()` returns the complete error, and `stats().last_report` retains partial settlement counts. Earlier batches may have committed. Reports and failure events expose aggregate counts without identifying individual input records; use application-owned business keys to reconcile stored rows before selecting records to replay. `stats().last_failure.error` preserves the code, details, timeout reason, HTTP metadata, and source in Continue mode as well.

Dropping an enqueued flush or started shutdown future stops waiting, but settlement continues. Call `shutdown()` again to observe the final result. Dropping all producer handles drains accepted rows while the Tokio runtime remains alive; runtime termination cannot guarantee delivery.

### Continue after a batch fails

For logs or telemetry that should keep running after a batch fails, set `AppendFailurePolicy::Continue`. Read the report returned by `flush()` or `shutdown()` to see the write results.

```rust
use scopedb_client::AppendFailurePolicy;

# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let stream = client
    .table("events")
    .append_stream()
    .failure_policy(AppendFailurePolicy::Continue)
    .build()?;

stream.send(&serde_json::json!({"name": "request.completed"})).await?;
let report = stream.shutdown().await?;
println!("Write results: {report:?}");
# Ok(())
# }
```

### Append NDJSON

Use `append()` when your data is already newline-delimited JSON: one JSON object per line.

ScopeDB 0.6's NDJSON decoder accepts integer inputs only within the signed 64-bit range, including inputs for `uint` columns and nested JSON values. Larger integers are rejected by the server; the SDK does not silently narrow them. Query results still support the full `u64` range.

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let table = client.table("events");
let ndjson = r#"{"id": 1, "name": "first"}
{"id": 2, "name": "second"}"#;

let result = table.append(ndjson).await?;
println!("Written: {} rows", result.num_rows_inserted);
# Ok(())
# }
```

### More examples

| Task                    | Example                     |
| ----------------------- | --------------------------- |
| Write NDJSON            | `examples/append.rs`        |
| Send rows with a stream | `examples/append_stream.rs` |
| Import many rows        | `examples/bulk_append.rs`   |
| Write logs or events    | `examples/telemetry.rs`     |
| Import an NDJSON file   | `examples/import_ndjson.rs` |

## Inspecting errors

Use the error's methods to read its message, HTTP status, and request ID:

```rust
# fn inspect(error: &scopedb_client::Error) {
eprintln!("{}", error.message());
eprintln!("status = {:?}", error.http_status());
eprintln!("request_id = {:?}", error.request_id());
eprintln!("retryable = {}", error.is_retryable());
eprintln!("retry_after = {:?}", error.retry_after());
# }
```

When a statement reaches the failed state, `Error::statement_details()` preserves the server-provided code, message, and optional code-specific JSON:

```rust
# fn inspect(error: &scopedb_client::Error) {
if let Some(details) = error.statement_details() {
    eprintln!("statement failed with {}: {}", details.code, details.message);
    eprintln!("details = {:?}", details.details);
}
# }
```

## Table helper

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let table = client.table("events").with_schema("public");
println!("identifier = {}", table.identifier());

let snapshot = table.catalog().get().await?;
let description = snapshot.definition;
println!("etag = {}", snapshot.etag);
println!("columns = {}", description.columns.len());
# Ok(())
# }
```

## Migrate to ScopeDB 0.6

- Replace `with_exec_timeout` with `with_max_elapsed(Duration)`. `with_max_parallelism` is removed; `with_max_estimated(Duration)` independently limits estimated execution time and does not control parallelism. Durations must be positive. Elapsed time includes queueing; omitted limits use server defaults.
- Replace `list_*`, `fetch_*`, and `iterate_*` Catalog calls with `client.catalog().get()` or a database/schema/table/view selector followed by `.get()`. Reads return the complete `definition` and an `etag`, without pagination.
- Replace `Table::describe()` and `table_schema()` with `table.catalog().get()`, returning both `definition` and `etag`. Use `distinct` and `retention_days`, replacing `distinct_on` and `data_retention_days`. Typed definitions reject unsupported fields; `.raw()` preserves them. `Table::drop()` remains available; use Catalog `delete()` when a precondition or preview is needed.
- `cancel()` returns and caches the complete `StatementStatus`, including any finished result or original failure. Failed states require `error`; cancelled states have no server message. `Error::code()` and `details()` retain structured errors, including unknown codes. Progress includes `compute_seconds`.
- Table appends use `/v1/catalog/databases/{db}/schemas/{schema}/tables/{table}/ingest`. Rename `AppendRowsResult::append_state` and `AppendErrorDetails::append_state` to `status`, matching the native response. Missing or unreadable outcomes remain unknown. The default still retries only temporary rejections. The new `.retry_unknown(true)` option permits replaying transient unknown outcomes and may insert duplicates; client timeouts are never replayed.
- Replace `last_status()` with `snapshot()`, `status()` / `fetch_once()` with `refresh()`, and `fetch()` with `wait()`. Read progress from `snapshot().map(StatementStatus::progress)` and match `StatementStatus::Finished` to inspect cached results. `wait()` also returns cached results without another request.
- Replace the removed `json_rows()` alias with `raw_rows()`, which directly returns the slice. Replace stream builder aliases `batch_bytes`, `max_pending_bytes`, and `max_in_flight_requests` with `target_batch_bytes`, `max_buffered_bytes`, and `max_concurrent_batches`.
- Read `stats().last_failure.error` instead of separate message/status fields. Its complete `Error` retains diagnostics across barriers. `AppendStreamStats` no longer implements equality because errors carry sources.
- `Table::identifier()` always includes the database and schema defaults, so SQL queries and `drop()` select the same table as Append and Catalog, including when only the database is configured. The legacy `u_int` type spelling is removed; use `uint`.

Upgrading the server from 0.5 clears statement history and saved results. Previously saved statement handles cannot be resumed. The removed transform Ingest API requires the migration below.

## Migrate from Ingest

`Client::insert`, `Client::ingest_stream`, `IngestStream`, `IngestStreamBuilder`, and `IngestResult` have been removed. Use `Table::append` for one NDJSON request or `Table::append_stream` to batch Rust values. This SDK version requires ScopeDB 0.6.0 or later.

Append accepts rows matching the destination columns, not a ScopeQL statement. Move field renaming, type conversion, filtering, and routing into the producer before calling `send`. For transformations that must run in ScopeQL, append the original event into an `object` column in a staging table, then execute a separate `FROM ... SELECT ... INSERT INTO ...` statement. Staging and transformation are separate commits; track processed batches before retrying the transformation.

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let source = serde_json::json!({"ts": "2026-03-13T12:00:00Z", "name": "ScopeDB"});
let stream = client.table("events").append_stream()
    .max_concurrent_batches(1)
    .max_retries(0)
    .build()?;
let sent = stream.send(&serde_json::json!({
    "occurred_at": source["ts"],
    "name": source["name"],
})).await;
let report = stream.shutdown().await;
sent?;
println!("Committed rows: {}", report?.committed_rows);
# Ok(())
# }
```

Review these behavior changes when migrating:

- Append batches allow up to 8 MiB of uncompressed NDJSON and 200,000 rows. Reduce an old ingest batch-byte target above 8 MiB.
- Append stream `build()` returns a `Result`; handle configuration errors with `?` before sending records.
- `flush()` and `shutdown()` return an `AppendDeliveryReport` for activity since the previous report. Read `committed_rows` instead of `num_rows_inserted`, and check `outcome` when using the continue policy. Direct `append()` still returns `num_rows_inserted`.
- Append streams send up to four batches concurrently by default. Use `.max_concurrent_batches(1)` for sequential requests; queries still require `ORDER BY` for ordered results.
- Append streams retry temporary rejections, where no rows committed. Use `.retry_unknown(true)` only if duplicate rows are acceptable, or `.max_retries(0)` to disable all retries. Client timeouts are never retried. Direct `append()` makes one attempt.

## Examples

The example guide in `examples/README.md` includes setup instructions and commands to run each example from the repository root.

The wire-level endpoint and payload reference is in `docs/rust-http-api.md`. Release history is in the repository's `CHANGELOG.md`.

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
