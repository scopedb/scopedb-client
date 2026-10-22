# ScopeDB SDK for Rust

`scopedb-client` is an async Rust client for ScopeDB. It supports statements, read-only REST catalog discovery, direct NDJSON table appends, and bounded concurrent streaming writes.

This development version requires ScopeDB 0.6.0 or later and uses its native Statement, declarative Catalog, and table ingest APIs. These changes are not yet published.

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
- [Examples][example-guide]
- [Changelog][changelog]
- [Report an issue](https://github.com/scopedb/scopedb-client/issues)

This SDK sends ScopeQL statements to ScopeDB; the language is documented separately. Use these canonical entry points:

- [Quickstart](https://www.scopedb.io/docs/guides/quickstart)
- [Query guide](https://www.scopedb.io/docs/guides/query-events)
- [Language reference](https://www.scopedb.io/docs/reference)

## Client configuration

Applications that own TLS, proxy, timeout, or pooling settings can pass a compatible HTTP client through `.http_client(...)`. `Client::new(endpoint, http_client)` remains available when authentication is already configured on that client. Use the reqwest version re-exported as `scopedb_client::reqwest` to avoid dependency-version mismatches.

Every table append or Catalog mutation HTTP request has a client timeout of 30 seconds by default. Its budget starts after local encoding, compression, and request preparation and covers one HTTP attempt through reading the complete response body. Configure it with `Client::builder(endpoint).write_timeout(Duration::from_secs(60))` before `build()`; it must be nonzero. `Client::new` uses the same default. Streams inherit the client timeout unless an append stream explicitly sets `attempt_timeout`. Shorter timeouts on a custom HTTP client still apply. Admission, retry backoff, and an entire `flush()`/`shutdown()` barrier are outside this budget.

Client timeouts after an HTTP attempt begins preserve the existing error kind and source and add `Error::write_details()` with `reason: WriteErrorReason::Timeout` and `outcome: AppendState::Unknown`; `is_retryable()` is false. The structured details, source, and HTTP diagnostics remain available on subsequent errors returned by the stream and batch failure events. A timeout does not roll back a write: reconcile a client-interrupted write before replaying it. A server HTTP 408 is still a server response; an explicitly rejected append follows the existing safe retry policy. Append streams retain their existing at-least-once retry policy for other transient unknown outcomes; those retries may duplicate committed rows.

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

For lifecycle control, call `client.statement(scopeql).submit()` and retain the returned handle. `last_status()` reads the latest cached snapshot without a network request, `status().await` fetches one current status, `wait()` polls to a terminal result, and `cancel()` requests cancellation. The statement ID and initial status snapshot are available immediately after submission.

## Browse the catalog

Catalog reads return complete declarative definitions and an opaque ETag. Select the smallest subtree you need. Apply replaces that entire subtree, including its children; omitted fields are cleared. Preview with `dry_run`, apply with `if_match` to reject concurrent changes, and opt into `allow_destructive` only after reviewing data loss. Mutations never retry automatically. An interrupted or malformed mutation response has code `catalog_commit_outcome_unknown`; read the current state before deciding whether to retry. Use `raw()` to retain fields introduced by newer servers.

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

See [`examples/catalog.rs`][catalog-example] for read-only catalog discovery.

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

`send()` adds one row to the SDK's pending writes; `send_all()` adds rows from an iterator. These calls do not wait for the rows to be written. Call `shutdown()` when you are finished to wait for writing to complete and close the stream.

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

| Task                                 | Example                                           |
| ------------------------------------ | ------------------------------------------------- |
| Write NDJSON                         | [`append.rs`][append-example]                     |
| Send rows with a stream              | [`append_stream.rs`][append-stream-example]       |
| Import many rows                     | [`bulk_append.rs`][bulk-append-example]           |
| Write logs or events                 | [`telemetry.rs`][telemetry-example]               |
| Shape source fields before appending | [`append_transform.rs`][append-transform-example] |

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

let description = table.describe().await?;
println!("columns = {}", description.columns.len());
# Ok(())
# }
```

## Migrate to ScopeDB 0.6

- Replace `with_exec_timeout` with `with_max_elapsed(Duration)`; `with_max_estimated(Duration)` independently limits estimated execution time. Durations must be positive. Elapsed time includes queueing; omitted limits use server defaults.
- Replace `list_*`, `fetch_*`, and `iterate_*` Catalog calls with `client.catalog().get()` or a database/schema/table/view selector followed by `.get()`. Reads return the complete `definition` and an `etag`, without pagination.
- `Table::describe()` returns `CatalogTable`; use `distinct` and `retention_days`, replacing `distinct_on` and `data_retention_days`. Use `Table::apply()` or a catalog resource for complete replacements. `raw()` retains fields unknown to this SDK.
- `cancel()` returns and caches the complete `StatementStatus`, including any finished result or original failure. Failed states require `error`; cancelled states have no server message. `Error::code()` and `details()` retain structured errors, including unknown codes. Progress includes `compute_seconds`.
- Table appends use `/v1/catalog/databases/{db}/schemas/{schema}/tables/{table}/ingest`. The public `append_state` field and stream delivery rules remain; serde maps the new wire `status` and nested `details.status` fields. Missing or unreadable outcomes remain unknown.

Upgrading the server from 0.5 clears statement history and saved results. Previously saved statement handles cannot be resumed. The removed transform Ingest API requires the migration below.

## Migrate from Ingest

`Client::insert`, `Client::ingest_stream`, `IngestStream`, `IngestStreamBuilder`, and `IngestResult` have been removed. Use `Table::append` for one NDJSON request or `Table::append_stream` to batch Rust values. This SDK version requires ScopeDB 0.6.0 or later.

Append accepts rows matching the destination columns, not a ScopeQL statement. Move field renaming, type conversion, filtering, and routing into the producer before calling `send`. See [`append_transform.rs`][append-transform-example] for a runnable example. For transformations that must run in ScopeQL, append the original event into an `object` column in a staging table, then execute a separate `FROM ... SELECT ... INSERT INTO ...` statement. Staging and transformation are separate commits; track processed batches before retrying the transformation.

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
- Append streams may retry transient unknown outcomes, which can duplicate rows. Use `.max_retries(0)` to preserve the old stream's no-replay behavior. Client timeouts are never retried. Direct `append()` makes one attempt.

## Examples

The [example guide][example-guide] includes setup instructions and commands to run each example from the repository root.

The wire-level endpoint and payload reference is in [`docs/rust-http-api.md`][rust-http-api]. Release history and the maintainer runbook are in [`CHANGELOG.md`][changelog] and [`RELEASE.md`][release].

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

[append-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/append.rs
[append-stream-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/append_stream.rs
[bulk-append-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/bulk_append.rs
[catalog-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/catalog.rs
[changelog]: https://github.com/scopedb/scopedb-client/blob/main/CHANGELOG.md
[example-guide]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/README.md
[append-transform-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/append_transform.rs
[release]: https://github.com/scopedb/scopedb-client/blob/main/RELEASE.md
[rust-http-api]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/docs/rust-http-api.md
[telemetry-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/telemetry.rs

## Live protocol validation

From a source checkout, set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` to an authenticated ScopeDB 0.6 endpoint, then run:

```sh
cargo test -p scopedb-client --test native_06 -- --ignored --nocapture
```

The test creates a uniquely named schema and checks dry-run, ETag conflicts, committed and rejected appends, statement errors, elapsed timeout, and a cancellation racing with completion. It deletes its schema before returning. Running cancellation is covered separately with controlled HTTP responses in the unit tests.
