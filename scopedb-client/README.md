# ScopeDB SDK for Rust

`scopedb-client` is an async Rust client for ScopeDB. It supports statements, read-only REST catalog discovery, direct NDJSON table appends, and bounded concurrent streaming writes.

## Installation

`scopedb-client` requires Rust 1.91.0 or later.

```sh
cargo add scopedb-client serde_json
cargo add tokio --features macros,rt-multi-thread
```

## Create a client

Use the builder for the common API-key path. An API key is a server credential: keep it in a trusted process and never compile or return it to an untrusted client.

```rust,no_run
use scopedb_client::Client;

let client = Client::builder("http://127.0.0.1:6543")
    .api_key(std::env::var("SCOPEDB_API_KEY").expect("SCOPEDB_API_KEY is required"))
    .build()?;
# Ok::<(), scopedb_client::Error>(())
```

Applications that own TLS, proxy, timeout, or pooling settings can pass a compatible HTTP client through `.http_client(...)`. `Client::new(endpoint, http_client)` remains available when authentication is already configured on that client. Use the reqwest version re-exported as `scopedb_client::reqwest` to avoid dependency-version mismatches.

Every table append HTTP request has a client timeout of 30 seconds by default. Its budget starts after local encoding, compression, and request preparation and covers one HTTP attempt through reading the complete response body. Configure it with `Client::builder(endpoint).write_timeout(Duration::from_secs(60))` before `build()`; it must be nonzero. `Client::new` uses the same default. Streams inherit the client timeout unless an append stream explicitly sets `attempt_timeout`. Shorter timeouts on a custom HTTP client still apply. Admission, retry backoff, and an entire `flush()`/`shutdown()` barrier are outside this budget.

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

## ScopeQL

This SDK sends ScopeQL statements to ScopeDB; the language is documented separately. Use these canonical entry points:

- [Quickstart](https://docs.scopedb.io/guides/quickstart)
- [Query guide](https://docs.scopedb.io/guides/query-events)
- [Language reference](https://docs.scopedb.io/reference/)

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

Catalog iterators follow opaque continuation tokens automatically and fetch the next page only when needed. List methods remain available when the application needs explicit page boundaries; fetch methods return one full resource.

```rust
use scopedb_client::CatalogListOptions;

# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let mut databases = client
    .iterate_databases(CatalogListOptions {
        page_size: Some(100),
        page_token: None,
    });
while let Some(database) = databases.next().await? {
    println!("database = {}", database.name);
}

let database = client.fetch_database("scopedb").await?;
let schema = client.fetch_schema("scopedb", "public").await?;
let table = client
    .fetch_table("scopedb", "public", "events")
    .await?;

println!("{} {} {}", database.name, schema.name, table.name);
# Ok(())
# }
```

See [`examples/catalog.rs`][catalog-example] for complete database pagination and table metadata discovery.

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

| Task | Example |
| --- | --- |
| Write NDJSON | [`append.rs`][append-example] |
| Send rows with a stream | [`append_stream.rs`][append-stream-example] |
| Import many rows | [`bulk_append.rs`][bulk-append-example] |
| Write logs or events | [`telemetry.rs`][telemetry-example] |
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

When a statement reaches the failed state, `Error::statement_details()`
preserves the server-provided code, message, and optional code-specific JSON:

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

## Migrate from Ingest

`Client::insert`, `Client::ingest_stream`, `IngestStream`, `IngestStreamBuilder`, and `IngestResult` have been removed. Use `Table::append` for one NDJSON request or `Table::append_stream` to batch Rust values. Append requires ScopeDB 0.5.6 or later.

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
