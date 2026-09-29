# ScopeDB SDK for Rust

`scopedb-client` is an async Rust client for ScopeDB. It supports statements, read-only REST catalog discovery, direct NDJSON table appends, bounded concurrent streaming writes, and transform-oriented JSON ingest.

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

Statement and transform-ingest JSON request bodies and all table append requests use zstd compression by default. The default HTTP client also negotiates compressed responses. Append limits are based on the uncompressed NDJSON body.

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
| Transform rows with SQL before writing | [`ingest_transform.rs`][ingest-transform-example] |

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

## Transform-oriented ingest

`IngestStream` remains useful when input JSON needs a SQL transform before insertion. Prefer `Table::append` or `Table::append_stream` when records already match the destination table.

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", scopedb_client::reqwest::Client::new())?;
let stream = client
    .ingest_stream(
        r#"
        SELECT
            $0["ts"]::timestamp as occurred_at,
            $0["name"]::string as name
        INSERT INTO public.events (occurred_at, name)
        "#,
    )
    .build();

stream
    .send(&serde_json::json!({
        "ts": "2026-03-13T12:00:00Z",
        "name": "ScopeDB",
    }))
    .await?;
stream.shutdown().await?;
# Ok(())
# }
```

## Examples

The [example guide][example-guide] includes setup instructions and commands to run each example from the repository root.

The wire-level endpoint and payload reference is in [`docs/rust-http-api.md`][rust-http-api]. Release history and the maintainer runbook are in [`CHANGELOG.md`][changelog] and [`RELEASE.md`][release].

[append-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/append.rs
[append-stream-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/append_stream.rs
[bulk-append-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/bulk_append.rs
[catalog-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/catalog.rs
[changelog]: https://github.com/scopedb/scopedb-client/blob/main/CHANGELOG.md
[example-guide]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/README.md
[ingest-transform-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/ingest_transform.rs
[release]: https://github.com/scopedb/scopedb-client/blob/main/RELEASE.md
[rust-http-api]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/docs/rust-http-api.md
[telemetry-example]: https://github.com/scopedb/scopedb-client/blob/main/scopedb-client/examples/telemetry.rs
