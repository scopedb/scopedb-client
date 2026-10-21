# ScopeDB SDK for Rust

[![Version](https://img.shields.io/crates/v/scopedb-client)](https://crates.io/crates/scopedb-client) [![Docs](https://img.shields.io/badge/docs-scopedb.io-blue)](https://www.scopedb.io/docs/developer/rust) [![CI](https://img.shields.io/github/actions/workflow/status/scopedb/scopedb-client/ci.yml?branch=main&label=CI)](https://github.com/scopedb/scopedb-client/actions/workflows/ci.yml) [![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)

The official async Rust client for [ScopeDB](https://www.scopedb.io). Run ScopeQL queries, browse the catalog, and write rows with bounded concurrent streams. For application writes, start with `Table::append_stream`.

## Runtime support

Requires Rust 1.91.0 or later and a Tokio runtime. Use the SDK in a trusted server-side process and keep API keys out of source control.

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

Every table append and transform-ingest HTTP request has a client timeout of 30 seconds by default. Its budget starts after local encoding, compression, and request preparation and covers one HTTP attempt through reading the complete response body. Configure it with `Client::builder(endpoint).write_timeout(Duration::from_secs(60))` before `build()`; it must be nonzero. `Client::new` uses the same default. Streams inherit the client timeout unless an append stream explicitly sets `attempt_timeout`. Shorter timeouts on a custom HTTP client still apply. Admission, retry backoff, and an entire `flush()`/`shutdown()` barrier are outside this budget.

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

Statement and transform-ingest JSON request bodies and all table append requests use zstd compression by default. The default HTTP client also negotiates compressed responses. Append limits are based on the uncompressed NDJSON body.

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

Transform ingest does not report a rejected-versus-unknown commit state. Failed requests are therefore never automatically retried, including HTTP 429/5xx, transport errors, timeouts, and invalid success responses. A failed stream closes admission and rejects its pending `flush()` and `shutdown()` barriers. Later operations preserve the terminal error's HTTP status, request ID, and `Retry-After` diagnostics. Check the destination before replaying data that may already have committed; use a durable outbox when payloads must survive process failure.

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

## Development

See the [repository development guide](https://github.com/scopedb/scopedb-client#development) for workspace checks and [RELEASE.md][release] for the publishing procedure.

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

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
