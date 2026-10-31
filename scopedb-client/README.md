# ScopeDB SDK for Rust

Query ScopeDB and write rows from async Rust applications.

Upgrading an existing application? See [Upgrade from 0.4 to 0.5](docs/migration-0.5.md) or [Upgrade from 0.3 to 0.4](docs/migration-0.4.md), including the move from Ingest to Append.

## Install

Requires Rust 1.91.0 or later and Tokio.

```sh
cargo add scopedb-client serde_json
cargo add tokio --features macros,rt-multi-thread
```

## Run a query

Set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` using the [connection guide](https://www.scopedb.io/docs/guides/connect-to-scopedb). Save this as `src/main.rs` and run `cargo run`:

```rust,no_run
use std::error::Error;

use scopedb_client::Client;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = std::env::var("SCOPEDB_ENDPOINT")?;
    let api_key = std::env::var("SCOPEDB_API_KEY")?;
    let client = Client::builder(endpoint)
        .api_key(api_key)
        .build()?;

    let result = client.query("SELECT 1 AS ready").await?;
    println!("{:?}", result.into_objects()?);
    Ok(())
}
```

`query()` waits for the result. `into_objects()` returns rows keyed by column name.

## Write rows

With an existing `events` table containing `id int` and `name string` columns:

```rust,no_run
# async fn example(client: scopedb_client::Client) -> Result<(), scopedb_client::Error> {
let stream = client.table("events")
    .append_stream()
    .build()?;
let row = serde_json::json!({"id": 1, "name": "page_view"});
stream.send(&row).await?;

let report = stream.shutdown().await?;
println!("Written: {} rows", report.committed_rows);
# Ok(())
# }
```

`send()` queues a row; `shutdown()` waits for pending writes and closes the stream. Use `flush()` to wait while keeping it open. If you already have NDJSON, use [`Table::append`](https://docs.rs/scopedb-client/latest/scopedb_client/struct.Table.html#method.append).

## Find what you need

| Task                         | API reference                                                                                                                  |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Read query results           | [`ResultSet`](https://docs.rs/scopedb-client/latest/scopedb_client/struct.ResultSet.html)                                      |
| Track or cancel a query      | [`StatementHandle`](https://docs.rs/scopedb-client/latest/scopedb_client/struct.StatementHandle.html)                          |
| Read or change table schemas | [`Catalog`](https://docs.rs/scopedb-client/latest/scopedb_client/struct.Catalog.html)                                          |
| Write a finite slice of rows | [`Table::append_writer`](https://docs.rs/scopedb-client/latest/scopedb_client/struct.Table.html#method.append_writer)            |
| Tune batching and retries    | [`AppendStreamBuilder`](https://docs.rs/scopedb-client/latest/scopedb_client/struct.AppendStreamBuilder.html)                  |
| Write logs or telemetry      | [Continue after a batch fails](https://docs.rs/scopedb-client/latest/scopedb_client/struct.AppendStreamBuilder.html#telemetry) |
| Set timeouts or HTTP options | [`ClientBuilder`](https://docs.rs/scopedb-client/latest/scopedb_client/struct.ClientBuilder.html)                              |
| Inspect a failure            | [`Error`](https://docs.rs/scopedb-client/latest/scopedb_client/struct.Error.html)                                              |

For complete programs, see the [examples](https://github.com/scopedb/scopedb-client/tree/main/scopedb-client/examples). For ScopeQL syntax, see the [query guide](https://www.scopedb.io/docs/guides/query-events).
