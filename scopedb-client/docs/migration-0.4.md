# Upgrade from 0.3 to 0.4

Update your dependency with `cargo add scopedb-client@0.4`, then follow the sections for APIs your application uses. Rust 1.91.0 remains the minimum supported version, unchanged from 0.3.

## Replace Ingest with Append

| In 0.3                  | In 0.4                 |
| ----------------------- | ---------------------- |
| `Client::insert`        | `Table::append`        |
| `Client::ingest_stream` | `Table::append_stream` |
| `IngestStream`          | `AppendStream`         |
| `IngestStreamBuilder`   | `AppendStreamBuilder`  |

Append writes rows whose fields match an existing destination table. It does not take a ScopeQL transform. Move field renaming, conversion, filtering, and routing into your application before sending:

```rust,no_run
# async fn example(client: scopedb_client::Client) -> Result<(), scopedb_client::Error> {
let source = serde_json::json!({
    "ts": "2026-03-13T12:00:00Z",
    "name": "page_view",
});
let stream = client.table("events")
    .append_stream()
    .build()?;
stream.send(&serde_json::json!({
    "occurred_at": source["ts"],
    "name": source["name"],
})).await?;
let report = stream.shutdown().await?;
println!("Written: {} rows", report.committed_rows);
# Ok(())
# }
```

For a transformation that needs ScopeQL, append raw events to an `object` column in a staging table, then run a separate `FROM ... SELECT ... INSERT INTO ...` statement. These are two writes: track which batches were transformed before retrying.

Direct writes now return `AppendRowsResult`. Stream flushes and shutdowns return `AppendDeliveryReport`, replacing `IngestResult`.

When replacing an Ingest stream:

- Handle `append_stream().build()` with `?`; it now validates configuration and returns a `Result`.
- Read `committed_rows` from the report returned by `flush()` or `shutdown()`. Each report covers activity since the previous report. In Continue mode, also inspect `outcome`. Direct `append()` still returns `num_rows_inserted`.
- Lower batch sizes above 8 MiB. An Append request allows at most 8 MiB of uncompressed NDJSON and 200,000 rows.
- Set `.max_concurrent_batches(1)` if you need sequential requests. The default is four concurrent batches per stream; query result order still requires `ORDER BY`.

Streams retry temporary rejections by default. They do not replay an unknown write outcome unless you opt into `.retry_unknown(true)`, which can duplicate rows. Client timeouts are never replayed automatically. Use `.max_retries(0)` to disable retries. Direct `append()` makes one attempt.

## Update Catalog reads

Replace paginated `list_*`, `fetch_*`, and `iterate_*` calls with a resource selector followed by `get()`. Each read returns a complete `definition` and an `etag` for conditional updates. In the table below, `catalog` is `client.catalog()`.

| In 0.3                          | In 0.4                             |
| ------------------------------- | ---------------------------------- |
| List databases                  | `catalog.get()`                    |
| List schemas in a database      | `catalog.database(db).get()`       |
| List tables in a schema         | `catalog.schema(db, schema).get()` |
| `describe()` / `table_schema()` | `table.catalog().get()`            |
| `distinct_on`                   | `definition.distinct.on`           |
| `data_retention_days`           | `definition.retention_days`        |

For example, list the tables in a schema:

```rust,no_run
# async fn example(client: scopedb_client::Client) -> Result<(), scopedb_client::Error> {
let snapshot = client.catalog()
    .schema("scopedb", "public")
    .get().await?;
for table in snapshot.definition.tables {
    println!("{}: {} columns", table.name, table.columns.len());
}
# Ok(())
# }
```

Typed definitions reject unsupported fields. Use `raw().get()` to keep them as JSON. `Table::drop()` remains available; use [`CatalogResource::delete`](https://docs.rs/scopedb-client/latest/scopedb_client/struct.CatalogResource.html#method.delete) when you need a preview or ETag precondition.

## Update statements and timeouts

| In 0.3                                         | In 0.4                                |
| ---------------------------------------------- | ------------------------------------- |
| `with_exec_timeout(...)`                       | `with_max_elapsed(Duration)`          |
| `with_max_parallelism(...)`                    | Removed                               |
| `handle.last_status()`                         | `handle.snapshot()`                   |
| `handle.status().await` / `fetch_once().await` | `handle.refresh().await`              |
| `handle.fetch().await`                         | `handle.wait().await`                 |
| Handle progress/result accessors               | Inspect `snapshot()` or call `wait()` |

Use `std::time::Duration` in place of `jiff::SignedDuration`. `with_max_elapsed` includes queueing time. The separate `with_max_estimated` option rejects queries whose estimated duration exceeds a limit; it does not control parallelism. Both take positive durations. Omitted limits use service defaults.

```rust,no_run
use std::time::Duration;

# async fn example(client: scopedb_client::Client) -> Result<(), scopedb_client::Error> {
let mut handle = client.statement("SELECT 1 AS ready")
    .with_max_elapsed(Duration::from_secs(60))
    .submit().await?;
let result = handle.wait().await?;
println!("{:?}", result.into_objects()?);
# Ok(())
# }
```

`cancel()` now returns the complete [`StatementStatus`](https://docs.rs/scopedb-client/latest/scopedb_client/enum.StatementStatus.html). A query can finish before cancellation takes effect. Pending or running statuses still need `wait()`; observed cancellation makes `wait()` return `ErrorKind::Cancelled`.

When matching status variants directly, read `failed.error` instead of `failed.message`. Cancelled statuses no longer carry a message. Read cached progress with `snapshot().map(StatementStatus::progress)` and match `StatementStatus::Finished` for a cached result.

If `query()` or `execute()` fails after submission, use `error.statement_id()` with `client.statement_handle(id)` to resume that execution. Do not resubmit the whole statement to recover from a failed status read.

## Update HTTP clients

The re-exported reqwest version changes from 0.12 to 0.13. If you supply your own HTTP client, use the SDK's re-export and disable redirects and transport retries to avoid replaying writes:

```rust
use scopedb_client::reqwest;

let http_client = reqwest::Client::builder()
    .redirect(reqwest::redirect::Policy::none())
    .retry(reqwest::retry::never())
    .build()?;
# Ok::<(), reqwest::Error>(())
```

Pass it to `Client::builder(endpoint).http_client(http_client)`. The default client already uses these policies.

Appends and Catalog mutations now have a 30-second timeout per HTTP attempt, through reading the response body. Increase it with `ClientBuilder::write_timeout` or a stream's `attempt_timeout`; the Rust SDK requires a finite, positive duration. A timed-out write may have committed, so reconcile it before replaying.

## Rename remaining fields and methods

| In 0.3                                      | In 0.4                                         |
| ------------------------------------------- | ---------------------------------------------- |
| `result.json_rows()`                        | `result.raw_rows()` (returns a slice directly) |
| `batch_bytes`                               | `target_batch_bytes`                           |
| `max_pending_bytes`                         | `max_buffered_bytes`                           |
| `max_in_flight_requests`                    | `max_concurrent_batches`                       |
| `AppendRowsResult`: `append_state`          | `status`                                       |
| `AppendErrorDetails`: `append_state`        | `status`                                       |
| Separate last-failure message/status fields | `stats().last_failure.error`                   |
| `u_int`                                     | `uint`                                         |

`AppendStreamStats` no longer implements equality because errors retain their sources. `Table::identifier()` now always includes the database and schema, using `scopedb` and `public` when omitted.
