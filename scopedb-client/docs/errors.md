## Classify the failure and identify the operation

`Error::kind()` classifies the failure category, and `Error::operation()` identifies the SDK operation that failed. For example, a missing table returns `ErrorKind::NotFound` during both `ErrorOperation::Append` and `ErrorOperation::ReadCatalog`. `ErrorKind::Incomplete` indicates an aggregate operation with unconfirmed input; its attached report describes what happened to the rows.

All async SDK operations return `Result<T, Error>`, so functions can compose with `?` while preserving recovery data:

```rust,no_run
use scopedb_client::Client;
use scopedb_client::Error;

async fn write(client: &Client) -> Result<(), Error> {
    let stream = client.table("events").append_stream().build()?;
    stream.send(&serde_json::json!({"id": 1})).await?;
    let report = stream.shutdown().await?;
    println!("committed: {}", report.committed_rows);
    Ok(())
}
```

## Recover partial progress

| Failed operation                          | Recovery accessors                              |
| ----------------------------------------- | ----------------------------------------------- |
| `AppendStream::send_all`                  | `append_admission()`                            |
| `AppendWriter::write`                     | `append_write_report()`, `append_input_index()` |
| `AppendStream::flush` / `shutdown`        | `append_delivery_report()`                      |
| `AppendTrySendError` converted to `Error` | `try_send_error()`                              |
| Statement operations                      | `statement_id()`, `statement_details()`         |
| Catalog mutations                         | `catalog_outcome()`                             |

Failed `send_all()`, `write()`, `flush()`, and `shutdown()` calls always attach their progress reports. These details survive `?` and cloning. `try_send_error()` retains the original `AppendTrySendError` after conversion to `Error`.

Admitted rows belong to the stream; use `flush()` or `shutdown()` to observe their delivery. A finite-write report (`AppendWriteReport`) records every scheduled batch in input order, `AppendRequestResult::input_index` maps server row errors back to the input slice, and `unsubmitted()` identifies the suffix that was never sent. Stream delivery reports (`AppendDeliveryReport`) provide interval counts since the previous completed barrier and are shared across concurrent terminal waiters.

```rust,no_run
# async fn settle(stream: scopedb_client::AppendStream) -> Result<(), scopedb_client::Error> {
if let Err(error) = stream.shutdown().await {
    let report = error.append_delivery_report().unwrap();
    eprintln!("committed: {}, unknown: {}", report.committed_rows, report.unknown_rows);
    return Err(error);
}
# Ok(())
# }
```

Always inspect the attached report before replaying failed input, as earlier batches may have committed or finished with `AppendState::Unknown`.

## Decide whether to retry

`is_retryable()` indicates whether repeating the same SDK call as-is is safe and useful. It returns `false` when a write or catalog mutation has an unknown commit outcome, when an error carries an aggregate progress report, or when a SQL submission or execution call failed after submission.

After losing a submission response or a later status-read response, use the ID from `error.statement_id()` with `Client::statement_handle()` to inspect the existing execution. Transient polling errors from `StatementHandle::refresh()` or `StatementHandle::wait()` can be retried on the same handle.

`retries()` reports how many retry attempts a failed append request made, and `retry_exhausted()` indicates whether a retryable failure reached the configured retry limit. `AppendRequestResult::retries()` and `AppendDeliveryReport::retries` record retry counts for finite writes and stream intervals. Opting into `.retry_unknown(true)` retries transient unknown append outcomes and can insert duplicate rows.

## Diagnose the failure

`code()`, `details()`, `http_status()`, `request_id()`, and `retry_after()` expose the server diagnostics from the last failed response. Client-side failures leave `code()` as `None`.

After append retries, `details()` describes the last response, while `append_details()` preserves `AppendState::Unknown` if any earlier attempt had an unknown commit outcome. Client-side append timeouts also populate `write_details()`.

`Display` formats the operation, category, and key recovery fields. `std::error::Error::source()` and `Debug` expose the underlying transport, serialization, or parse error. Wrap `Error` as a source in your own error type when attaching application context.
