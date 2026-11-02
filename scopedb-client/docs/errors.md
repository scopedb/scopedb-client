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

Admitted rows belong to the stream; use `flush()` or `shutdown()` to observe their delivery. A finite-write report (`AppendWriteReport`) records every scheduled batch in input order, including its input range and commit outcome, and `unsubmitted()` identifies the suffix that was never sent. Stream delivery reports (`AppendDeliveryReport`) provide interval counts since the previous completed barrier and are shared across concurrent terminal waiters.

```rust,no_run
# async fn settle(stream: scopedb_client::AppendStream) -> Result<(), scopedb_client::Error> {
if let Err(error) = stream.shutdown().await {
    let report = error.append_delivery_report().unwrap();
    eprintln!("committed: {}, ambiguous: {}", report.committed_rows, report.ambiguous_rows);
    return Err(error);
}
# Ok(())
# }
```

Always inspect the attached report before replaying failed input, as earlier batches may have committed or finished with `AppendOutcome::Ambiguous`.

## Determine the append outcome

`AppendOutcome` describes what the SDK knows about the entire request, not a field to look for in the server response:

| SDK outcome       | Meaning                                                                | Where to read it                 |
| ----------------- | ---------------------------------------------------------------------- | -------------------------------- |
| `Succeeded`       | All request rows committed.                                            | `AppendRowsResult::status`       |
| `Failed`          | No rows took effect or can take effect later.                          | `Error::append_details().status` |
| `Ambiguous`       | The request may have taken effect or may still take effect later.      | `Error::append_details().status` |

A complete HTTP 200 response with a valid, matching `num_rows_inserted` confirms commit. A valid HTTP 4xx error establishes a definite failure. HTTP 5xx errors, interrupted requests, and invalid responses leave the outcome ambiguous. Local validation can also establish a definite failure before sending a request. A server `408 request_timeout` is a definite failure; a client-side timeout is ambiguous.

The ingest API reports failures for the entire batch, without per-row results. Handle the batch as a unit and do not parse diagnostic messages to select rows for replay. After retries, a later definite failure leaves an earlier ambiguous attempt unresolved.

## Decide whether to retry

`is_retryable()` indicates whether repeating the same SDK call as-is is safe and useful. It returns `false` when a write or catalog mutation has an uncertain write outcome, when an error carries an aggregate progress report, or when a SQL submission or execution call failed after submission.

`Table::append` makes one attempt. Finite writers and append streams automatically retry validated HTTP 429 admission refusals within their retry budget. Other 4xx responses require caller handling. Ambiguous outcomes require application reconciliation before replay; `.retry_ambiguous(true)` explicitly accepts the risk of duplicate rows when retrying transient ambiguous failures. Client-side append timeouts are never retried automatically.

After losing a submission response or a later status-read response, use the ID from `error.statement_id()` with `Client::statement_handle()` to inspect the existing execution. Transient polling errors from `StatementHandle::refresh()` or `StatementHandle::wait()` can be retried on the same handle.

`retries()` reports how many retry attempts a failed append request made, and `retry_exhausted()` indicates whether a retryable failure reached the configured retry limit. `AppendRequestResult::retries()` and `AppendDeliveryReport::retries` record retry counts for finite writes and stream intervals.

## Diagnose the failure

`http_status()` returns the HTTP status, such as 429. `code()` returns the response body's string error category, such as `too_many_requests`. `kind()` returns the SDK category, such as `ErrorKind::RateLimited`. These describe the failure; use `append_details().status` to determine the append's commit outcome. The [HTTP API reference](rust-http-api.md#table-ingest) lists the status/code pairs and their recovery guarantees. Known codes must match their HTTP status; a mismatch is `InvalidResponse` with an ambiguous append outcome. Unrecognized codes are preserved and follow the HTTP status guarantee when the error response is otherwise valid.

`details()`, `request_id()`, and `retry_after()` expose structured diagnostics, the request identifier, and the server's retry delay from the last failed response. Client-side failures leave `code()` as `None`.

After append retries, `details()` describes the last response, while `append_details()` preserves `AppendOutcome::Ambiguous` if any earlier attempt had an ambiguous outcome. Client-side append timeouts also populate `write_details()`.

`Display` formats the operation, category, and key recovery fields. `std::error::Error::source()` and `Debug` expose the underlying transport, serialization, or parse error. Wrap `Error` as a source in your own error type when attaching application context.
