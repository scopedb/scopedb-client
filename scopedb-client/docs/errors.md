## Classify the failure and identify the operation

`Error::kind()` is a stable category for application policy. `Error::operation()` identifies the public SDK operation that failed. A missing table can therefore be `ErrorKind::NotFound` during either `ErrorOperation::Append` or `ErrorOperation::ReadCatalog`. `ErrorKind::Incomplete` means an aggregate operation left input unconfirmed; it does not imply that any particular row committed.

SDK-only functions return `Result<T, Error>` and can compose with `?` without erasing recovery data:

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

| Failed operation                   | Recovery data                                   |
| ---------------------------------- | ----------------------------------------------- |
| `AppendStream::send_all`           | `Error::append_admission()`                     |
| `AppendWriter::write`              | `append_write_report()`, `append_input_index()` |
| `AppendStream::flush` / `shutdown` | `append_delivery_report()`                      |
| Statement operations               | `statement_id()`, `statement_details()`         |
| Catalog mutations                  | `catalog_outcome()`                             |

The corresponding append accessor is present on every failure returned by that operation. These facts survive `?` and cloning the error. The original `AppendTrySendError` also remains available through `try_send_error()` after conversion to `Error`, including record-size and buffer details.

Admitted rows belong to the stream and are not remotely acknowledged. A finite-write report retains every scheduled batch, including successes, and an unsubmitted suffix known not to have committed in that call. Use each batch's input range and index mapping to associate request-relative row errors with the original input. Delivery reports contain interval counts rather than row identities.

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

An aggregate error can retain a local validation failure while its report contains an unknown remote outcome. Always inspect the report before replaying any part of the input. Replaying an entire failed write or interval can duplicate committed rows.

## Decide whether to retry

`is_retryable()` answers whether repeating the same public operation is safe and useful. Failure kind alone cannot establish that: rate limiting can accompany a known rejection or a response with no reliable commit evidence. Unknown writes are never advertised as safe to replay. A known transient Catalog rejection can be retryable; the SDK still does not automatically retry Catalog operations.

Submitted query execution errors must be resumed through their statement ID. A transient failure from `StatementHandle::wait()` can be retried on that handle; the same status-read failure returned by `Client::query()` does not authorize another SQL submission. An aggregate append error requires inspecting its progress, rather than retrying the whole input.

`retry_exhausted()` records whether the retained failed append request exhausted its configured budget. `retries()` counts its additional attempts. Finite-write reports retain those facts separately on each failed batch, and delivery reports carry interval retry counts. Exhaustion does not establish whether any attempt committed. Explicit `.retry_unknown(true)` remains a delivery policy that can duplicate rows, including when a later acknowledgement makes logical delivery successful.

## Diagnose the failure

`code()`, `details()`, `http_status()`, `request_id()`, and `retry_after()` retain the original server diagnostics. Unknown codes and structured JSON details are preserved. Client-side failures do not manufacture server codes; use typed operation and commit fields to classify them.

After append retries, raw server details describe the last failed response, while `append_details().status` retains uncertainty across all attempts. A final rejection cannot prove that an earlier unacknowledged attempt did not commit.

`Display` describes the SDK operation and its key context. The standard `std::error::Error::source()` chain retains native transport, serialization, and parsing errors for diagnostics; `Debug` expands that chain. Recovery does not require downcasting native causes or parsing display strings.

The SDK constructs and classifies its errors at semantic boundaries. Applications that need their own configuration, file-I/O, or request context should use application-owned errors and preserve the SDK failure as a cause. Type erasure at an application's reporting boundary is separate from the concrete error contract used to compose SDK operations.
