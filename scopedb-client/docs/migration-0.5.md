# Upgrade from 0.4 to 0.5

Version 0.5 makes partial stream progress available on failure. Applications upgrading from 0.3 should first follow [Upgrade from 0.3 to 0.4](migration-0.4.md).

## Match failure categories and operations

`ErrorKind` describes a stable failure category, while `ErrorOperation` identifies the public operation. A missing table is `NotFound` in both Append and Catalog; a partial aggregate operation is `Incomplete` and retains its progress report.

| In 0.4                                         | In 0.5                                                        |
| ---------------------------------------------- | ------------------------------------------------------------- |
| `ErrorKind::ConfigInvalid`                     | `ErrorKind::InvalidInput`                                     |
| `ErrorKind::StatementCancelled`                | `ErrorKind::Cancelled`                                        |
| `AppendRowsFailed` / `CatalogFailed`           | Normalized category plus `Error::operation()` and commit data |
| `ErrorKind::StatementFailed`                   | `statement_details()` and the normalized server-code category |
| `ErrorKind::Unexpected`                        | Specific category such as `InvalidResponse` or `Internal`     |
| `is_temporary()` / `is_persistent()`           | `is_retryable()` plus typed progress and commit outcomes      |
| `is_permanent()`                               | Failure kind and `is_retryable()`                             |
| Diagnostic retry counts                        | `retries()` and `retry_exhausted()` on the failed request     |
| `ErrorKind::into_static()` / string conversion | `ErrorKind::as_str()`                                         |

`Error::new`, `with_context`, `set_source`, and retry-state setters are internal. Applications should represent their own failures with application-owned error types, retaining the SDK error as a cause when needed. SDK-only functions return `Result<T, Error>` without a dynamic error wrapper. See the [error-handling guide](errors.md) for recovery contracts and diagnostics.

## Inspect failed delivery reports

`AppendStream::flush()` and `shutdown()` keep returning `Result<AppendDeliveryReport, Error>`. A failure now retains its terminal interval in `Error::append_delivery_report()`, including committed, rejected, unknown, and dropped row counts. Returning that error through `?` preserves the report and the original diagnostics.

```rust,no_run
# async fn example(stream: scopedb_client::AppendStream) -> Result<(), scopedb_client::Error> {
match stream.shutdown().await {
    Ok(report) => println!("committed: {}", report.committed_rows),
    Err(error) => {
        eprintln!("delivery: {:?}", error.append_delivery_report().unwrap());
        return Err(error);
    }
}
# Ok(())
# }
```

The terminal failure report covers activity since the previous completed barrier, including all rows settled while stopping. Concurrent terminal waiters receive the same report. A partial report can include unknown outcomes; counts do not identify individual rows or make replaying the whole interval safe. `stats().last_report` remains available for monitoring. If the worker terminates unexpectedly, unresolved rows are unknown and the report duration is zero because the barrier wait time is unavailable.

## Inspect partial admission and finite writes

`AppendStream::send_all()` keeps returning `Error` on failure. Read `error.append_admission().unwrap().accepted_rows` instead of parsing the diagnostic message for the prefix admitted by that call. Those rows belong to the stream and may still be written: settle it before deciding what to replay.

Finite writers also return `Error`. `append_write_report()` retains successful and failed batches, input-index mappings, and the unsubmitted suffix. `append_input_index()` identifies local encoding failures. SDK operations compose through `?` without converting errors or discarding these recovery facts.

## Recover submitted statements

Every statement now carries an ID before submission. After losing a submission response, use `error.statement_id()` with `client.statement_handle(id)` to inspect the existing execution. `with_statement_id()` still accepts an application-owned ID. Neither possessing an ID nor receiving a retryable status-read error authorizes another submission of the SQL.

## Inspect Catalog mutation outcomes

Use `Error::catalog_outcome()` to distinguish `CatalogMutationOutcome::Rejected` from `Unknown`. Unknown requires reading the resource before replaying the mutation, including when a gateway supplies a structured error code. Reads, dry runs, and local validation failures return None. Replace checks for the client-generated `catalog_commit_outcome_unknown` code with this typed outcome. `code()`, `details()`, and HTTP metadata retain original server diagnostics; client-side uncertainty no longer synthesizes server codes.
