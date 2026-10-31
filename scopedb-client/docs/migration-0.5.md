# Upgrade from 0.4 to 0.5

Version 0.5 makes partial stream progress available on failure. Applications upgrading from 0.3 should first follow [Upgrade from 0.3 to 0.4](migration-0.4.md).

## Inspect failed delivery reports

`AppendStream::flush()` and `shutdown()` return `Result<AppendDeliveryReport, AppendDeliveryError>` instead of an SDK `Error`. Both success and failure now carry settlement information. Read `failure.report()` for acknowledged, rejected, unknown, and dropped row counts; read `failure.cause()` for the original SDK diagnostics.

```rust,no_run
# async fn example(stream: scopedb_client::AppendStream) -> Result<(), Box<dyn std::error::Error>> {
match stream.shutdown().await {
    Ok(report) => println!("committed: {}", report.committed_rows),
    Err(failure) => {
        eprintln!("delivery: {:?}", failure.report());
        eprintln!("request: {}", failure.cause());
        return Err(failure.into());
    }
}
# Ok(())
# }
```

The terminal failure report covers activity since the previous completed barrier, including all rows settled while stopping. Concurrent terminal waiters receive the same report. A partial report can include unknown outcomes; counts do not identify individual rows or make replaying the whole interval safe. `stats().last_report` remains available for monitoring. If the worker terminates unexpectedly, unresolved rows are unknown and the report duration is zero because the barrier wait time is unavailable.

When an existing function must return the SDK `Error`, explicitly discard the report with `.map_err(|failure| failure.into_cause())?` after inspecting any recovery information it needs.

## Inspect partial admission

`AppendStream::send_all()` returns `AppendAdmissionError` instead of `Error` on failure. Its `accepted_rows()` is the prefix admitted by that call, independently of other producers. Those rows belong to the stream and may still be written: settle the stream before deciding what to replay. `cause()` and `into_cause()` expose the underlying error.

## Recover submitted statements

Every statement now carries an ID before submission. After losing a submission response, use `error.statement_id()` with `client.statement_handle(id)` to inspect the existing execution. `with_statement_id()` still accepts an application-owned ID. Neither possessing an ID nor receiving a retryable status-read error authorizes another submission of the SQL.

## Inspect Catalog mutation outcomes

Use `Error::catalog_outcome()` to distinguish `CatalogMutationOutcome::Rejected` from `Unknown`. Unknown requires reading the resource before replaying the mutation, including when a gateway supplies a structured error code. Reads, dry runs, and local validation failures return None. `code()`, `details()`, and HTTP metadata retain the original diagnostics.
