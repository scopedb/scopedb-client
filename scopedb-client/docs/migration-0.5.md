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

`AppendStream::flush()` and `shutdown()` keep returning `Result<AppendDeliveryReport, Error>`. A failure now retains its terminal interval in `Error::append_delivery_report()`, including committed, failed, ambiguous, and dropped row counts. Returning that error through `?` preserves the report and the original diagnostics.

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

The terminal failure report covers activity since the previous completed barrier, including in-flight batches settled while stopping, and is shared by concurrent waiters. Because report counts do not identify individual rows and may include ambiguous outcomes, reconcile before replaying. `stats().last_report` also retains the latest report.

## Inspect partial admission and finite writes

`AppendStream::send_all()` keeps returning `Error` on failure. Read `error.append_admission().unwrap().accepted_rows` instead of parsing the diagnostic message for the prefix admitted by that call. Those rows remain owned by the stream; use `flush()` or `shutdown()` to observe their delivery. The remaining suffix was not admitted.

Finite writers also return `Error`. `append_write_report()` retains successful and failed batches, input-index mappings, and the unsubmitted suffix. `append_input_index()` identifies local encoding failures. SDK operations compose through `?` without converting errors or discarding these recovery facts.

## Handle table append confirmations and retries

Append outcomes describe success, definite failure, and ambiguity. Update enum matches, retry configuration, and report accessors:

| In 0.4                                  | In 0.5                                    |
| --------------------------------------- | ----------------------------------------- |
| `AppendState`                           | `AppendOutcome`                           |
| `AppendState::Committed`                | `AppendOutcome::Succeeded`                |
| `AppendState::Rejected`                 | `AppendOutcome::Failed`                   |
| `AppendState::Unknown`                  | `AppendOutcome::Ambiguous`                |
| `AppendDeliveryOutcome::Ok`             | `AppendDeliveryOutcome::Succeeded`        |
| `AppendDeliveryOutcome::Unknown`        | `AppendDeliveryOutcome::Ambiguous`        |
| `AppendStreamBuilder::retry_unknown()`  | `AppendStreamBuilder::retry_ambiguous()`  |
| `AppendDeliveryReport::unknown_rows`    | `AppendDeliveryReport::ambiguous_rows`    |
| `AppendDeliveryReport::unknown_batches` | `AppendDeliveryReport::ambiguous_batches` |
| `AppendStreamStats::unknown_rows`       | `AppendStreamStats::ambiguous_rows`       |

`Failed` guarantees that this attempt took effect on no rows and will not take effect later. Retry decisions still depend on the cause: invalid input needs correction, while admission limits may clear. `Ambiguous` means the attempt may have taken effect or may still take effect later. `AppendOutcome` retains the serialized values `committed`, `rejected`, and `unknown`; its `Display` output uses `succeeded`, `failed`, and `ambiguous`.

`AppendRowsResult::status` is now `AppendOutcome::Succeeded` after a valid success response, including responses that omit the wire `status` field. Update matches on `AppendState::Committed` to use the new variant. Success still confirms that the entire batch committed. The SDK accepts both success formats during service upgrades.

The SDK also accepts older flat error responses with explicit `details.status: rejected` on HTTP 4xx. Missing or unrecognized rejection details leave those responses ambiguous. Older row diagnostics remain available through `append_details()` when supplied; `AppendRequestResult::input_index` maps their zero-based request indexes back to the input slice. See the [HTTP API reference](rust-http-api.md#table-ingest) for the current response format and status/code correspondence.

Use `Error::append_details()` for outcome certainty instead of inspecting `details.status` in the raw server error. A valid modern 4xx error establishes `Failed`; 5xx and interrupted or invalid responses are `Ambiguous`. Only validated HTTP 429 admission refusals are retried by default. In particular, older HTTP 503 responses no longer qualify as safe retries even when they contain `status: rejected` or `retryable: true`. Keep ambiguous batches available for application reconciliation; `.retry_ambiguous(true)` explicitly accepts duplicate-write risk.

Row-error lists may be empty because modern responses provide a diagnostic message instead of structured row errors. Handle the entire failed batch; do not parse diagnostic text to select individual rows for replay. A later definite failure leaves an earlier ambiguous attempt unresolved.

## Recover submitted statements

Every statement now carries an ID before submission. After losing a submission response or failing while waiting, use `error.statement_id()` with `client.statement_handle(id)` to inspect or resume waiting on the existing execution rather than resubmitting the SQL. `with_statement_id()` still accepts an application-owned ID.

## Inspect Catalog mutation outcomes

Rename the Catalog version fields and pass their opaque tokens unchanged:

| In 0.4                     | In 0.5                             |
| -------------------------- | ---------------------------------- |
| `CatalogSnapshot::etag`    | `CatalogSnapshot::version`         |
| `CatalogResult::etag`      | `CatalogResult::version`           |
| `CatalogOptions::if_match` | `CatalogOptions::expected_version` |

The token identifies the entire tenant catalog, including when reading or changing a single resource. Use a read or dry-run result's `version` as `expected_version` when applying a change. A stale condition returns HTTP 412 with code `precondition_failed` and `ErrorKind::Conflict`; read the resource again and revise the change. See the [HTTP API reference](rust-http-api.md#declarative-catalog) for transport details.

Use `Error::catalog_outcome()` to distinguish `CatalogMutationOutcome::Rejected` from `Unknown`. `Unknown` requires reading the resource before replaying the mutation, including when a gateway supplies a structured error code. Reads, dry runs, and local validation failures return `None`. Replace checks for the client-generated `catalog_commit_outcome_unknown` code with this typed outcome. `code()`, `details()`, and HTTP metadata retain original server diagnostics without synthesizing client-side server codes.
