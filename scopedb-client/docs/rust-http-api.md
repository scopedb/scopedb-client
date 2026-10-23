# Rust HTTP API reference

All requests identify the client through `X-ScopeDB-Client: scopedb-client`. An optional API key is sent as a Bearer token.

## Declarative Catalog

```text
GET, PUT         /v1/catalog
GET, PUT, DELETE /v1/catalog/databases/{database}
GET, PUT, DELETE /v1/catalog/databases/{database}/schemas/{schema}
GET, PUT, DELETE /v1/catalog/databases/{database}/schemas/{schema}/tables/{table}
GET, PUT, DELETE /v1/catalog/databases/{database}/schemas/{schema}/views/{view}
```

Names occupy individual percent-encoded path segments. Empty names and dot segments are rejected before sending. `client.catalog()` selects the root; its `database`, `schema`, `table`, and `view` methods select subtrees. GET returns the complete definition and its opaque `ETag`, without pagination.

PUT replaces the selected subtree, including all its children. Omitted collections are empty and omitted optional fields are cleared. The definition's `name` must match the path. DELETE removes the selected resource. A mutation returns `before`, `after`, and `destructive`, with absent resources represented by JSON null, plus an `ETag` header.

`CatalogOptions` maps `dry_run` and `allow_destructive` to query parameters and `if_match` to the `If-Match` header. A dry run validates and rolls back the changes; its ETag identifies the base version. A stale precondition returns HTTP 412 with code `catalog_version_mismatch`. Potentially destructive changes require explicit authorization. Keep the ETag exactly as returned, including its quotes.

Definitions include databases, schemas, tables, views, comments, computed-column `expression`, explicit column `indexes`, partitioning, clustering, `distinct`, and `retention_days`. Storage is accepted on creation and omitted from exports. Typed definitions reject unknown fields at every modeled level. `raw()` reads and applies `serde_json::Value` to retain unknown fields and arbitrary-precision JSON numbers.

Mutations use the client's `write_timeout` and never retry automatically. A transport or response-decoding interruption after sending produces `catalog_commit_outcome_unknown`; read the current definition before deciding whether to replay. A dry run cannot commit, so it does not receive this code solely for a transport failure.

## Table ingest

```text
POST /v1/catalog/databases/{database}/schemas/{schema}/tables/{table}/ingest
Content-Type: application/x-ndjson
Content-Encoding: zstd
```

Each non-empty line is one row object matching an existing table. The SDK and server limit each uncompressed request to 8 MiB and 200,000 rows. A successful response is:

```json
{ "status": "committed", "num_rows_inserted": 2 }
```

A rejected request can include row-level errors:

```json
{
  "code": "invalid_schema",
  "message": "row validation failed",
  "details": {
    "status": "rejected",
    "row_errors": [{ "row_index": 0, "column": "id", "message": "invalid value" }],
    "row_errors_truncated": false
  }
}
```

`committed` means all request rows committed, `rejected` means none committed, and `unknown` means the outcome cannot be determined. Row indexes are zero-based within the request. Missing or malformed outcome details are treated as unknown. Both `AppendRowsResult::status` and `AppendErrorDetails::status` match the native wire fields.

Direct `Table::append` makes one attempt. `Table::append_stream` batches values and retries temporary rejections. Replaying transient unknown outcomes requires `.retry_unknown(true)` and can duplicate rows. Client timeouts are never automatically replayed. Use `flush` or `shutdown` to observe completion and delivery reports. See the [writing guide](../README.md#writing-rows) for batching, concurrency, and failure policies.

NDJSON input is limited to signed 64-bit integers, even for `uint` columns and nested JSON values. This input restriction does not apply to query results, whose `uint` cells support the full unsigned 64-bit range.

## Statements

```text
POST /v1/statements
GET  /v1/statements/{statement_id}?format=json
POST /v1/statements/{statement_id}/cancel?format=json
```

A submission request is:

```json
{
  "statement": "SELECT 1",
  "max_elapsed_seconds": 30,
  "max_estimated_seconds": 5,
  "format": "json"
}
```

`statement_id` is optional. Duration limits must be positive and finite; omitted values use server defaults. `with_max_elapsed(Duration)` includes queueing time, while `with_max_estimated(Duration)` limits estimated execution duration.

All three endpoints return the same complete status family: pending, running, finished, failed, or cancelled. Each includes `statement_id`, `created_at`, and progress with `compute_seconds`. Finished responses contain `result_set`; failed responses contain required `error: { code, message, details? }`. Neither failed nor cancelled responses require a top-level message. HTTP success alone does not mean execution succeeded.

`snapshot` returns the cached snapshot, `refresh` fetches an active statement once, and `wait` polls to a terminal state. `cancel` returns and caches the complete response; when execution finishes before cancellation, its result or original failure remains available. Pending or running cancellation responses are observations, not cancellation acknowledgements; continue waiting for a terminal state. `wait()` returns `StatementCancelled` for cancellation and `StatementFailed` for execution failure. Terminal cached states do not trigger another request. Dropping a waiting future does not cancel server execution. Upgrading from 0.5 clears saved statement history and results.

The JSON result format encodes cells as strings or null, with field metadata providing their types. `raw_rows` preserves those wire cells. Use value or object conversions for typed values; duplicate field names are rejected by object conversion.

## Errors and migration

HTTP errors use `{ "code": "...", "message": "...", "details": ... }`. `Error::code()` and `details()` preserve unknown codes and structured details. `http_status()`, `request_id()`, and `retry_after()` expose transport diagnostics. Failed statements additionally expose `statement_details()`; append failures expose `append_details()`.

The legacy transform Ingest API is removed. Shape events before appending, or append raw events and execute a separate ScopeQL transformation. See [Upgrade from 0.3 to 0.4](migration-0.4.md#replace-ingest-with-append).
