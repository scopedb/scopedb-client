# Rust HTTP API reference

This document describes the HTTP surface modeled by the Rust SDK and the delivery rules that its higher-level helpers preserve. Public endpoints are rooted at `/v1`.

## REST catalog API

Catalog endpoints are read-only. Every user-provided database, schema, and table name occupies one URL path segment; the SDK percent-encodes those segments.

### Databases

```text
GET /v1/databases
GET /v1/databases/{database}
```

### Schemas

```text
GET /v1/databases/{database}/schemas
GET /v1/databases/{database}/schemas/{schema}
```

### Tables

```text
GET /v1/databases/{database}/schemas/{schema}/tables
GET /v1/databases/{database}/schemas/{schema}/tables/{table}
```

### Pagination

All list endpoints accept:

- `page_size`: optional integer from 1 through 1000; the default is 100
- `page_token`: optional opaque token returned by the previous page

The Rust SDK exposes these as `CatalogListOptions`. A list response has the same shape for every resource type:

```json
{
  "items": [],
  "next_page_token": "opaque-token"
}
```

`next_page_token` is omitted when there is no next page. Clients must pass it back unchanged and must not parse or synthesize it.

`Client::iterate_databases`, `iterate_schemas`, and `iterate_tables` follow the token automatically, fetch pages lazily, and reject a repeated token instead of looping forever. The `list_*` methods expose one explicit page when an application needs page boundaries.

### Resource shapes

Database:

```json
{
  "name": "scopedb",
  "comment": "optional description"
}
```

Schema:

```json
{
  "database": "scopedb",
  "name": "public",
  "comment": "optional description"
}
```

Table list endpoints return summaries:

```json
{
  "database": "scopedb",
  "schema": "public",
  "name": "events",
  "comment": "optional description"
}
```

Fetching one table returns its full public specification:

```json
{
  "database": "scopedb",
  "schema": "public",
  "name": "events",
  "columns": [
    {
      "name": "occurred_at",
      "data_type": "timestamp",
      "comment": null
    }
  ],
  "partition_by": [],
  "cluster_by": [],
  "distinct_on": {
    "on": [],
    "by": []
  },
  "data_retention_days": null,
  "comment": null
}
```

## Streaming write API

The streaming write API appends rows that already match an existing table. It supports NDJSON only.

### `POST /v1/databases/{database}/schemas/{schema}/tables/{table}/rows`

Required request header:

```http
Content-Type: application/x-ndjson
```

Request body:

```ndjson
{"id":1,"name":"first"}
{"id":2,"name":"second"}
```

Each line is one complete JSON row object. A JSON array is not a valid replacement for multiple NDJSON lines. The body must not be empty. One request is limited to 16 MiB and 200,000 rows.

A committed response is:

```json
{
  "append_state": "committed",
  "num_rows_inserted": 2
}
```

`num_rows_inserted` is the number of rows written.

### Structured append errors

An append failure uses this payload when the outcome is known:

```json
{
  "message": "row validation failed",
  "append_state": "rejected",
  "row_errors": [
    {
      "row_index": 0,
      "column": "id",
      "message": "invalid value"
    }
  ],
  "row_errors_truncated": false
}
```

`append_state` has three meanings:

- `committed`: all request rows committed
- `rejected`: no request row committed
- `unknown`: the commit outcome cannot be determined

`row_index` is zero-based within the submitted NDJSON request. The server may truncate the row-error list; `row_errors_truncated` preserves that fact.

### Writing with the SDK

Create a stream with `table.append_stream().build()`. The SDK converts Rust values to NDJSON and writes them in batches.

| Method | Use |
| --- | --- |
| `send()` | Add one row to the SDK's pending writes. |
| `send_all()` | Add rows from an iterator. |
| `try_send()` | Try to add a row without waiting; check the returned result. |
| `flush()` | Wait for pending writes while keeping the stream open. |
| `shutdown()` | Wait for writing to complete and close the stream. |

`send()` and `send_all()` add rows to the SDK without waiting for the writes to finish. Call `shutdown()` when you are done sending rows.

Set `.failure_policy(AppendFailurePolicy::Continue)` to keep the stream running after a batch fails. Read the report returned by `flush()` or `shutdown()` for the write results.

See the [stream example](../README.md#write-with-a-stream) for a complete usage example.

## Statement API

### `POST /v1/statements`

Submits a statement for execution.

```json
{
  "statement_id": "uuid-v7-or-user-provided",
  "statement": "SELECT 1",
  "exec_timeout": "PT1S",
  "format": "json"
}
```

Request fields:

- `statement_id`: optional from the SDK perspective
- `statement`: required
- `exec_timeout`: optional
- `format`: `json` for the public Rust SDK

The response is a tagged statement-state payload: `pending`, `running`, `finished`, `failed`, or `cancelled`. Statement failure and cancellation are in-band states, so HTTP success does not imply statement success.

### `GET /v1/statements/{statement_id}?format=json`

Fetches the latest state for a submitted statement and returns the same state payload family as statement submission. `StatementHandle::status().await` performs at most one fetch and updates the snapshot returned by `StatementHandle::last_status`; terminal snapshots are returned without another request. `StatementHandle::wait` polls with bounded exponential delay until a terminal state. The older `fetch_once` and `fetch` names remain deprecated aliases for `status` and `wait`, respectively.

### `POST /v1/statements/{statement_id}/cancel`

Cancels a pending or running statement. The response contains the post-cancel terminal status view:

```json
{
  "statement_id": "uuid",
  "status": "finished|failed|cancelled",
  "message": "statement is ...",
  "created_at": "timestamp"
}
```

### Statement result shape

A finished statement contains a JSON result set:

```json
{
  "status": "finished",
  "statement_id": "uuid",
  "created_at": "timestamp",
  "progress": {},
  "result_set": {
    "metadata": {
      "fields": [
        { "name": "col", "data_type": "string" }
      ],
      "num_rows": 1
    },
    "format": "json",
    "rows": [["value"]]
  }
}
```

## Transform-oriented ingest API

### `POST /v1/ingest`

Ingests JSON lines through a transformation statement.

```json
{
  "type": "committed",
  "data": {
    "format": "json",
    "rows": "{\"k\":1}\n{\"k\":2}"
  },
  "statement": "SELECT ... INSERT INTO target_table"
}
```

The Rust SDK uses committed ingest with JSON-line data. A successful response is:

```json
{
  "num_rows_inserted": 2
}
```

This endpoint is useful when each input record needs a SQL transform. For rows already shaped like a table, use the streaming write API.

## Generic error responses

Non-append non-2xx responses generally use:

```json
{
  "message": "..."
}
```

Use `Error::message()` to read the error message. When available, `http_status()`, `request_id()`, and `retry_after()` provide the HTTP status, request ID, and suggested retry delay.
