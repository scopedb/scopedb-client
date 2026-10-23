# Changelog

All significant changes to the ScopeDB Rust SDK are documented in this file.

## Unreleased

## v0.4.0 (2026-10-23)

### Breaking Changes

* Require ScopeDB 0.6.0 or later. Replace paginated Catalog APIs with complete declarative definitions and ETags; replace `with_exec_timeout` and `with_max_parallelism` with `with_max_elapsed` and `with_max_estimated` duration limits.
* Remove `Client::insert`, `Client::ingest_stream`, and the transform Ingest types. Use table append APIs and shape rows in the producer; see the [migration guide](scopedb-client/README.md#migrate-from-ingest).
* Consolidate table definitions and conditional changes under `Table::catalog()`, replacing `describe`, `table_schema`, and `apply` shortcuts. Typed definitions reject unsupported fields; use `raw()` to retain them.
* Rename statement cached reads to `snapshot()` and network refreshes to `refresh()`. Remove fetch aliases and redundant handle progress/result accessors; inspect the snapshot or call `wait()`. Distinguish `ErrorKind::StatementCancelled` from execution failure.
* Retry only temporary Append rejections by default. Replaying transient unknown outcomes requires `.retry_unknown(true)` and may insert duplicates; direct appends and client timeouts are never replayed automatically.
* Rename Append outcome fields to `status`, retain the complete error in `AppendLastFailure::error`, and remove deprecated result and stream builder aliases. Remove the unused `WriteErrorReason::Cancelled` variant: dropping a future does not produce a returned cancellation error. See the [0.6 migration guide](scopedb-client/README.md#migrate-to-scopedb-06) for replacements.
* Upgrade the re-exported reqwest API from 0.12 to 0.13. Custom HTTP clients must use reqwest 0.13 and disable redirects and transport retries to avoid replaying writes. The default HTTPS client uses platform certificate verification.

### New Features

* Add complete Catalog replacements and deletion with dry-run, ETags, conditional updates, views, computed columns, and explicit destructive-change authorization. Preserve structured error codes/details and statement compute seconds.
* Bound Append and Catalog mutation HTTP requests with a configurable `write_timeout`, defaulting to 30 seconds through response-body reads. Streams can override it with `attempt_timeout`. Append timeout errors expose `WriteErrorReason::Timeout` and an unknown outcome; Catalog interruptions use `catalog_commit_outcome_unknown`.
* Add `PartialEq` for typed result values so applications can compare cells and rows directly. Encoded composite and binary values compare as strings; floats use Rust's usual equality rules.

### Bug Fixes

* Settle every admitted row before publishing stream shutdown completion, including after the waiting future is dropped. Keep partial delivery reports on Stop-mode failures and preserve original diagnostics through subsequent stream errors.
* Preserve a batch's unknown outcome when later retries are rejected or exhausted, and honor explicit server retry refusal. Retain rejection details from nested error envelopes instead of misclassifying them as unknown.
* Fully qualify table identifiers so database-only selectors target the same table through SQL, Catalog, and Append.
* Preserve large integers and decimal JSON in raw Catalog definitions and error details. Reject overflowing result numbers with row/column diagnostics, retain special float values, and avoid double-encoding binary results.

### Improvements

* Use 4 MiB as the default stream batch target. Compress all table appends with zstd and enforce the 8 MiB uncompressed body and 200,000-row limits before sending.
* Validate endpoint and stream configuration before starting work, and disable redirects and transport retries on the SDK-owned HTTP client.
* Replace the transform-named example with a bounded NDJSON file importer that settles accepted rows after an input failure. Document ScopeDB 0.6's signed 64-bit NDJSON integer limit separately from full-range unsigned query results.

## v0.3.3 (2026-08-24)

### Improvements

* Pointed the crate repository metadata and packaged documentation links at the standalone `scopedb-client` repository. The public API and runtime behavior are unchanged.

## v0.3.2 (2026-08-22)

### Improvements

* `AppendStream` now caps each uncompressed NDJSON request at 8 MiB.

## v0.3.1 (2026-08-20)

### Improvements

* JSON request bodies and streaming table append batches now use zstd compression by default, and the default HTTP client transparently negotiates compressed responses. Direct caller-encoded table appends remain identity-encoded.

## v0.3.0 (2026-08-06)

### Breaking Changes

* Raised the minimum supported Rust version from 1.85.0 to 1.91.0 so fresh consumers can resolve the current dependency graph without an inherited lockfile.
* Renamed the synchronous `StatementHandle::status()` snapshot accessor to `last_status()` and use the asynchronous `status().await` operation for one remote status check. The previous synchronous name cannot remain as an alias because Rust does not support overloading it with the asynchronous operation.

### New Features

* Added an API-key client builder, `Client::query`, full table descriptions, lazy catalog iterators, and `StatementHandle::status` / `wait` application paths. The released `fetch_once` and `fetch` methods remain as deprecated aliases for `status` and `wait`.
* Added raw, typed-value, keyed-object, and first-row result conversions. Object conversion rejects duplicate output column names instead of silently dropping a value.
* Added streaming-write builder names centered on batches and buffering, plus a configurable `max_batch_rows` below the protocol ceiling. Existing builder names remain compatible deprecated aliases.

### Improvements

* Preserve HTTP status, request ID, `Retry-After`, and explicit server retry classification on errors while keeping server messages unchanged.
* Honor delta-seconds and HTTP-date `Retry-After` values for safe rejected-batch retries, capped by the configured maximum backoff.
* Return typed errors instead of panicking when result metadata and row shapes disagree, and reject repeated catalog page tokens.
* Included the Apache-2.0 license text in the published crate archive.
* Added the user guide to the crate root so docs.rs starts with installation, query, catalog, streaming-write, and error-handling paths.
* Reworked runnable examples around read-only discovery, direct append, asynchronous batching, bulk imports, and best-effort telemetry journeys.

## v0.2.2 (2026-08-05)

### Bug Fixes

* Removed `Client::health_check`, which targeted an internal liveness endpoint and could report non-success HTTP responses as healthy.
* Decode error messages from both direct and nested API error envelopes, including unknown-outcome append failures.

### Improvements

* Prefer `SCOPEDB_API_KEY` in examples while retaining `SCOPEDB_TOKEN` as a backward-compatible fallback, and mark authorization headers as sensitive.

## v0.2.1 (2026-08-05)

### Bug Fixes

* Re-exported the SDK's compatible reqwest version and corrected the installation guide so new consumers do not resolve an incompatible reqwest major.

## v0.2.0 (2026-08-05)

### Breaking Changes

* Replaced the `Connection` and Arrow record-batch API with the async `Client`, `Statement`, `StatementHandle`, and `Table` APIs.
* Replaced Arrow results with the SDK's typed JSON result and value model.
* Set the minimum supported Rust version to 1.85.0.

### New Features

* Added typed statement lifecycle, cancellation, and progress APIs.
* Added JSON ingest with transform support and a bounded asynchronous ingest stream.
* Added read-only REST catalog list and fetch APIs for databases, schemas, and tables.
* Added direct NDJSON table appends and a bounded, concurrent asynchronous append stream.
* Added strict and best-effort delivery policies, settlement reports, retry and unknown-outcome handling, circuit breaking, and local drop statistics.
* Added runnable examples for catalog discovery, direct appends, bulk writes, telemetry, and transform-oriented ingest.

### Improvements

* Preserved structured append rejection details and kept ambiguous commit outcomes distinct from safe-to-retry rejections.
* Added backpressure for queued rows and bytes, bounded HTTP concurrency, and delivery barriers for flushing and shutdown.

## v0.1.1 (2025-02-21)

### New Features

* Added Arrow record-batch insertion with a custom transform.

## v0.1.0 (2025-02-20)

### New Features

* Initial crates.io release.
