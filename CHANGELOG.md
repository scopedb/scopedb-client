# Changelog

All significant changes to the ScopeDB Rust SDK are documented in this file.

## Unreleased

### Breaking Changes

* Require ScopeDB 0.6.0 and use native Statement, declarative Catalog, and table ingest APIs. Replace paginated Catalog APIs and the legacy execution timeout option; cancellation retains complete terminal responses.
* Removed `Client::insert`, `Client::ingest_stream`, and the transform Ingest types. Use table append APIs and transform rows in the producer; see the [migration guide](scopedb-client/README.md#migrate-from-ingest).

* `AppendStream` now retries transient unknown outcomes other than client timeouts, matching the Go and JavaScript SDKs. A successfully acknowledged batch has at-least-once delivery and retries may insert duplicates. Direct append calls still send one request and conservatively classify unknown outcomes as non-retryable.
* Upgraded the re-exported reqwest API from 0.12 to 0.13. Custom HTTP clients must now use reqwest 0.13, and the default HTTPS client uses reqwest's platform certificate verification.
* Removed the statement `with_max_parallelism` option. Elapsed and estimated duration limits replace the old execution timeout option.

### New Features

* Add complete Catalog replacements and deletion with dry-run, ETags, conditional updates, views, computed columns, and explicit destructive-change authorization. Preserve structured error codes/details and statement compute seconds.

* Table append requests now have a configurable client `write_timeout`, defaulting to 30 seconds including response-body reads. Streams inherit it unless an append stream explicitly sets `attempt_timeout`. Timeout outcomes remain unknown and are never automatically retried. The write timeout starts after local encoding, compression, and request preparation. `Error::write_details()` exposes a client timeout's `timeout` reason and `unknown` outcome while preserving existing kinds, sources, HTTP diagnostics, and subsequent stream errors.

### Improvements

* Preserve a batch's unknown outcome when subsequent retries are rejected or exhausted, including the last attempt's structured details, HTTP metadata, and typed cause through delivery barriers. Report committed rows as logical input rows acknowledged at least once.
* `AppendStream` now targets 4 MiB per batch by default; the maximum configurable target and uncompressed request limit remain 8 MiB.
* Direct caller-encoded table appends now use the same zstd compression and 8 MiB uncompressed request limit as streaming append batches.
* Preserve structured failed-statement details on terminal errors and correct binary result formatting so wire-format hex strings are not encoded a second time.

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
