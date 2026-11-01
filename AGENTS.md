# Repository Guidelines

## Design

Organize code around resource identity, operation semantics, and ownership. Reuse existing resource types instead of copying their identity into parallel representations. Keep shared encoding and request execution independent of producer queues and background worker lifecycles.

Keep abstractions that own an invariant, resource, or distinct lifecycle. Remove forwarding helpers, wrappers, and duplicate state that have no independent role. A separate file does not by itself justify a module interface. Place public operations before their private implementation helpers.

Single-request writes, finite writes, and append streams have different completion and cancellation contracts. Preserve those distinctions. A finite write owns its pending requests and retries; a stream owns admitted rows independently of an individual caller. Reports must distinguish acknowledged writes, known rejections, unknown outcomes, and input that was never submitted. Dropping a future cannot roll back a request already sent to the server.

Preserve resource ownership and destruction order when simplifying concurrency code. A retained permit or guard can enforce a memory bound or release protocol even when its value is never read. Use conventional local guard bindings and explain non-obvious ownership fields. Avoid defensive state and recovery paths without an explicit contract or realistic failure scenario.

## Rust style

Name types by their domain role and scope. Distinguish reusable writers, one write call, and individual requests. Use `Options` for editable inputs awaiting validation and `Config` for validated or resolved configuration. Introduce separate representations only when validation or runtime requirements make their distinction useful.

Declare restricted visibility at module boundaries and use `pub` for their internal interfaces. Keep items private when only their defining module and descendants need them. For publicly reachable modules and re-exported types, reserve unrestricted `pub` for intentional public API and use narrower visibility for internal callers. Do not widen visibility solely to let another module's tests reach an implementation detail.

Import SDK symbols from `crate` instead of spelling `crate::...` or `super::...` inline. Preserve imported names and retain meaningful qualification such as `tokio::time::sleep` when it identifies the operation or distinguishes names. Avoid aliases without a concrete reason. Reserve `use super::*` for test modules. Re-export a module's own children with explicit `pub use self::...` declarations.

Use inline fully qualified paths for `std::error::Error` and operations such as `std::panic::catch_unwind`, rather than importing the `error` or `panic` modules.

In xtask, `use std::process::Command as StdCommand` is allowed to distinguish process commands from command-line parsing types.

## Public API contracts

Describe what an operation accepts, when it completes, and what a failure establishes. Preserve structured information needed for recovery on both success and failure paths. Diagnostic strings are not a substitute for typed partial-progress information.

Return the concrete SDK `Error` from asynchronous SDK operations so callers can compose them without type erasure or losing progress. Keep failure category, public operation, commit knowledge, and retry-budget facts separate. Normalize lower-level failures where their operation semantics are known; reserve the source chain for diagnostics. Error construction and retry classification belong to the SDK, while callers read typed recovery data.

Keep public entry points and exported types deliberate. Re-export a dependency when callers must supply a compatible type from that dependency. Review compatibility against the latest release; unreleased APIs can be corrected before publication, while changes to released APIs need migration guidance and an appropriate release version.

Document return values, failure conditions, and cancellation behavior on the relevant methods. Describe types with noun phrases and operations with present-tense verbs. Add Errors, Panics, and Examples sections only where they describe actual contracts. Keep examples focused on realistic callers and use repository-relative links for repository content. Use public API names as intra-doc link labels and put crate-relative resolution paths in explicit targets; keep internal path prefixes out of rendered prose.

## Tests and validation

Test observable behavior through the public API when practical. Keep tests that require private implementation access next to the implementation, and keep test-server behavior tests with the fixture. Shared fixtures should model concrete protocol and timing scenarios rather than grow into a general framework.

Cover partial progress, malformed responses, resource limits, and cancellation at meaningful boundaries. Prefer deterministic coordination to sleeps. Preserve lifecycle and retry invariants when moving tests or simplifying implementation details.

Use `cargo x` as the workflow entry point. Read `cargo x --help` and the relevant subcommand's help before using it. Before submitting a change, run:

```sh
cargo x check
cargo x test
cargo x lint
```

## Markdown

- Keep each semantic prose paragraph and each list item on one source line; do not hard-wrap within a paragraph or list item.
- Format Markdown tables with aligned pipe columns.

## User documentation

- Keep the crate landing page short: installation, a runnable query, a small write example, and links by task. Put configuration and error contracts on the relevant API items instead of expanding the introduction into a manual.
- Organize migration guides by SDK versions, with concrete old-to-new replacements. Include Ingest and other API changes in the same guide for that upgrade.
- ScopeDB Cloud manages its service upgrades. Do not present internal backend versions or rollout steps as SDK user prerequisites or migration tasks.

## Releases

- Prepare version changes in a PR: update `scopedb-client/Cargo.toml`, refresh `Cargo.lock`, and update `CHANGELOG.md` and affected user documentation before merging into `main`.
- Release tags use `vX.Y.Z` and must match the crate version. Preserve published versions and tags; do not move an existing release tag to another commit.
- After the release workflow succeeds, create the GitHub release using the matching changelog entry and verify the crates.io version, docs.rs build, and a fresh consumer project.

## Changelog

- Keep an `## Unreleased` section and dated `## vX.Y.Z (YYYY-MM-DD)` release headings. Preserve published version numbers and dates. Use `*` bullets and only nonempty sections, ordered as `Breaking Changes`, `New Features`, `Bug Fixes`, and `Improvements`.
- Compare the final behavior with the latest published release. Describe user-visible outcomes, migration steps for breaking changes, and replacements for deprecated APIs. Include documentation or metadata changes only when they affect users; omit internal tests, refactors, CI maintenance, and unreleased intermediate changes.
- Before adding a bug-fix entry, verify that the faulty behavior shipped in the latest release. If the affected API or behavior is itself unreleased, describe only its final contract in the relevant feature entry and omit the development-only correction.
