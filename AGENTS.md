# Repository Guidelines

## Design

Organize code around resource identity, operation semantics, and ownership. Reuse existing resource types instead of copying their identity into parallel representations. Keep shared encoding and request execution independent of producer queues and background worker lifecycles.

Keep abstractions that own an invariant, resource, or distinct lifecycle. Remove forwarding helpers, wrappers, and duplicate state that have no independent role. A separate file does not by itself justify a module interface. Place public operations before their private implementation helpers.

Single-request writes, finite writes, and append streams have different completion and cancellation contracts. Preserve those distinctions. A finite write owns its pending requests and retries; a stream owns admitted rows independently of an individual caller. Reports must distinguish acknowledged writes, known rejections, unknown outcomes, and input that was never submitted. Dropping a future cannot roll back a request already sent to the server.

Preserve resource ownership and destruction order when simplifying concurrency code. A retained permit or guard can enforce a memory bound or release protocol even when its value is never read. Use conventional local guard bindings and explain non-obvious ownership fields. Avoid defensive state and recovery paths without an explicit contract or realistic failure scenario.

## Rust style

Name types by their domain role and scope. Distinguish reusable writers, one write call, and individual requests. Use `Options` for editable inputs awaiting validation and `Config` for validated or resolved configuration. Introduce separate representations only when validation or runtime requirements make their distinction useful. Keep a type and its primary `impl` block together (public API before private helpers), and reserve `len` and `is_empty` for collection or slice-like views. Use `new` (implementing `Default` when parameterless and `const fn` when trivial) for the primary constructor, `with_*` for alternative required inputs, and `*Builder` for multi-option construction.

Order module headers as `//!` docs, `use` imports (`group_imports = "StdExternalCrate"`, `imports_granularity = "Item"`), out-of-line `mod` declarations, and items. Use `<module>/mod.rs` for multi-file modules, and do not split cohesive files prematurely. Group `mod` declarations with blank lines between `pub mod`, private `mod` with immediate `pub use self::...;` re-exports, and implementation-only private `mod` (`#[cfg(test)]` last), sorted alphabetically within each group; re-export subsystem interfaces from `mod.rs` and attach `#[cfg]` gates at module boundaries when possible.

Use three visibility tiers: private by default, plain `pub` inside private ancestor module trees, and `pub(crate)` for internal fields, constructors, or helpers reachable through `pub mod` or `pub use` re-exported types. Avoid `pub(super)` and `pub(in ...)`, and never widen visibility solely to let another module's tests reach an implementation detail.

Import symbols at the top of the file from `crate::...` rather than placing `use` statements inside functions or spelling `crate::...` or `super::...` inline (avoiding `super::...` and private `self::...`, reserving `use super::*;` for `#[cfg(test)]` modules). Preserve original symbol names with meaningful module or crate qualification (such as `io::Error`, `process::Command`, or `tokio::time::sleep`), and avoid `use ... as ...` aliases except `use ...::Trait as _;`. Use inline fully qualified paths when importing a parent module would collide with a primary local concept (such as `std::error::Error`) or for rare runtime hooks such as `std::panic::catch_unwind`.

Do not prefix struct fields with `_` to silence unused-field warnings; keep descriptive names and add `#[expect(dead_code, reason = "...")]` on unread ownership or guard fields. Derive applicable value traits (`Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`) on plain data types and configuration enums, omitting `PartialEq` and `Eq` when field equality would expose internal layout or history; avoid unnecessary generic bounds on custom `Debug` impls. Reserve `#[must_use = "..."]` for lazy values or `&self` methods easily mistaken for mutators, and prefer `#[expect(..., reason = "...")]` over `#[allow(...)]`.

Keep abstractions at function signatures, cohesive types, and module boundaries; inside functions, prefer linear control flow with early guard returns, explicit `match` or `if let`, and explicit `for` loops for multi-step or fallible logic over premature helpers or uncontracted defensive glue. Use `Result` (keeping failing mutations atomic when practical) for recoverable failures, empty values or `Option` for absent queries, and fail-fast panics for violated internal invariants.

## Public API contracts

Describe what an operation accepts, when it completes, and what a failure establishes. Preserve structured information needed for recovery on both success and failure paths. Diagnostic strings are not a substitute for typed partial-progress information.

Return the concrete SDK `Error` from asynchronous SDK operations so callers can compose them without type erasure or losing progress. Keep failure category, public operation, commit knowledge, and retry-budget facts separate. Normalize lower-level failures where their operation semantics are known; reserve the source chain for diagnostics. Error construction and retry classification belong to the SDK, while callers read typed recovery data.

Keep public entry points and exported types deliberate. Re-export a dependency when callers must supply a compatible type from that dependency. Review compatibility against the latest release; unreleased APIs can be corrected before publication, while changes to released APIs need migration guidance and an appropriate release version.

Document caller-observable contracts (return values, failure conditions, ownership, and cancellation behavior) on the relevant methods without leaking implementation details. Describe types with noun phrases and operations with third-person present-tense verbs. Explain straightforward return values in the summary or body, and add `# Errors`, `# Panics`, `# Safety`, and trailing `# Examples` sections only where they describe actual contracts. Keep examples focused on realistic callers with visible imports, use repository-relative links for repository content, and use public API names as intra-doc link labels with crate-relative resolution paths in explicit targets so internal path prefixes stay out of rendered prose. Reserve `//` comments for non-obvious invariants, `// SAFETY:` preconditions, or external quirks rather than restating code.

## Tests and validation

Test observable behavior through the public API when practical. Keep `#[cfg(test)]` tests that require private implementation access next to the implementation, and keep test-server behavior tests with the fixture. Shared fixtures should model concrete protocol and timing scenarios rather than grow into a general framework.

Cover partial progress, malformed responses, resource limits, and cancellation at meaningful boundaries. Every test must guard a realistic regression without mirroring literals, trivial control flow, static type guarantees, or non-contract `Debug` wording. Avoid test-only production hooks, prefer deterministic coordination to sleeps, and preserve lifecycle and retry invariants when moving tests or simplifying implementation details.

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
