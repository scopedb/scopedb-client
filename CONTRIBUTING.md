# Contributing

## Design

Organize code around resource identity, operation semantics, and ownership. Reuse existing resource types instead of copying their identity into parallel representations. Keep shared encoding and request execution independent of producer queues and background worker lifecycles.

Keep abstractions that own an invariant, resource, or distinct lifecycle. Remove forwarding helpers, wrappers, and duplicate state that have no independent role. A separate file does not by itself justify a module interface. Place public operations before their private implementation helpers.

Single-request writes, finite writes, and append streams have different completion and cancellation contracts. Preserve those distinctions. A finite write owns its pending requests and retries; a stream owns admitted rows independently of an individual caller. Reports must distinguish acknowledged writes, known rejections, unknown outcomes, and input that was never submitted. Dropping a future cannot roll back a request already sent to the server.

Preserve resource ownership and destruction order when simplifying concurrency code. A retained permit or guard can enforce a memory bound or release protocol even when its value is never read. Use conventional local guard bindings and explain non-obvious ownership fields. Avoid defensive state and recovery paths without an explicit contract or realistic failure scenario.

## Rust style

Name types by their domain role and scope. Distinguish reusable writers, one write call, and individual requests. Use `Options` for editable inputs awaiting validation and `Config` for validated or resolved configuration. Introduce separate representations only when validation or runtime requirements make their distinction useful.

Declare restricted visibility at module boundaries and use `pub` for their internal interfaces. Keep items private when only their defining module and descendants need them. For publicly reachable modules and re-exported types, reserve unrestricted `pub` for intentional public API and use narrower visibility for internal callers. Do not widen visibility solely to let another module's tests reach an implementation detail.

Import referenced symbols from `crate` and preserve their names. Retain meaningful qualification such as `error::Error` or `tokio::time::sleep` when it identifies the operation or distinguishes names. Avoid aliases without a concrete reason. Reserve `use super::*` for test modules. Re-export a module's own children with explicit `pub use self::...` declarations.

## Public API contracts

Describe what an operation accepts, when it completes, and what a failure establishes. Preserve structured information needed for recovery on both success and failure paths. Diagnostic strings are not a substitute for typed partial-progress information.

Keep public entry points and exported types deliberate. Re-export a dependency when callers must supply a compatible type from that dependency. Review compatibility against the latest release; unreleased APIs can be corrected before publication, while changes to released APIs need migration guidance and an appropriate release version.

Document return values, failure conditions, and cancellation behavior on the relevant methods. Describe types with noun phrases and operations with present-tense verbs. Add Errors, Panics, and Examples sections only where they describe actual contracts. Keep examples focused on realistic callers and use repository-relative links for repository content.

## Tests and validation

Test observable behavior through the public API when practical. Keep tests that require private implementation access next to the implementation, and keep test-server behavior tests with the fixture. Shared fixtures should model concrete protocol and timing scenarios rather than grow into a general framework.

Cover partial progress, malformed responses, resource limits, and cancellation at meaningful boundaries. Prefer deterministic coordination to sleeps. Preserve lifecycle and retry invariants when moving tests or simplifying implementation details.

Use `cargo x` as the workflow entry point. Read `cargo x --help` and the relevant subcommand's help before using it. Before submitting a change, run:

```sh
cargo x check
cargo x test
cargo x lint
```

Follow [AGENTS.md](AGENTS.md) for documentation, changelog, and release requirements.
