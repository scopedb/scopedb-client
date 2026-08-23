# ScopeDB Rust SDK workspace

This repository is a Cargo workspace for the ScopeDB Rust SDK. The publishable [`scopedb-client`](scopedb-client/README.md) crate, its API documentation, tests, and runnable examples live under [`scopedb-client/`](scopedb-client/); repository automation lives under [`xtask/`](xtask/).

## Development

Run repository checks from the workspace root:

```sh
cargo x lint
cargo x check
cargo x test
```

Use `cargo x lint --fix` to apply Clippy, rustfmt, and license-header fixes. Run `cargo x --help` to list all development tasks.

Release history and the maintainer runbook are in [`CHANGELOG.md`](CHANGELOG.md) and [`RELEASE.md`](RELEASE.md).
