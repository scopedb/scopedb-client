# ScopeDB SDK for Rust

[![Version](https://img.shields.io/crates/v/scopedb-client)](https://crates.io/crates/scopedb-client) [![Docs](https://img.shields.io/badge/docs-scopedb.io-blue)](https://www.scopedb.io/docs/developer/rust) [![CI](https://img.shields.io/github/actions/workflow/status/scopedb/scopedb-client/ci.yml?branch=main&label=CI)](https://github.com/scopedb/scopedb-client/actions/workflows/ci.yml) [![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)

The official async Rust client for [ScopeDB](https://www.scopedb.io). Run ScopeQL queries, browse the catalog, and write rows with bounded concurrent streams. For application writes, start with `Table::append_stream`.

## Runtime support

Requires Rust 1.91.0 or later and a Tokio runtime. Use the SDK in a trusted server-side process and keep API keys out of source control. Applications upgrading from SDK 0.3 should follow [Upgrade from 0.3 to 0.4](scopedb-client/docs/migration-0.4.md).

## Installation

From your application crate:

```sh
cargo add scopedb-client serde_json
cargo add tokio --features macros,rt-multi-thread
```

## Quickstart

Set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` using the [connection guide](https://www.scopedb.io/docs/guides/connect-to-scopedb). Save this program as `src/main.rs` and run `cargo run`:

```rust,no_run
use std::error::Error;

use scopedb_client::Client;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = std::env::var("SCOPEDB_ENDPOINT")?;
    let api_key = std::env::var("SCOPEDB_API_KEY")?;
    let client = Client::builder(endpoint)
        .api_key(api_key)
        .build()?;

    let result = client.query("SELECT 1 AS ready").await?;
    println!("{:?}", result.into_objects()?);
    Ok(())
}
```

## Documentation

- [SDK guide](https://www.scopedb.io/docs/developer/rust)
- [API reference](https://docs.rs/scopedb-client)
- [Quickstart](scopedb-client/README.md)
- [Upgrade from 0.3 to 0.4](scopedb-client/docs/migration-0.4.md)
- [Examples](scopedb-client/examples/README.md)
- [HTTP wire reference](scopedb-client/docs/rust-http-api.md)
- [Changelog](CHANGELOG.md)
- [Report an issue](https://github.com/scopedb/scopedb-client/issues)

This SDK sends ScopeQL statements to ScopeDB; the language is documented separately. Use these canonical entry points:

- [Quickstart](https://www.scopedb.io/docs/guides/quickstart)
- [Query guide](https://www.scopedb.io/docs/guides/query-events)
- [Language reference](https://www.scopedb.io/docs/reference)

## Development

This repository is a Cargo workspace. The publishable crate, API documentation, tests, and examples live in [scopedb-client/](scopedb-client/); repository automation lives in [xtask/](xtask/). Run checks from the workspace root:

```sh
cargo x lint
cargo x check
cargo x test --no-capture
```

Use `cargo x lint --fix` to apply Clippy, rustfmt, Taplo, and license-header fixes. Run `cargo x --help` to list all development tasks.

### Live protocol validation

From a source checkout, set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` for your test environment, then run:

```sh
cargo test -p scopedb-client --test native_06 -- --ignored --nocapture
```

The test creates a uniquely named schema and checks dry-run, version conflicts, direct and concurrent streaming appends, row rejections, statement errors, elapsed timeout, and cancellation racing with completion. It deletes its schema before returning. For an isolated local engine, `SCOPEDB_TENANT_ID` supplies tenant identity instead of a platform API key. Running cancellation, lost acknowledgements, and stale responses after dropping a future are covered by controlled HTTP tests.

To publish, push an annotated `vX.Y.Z` tag on the merged release commit in `main`; [release.yml](.github/workflows/release.yml) runs the checks and publishes to crates.io.

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
