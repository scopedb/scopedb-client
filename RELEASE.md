# How to release the ScopeDB Rust SDK

Rust SDK releases are published from a clean `main` checkout after the release PR has merged. The crate version, changelog entry, Git tag, and crates.io version must all match. Tags with the legacy `rust/vX.Y.Z` form were retained only to preserve release history from the former monorepo; new releases use `vX.Y.Z`.

## Changelog format

Use `# Changelog` with a short introduction, an `## Unreleased` section, and `## vX.Y.Z (YYYY-MM-DD)` release headings. Preserve published version numbers and dates. Use `*` bullets with one coherent, user-observable change per source line. Order nonempty sections as `Breaking Changes`, `New Features`, `Bug Fixes`, and `Improvements`.

Compare each entry with the latest released version. Explain the impact and migration path for breaking changes, and describe deprecations alongside their replacements. Include documentation or metadata changes when they affect users; omit internal tests, refactors, and CI maintenance. GitHub release notes should use the matching changelog entry.

## Prepare

1. Set the SDK version without a leading `v`, then confirm that exact version is not already present on crates.io:

   ```sh
   export scopedb_rust_version=0.3.3
   if cargo info --registry crates-io "scopedb-client@$scopedb_rust_version" >/dev/null 2>&1; then
     echo "scopedb-client $scopedb_rust_version already exists" >&2
     exit 1
   fi
   cargo owner --list scopedb-client
   ```

2. Update `scopedb-client/Cargo.toml`, refresh and commit `Cargo.lock`, and update `CHANGELOG.md` and user-facing documentation in a release PR. The lockfile is tracked for the workspace's xtask binary even though `scopedb-client` itself is a library. Do not publish directly from a feature branch.

   ```sh
   cargo +1.91.0 check --package scopedb-client
   git diff -- scopedb-client/Cargo.toml Cargo.lock
   ```

3. After the release PR merges, update local `main`, fetch the remote, and verify that the checkout is clean and points at exactly `origin/main`:

   ```sh
   git switch main
   git pull --ff-only
   git fetch origin
   test "$(git branch --show-current)" = main
   test -z "$(git status --porcelain)"
   test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
   export scopedb_rust_version=0.3.3
   if cargo info --registry crates-io "scopedb-client@$scopedb_rust_version" >/dev/null 2>&1; then
     echo "scopedb-client $scopedb_rust_version already exists" >&2
     exit 1
   fi
   ```

## Validate and publish

Run the same checks used by CI, build the documentation with warnings denied, and verify the exact package that Cargo will upload:

```sh
manifest_version="$(cargo +1.91.0 pkgid --package scopedb-client | sed -E 's/.*[#@]([^#@]+)$/\1/')"
test "$manifest_version" = "$scopedb_rust_version"
cargo x lint
cargo x check
cargo x test --no-capture
cargo x semver --release-version "$scopedb_rust_version"
cargo +1.91.0 publish --package scopedb-client --dry-run --locked
cargo +1.91.0 package --package scopedb-client --list --locked
```

Publish once from that same clean commit:

```sh
cargo +1.91.0 publish --package scopedb-client --locked
```

Wait until the published version is visible before tagging:

```sh
cargo info --registry crates-io "scopedb-client@$scopedb_rust_version"
```

## Tag and verify

Create an annotated tag on the published commit and push it:

```sh
git tag -a "v$scopedb_rust_version" \
  -m "Release v$scopedb_rust_version for Rust SDK"
git push origin "v$scopedb_rust_version"
```

Create a GitHub release whose notes are the matching `CHANGELOG.md` entry. Then verify the crate page, the docs.rs build, and a fresh consumer project that depends on the published version.
