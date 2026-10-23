# How to release the ScopeDB Rust SDK

Rust SDK releases are published to crates.io by [release.yml](.github/workflows/release.yml) when a `vX.Y.Z` tag is pushed after the release PR has merged into `main`. The crate version, changelog entry, Git tag, and crates.io version must all match. Tags with the legacy `rust/vX.Y.Z` form were retained only to preserve release history from the former monorepo; new releases use `vX.Y.Z`.

## Configure Trusted Publishing

Before pushing the first release tag, an owner of `scopedb-client` must add a GitHub trusted publisher under the crate's [Settings → Trusted Publishing](https://crates.io/crates/scopedb-client/settings):

| Field             | Value            |
| ----------------- | ---------------- |
| Repository owner  | `scopedb`        |
| Repository name   | `scopedb-client` |
| Workflow filename | `release.yml`    |
| Environment       | `release`        |

Use only the workflow filename, not `.github/workflows/release.yml`. The environment must match `environment: release` in the publishing job. No crates.io API token needs to be stored in GitHub secrets: `rust-lang/crates-io-auth-action` exchanges the workflow's OpenID Connect identity for a temporary token immediately before publishing and revokes it when the job ends. See the [crates.io Trusted Publishing documentation](https://crates.io/docs/trusted-publishing).

GitHub creates the `release` environment when the workflow first uses it if it does not already exist. To restrict who can approve releases or which tags may publish, configure the environment's protection rules in the repository's [environment settings](https://github.com/scopedb/scopedb-client/settings/environments) before pushing a release tag. If deployment restrictions are enabled, allow version tags such as `v*`; allowing only the `main` branch does not allow a tag-triggered release.

## Changelog format

Use `# Changelog` with a short introduction, an `## Unreleased` section, and `## vX.Y.Z (YYYY-MM-DD)` release headings. Preserve published version numbers and dates. Use `*` bullets with one coherent, user-observable change per source line. Order nonempty sections as `Breaking Changes`, `New Features`, `Bug Fixes`, and `Improvements`.

Compare each entry with the latest released version. Explain the impact and migration path for breaking changes, and describe deprecations alongside their replacements. Include documentation or metadata changes when they affect users; omit internal tests, refactors, and CI maintenance. GitHub release notes should use the matching changelog entry.

## Prepare

1. Set the SDK version without a leading `v`, then confirm that exact version is not already present on crates.io:

   ```sh
   export scopedb_rust_version=0.4.0
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
   export scopedb_rust_version=0.4.0
   if cargo info --registry crates-io "scopedb-client@$scopedb_rust_version" >/dev/null 2>&1; then
     echo "scopedb-client $scopedb_rust_version already exists" >&2
     exit 1
   fi
   ```

## Validate before tagging

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

## Tag, publish, and verify

Create an annotated tag on the validated commit and push it. Pushing this tag starts publication, so complete the Trusted Publishing setup first:

```sh
git tag -a "v$scopedb_rust_version" \
  -m "Release v$scopedb_rust_version for Rust SDK"
git push origin "v$scopedb_rust_version"
```

The release workflow reuses the full CI checks and tests, verifies that the tagged commit is part of `main`, checks that the tag matches the crate version and a dated changelog entry, runs the version compatibility check and package dry run, then publishes with a temporary crates.io token. Only the publishing job can request that token. Releases run serially, and an in-progress publication is not cancelled by another release tag.

Wait for the [Release (Rust SDK) workflow](https://github.com/scopedb/scopedb-client/actions/workflows/release.yml) to succeed, then verify the published version:

```sh
cargo info --registry crates-io "scopedb-client@$scopedb_rust_version"
```

Create a GitHub release whose notes are the matching `CHANGELOG.md` entry. Then verify the crate page, the docs.rs build, and a fresh consumer project that depends on the published version.

If a run fails before publication, fix the external configuration and rerun the failed jobs on the existing tag. If the tagged source needs a correction, prepare a new version and tag. If publication reports an error after uploading, check crates.io before retrying: published versions cannot be overwritten. Once the version exists, finish the GitHub release and verification steps without rerunning `cargo publish`. Do not move a release tag to a different commit.
