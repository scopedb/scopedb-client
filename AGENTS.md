# Repository Guidelines

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
