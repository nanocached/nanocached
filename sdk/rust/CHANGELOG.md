# Changelog

All notable changes to the Rust SDK are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow the `sdk/rust/vX.Y.Z` tags.

## [Unreleased]

### Changed

- **MSRV raised from 1.75 to 1.85.** The declared `rust-version = "1.75"`
  was never a real floor: `zeroize` 1.9 (pulled in through `rustls` by the
  default `tls` feature) requires edition 2024, i.e. Cargo 1.85 or newer.
  The manifest now states 1.85 and CI checks it, so the value can no
  longer drift silently (PR #52).
- The crate declares an empty `[workspace]` so Cargo no longer walks up
  to the repository-root server manifest when building from a checkout
  (PR #52). No effect on consumers from crates.io.

## [0.1.1]

- First tag-driven release of the aligned SDK line (see the repository
  release history for earlier changes).

[Unreleased]: https://github.com/nanocached/nanocached/compare/sdk/rust/v0.1.1...HEAD
[0.1.1]: https://github.com/nanocached/nanocached/releases/tag/sdk/rust/v0.1.1
