# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
### Added
- Implemented a working Redis/Valkey TUI with overview/detail/help screens.
- Added polling engine with bounded concurrency, latency window tracking, and per-instance status/error handling.
- Added INFO and CLUSTER NODES parsing with unit tests.
- Added topology builder for tree view grouping with replication parent/child mapping and tests.
- Added clap-based CLI with target parsing, duration/timeouts, sort/view toggles, auth/user overrides, and config merge behavior.
- Added TOML config loader with default path discovery and validation for missing/invalid targets.
- Added project `README.md` with usage, key bindings, CLI examples, and config schema.
### Changed
- Expanded data model to include runtime settings, instance metrics, detail fields, and rolling latency aggregates.
- Wired `main.rs` to full application modules (`app`, `cli`, `config`, `poller`, `tui`, `parse`, `topology`).
### Deprecated
### Removed
### Fixed
