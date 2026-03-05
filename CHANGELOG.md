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
- Added `build.rs`-driven version metadata so `--version` includes build date and git SHA (`-dirty` when applicable).
- Added a dedicated full-screen help page opened with `H`, including key/action descriptions.
- Added htop-style bottom key bar with function key labels: `F1 Help`, `F3 Search`, `F4 Filter`, `F5 Tree`, `F6 SortBy`.
- Added `--cluster <HOST:PORT>` CLI option to seed cluster auto-discovery and monitor all discovered primary/replica nodes.
### Changed
- Expanded data model to include runtime settings, instance metrics, detail fields, and rolling latency aggregates.
- Wired `main.rs` to full application modules (`app`, `cli`, `config`, `poller`, `tui`, `parse`, `topology`).
- Updated CLI binary name/docs to `reditop`.
- Updated detail view metric rendering to aligned key/value columns with thousands-separated numeric formatting for readability.
- Tree view rendering now shows primaries as top-level rows and indents replicas beneath their assigned primary.
- Overview table now omits host rendering by default when all monitored targets share the same host, with runtime toggle `h` to force showing hosts.
- Overview rows now use compact type labels (`PRI`, `REP`, `CLU`, `STD`) and narrower Type/Cluster columns.
- Cluster column now shows logical cluster IDs (`1`, `2`, ...) mapped from distinct discovered clusters instead of raw node IDs.
- Tree view replica branch markers no longer include a leading left padding before `└─`/`├─`.
- Overview memory display now uses one `Memory` column that shows `used/maxmemory` when maxmemory is configured, otherwise just `used`.
- Overview now shows live user input in the bottom status bar while search/filter editing is active.
- Added function-key aliases for existing overview actions (`F1` help, `F5` view toggle, `F6` sort cycle) and wired `F3`/`F4` into search/filter input entry.
- Reworked overview sorting: `F6` now opens a selector built from currently displayed columns, sorting supports explicit direction, and the active header shows an `↑`/`↓` indicator.
- Launch configuration now performs startup cluster-node expansion via `CLUSTER SHARDS` for `--cluster` seeds, then merges/disambiguates discovered targets with existing CLI/config targets.
### Deprecated
### Removed
### Fixed
- Fixed tree view dropping replicas when their `parent_addr` did not exactly match a primary key; parent lookup now resolves by key or address and keeps unresolved replicas visible.
- Fixed topology grouping that could flatten replication trees by splitting related nodes into separate groups.
- Fixed TCP target parsing to accept port-only values (for example `6379`) as `127.0.0.1:6379` in both CLI and config.
