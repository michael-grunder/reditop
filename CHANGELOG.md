## Unreleased

### Added

- Add background Redis/Valkey autodiscovery with curated host port probing,
  localhost socket/process hints, Redis verification, and live TUI updates.
- Add `--host <HOST>` for remote autodiscovery, including repeated `--host`
  usage for scanning multiple hosts in one session.

### Fixed

- Make `q` close the active overlay window instead of exiting the TUI, while
  keeping `Ctrl+C` as an immediate full exit.
- Restore `q` and `Esc` quitting from the main overview when no overlay is
  open, while still making both keys close the active overlay first.
- Preserve configured overview column order on startup by deserializing column
  definitions with insertion order instead of hash order.
