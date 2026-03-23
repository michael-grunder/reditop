## Unreleased

### Fixed

- Make `q` close the active overlay window instead of exiting the TUI, while
  keeping `Ctrl+C` as an immediate full exit.
- Restore `q` and `Esc` quitting from the main overview when no overlay is
  open, while still making both keys close the active overlay first.
- Preserve configured overview column order on startup by deserializing column
  definitions with insertion order instead of hash order.
