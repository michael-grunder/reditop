## Unreleased

### Added

- Add richer Redis instance statuses such as `PROTECTED` and `AUTH`, and show
  full server-reported error details in the detail summary view.
- Add `[global].still_autodiscover`, defaulting to `true`, so config-defined
  targets can provide credentials without disabling background autodiscovery.
- Add `--once` to print the overview status table a single time for polled and
  autodiscovered instances, then exit without starting the interactive TUI.

### Changed

- Shorten the autodiscovery footer to a compact in-progress spinner label and
  clear it automatically once discovery completes.
- Add background Redis/Valkey autodiscovery with curated host port probing,
  localhost socket/process hints, Redis verification, and live TUI updates.
- Add `--host <HOST>` for remote autodiscovery, including repeated `--host`
  usage for scanning multiple hosts in one session.
- Add an available `connected_clients` overview column backed by Redis `INFO`
  clients output.
- Move the default config file lookup to flat `redis-top.toml` files under
  `$XDG_CONFIG_HOME` or `~/.config`, and reuse configured TCP credentials during
  autodiscovery for matching endpoints.

### Fixed

- Record timed-out poll attempts as observed latency samples so `LatMax` and
  `LatLast` reflect command and connection timeouts instead of keeping the last
  successful latency.
- Make `LatMax` overview emphasis flash only on frames where a new overall
  maximum is first observed, instead of keeping the current record holder
  highlighted indefinitely.
- Update the CI Zig installation step to use `mlugg/setup-zig@v2.2.1`,
  fixing musl `cargo zigbuild` jobs that were still pinned to `v1`.
- Keep autodiscovery active when config-defined TCP targets provide credentials
  by reusing those credentials only for exact matching endpoints instead of
  applying them to every discovered port on the same host.
- Make `q` close the active overlay window instead of exiting the TUI, while
  keeping `Ctrl+C` as an immediate full exit.
- Restore `q` and `Esc` quitting from the main overview when no overlay is
  open, while still making both keys close the active overlay first.
- Preserve configured overview column order on startup by deserializing column
  definitions with insertion order instead of hash order.
- Add config support for `user`/`username`, plaintext `password`, and
  env-backed `password_env`, including loopback defaults for hostless TCP
  addresses such as `:6380`.
