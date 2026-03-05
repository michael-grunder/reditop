# reditop

`reditop` is a terminal UI for monitoring Redis/Valkey instances.

## Implemented MVP

- Polls one or more Redis targets every second (default, configurable)
- Overview screen with:
  - alias/address
  - type
  - cluster group
  - memory usage (`used` or `used/maxmemory` when configured)
  - ops/sec
  - last/max latency
  - status
- Detail screen with summary, latency, and raw `INFO`
- Tree and flat overview modes
- Sorting and substring filtering
- Config loading from TOML + CLI target merge
- Handles per-instance failures without crashing UI

## Key Bindings

- `q`: quit
- `H`: open full help page
- `?`: toggle help
- `Up/Down`: move selection
- `Enter`: open detail
- `Esc`: back to previous view (or stop filter editing)
- `Tab` / `Left` / `Right`: cycle detail tabs
- `t`: toggle tree/flat
- `s`: cycle sort mode
- `h`: toggle host rendering (default auto-hides host when all targets share one host)
- `/`: start filter input
- `r`: refresh now

## CLI

Examples:

```bash
reditop 127.0.0.1:6379 127.0.0.1:6380
reditop 6379 6380
reditop --unix /tmp/redis.sock --tcp 10.0.0.12:6379
reditop --config ~/.config/redis-top/config.toml
reditop -c config.toml 127.0.0.1:6379
```

For TCP targets, you can pass just a port (for example `6379`), and it is treated as
`127.0.0.1:6379`.

Important options:

- `-c, --config <PATH>`
- `--refresh <DURATION>`
- `--connect-timeout <DURATION>`
- `--command-timeout <DURATION>`
- `-n, --concurrency <N>`
- `--view <flat|tree>`
- `--sort <address|mem|ops|lat|status>`
- `--no-config`
- `-a, --auth <PASSWORD>`
- `--user <USERNAME>`
- `-v, --verbose`

Version output includes build metadata:

```bash
reditop --version
# reditop x.y.z [YYYY-MM-DD] (<gitsha>[-dirty])
```

## Config

Search order when `--config` is not provided:

1. `$XDG_CONFIG_HOME/redis-top/config.toml`
2. `~/.config/redis-top/config.toml`
3. `./redis-top.toml`

Example:

```toml
[global]
refresh_interval_ms = 1000
connect_timeout_ms = 300
command_timeout_ms = 500
concurrency_limit = 16
view_default = "tree"
sort_default = "address"

[[targets]]
alias = "local"
addr = "127.0.0.1:6379"
protocol = "tcp"
enabled = true
```
