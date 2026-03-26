# reditop

`reditop` is a terminal UI for monitoring Redis/Valkey instances.

## Implemented MVP

- Polls one or more Redis targets every second (default, configurable)
- Starts immediately and runs Redis/Valkey autodiscovery in the background
- Overview screen with:
  - generic, configurable columns (INFO-backed + calculated)
  - defaults for alias/address/type/memory/ops/latency/status plus a cluster/replication color gutter
- Detail screen with summary, latency, raw `INFO`, `INFO COMMANDSTATS`, and an on-demand `bigkeys` view
  including full server-reported error details when polling fails
- Tree and flat overview modes
- Sorting by currently visible column keys and substring filtering
- Bottom status/key bar with htop-style function key labels and live search/filter input echo
- Live discovery status in the footer, including queued/probing/verified counts
- Config loading from TOML + CLI target merge
- Handles per-instance failures without crashing UI
- Surfaces richer instance states such as `PROTECTED`, `AUTH`, `LOADING`, and `DOWN`

## Key Bindings

- `q`: quit from the overview, or close the active overlay window
- `Ctrl+C`: quit immediately
- `F1`: open full help page
- `F3`: start search input (overview)
- `F4`: start filter input and clear existing filter (overview)
- `F5`: toggle tree/flat (overview)
- `F6`: open sort picker from currently visible overview columns
- `F7`: open overview column picker for toggling and reordering visible columns
- `H`: open full help page
- `?`: toggle help
- `Up/Down`: move selection in overview, or scroll the active detail pane when it has more rows than fit
- `Enter`: open detail
- `Esc`: quit from the overview, close the active overlay window, go back from detail/help, stop filter editing, or leave detail view and clear its active pane filters
- `Tab` / `Left` / `Right`: cycle detail tabs
- `S` / `L` / `I` / `C` / `B`: jump to `Summary` / `Latency` / `Info Raw` / `Commandstats` / `Bigkeys` in detail view
- `t`: toggle tree/flat
- `s`: cycle sort column
- `v`: open overview column picker
- `Shift+Up/Down`: reorder columns inside the overview column picker
- `h`: toggle host rendering (default auto-hides host when all targets share one host)
- `/`: start filter input in overview, or filter the active detail pane in detail view (`Summary`, `Latency`, `Info Raw`, `Commandstats`, or `Bigkeys`)
- `r` / `R`: refresh now, or rerun the on-demand `Bigkeys` scan while that tab is open

The `Bigkeys` detail tab mirrors `redis-cli --bigkeys`: it scans the keyspace with
`SCAN`, fetches each key's type, runs the matching cardinality/length command
(`STRLEN`, `LLEN`, `SCARD`, `ZCARD`, `HLEN`, `XLEN`), and shows the largest keys
found. The `Length` column shows that type-specific cardinality/length value,
while `Memory` shows a humanized `MEMORY USAGE` estimate when supported. Unlike
normal polling, this scan is
performed on demand when the `Bigkeys` tab is opened or refreshed. The header
shows when a scan is in progress, and after completion it shows the result age
in seconds.

## CLI

Examples:

```bash
reditop 127.0.0.1:6379 127.0.0.1:6380
reditop 6379 6380
reditop
reditop 192.168.0.148
reditop --autodiscover 192.168.0.148 --autodiscover 192.168.0.149
reditop 6379 --autodiscover
reditop 6379 --autodiscover 192.168.0.148
reditop --unix /tmp/redis.sock --tcp 10.0.0.12:6379
reditop --cluster 7000
reditop --cluster 10.0.0.11:7000 --cluster 10.0.0.12:7000
reditop --once
reditop --autodiscover 10.0.0.12 --once
reditop --config ~/.config/redis-top.toml
reditop -c config.toml 127.0.0.1:6379
```

For TCP targets, you can pass just a port (for example `6379`), and it is treated as
`127.0.0.1:6379`.

Host-only positional values such as `192.168.0.148` are treated as autodiscovery
hosts, not fixed monitored instances. Exact TCP targets such as `6379` or
`192.168.0.148:6380` disable autodiscovery by default and only connect to the
requested server(s).

When you provide explicit targets, `reditop` does not also add unrelated
`[[targets]]` entries from `redis-top.toml`. If an explicit target matches a
configured TCP or Unix target, `reditop` still reuses that target's context
such as alias, username, password, and tags.

If you do not provide explicit targets, `reditop` autodiscovers on `127.0.0.1`
by default. `--autodiscover[=<HOST>]` opt back into autodiscovery when you also
provide exact targets. With no value it autodiscovers on localhost; with a
value it probes the provided host. `--host <HOST>` remains available as an
alias for compatibility.

Explicit `--cluster <HOST:PORT>` values are treated as seed nodes: the TUI
starts immediately, the seed is monitored right away, and the background
discovery pipeline expands cluster, replication, and sentinel topology as it
verifies peers. Like `--tcp`, a port-only value such as `--cluster 7000` is
treated as `127.0.0.1:7000`.

Important options:

- `-c, --config <PATH>`
- `--once`
- `--refresh <DURATION>`
- `--connect-timeout <DURATION>`
- `--command-timeout <DURATION>`
- `-n, --concurrency <N>`
- `--autodiscover [HOST]`
- `--cluster <HOST:PORT>`
- `--view <flat|tree>`
- `--sort <alias|address|type|cluster|memory|mem|ops|lat|latmax|status>`
- `--no-config`
- `-a, --auth <PASSWORD>`
- `--user <USERNAME>`
- `-v, --verbose`

`--once` skips the interactive TUI. It performs one polling pass for explicit
targets, runs autodiscovery/verification for any configured discovery hosts,
prints the overview table to stdout, and exits.

Version output includes build metadata:

```bash
reditop --version
# reditop x.y.z [YYYY-MM-DD] (<gitsha>[-dirty])
```

## Building release binaries

CI builds upload release artifacts for:

- `aarch64-apple-darwin`
- `x86_64-unknown-linux-musl`
- `aarch64-unknown-linux-musl`

### Linux static musl binary

Install the musl target once:

```bash
rustup target add x86_64-unknown-linux-musl
```

Then build a release binary with:

```bash
cargo build-musl
```

Output binary:

```bash
target/x86_64-unknown-linux-musl/release/reditop
```

### macOS Apple Silicon binary

On an Apple Silicon macOS host, build the native release binary with:

```bash
cargo build --release --target aarch64-apple-darwin
```

Output binary:

```bash
target/aarch64-apple-darwin/release/reditop
```

## Testing

Run the full test suite with:

```bash
cargo test
```

The integration suite includes live, read-only Redis checks for both a
standalone instance and a Redis Cluster. By default it probes:

- standalone Redis at `localhost:6379`
- cluster Redis at `localhost:7000`

Override those endpoints with:

```bash
REDITOP_TEST_REDIS_ADDR=redis.example:6379 \
REDITOP_TEST_REDIS_CLUSTER_ADDR=redis-cluster.example:7000 \
cargo test
```

If a live endpoint is unreachable, the corresponding integration test exits
early and the rest of the suite still runs.

## Config

Search order when `--config` is not provided:

1. `$XDG_CONFIG_HOME/redis-top.toml`
2. `~/.config/redis-top.toml`
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
still_autodiscover = true

[theme]
background_color = "black"
foreground_color = "white"
carat_color = "white"
warning_color = "yellow"
critical_color = "red"

[[targets]]
alias = "local"
addr = ":6379"
protocol = "tcp"
user = "default"
password_env = "REDIS_PASSWORD"
enabled = true

[columns.used_mem]
type = "info"
header = "Mem"
info_key = "used_memory"
value_type = "bytes"
format = "bytes_human"

[columns.maxmem_pct]
type = "calc"
header = "%MaxMem"
calc = "maxmemory_percent"
format = "pct:1"

[columns.lat_max]
type = "calc"
header = "LatMax"
calc = "latency_max_ms"
format = "ms:2"
emphasis = "max"

[view.overview.emphasis_style]
bold = true
italic = false
foreground_color = "yellow"

[columns.lat_max.emphasis_style]
foreground_color = "red"

[view.overview]
visible = ["alias", "addr", "role", "used_mem", "ops", "lat_last", "lat_max", "status"]

[view.overview.sort]
by = "ops"
dir = "desc"
```

`[theme]` colors support: `black`, `red`, `green`, `yellow`, `blue`,
`magenta`, `cyan`, `gray`/`grey`, `white`.

`[global].still_autodiscover` defaults to `true`. Leave it enabled if you want
saved targets to provide credentials or fixed instances without suppressing
background autodiscovery. Set it to `false` if config-defined `[[targets]]`
should behave like an explicit fixed target list.

`[[targets]]` accepts `user` or `username`, plus either `password` or
`password_env`. If you omit the host from a TCP `addr`, `reditop` assumes
`localhost`, so `:6380` and `6380` both resolve to loopback addresses. Configured
TCP target credentials are also reused by autodiscovery when it verifies the
same `host:port`.

Overview columns also support `emphasis = "max"` or `emphasis = "min"` to mark
the highest or lowest visible value each frame.

Emphasis styling is configurable with `[view.overview.emphasis_style]` and may be
overridden per column with `[columns.<key>.emphasis_style]`. Supported style
keys are `bold`, `italic`, `underlined`, `dim`, `reversed`, and
`foreground_color`.
