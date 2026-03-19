# reditop

`reditop` is a terminal UI for monitoring Redis/Valkey instances.

## Implemented MVP

- Polls one or more Redis targets every second (default, configurable)
- Overview screen with:
  - generic, configurable columns (INFO-backed + calculated)
  - defaults for alias/address/type/memory/ops/latency/status plus a cluster/replication color gutter
- Detail screen with summary, latency, raw `INFO`, `INFO COMMANDSTATS`, and an on-demand `bigkeys` view
- Tree and flat overview modes
- Sorting by currently visible column keys and substring filtering
- Bottom status/key bar with htop-style function key labels and live search/filter input echo
- Config loading from TOML + CLI target merge
- Handles per-instance failures without crashing UI

## Key Bindings

- `q` / `Ctrl+C`: quit
- `F1`: open full help page
- `F3`: start search input (overview)
- `F4`: start filter input and clear existing filter (overview)
- `F5`: toggle tree/flat (overview)
- `F6`: open sort picker from currently visible overview columns
- `F7`: open overview column picker
- `H`: open full help page
- `?`: toggle help
- `Up/Down`: move selection in overview, or scroll/paginate `Commandstats` and `Bigkeys` in detail view
- `Enter`: open detail
- `Esc`: back to previous view (or stop filter editing)
- `Tab` / `Left` / `Right`: cycle detail tabs
- `S` / `L` / `I` / `C` / `B`: jump to `Summary` / `Latency` / `Info Raw` / `Commandstats` / `Bigkeys` in detail view
- `t`: toggle tree/flat
- `s`: cycle sort column
- `v`: open overview column picker
- `h`: toggle host rendering (default auto-hides host when all targets share one host)
- `/`: start filter input in overview, or filter `Commandstats` or `Bigkeys` rows in detail view
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
reditop --unix /tmp/redis.sock --tcp 10.0.0.12:6379
reditop --cluster 7000
reditop --cluster 10.0.0.11:7000 --cluster 10.0.0.12:7000
reditop --config ~/.config/redis-top/config.toml
reditop -c config.toml 127.0.0.1:6379
```

For TCP targets, you can pass just a port (for example `6379`), and it is treated as
`127.0.0.1:6379`.

`--cluster <HOST:PORT>` uses the provided TCP seed node(s) to run `CLUSTER SHARDS`
and auto-discover every primary/replica endpoint in the cluster for monitoring.
Realtime cluster role/parent updates also use `CLUSTER SHARDS` (not deprecated
`CLUSTER NODES`) so startup discovery and ongoing topology mapping stay aligned.
Like `--tcp`, a port-only value such as `--cluster 7000` is treated as
`127.0.0.1:7000`.

Important options:

- `-c, --config <PATH>`
- `--refresh <DURATION>`
- `--connect-timeout <DURATION>`
- `--command-timeout <DURATION>`
- `-n, --concurrency <N>`
- `--cluster <HOST:PORT>`
- `--view <flat|tree>`
- `--sort <alias|address|type|cluster|memory|mem|ops|lat|latmax|status>`
- `--no-config`
- `-a, --auth <PASSWORD>`
- `--user <USERNAME>`
- `-v, --verbose`

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

[theme]
background_color = "black"
foreground_color = "white"
carat_color = "white"
warning_color = "yellow"
critical_color = "red"

[[targets]]
alias = "local"
addr = "127.0.0.1:6379"
protocol = "tcp"
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

Overview columns also support `emphasis = "max"` or `emphasis = "min"` to mark
the highest or lowest visible value each frame.

Emphasis styling is configurable with `[view.overview.emphasis_style]` and may be
overridden per column with `[columns.<key>.emphasis_style]`. Supported style
keys are `bold`, `italic`, `underlined`, `dim`, `reversed`, and
`foreground_color`.
