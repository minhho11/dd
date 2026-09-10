# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`dd` is a high-volume HTTP load tool. It fires many requests across a fixed-size
worker pool, optionally routing each request through a proxy. **Postgres is
required** (via go-bun): the run parameters live in a key/value `config` table and
the targets in a `urls` table; proxies are stored and rotated round-robin. The tool
loads config + targets, partitions its workers across the targets by weight (a
dedicated worker group per URL), and restarts whenever config or urls change
(LISTEN/NOTIFY). The HTTP client is go-resty/resty.

Each target has a **mode**: `http` (the default — one resty request per job) or
`browser`, which drives a headless Chromium (chromedp) through a scripted flow —
navigate, fill and submit a form, wait, assert — for automation testing or
browser-based load. Both modes share the worker pool, proxy rotation, block/health
tracking, retries, and reporting; browser flows reuse the same `{{...}}` generators.
(chromedp requires a Chrome/Chromium binary on the host and pins the module to Go
1.26.)

## Commands

```bash
make build            # go build -o bin/dd .
make test             # go test ./...
make fmt vet tidy
make run ARGS="-urls aaa.com,xxx.vn -workers 20 -requests 500 -verbose"

go test -run TestPoolRun ./internal/pool/   # single test
go test -race ./...

# run directly (-dsn / DATABASE_DSN is required)
DATABASE_DSN="postgres://u:p@localhost:5432/db?sslmode=disable" ./bin/dd -urls "https://a.com,https://b.vn" -workers 20
DATABASE_DSN="postgres://u:p@localhost:5432/db?sslmode=disable" ./bin/dd -urls a.com -proxies "http://user:pass@host:8080"
```

`-dsn`/`DATABASE_DSN` is **required**. On first run the CLI *seeds* the DB from its
flag values; after that the `config` and `urls` tables are the source of truth (edit
them to change a running instance — the trigger restarts the run). The run always
watches for changes and always reports.

**Seed flags** (only used to seed the DB on first run): `-workers` (10; total workers,
split across targets by weight), `-urls` (comma-separated; seeds one GET row per URL
into the `urls` table), `-requests` (100; default per-URL count, 0 = until every proxy
is blocked — per-URL value lives in the `urls` table), `-retries` (2), `-rps`
(0 = unlimited), `-timeout`, `-human` (true), `-user-agent`, `-insecure`, `-cache-bust`
(+`-cache-bust-param`, default `_`).

**Operational flags** (stay on the CLI): `-dsn` (or `DATABASE_DSN` env), `-proxies`
(seed the DB then run), `-block-after` (5), `-block-for` (30m), `-block-ttl` (5s
per-domain cache trust), `-block-cache-max` (1024 domains, LRU), `-block-prune` (24h
startup cleanup), `-proxy-fail-limit` (3), `-proxy-cooldown` (1m), `-out
results.csv|.jsonl`, `-error-log err.log` (errors only, appended), `-report-interval`
(30s; `<=0` uses 30s), `-verbose`, `-headless` (true; run browser-mode targets
headless, set false to watch the browser).

**`urls` table (per target):** `url`, `mode` (`http` default / `browser`), `method`
(GET/POST/PUT/PATCH/DELETE/HEAD; ignored in browser mode), `params` (JSON — for http:
query string for GET/HEAD/DELETE, request body for POST/PUT/PATCH; for browser: a
`{"steps":[...]}` flow, see below), `weight` (relative share of the worker pool,
normalized), `requests` (0 = until blocked; in browser mode `1` runs the flow once as
a single test, `N` repeats it under load), `enabled`. New columns are added on startup
via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` (still no migration tool).

**Browser mode (`mode='browser'`):** `params` is a JSON flow run in a headless
Chromium against `url` (`internal/browser`, via chromedp). Shape:
`{"steps":[{"action":..., ...}]}`. The entry `url` is navigated first, then each step
runs in order; any step error (including a failed assertion) fails the run. Actions:
`navigate`/`goto` (`url`), `fill`/`type`/`sendkeys` (`selector`,`value`), `setvalue`
/`select` (`selector`,`value`), `click` (`selector`), `submit` (`selector`),
`waitVisible`/`waitReady`/`assertVisible` (`selector`), `assertText`
(`selector`,`contains`), `sleep`/`wait` (`value` duration, e.g. `"500ms"`). Selectors
are CSS (ByQuery). `value`/`url` support the same `{{...}}` generators (expanded per
run); `contains` is literal. An optional top-level `"vars"` object holds values
generated **once per run** and referenced from step values as `{{name}}`, so
several fields can share one value (e.g. password + confirm):
`{"vars":{"pw":"{{randString:12}}"},"steps":[{"action":"fill","selector":"#password","value":"{{pw}}"},{"action":"fill","selector":"#confirm","value":"{{pw}}"}]}`.
Optional per-step `timeout` (Go duration) overrides the
default (`-timeout`). Authenticated proxies work — credentials are answered over the
CDP Fetch domain since Chrome's `--proxy-server` takes none. Blocks are detected the
same way as http (status 403/429/503, body challenge markers, or a block URL); a
block or a connection failure retries through another proxy, a page/assertion failure
does not. **SOCKS proxies are skipped for browser jobs** (`isSOCKS` in `pool.pick`/
`AnyUsable`) — Chromium cannot authenticate SOCKS5 proxies, so a browser flow can
only use HTTP/HTTPS proxies (dd answers their `407` over the CDP Fetch domain) or run
direct; http-mode jobs still use SOCKS proxies normally. **Per-step debug logging** is
toggled by the `browser_debug` config key (seed flag `-browser-debug`): when on, each
browser navigation/step logs ok/FAIL during the run (`pool.fireBrowser` builds a
prefixed `browser.Options.Logf`); flip it live in the `config` table. Example:
`{"steps":[{"action":"fill","selector":"#email","value":"{{randEmail}}"},{"action":"fill","selector":"#pass","value":"{{randString:12}}"},{"action":"click","selector":"button[type=submit]"},{"action":"waitVisible","selector":".dashboard"},{"action":"assertText","selector":".welcome","contains":"Welcome"}]}`

**Manual confirm (`-browser-test URL`):** runs one target's browser flow **once** and
exits — no pool, no reporter, no "until blocked" flooding — for verifying a flow works.
It loads the `urls` row matching `URL`, runs the flow direct (or through
`-browser-test-proxy URL`), logs each navigation/step ok/FAIL, and prints the final
status + PASSED/FAILED. Pair with `-headless=false` to watch the browser and
`-browser-test-hold 30s` to keep it open afterward. `runBrowserTest` in `main.go`
(via `TargetRepo.LoadByURL` + `browser.Execute` with a `Logf`/`HoldOpen`). Exits 0
whether the flow passed or failed (it errors only on setup problems). Example:
`./bin/dd -dsn ... -browser-test https://site/register -headless=false -browser-test-hold 20s`.

**Random params (`{{...}}` generators):** params/flow values may contain placeholders
expanded **fresh on every request/run** (`internal/tmpl`, `tmpl.Expand`, called from
`fire` for http params and from `internal/browser` for step values), so each gets
unique data. Supported: `{{randString:N}}` (alphanumeric, default
10), `{{randDigits:N}}` / `{{randNumber:N}}`, `{{randHex:N}}`, `{{randInt:min:max}}`
(inclusive), `{{randEmail}}` or `{{randEmail:domain.com}}`, `{{randBool}}`, `{{uuid}}`
(v4), `{{timestamp}}` (unix). Unknown names are left untouched. Place a token inside a
JSON string for a string value (`"email":"{{randEmail}}"`) or bare for a JSON number
(`"code":{{randInt:1:9}}`). Example:
`{"email":"{{randEmail}}","name":"{{randString:8}}","amount":{{randInt:1:100}}}`.

## Architecture

`main.go` is orchestration only: parse flags → connect Postgres → load proxies →
ensure/seed the `config` + `urls` tables → build the client pool → build the worker
pool → allocate workers per target by weight → feed each target's jobs → print
summary, restarting on any config/urls change. All logic sits behind
`run(ctx, args, out)` with injected args/writer so it stays testable; keep new wiring
there, not in `main()`. `allocateWorkers(weights, total)` does the dedicated split
(relative weights, min 1 worker each); `executeOneRun` builds a `pool.Group` per
target and calls `pool.RunGroups`.

Packages under `internal/`:

- **`config`** — the DB source of truth for a run, in two parts:
  - `Setting` (key/value `config` table) + `Repo`: the *run params* (workers, requests,
    retries, rps, timeout, cache-bust, human, user-agent, insecure), encoded as k/v rows.
    `Repo` has `EnsureSchema` (table + the shared `dd_config_notify` trigger),
    `EnsureDefault` (seed the k/v rows only if the table is empty), `Load` (assemble a
    `Config`), `Save` (upsert k/v), and `Watch(ctx)` which turns Postgres
    `LISTEN config_changed` into a Go channel. `Config.Requests` is the global default
    used to seed new target rows and as the direct-mode fallback.
  - `Target` (table `urls`) + `TargetRepo`: one row per URL with `mode` (`http`/
    `browser`), `method`, `params` (JSON), `weight`, `requests`, `enabled`.
    `EnsureSchema` creates the table, adds later columns idempotently (`ALTER TABLE
    ... ADD COLUMN IF NOT EXISTS`, e.g. `mode`), and installs a NOTIFY trigger on
    `urls` firing the same `config_changed` channel, so editing a target restarts the
    run. `LoadEnabled` returns enabled rows; `Seed` bootstraps rows from `-urls`.
  - Operational plumbing (dsn, block/proxy internals, output files) stays on the CLI.
- **`proxy`** — two bun models and their repos, plus the block cache:
  - `Proxy` (table `proxies`) + `Repo`: the proxy list.
  - `ProxyBlock` (table `proxies_blocked`, unique on `(proxy_id, domain)`) + `BlockRepo`:
    the per-(proxy, domain) quarantine. `OnBlocked` increments `fail_count`; at
    `-block-after` it sets `blocked_until = now + block-for` and resets the counter.
    `OnSuccess` deletes the row (clears the block). `Available` is false while
    `blocked_until` is in the future.
  - `BlockCache` (over the `BlockSource` interface, satisfied by `BlockRepo`): a **bounded,
    lazy, per-domain** cache so proxy selection stays off the DB hot path *without*
    unbounded memory. It caches, per target domain, the set of currently-blocked proxy IDs
    (`LoadDomain` = one query per domain per `-block-ttl` window), holds at most
    `-block-cache-max` domains (LRU eviction), and fails open on a load error. Memory is
    bounded by `min(domains, cache-max) × blocked-proxies-per-domain` — it never loads the
    whole table. `OnBlocked`/`OnSuccess` write through to Postgres and update the cached set.
    This is what `main` passes to the pool as its `BlockStore`.
  - `BlockRepo.Prune` runs at startup (`-block-prune`) to delete expired quarantines and
    stale partial-failure rows so `proxies_blocked` doesn't grow without bound.
  - Schemas are created on startup with `CREATE TABLE IF NOT EXISTS`; no migration tool.
- **`db`** — opens the bun handle over `pgdriver`/`pgdialect` and pings it. `-verbose`
  attaches the `bundebug` query hook.
- **`httpclient`** — resty clients + `Pool`. resty binds a proxy to a client's transport
  (`client.SetProxy`), so the pool holds **one `Client` per proxy**, each tagged with its
  `ProxyID` (0 == direct). `Clients()` exposes them; the worker pool selects among them.
- **`pool`** — the worker pool and all request-time policy. A `Job` carries `URL`,
  `Mode` (`""`/`http` or `browser`), `Method`, and `Params` (JSON). `RunGroups(ctx,
  groups)` starts each `Group`'s workers over its own job channel (the dedicated split
  — one group per target), all sharing the client pool and one `Summary`; `Run(ctx,
  jobs)` is the single-group form. Each worker calls `do(job)` which:
  1. `pick()` — chooses a **random** usable client (`rand.Perm`), skipping proxies that are
     health-cooled-down or quarantined for the domain, and any already tried this job;
  2. execute — `fire()` for http, or `fireBrowser()` for `mode=browser`, gated by the
     same optional `-rps` rate limit (`x/time/rate`). `fire()` sends the request via
     `req.Execute(method, url)`: for GET/HEAD/DELETE the JSON `Params` merge into the
     query (`jsonToQuery`), for POST/PUT/PATCH they become the JSON body (`params.go`).
     `fireBrowser()` parses the flow and calls `internal/browser.Execute` through the
     picked proxy. Both classify blocks via shared `looksBlockedText`/`looksBlockedURL`
     (status 403/429/503 **or** challenge-page body markers **or** a block URL) and feed
     `health` (transport errors) and the `BlockStore` (blocks vs. success);
  3. **retries** through a different proxy when the result is retryable (`Result.retry`:
     a block or a transport/navigation failure — a browser page/assertion failure is
     not), up to `-retries`.
  `health` (`health.go`) is an in-memory circuit breaker: N consecutive transport failures
  cool a proxy down proxy-wide (distinct from the per-domain block store). `Summary` is
  mutex-guarded (success / blocked / non-2xx / failed / skipped / retries + status histogram).
- **`browser`** — the browser-mode executor (chromedp). `ParseFlow(params)` decodes a
  `{"steps":[...]}` flow; `Execute(ctx, entryURL, flow, proxyURL, opts)` launches a
  **fresh** headless Chromium per call (a new process, so each run is isolated and can
  use its own proxy — proxy is a browser-level setting), navigates the entry URL, runs
  each step under a per-step timeout, and returns an `Outcome` (status, final URL, body
  text for block detection, steps run, error, and a `Transport` flag distinguishing a
  connection failure from a page/assertion failure). Handles proxy auth over the CDP
  Fetch domain and captures the main document's status over the Network domain. No
  dependency on `pool`, so `pool` imports it without a cycle.
- **`tmpl`** — the shared `{{...}}` generator expansion (`Expand`), used by both `pool`
  (http params in `fire`) and `browser` (step values). Extracted here so both can use it
  without an import cycle.
- **`report`** — streams each `Result` to `-out` as CSV or JSONL (by extension), concurrency-safe.
- **`metrics`** — the in-memory per-URL success/fail variable (`Metrics`) plus the `report`
  **table** + `Repo`. `Record` runs on every result (in-memory, cheap); a ticker (`Run`) upserts
  each URL's *cumulative* totals into a single per-URL row (`url` is the primary key) every
  `-report-interval` (default 30s), so each URL has one row that is updated in place rather than a
  new row per interval. URLs unchanged since the last flush are skipped. Cumulative totals are also
  kept in memory (`Totals`), with a final flush on shutdown. Lives for the whole process, so the
  tally spans config/urls restarts. (Distinct from the `report` package above — same word,
  different concern.)

Two independent quarantine mechanisms, don't conflate them: **block store** = per-(proxy,
domain), persisted in Postgres, triggered by *blocked HTTP responses*. **health** =
per-proxy, in-memory, triggered by *transport failures* (dead/unreachable proxy).

**Run params + targets come from the DB.** `run` requires a DSN, then `runFromDB`
ensures the `config` + `urls` schemas, seeds both from the CLI defaults on first run
(`cliRunConfig` → `EnsureDefault`, `-urls` → `TargetRepo.Seed`), and supervises: it
loads the current config + enabled targets, runs them via `executeOneRun`, and on a
`LISTEN config_changed` event (fired by either table's trigger on any change) cancels
the current run and restarts with the freshly-loaded state. A finished run idles until
the next change.

`executeOneRun` builds the client pool, calls `allocateWorkers` to split the workers
across targets by weight, then starts one `produceTarget` goroutine + `pool.Group` per
target: each producer feeds its target's jobs — a fixed per-URL `requests` count, or
(count 0 with proxies) until no proxy is usable for that target; direct runs with an
unset count fall back to the global default. `pool.RunGroups` runs them all.

The DB is **required**: with no `-dsn`/`DATABASE_DSN` the tool exits with an error,
since config and targets live in Postgres.
