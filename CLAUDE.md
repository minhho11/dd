# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`dd` is a high-volume HTTP load tool. It fires many requests across a fixed-size
worker pool, optionally routing each request through a proxy. Proxies are stored
in Postgres (via go-bun) and rotated round-robin. The HTTP client is go-resty/resty.

## Commands

```bash
make build            # go build -o bin/dd .
make test             # go test ./...
make fmt vet tidy
make run ARGS="-urls aaa.com,xxx.vn -workers 20 -requests 500 -verbose"

go test -run TestPoolRun ./internal/pool/   # single test
go test -race ./...

# run directly
./bin/dd -urls "https://a.com,https://b.vn" -workers 20 -requests 500
DATABASE_DSN="postgres://u:p@localhost:5432/db?sslmode=disable" ./bin/dd -urls a.com -proxies "http://user:pass@host:8080"
```

Key flags: `-workers` (10), `-urls` (comma-separated), `-requests` (per URL, 100;
**omit it with proxies to run until every proxy is blocked/unusable** — direct mode
falls back to the fixed count),
`-dsn` (or `DATABASE_DSN` env), `-proxies` (seed the DB then run), `-retries` (2,
extra attempts through another proxy on block/failure), `-rps` (0 = unlimited),
`-block-after` (5), `-block-for` (30m), `-block-ttl` (5s per-domain cache trust),
`-block-cache-max` (1024 domains, LRU), `-block-prune` (24h startup cleanup),
`-proxy-fail-limit` (3), `-proxy-cooldown` (1m), `-out results.csv|.jsonl`,
`-error-log err.log` (errors only, appended), `-human` (true; realistic browser
headers, stable identity per proxy), `-user-agent` (override UA), `-cache-bust`
(+`-cache-bust-param`, default `_`; unique query per request to bypass nginx/CDN
caches), `-timeout`, `-insecure`, `-verbose`. Total requests = `requests × len(urls)`.

## Architecture

`main.go` is orchestration only: parse flags → optionally connect Postgres and load
proxies → build the client pool → build the worker pool → feed jobs → print summary.
All logic sits behind `run(ctx, args, out)` with injected args/writer so it stays
testable; keep new wiring there, not in `main()`.

Packages under `internal/`:

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
- **`pool`** — the worker pool and all request-time policy. `Run(ctx, jobs)` starts N
  workers; each calls `do(job)` which:
  1. `pick()` — chooses a **random** usable client (`rand.Perm`), skipping proxies that are
     health-cooled-down or quarantined for the domain, and any already tried this job;
  2. `fire()` — optional `-rps` rate limit (`x/time/rate`), then the GET; classifies the
     result via `looksBlocked` (status 403/429/503 **or** challenge-page body markers **or**
     a block-page redirect); feeds `health` (transport errors) and the `BlockStore` (blocks
     vs. success);
  3. **retries** through a different proxy on a block or transport error, up to `-retries`.
  `health` (`health.go`) is an in-memory circuit breaker: N consecutive transport failures
  cool a proxy down proxy-wide (distinct from the per-domain block store). `Summary` is
  mutex-guarded (success / blocked / non-2xx / failed / skipped / retries + status histogram).
- **`report`** — streams each `Result` to `-out` as CSV or JSONL (by extension), concurrency-safe.

Two independent quarantine mechanisms, don't conflate them: **block store** = per-(proxy,
domain), persisted in Postgres, triggered by *blocked HTTP responses*. **health** =
per-proxy, in-memory, triggered by *transport failures* (dead/unreachable proxy).

The DB is optional by design: no `-dsn`/`DATABASE_DSN` means direct requests, not an error.
