# dd — Architecture

`dd` is a high-volume HTTP tool. It fires many requests across a fixed-size worker
pool, routes each request through a **randomly chosen proxy**, learns which proxies
a target has blocked and **quarantines** them per-domain, **cools down** dead
proxies, **retries** blocked requests through a different proxy, can **cap the
request rate**, and **exports** every result.

- Module: `github.com/minhho11/dd`
- Language/toolchain: Go 1.25
- Storage: PostgreSQL via [go-bun](https://bun.uptrace.dev/)
- HTTP client: [go-resty/resty v2](https://github.com/go-resty/resty) (`SetProxy` binds a proxy to the transport)
- Rate limiting: `golang.org/x/time/rate`

---

## Table of contents

1. [Design goals](#design-goals)
2. [Package layout](#package-layout)
3. [Data model](#data-model)
4. [Workflow — startup](#workflow--startup)
5. [Workflow — worker pool (fan-out)](#workflow--worker-pool-fan-out)
6. [Workflow — per-request lifecycle](#workflow--per-request-lifecycle)
7. [Proxy quarantine state machine](#proxy-quarantine-state-machine)
8. [The two independent quarantine mechanisms](#the-two-independent-quarantine-mechanisms)
9. [The block cache — bounded and lazy](#the-block-cache--bounded-and-lazy)
10. [Block detection](#block-detection)
11. [Configuration (flags)](#configuration-flags)
12. [Concurrency & safety](#concurrency--safety)
13. [Failure modes & degradation](#failure-modes--degradation)
14. [Testing](#testing)
15. [Extension points](#extension-points)

---

## Design goals

- **Throughput** — saturate the network with a fixed pool of workers.
- **Source distribution** — spread requests across many proxy IPs, chosen randomly.
- **Adaptivity** — stop using proxies a target blocks; recover them automatically.
- **Resilience** — skip dead proxies; retry through healthy ones.
- **Bounded resources** — memory and DB growth must be capped, not open-ended.
- **Observability** — a summary plus optional per-request export.
- **Graceful degradation** — no database? Run direct. No proxies? Run direct.

---

## Package layout

```
dd/
├── main.go                      # CLI: flags, wiring, orchestration, summary/export
└── internal/
    ├── db/          db.go        # open + ping the bun handle (pgdriver/pgdialect)
    ├── proxy/       proxy.go     # Proxy model + Repo (the proxy list)
    │                block.go     # ProxyBlock model + BlockRepo + BlockCache
    ├── httpclient/  httpclient.go# resty Client-per-proxy Pool
    │                profiles.go   # realistic per-browser header profiles
    │                tls.go        # opt-in InsecureSkipVerify
    ├── pool/        pool.go      # worker pool, pick/fire/retry, block detection
    │                health.go    # in-memory dead-proxy circuit breaker
    └── report/      report.go    # streaming CSV / JSONL exporter
```

`main` is orchestration only. All logic sits behind `run(ctx, args, out)` with an
injected args slice and output writer, so it is testable without a process.

---

## Data model

Two tables, created on startup with `CREATE TABLE IF NOT EXISTS` (no migration tool).

### `proxies` — the proxy list

| column       | type        | notes                                            |
|--------------|-------------|--------------------------------------------------|
| `id`         | bigint PK   | autoincrement                                    |
| `url`        | text UNIQUE | full proxy URL: `http://user:pass@host:port`, `socks5://host:port` |
| `active`     | bool        | only active proxies are loaded                   |
| `created_at` | timestamptz |                                                  |

### `proxies_blocked` — per-(proxy, domain) quarantine

| column          | type        | notes                                                    |
|-----------------|-------------|----------------------------------------------------------|
| `id`            | bigint PK   | autoincrement                                            |
| `proxy_id`      | bigint      | the proxy (FK-by-convention to `proxies.id`)             |
| `domain`        | text        | target host                                              |
| `fail_count`    | bigint      | consecutive blocked responses toward the threshold       |
| `blocked_until` | timestamptz | NULL = not quarantined; future = quarantined             |
| `updated_at`    | timestamptz |                                                          |
| `created_at`    | timestamptz |                                                          |
| **UNIQUE**      | —           | `(proxy_id, domain)` — one row per pair, enables upsert   |

---

## Workflow — startup

```
 CLI flags (-workers -urls -requests -dsn -proxies -retries -rps
            -block-* -proxy-* -out ...)
      │
      ▼
 -dsn set? ──no──►  run DIRECT (single no-proxy client)
      │ yes
      ▼
 db.Open (bun over pgdriver) ──► EnsureSchema: proxies + proxies_blocked
      │
      ▼
 seed -proxies (INSERT ... ON CONFLICT DO NOTHING)
      │
      ▼
 BlockRepo.Prune  ── delete expired quarantines + stale partial rows
      │
      ▼
 Repo.LoadActive ──► build ONE resty client per proxy
                       (client.SetProxy(url), tagged with ProxyID)
      │
      ▼
 NewBlockCache (lazy, per-domain, LRU-bounded)   ──►  pool.BlockStore
```

---

## Workflow — worker pool (fan-out)

```
 producer goroutine                         jobs channel (buffered = workers)
 for requests × urls:  ──►  [ job job job job … ]
   push Job{URL}                                 │
                                 ┌───────────────┼───────────────┐
                                 ▼               ▼               ▼
                              worker 1        worker 2   …    worker N
                                 └───────────────┼───────────────┘
                                                 ▼
                                      each worker loops:
                                        job := <-jobs
                                        res := do(job)      # pick → fire → retry
                                        summary.record(res)
                                        OnResult(res)       # verbose + export
                                                 │
                                                 ▼
                            Summary (total / success / blocked / non-2xx /
                             failed / skipped / retries / RPS / status histogram)
```

Cancellation (SIGINT/SIGTERM, or run completion) flows through a `context.Context`
to workers and the producer.

### Two dispatch modes

- **Fixed count** (`-requests` set, or direct mode): the producer emits
  `requests × len(urls)` jobs, then closes the channel.
- **Until blocked** (`-requests` omitted **and** proxies present): the producer emits
  jobs round-robin over the URLs and, before each round, calls `pool.AnyUsable(urls)`.
  Workers update the block/health state as they process, so once **no proxy is usable
  for any target** (all quarantined or health-cooled-down) `AnyUsable` returns false and
  the producer stops. If the targets never block, the run continues until interrupted
  (Ctrl-C). Direct mode has no proxies to exhaust, so it falls back to the fixed count.

```
 produceUntilBlocked:
   loop:
     AnyUsable(urls)? ── no ──► close jobs, stop
        │ yes
        ▼
     emit one job per URL (blocking sends throttle to worker pace)
```

---

## Workflow — per-request lifecycle

`do(job)` is the core. It picks a proxy, fires, and retries through a different
proxy on a block or transport failure.

```
   take Job{URL}
        │
        ▼
   domain = host(URL)
        │
        ▼
 ┌───────────────────────── retry loop (≤ 1 + -retries attempts) ──────────────────┐
 │      │                                                                          │
 │      ▼                                                                          │
 │   pick(domain, exclude)  ── random order (rand.Perm) over the client pool,      │
 │      │                       skipping: already-tried · health-cooled-down ·     │
 │      │                       quarantined-for-domain (BlockStore.Available)      │
 │      │                                                                          │
 │      ├── none usable ──► SKIPPED (ErrAllBlocked) ─────────────────────────► out │
 │      ▼                                                                          │
 │   fire(client)                                                                  │
 │      │  1. rate limit: limiter.Wait(ctx)   (if -rps > 0)                        │
 │      │  2. cache-bust: append unique ?_=<n>  (if -cache-bust)                   │
 │      │  3. resty GET via that proxy                                             │
 │      │  4. classify: looksBlocked(resp)                                         │
 │      │                                                                          │
 │      ▼                                                                          │
 │   feedback:                                                                     │
 │      • transport error   → health.onFail(proxy)     (dead-proxy breaker)        │
 │      • any HTTP response → health.onOK(proxy)                                   │
 │      • blocked           → BlockStore.OnBlocked(proxy, domain)                  │
 │      • non-blocked resp  → BlockStore.OnSuccess(proxy, domain)                  │
 │      │                                                                          │
 │      ▼                                                                          │
 │   retryable(res) = (transport error) OR (blocked)                               │
 │      │                                                                          │
 │      ├── yes & attempts ≤ -retries ──► exclude this proxy, loop again ──────────┘
 │      └── no  ──► return res
 └─────────────────────────────────────────────────────────────────────────────────
        │
        ▼
   record Result → Summary   (+ verbose print / CSV·JSONL export)
```

Result fields: `URL, Domain, ProxyURL, StatusCode, Blocked, Attempts, Latency, Err`.

---

## Proxy quarantine state machine

Per `(proxy, domain)` row in `proxies_blocked`. Threshold = `-block-after` (5),
window = `-block-for` (30m).

```
        ┌──────────────┐   blocked response (403/429/503/challenge)   ┌───────────────────┐
        │   HEALTHY    │ ────────────────────────────────────────────►│    COUNTING       │
        │  (no row)    │            fail_count = 1                    │ fail_count 1..N-1 │
        └──────────────┘                                              └───────────────────┘
              ▲                                                          │            │
              │ non-blocked response (OnSuccess: DELETE row)             │ success    │ Nth block
              │                                                          │(DELETE)    ▼
              │                                                          │      ┌──────────────────────┐
              │                                                          │      │    QUARANTINED       │
              │                                                          └──────│ blocked_until =      │
              │                                                                 │   now + block-for    │
              │                                                                 │ pick() skips it      │
              │                                                                 └──────────────────────┘
              │       non-blocked response on retry                                     │
              │       (OnSuccess: DELETE row)                          block-for elapse │
              │                                           ┌──────────────┐              ▼
              └───────────────────────────────────────────│  PROBATION   │◄──  blocked_until ≤ now
                                                          │ eligible,    │     (pick() allows it again)
                                                          │ tried again  │
                                                          └──────────────┘
                                                                 │
                                                                 │ blocked again → COUNTING
                                                                 ▼
```

Two rules in one line each:

- **Block:** N blocked responses for a domain → quarantine that proxy for `block-for`,
  **for that domain only** (other domains keep using it).
- **Recover:** after the window it is retried; a non-blocked response deletes the row
  (unblocked), another block re-arms the counter.

`OnBlocked` is a single upsert with `RETURNING fail_count`:

```sql
INSERT INTO proxies_blocked (proxy_id, domain, fail_count, ...)
VALUES ($1, $2, 1, ...)
ON CONFLICT (proxy_id, domain) DO UPDATE
  SET fail_count = pb.fail_count + 1, updated_at = $now
RETURNING fail_count;
-- if fail_count >= threshold: SET blocked_until = now + window, fail_count = 0
```

---

## The two independent quarantine mechanisms

Do **not** conflate them — they react to different failures and live in different places.

| | **Block store** | **Health circuit breaker** |
|---|---|---|
| Scope | per **(proxy, domain)** | per **proxy** (all domains) |
| Trigger | *blocked HTTP responses* (403/429/503/challenge) | *transport failures* (timeout, connection refused) |
| Meaning | "the target refuses this proxy" | "this proxy is dead / unreachable" |
| Storage | Postgres `proxies_blocked` (+ cache) | in-memory map (`pool/health.go`) |
| Threshold | `-block-after` (5) | `-proxy-fail-limit` (3) |
| Cooldown | `-block-for` (30m) | `-proxy-cooldown` (1m) |
| Recovery | non-blocked response deletes the row | any successful transport resets it |

A 403 through a proxy means the proxy **works** (it reached the origin) but is blocked
— so it feeds the block store and marks the proxy *healthy*. A connection refused means
the proxy is **down** — it feeds the health breaker and does **not** create a block row.

---

## The block cache — bounded and lazy

Proxy selection must not hit Postgres on every request, but the cache must not grow
without bound either. `BlockCache` (in `proxy/block.go`) solves both.

**Keyed by domain, loaded lazily, capped by LRU.**

```
 Available(proxy, domain):
   ┌── domain cached & fresh (age < -block-ttl)? ──► read blocked-set from memory ──► answer
   └── miss / stale ──► LoadDomain(domain)   # ONE query: blocked proxies for THIS domain
                         store set, LRU-evict past -block-cache-max domains
                         ──► answer
```

- **Only caches the domains this run touches** (your `-urls`), never the whole table.
- **At most `-block-cache-max` domains** (default 1024); least-recently-used evicted.
- **Write-through:** `OnBlocked`/`OnSuccess` update Postgres **and** the cached set, so a
  just-quarantined proxy is skipped immediately.
- **Fails open:** a DB error on a miss returns "available" rather than stalling the run.
- The DB query runs **outside** the lock; all map access is **under** the lock.

**Memory bound:** `min(#domains, block-cache-max) × blocked-proxies-per-domain`.
For a handful of target domains this is single-digit megabytes regardless of how large
the proxy pool or the `proxies_blocked` table grows.

**DB growth** is bounded separately by `BlockRepo.Prune` at startup (`-block-prune`,
default 24h): it deletes expired quarantines and stale partial-failure rows.

```
Old design (removed): preload the ENTIRE proxies_blocked table into one map,
periodically refreshed. Memory ∝ proxies × all domains in the DB — unbounded.

New design: lazy per-domain sets + LRU cap. Memory is hard-capped.
```

---

## Block detection

`looksBlocked(resp)` in `pool/pool.go` decides whether a response counts as a block.
It matters for defense insight: real anti-bot systems often return **200 with a
challenge page**, not an obvious 4xx.

Signals (any one trips it):

1. **Status code** ∈ `{403, 429, 503}`.
2. **Body markers** (case-insensitive, first 8 KB scanned): `captcha`, `are you human`,
   `verify you are human`, `access denied`, `attention required`, `just a moment`
   (Cloudflare), `cf-chl`, `request blocked`, `unusual traffic`.
3. **Redirect target** path contains `/blocked`, `/captcha`, or `/challenge`.

A blocked result is retried through another proxy and recorded via `OnBlocked`; any
other HTTP response is treated as the proxy reaching the origin (`OnSuccess`).

## Request identity (browser headers)

With `-human` (default) each client is assigned a **stable, coherent browser profile**
at build time (`internal/httpclient/profiles.go`) — Chrome/Edge/Firefox/Safari across
Windows/macOS. Because the profile is per-client and one client is bound to one proxy,
each proxy IP consistently presents as the same "user" (a real browser keeps a stable
UA; flipping it every request is itself a bot tell). The profile sets a matching
`User-Agent`, `Accept`, `Accept-Language`, and the browser-appropriate `Sec-Ch-Ua*` /
`Sec-Fetch-*` / `Upgrade-Insecure-Requests` headers. `-user-agent` overrides to a fixed
UA; `-human=false` sends the tool's own `dd/1.0`.

**What this does and does not hide (defensive insight):** only header *values* are
browser-like. `Accept-Encoding` is intentionally left unset so Go still transparently
decompresses gzip (otherwise body-based block detection breaks). More importantly, the
underlying Go HTTP/TLS stack still has its own fingerprint — header **order and casing**,
the **TLS ClientHello (JA3)**, and **HTTP/2 settings** — which these headers do not
change. A detector that fingerprints the transport, not just the strings, can still tell
this apart from a real Chrome. That gap is exactly where a defense should look.

---

## Cache busting

By default every request to a URL is byte-identical, so a caching layer
(`nginx proxy_cache`, a CDN) keyed on URL + query serves one cached copy and the
requests never reach origin. With `-cache-bust`, `pool.fire` appends a unique
`?_=<n>` (name via `-cache-bust-param`) to each request — including each retry — so
the cache treats every request as distinct and forwards it. The value comes from an
atomic counter seeded with the wall clock (`cachebust.go`): unique under concurrency
and different across runs. Existing query params and any fragment are preserved.
`Result.URL` keeps the *clean* URL so the summary and export group logically.

## Configuration (flags)

| flag | default | meaning |
|---|---|---|
| `-workers` | 10 | concurrent workers |
| `-urls` | `https://example.com` | comma-separated targets, e.g. `aaa.com,xxx.vn` |
| `-requests` | 100 | requests **per URL** (total = requests × urls). **Omit it with proxies → "until blocked" mode** (below); direct mode falls back to the fixed count |
| `-dsn` | `$DATABASE_DSN` | Postgres DSN; empty → direct mode |
| `-proxies` | — | comma-separated proxy URLs to insert before running |
| `-retries` | 2 | extra attempts through another proxy on block/failure |
| `-rps` | 0 | global request rate cap (req/s); 0 = unlimited |
| `-timeout` | 30s | per-request timeout |
| `-insecure` | false | skip TLS verification |
| `-verbose` | false | print each request result |
| `-human` | true | send realistic browser headers (a stable browser identity per proxy) |
| `-user-agent` | — | override the User-Agent for all requests (disables `-human` rotation) |
| `-block-after` | 5 | blocked responses per domain before quarantine |
| `-block-for` | 30m | quarantine duration per (proxy, domain) |
| `-block-ttl` | 5s | how long a domain's blocked-set is cached |
| `-block-cache-max` | 1024 | max domains held in the cache (LRU; bounds memory) |
| `-block-prune` | 24h | startup delete of stale partial-failure rows (0 disables) |
| `-proxy-fail-limit` | 3 | consecutive transport failures before cooldown |
| `-proxy-cooldown` | 1m | how long an unhealthy proxy is skipped |
| `-out` | — | write per-request results to `.csv` or `.jsonl` |
| `-error-log` | — | append **errors only** (failed requests, skips, warnings, fatal) to this file |
| `-cache-bust` | false | append a unique query param to every request to bypass caches (nginx/CDN) |
| `-cache-bust-param` | `_` | the cache-buster query param name |

### Examples

```bash
# Direct, rate-capped, with CSV export
./bin/dd -urls "https://a.com,https://b.vn" -workers 20 -requests 500 -rps 50 -out results.csv

# Through Postgres-backed proxies, seeding two on the way in
DATABASE_DSN="postgres://u:p@localhost:5432/db?sslmode=disable" \
  ./bin/dd -urls a.com -proxies "http://user:pass@h1:8080,socks5://h2:1080" -verbose
```

---

## Concurrency & safety

- **Workers** read a shared buffered `jobs` channel; the producer closes it when done.
- **Summary** aggregation is mutex-guarded.
- **Block cache**: DB queries happen outside the lock; every map access is under the lock.
- **Health breaker**: a single mutex around its maps.
- **Proxy rotation** uses `math/rand/v2` (concurrency-safe top-level `rand.Perm`).
- **Rate limiter** (`x/time/rate`) is shared across all workers and consumed per request.
- Verified with `go test -race ./...`.

---

## Failure modes & degradation

| situation | behavior |
|---|---|
| No `-dsn` / `DATABASE_DSN` | direct mode (single no-proxy client), not an error |
| DSN set but no active proxies | direct mode |
| DB error while checking a block (cache miss) | fail open — proxy is used |
| Proxy returns a block | quarantined per-domain; request retried elsewhere |
| Proxy transport failure | health cooldown (proxy-wide); request retried elsewhere |
| All proxies unusable for a domain | request counted as **skipped** (`ErrAllBlocked`) |
| "Until blocked" mode, all proxies exhausted | producer stops; run ends with a summary |
| "Until blocked" mode, targets never block | runs until SIGINT/SIGTERM (by design) |
| SIGINT/SIGTERM | context cancels; workers and producer stop; summary prints |

---

## Testing

- **Unit** (`internal/pool`): random selection spreads across the pool; quarantined
  proxies are skipped; retry exhausts N proxies / recovers on a good one; health
  cooldown trip & reset; `looksBlocked` across status/body cases.
- **Unit** (`internal/proxy`): block cache serves within-TTL from memory, reloads after
  TTL, write-through block/clear, LRU eviction — all against a fake `BlockSource` (no DB).
- **Unit** (`internal/report`): CSV and JSONL round-trips.
- **Integration** (`internal/proxy`, opt-in via `DD_TEST_DSN`): the full quarantine
  lifecycle against real Postgres.

```bash
go test ./...                                   # unit
go test -race ./...                             # race detector
DD_TEST_DSN="postgres://.../dd_test?sslmode=disable" \
  go test -run TestBlockLifecycle ./internal/proxy/   # integration
```

---

## Extension points

- **HTTP method / headers / body** — today it is GET-only; `httpclient` + `pool.fire`
  are where to add them.
- **Smarter block detection** — extend `blockMarkers` / `urlMarkers` / status set.
- **Residential-proxy awareness / IP reputation** — proxy metadata could join `proxies`.
- **Metrics** — per-proxy success rate is derivable from the exported records or could
  be added to `Summary`.
- **Single-flight on cache misses** — collapse concurrent first-touch loads per domain
  if the initial burst of DB queries ever matters.
```
