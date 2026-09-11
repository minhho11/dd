// Command dd fires a large volume of HTTP requests across a worker pool,
// optionally routing them through proxies loaded from Postgres. The run
// parameters live in the `config` key/value table and the targets in the `urls`
// table, both in Postgres; the tool loads them, partitions its workers across the
// targets by weight (a dedicated worker group per URL), and restarts whenever the
// config or urls change (LISTEN/NOTIFY). Proxies that a domain repeatedly blocks
// are quarantined, dead proxies are cooled down, blocked requests retry through
// another proxy, and the request rate can be capped.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/minhho11/dd/internal/browser"
	cfgdb "github.com/minhho11/dd/internal/config"
	"github.com/minhho11/dd/internal/db"
	"github.com/minhho11/dd/internal/httpclient"
	"github.com/minhho11/dd/internal/metrics"
	"github.com/minhho11/dd/internal/pool"
	"github.com/minhho11/dd/internal/proxy"
	"github.com/minhho11/dd/internal/report"
)

// config holds the resolved command-line options. The run parameters (workers,
// requests, retries, …) and the target URLs are only used to *seed* the DB on
// first run; after that the `config` and `urls` tables are the source of truth.
type config struct {
	workers        int
	urls           []string
	requests       int
	dsn            string
	seed           []string
	timeout        time.Duration
	insecure       bool
	verbose        bool
	human          bool
	userAgent      string
	blockAfter     int
	blockFor       time.Duration
	blockTTL       time.Duration
	blockCacheMax  int
	blockPrune     time.Duration
	retries        int
	rps            float64
	proxyFails     int
	proxyCool      time.Duration
	out            string
	errorLog       string
	cacheBust      bool
	cacheBustParam string
	reportInterval time.Duration
	skipReport     bool          // disable the report-table reporter entirely
	dbMaxConns     int           // cap on the Postgres connection pool (0 = unbounded)
	watchPoll      time.Duration // fallback poll of config+urls when NOTIFY is lost (0 = off)
	headless       bool
	browserDebug   bool // seed: log each browser step during the run (config: browser_debug)
	browserMax     int  // seed: max concurrent browser flows (config: browser_max)
	browserReuse   bool // seed: reuse one browser across direct runs (config: browser_reuse)
	browserBlock   bool // seed: block fonts/media/trackers in flows (config: browser_block)

	browserTest      string        // one-shot: run this URL's browser flow once and exit
	browserTestProxy string        // optional proxy for the one-shot test ("" = direct)
	browserTestHold  time.Duration // keep the browser open this long after the test
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "dd:", err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string, out io.Writer) (err error) {
	cfg, err := parseFlags(args, out)
	if err != nil {
		return err
	}
	if cfg.dsn == "" {
		return fmt.Errorf("-dsn/DATABASE_DSN is required: config and urls are read from the database")
	}

	// Derived context so background goroutines stop when the run finishes, not
	// only on signal.
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	// Optional error log: failed requests and operational warnings only.
	var errLog *log.Logger
	if cfg.errorLog != "" {
		f, oerr := os.OpenFile(cfg.errorLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if oerr != nil {
			return fmt.Errorf("open error log %q: %w", cfg.errorLog, oerr)
		}
		defer f.Close()
		errLog = log.New(f, "", log.LstdFlags|log.LUTC)
		fmt.Fprintf(out, "writing error logs to %s\n", cfg.errorLog)
		defer func() {
			if err != nil {
				errLog.Printf("fatal: %v", err)
			}
		}()
	}

	// Quiet pgdriver's own listener chatter unless -verbose. The LISTEN connection
	// is periodically reaped by the network/pooler ("bun: ping timeout"); pgdriver
	// reconnects on its own and the -watch-poll fallback catches any change missed
	// meanwhile, so the warning is just noise. Keep it under -verbose for debugging.
	if !cfg.verbose {
		pgdriver.Logger = noopPGLogger{}
	}

	// Open the database once (shared by proxy state, config, urls, and reporting).
	database, err := db.Open(ctx, cfg.dsn, cfg.verbose, cfg.dbMaxConns)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	defer database.Close()

	// One-shot manual confirm: run a single browser flow once and exit, bypassing
	// the pool/reporter/proxy machinery entirely.
	if cfg.browserTest != "" {
		return runBrowserTest(ctx, database, cfg, out)
	}

	endpoints, cache, err := loadProxyState(ctx, database, cfg, out)
	if err != nil {
		return err
	}
	var blocks pool.BlockStore
	if cache != nil {
		blocks = cache
	}

	// Reporting: an in-memory per-URL tally upserted into the report table every
	// report interval. The interval lives in the config table (report_interval_seconds,
	// seeded from -report-interval) so it can be changed live; the reporter re-reads
	// it each cycle. On by default; -skip-report turns it off entirely (no schema,
	// no reporter goroutine, no per-result recording) — for when the report table
	// isn't wanted or its writes are unwelcome. Its stop+final-flush must run before
	// the DB is closed, so register it after the database defer (defers run LIFO).
	var mtx *metrics.Metrics
	if cfg.skipReport {
		fmt.Fprintln(out, "report table disabled (-skip-report)")
	} else {
		// The config table is the source of the interval, so ensure + seed it before
		// the reporter reads it. runFromDB ensures + seeds again (idempotent).
		cfgRepo := cfgdb.NewRepo(database)
		if err := cfgRepo.EnsureSchema(ctx); err != nil {
			return fmt.Errorf("ensure config schema: %w", err)
		}
		if err := cfgRepo.EnsureDefault(ctx, cliRunConfig(cfg)); err != nil {
			return fmt.Errorf("seed config: %w", err)
		}
		reportRepo := metrics.NewRepo(database)
		if err := reportRepo.EnsureSchema(ctx); err != nil {
			return fmt.Errorf("ensure report schema: %w", err)
		}
		// intervalOf reads the current report interval from the config table; on a load
		// error it keeps the last good value (0 first time → reporter's 30s default).
		last := cfg.reportInterval
		intervalOf := func() time.Duration {
			if rc, lerr := cfgRepo.Load(ctx); lerr == nil {
				last = rc.ReportInterval()
			}
			return last
		}
		mtx = metrics.New()
		reporterCtx, reporterCancel := context.WithCancel(ctx)
		reporterDone := make(chan struct{})
		go func() {
			defer close(reporterDone)
			mtx.Run(reporterCtx, reportRepo, intervalOf, out)
		}()
		defer func() { reporterCancel(); <-reporterDone }()
		fmt.Fprintf(out, "reporting per-URL success/fail to `report` table every %s (config.report_interval_seconds)\n", intervalOf())
	}

	onResult, closeHandlers, err := buildResultHandler(cfg, out, errLog, mtx)
	if err != nil {
		return err
	}
	defer closeHandlers()

	tuning := poolTuning{failLimit: cfg.proxyFails, cooldown: cfg.proxyCool, headless: cfg.headless}

	return runFromDB(ctx, database, cfg, out, endpoints, blocks, tuning, onResult)
}

// runBrowserTest loads one target by URL and runs its browser flow exactly once,
// direct or through -browser-test-proxy, logging each step and printing pass/fail.
// It never starts the pool, reporter, or "until blocked" producer, so it can't
// flood — the tool for manually confirming a flow works. It exits 0 whether the
// flow passed or failed (the flow ran); it errors only on setup problems.
func runBrowserTest(ctx context.Context, database *bun.DB, cfg config, out io.Writer) error {
	tgtRepo := cfgdb.NewTargetRepo(database)
	if err := tgtRepo.EnsureSchema(ctx); err != nil {
		return fmt.Errorf("ensure urls schema: %w", err)
	}
	t, err := tgtRepo.LoadByURL(ctx, cfg.browserTest)
	if err != nil {
		return fmt.Errorf("load target %q (check the url matches a urls row exactly): %w", cfg.browserTest, err)
	}
	if !strings.EqualFold(strings.TrimSpace(t.Mode), "browser") {
		return fmt.Errorf("target %q is mode=%q, not browser", t.URL, t.Mode)
	}
	flow, err := browser.ParseFlow(t.Params)
	if err != nil {
		return fmt.Errorf("parse flow: %w", err)
	}

	via := "direct"
	if cfg.browserTestProxy != "" {
		via = cfg.browserTestProxy
	}
	fmt.Fprintf(out, "browser-test: %s\n  via=%s headless=%t timeout=%s steps=%d\n",
		t.URL, via, cfg.headless, cfg.timeout, len(flow.Steps))

	oc := browser.Execute(ctx, t.URL, flow, cfg.browserTestProxy, browser.Options{
		Headless:  cfg.headless,
		Timeout:   cfg.timeout,
		Insecure:  cfg.insecure,
		UserAgent: cfg.userAgent,
		HoldOpen:  cfg.browserTestHold,

		BlockResources: cfg.browserBlock,
		Logf:           func(f string, a ...any) { fmt.Fprintf(out, "  "+f+"\n", a...) },
	})

	fmt.Fprintf(out, "\n─── browser-test result ───\n")
	fmt.Fprintf(out, "status:    %d\n", oc.Status)
	fmt.Fprintf(out, "steps run: %d/%d\n", oc.Steps, len(flow.Steps)+1)
	fmt.Fprintf(out, "final URL: %s\n", oc.FinalURL)
	if oc.Err != nil {
		fmt.Fprintf(out, "result:    FAILED — %v\n", oc.Err)
	} else {
		fmt.Fprintf(out, "result:    PASSED\n")
	}
	return nil
}

// noopPGLogger discards pgdriver's internal log output (listener reconnect
// warnings). Installed unless -verbose; see the note in run.
type noopPGLogger struct{}

func (noopPGLogger) Printf(context.Context, string, ...any) {}

// poolTuning holds the operational proxy-health knobs that stay on the CLI (not
// in the DB config), so the run path passes them through unchanged.
type poolTuning struct {
	failLimit int
	cooldown  time.Duration
	headless  bool // run browser-mode targets headless
}

// cliRunConfig maps CLI flags to a cfgdb.Config, used only to seed the config
// table on first run.
func cliRunConfig(cfg config) cfgdb.Config {
	return cfgdb.Config{
		Workers:               cfg.workers,
		Requests:              cfg.requests,
		Retries:               cfg.retries,
		RPS:                   cfg.rps,
		TimeoutSeconds:        int(cfg.timeout / time.Second),
		ReportIntervalSeconds: int(cfg.reportInterval / time.Second),
		CacheBust:             cfg.cacheBust,
		CacheBustParam:        cfg.cacheBustParam,
		Human:                 cfg.human,
		UserAgent:             cfg.userAgent,
		Insecure:              cfg.insecure,
		BrowserDebug:          cfg.browserDebug,
		BrowserMax:            cfg.browserMax,
		BrowserReuse:          cfg.browserReuse,
		BrowserBlock:          cfg.browserBlock,
	}
}

// buildResultHandler composes the per-result callback (metrics, verbose print,
// export, error log), plus a cleanup that closes the exporter.
func buildResultHandler(cfg config, out io.Writer, errLog *log.Logger, mtx *metrics.Metrics) (func(pool.Result), func(), error) {
	var handlers []func(pool.Result)
	cleanup := func() {}

	if mtx != nil {
		handlers = append(handlers, metricsHandler(mtx))
	}
	if cfg.verbose {
		handlers = append(handlers, verbosePrinter(out))
	}
	if cfg.out != "" {
		w, err := report.New(cfg.out)
		if err != nil {
			return nil, nil, fmt.Errorf("open report %q: %w", cfg.out, err)
		}
		cleanup = func() {
			if cerr := w.Close(); cerr != nil {
				fmt.Fprintln(out, "report close error:", cerr)
				if errLog != nil {
					errLog.Printf("report close error: %v", cerr)
				}
			}
		}
		handlers = append(handlers, exportHandler(w))
		fmt.Fprintf(out, "writing results to %s\n", cfg.out)
	}
	if errLog != nil {
		handlers = append(handlers, errorLogHandler(errLog))
	}

	if len(handlers) == 0 {
		return nil, cleanup, nil
	}
	return func(r pool.Result) {
		for _, h := range handlers {
			h(r)
		}
	}, cleanup, nil
}

// runFromDB loads the config and targets from the DB, seeds them from the CLI on
// first run, runs, and restarts whenever config or urls change (LISTEN/NOTIFY). A
// finished run idles until the next change. Returns when ctx is cancelled.
func runFromDB(ctx context.Context, database *bun.DB, cfg config, out io.Writer, endpoints []httpclient.ProxyEndpoint, blocks pool.BlockStore, tuning poolTuning, onResult func(pool.Result)) error {
	cfgRepo := cfgdb.NewRepo(database)
	if err := cfgRepo.EnsureSchema(ctx); err != nil {
		return fmt.Errorf("ensure config schema: %w", err)
	}
	tgtRepo := cfgdb.NewTargetRepo(database)
	if err := tgtRepo.EnsureSchema(ctx); err != nil {
		return fmt.Errorf("ensure urls schema: %w", err)
	}
	// Seed both tables from the CLI defaults on first run; existing rows are kept.
	if err := cfgRepo.EnsureDefault(ctx, cliRunConfig(cfg)); err != nil {
		return fmt.Errorf("seed config: %w", err)
	}
	if err := tgtRepo.Seed(ctx, cfg.urls, cfg.requests); err != nil {
		return fmt.Errorf("seed urls: %w", err)
	}

	changes, err := cfgRepo.Watch(ctx, cfgdb.WatchOptions{
		Poll: cfg.watchPoll,
		Logf: func(f string, a ...any) { fmt.Fprintf(out, f+"\n", a...) },
	})
	if err != nil {
		return fmt.Errorf("watch config: %w", err)
	}
	if cfg.watchPoll > 0 {
		fmt.Fprintf(out, "watching config + urls tables for changes (LISTEN config_changed, polling every %s)\n", cfg.watchPoll)
	} else {
		fmt.Fprintln(out, "watching config + urls tables for changes (LISTEN config_changed)")
	}

	for {
		rc, err := cfgRepo.Load(ctx)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
		targets, err := tgtRepo.LoadEnabled(ctx)
		if err != nil {
			return fmt.Errorf("load urls: %w", err)
		}
		fmt.Fprintf(out, "── config: workers=%d retries=%d rps=%s cache-bust=%t timeout=%s | %d target(s)\n",
			rc.Workers, rc.Retries, rpsLabel(rc.RPS), rc.CacheBust, rc.Timeout(), len(targets))

		runCtx, runCancel := context.WithCancel(ctx)
		done := make(chan *pool.Summary, 1)
		go func() {
			done <- executeOneRun(runCtx, out, rc, targets, endpoints, blocks, tuning, onResult)
		}()

		select {
		case <-ctx.Done():
			runCancel()
			<-done
			return nil
		case <-changes:
			fmt.Fprintln(out, "config/urls changed — restarting run")
			runCancel()
			<-done
			continue
		case summary := <-done:
			runCancel()
			printSummary(out, summary)
			fmt.Fprintln(out, "run complete — waiting for next config/urls change")
			select {
			case <-ctx.Done():
				return nil
			case <-changes:
				continue
			}
		}
	}
}

// executeOneRun builds the clients and pool for one config, partitions the
// workers across the targets by weight (a dedicated worker group per URL),
// dispatches each target's jobs, and returns the aggregated summary. It returns
// when the run completes or ctx is cancelled.
func executeOneRun(ctx context.Context, out io.Writer, rc cfgdb.Config, targets []cfgdb.Target, endpoints []httpclient.ProxyEndpoint, blocks pool.BlockStore, tuning poolTuning, onResult func(pool.Result)) *pool.Summary {
	if len(targets) == 0 {
		fmt.Fprintln(out, "no enabled targets in urls table — nothing to do")
		return &pool.Summary{}
	}

	clients := httpclient.New(httpclient.Config{
		Timeout:         rc.Timeout(),
		InsecureTLS:     rc.Insecure,
		FollowRedirects: true,
		Human:           rc.Human,
		UserAgent:       rc.UserAgent,
	}, endpoints)

	workers := rc.Workers
	if workers < 1 {
		workers = 1
	}
	wp := pool.New(clients, blocks, pool.Options{
		Workers:        workers,
		Retries:        rc.Retries,
		RPS:            rc.RPS,
		ProxyFailLimit: tuning.failLimit,
		ProxyCooldown:  tuning.cooldown,
		CacheBust:      rc.CacheBust,
		CacheBustParam: rc.CacheBustParam,
		Headless:       tuning.headless,
		BrowserTimeout: rc.Timeout(),
		Insecure:       rc.Insecure,
		UserAgent:      rc.UserAgent,
		BrowserDebug:   rc.BrowserDebug,
		BrowserLog:     func(f string, a ...any) { fmt.Fprintf(out, "  "+f+"\n", a...) },
		BrowserMax:     rc.BrowserMax,
		BrowserReuse:   rc.BrowserReuse,
		BrowserBlock:   rc.BrowserBlock,
	})
	wp.OnResult = onResult

	weights := make([]float64, len(targets))
	for i, t := range targets {
		weights[i] = t.Weight
	}
	alloc := allocateWorkers(weights, workers)
	hasProxies := len(endpoints) > 0

	fmt.Fprintf(out, "firing %d target(s), workers=%d, clients=%d, retries=%d, rps=%s\n",
		len(targets), workers, clients.Size(), rc.Retries, rpsLabel(rc.RPS))
	for i, t := range targets {
		kind := strings.ToUpper(t.Method)
		if strings.EqualFold(strings.TrimSpace(t.Mode), "browser") {
			kind = "BROWSER"
		}
		fmt.Fprintf(out, "  %-7s %-45s weight=%-4g → %d worker(s)  [%s]\n",
			kind, t.URL, t.Weight, alloc[i], targetMode(t, rc, hasProxies))
	}

	groups := make([]pool.Group, len(targets))
	for i := range targets {
		t := targets[i]
		jobs := make(chan pool.Job, alloc[i])
		go produceTarget(ctx, wp, t, rc, hasProxies, jobs)
		groups[i] = pool.Group{Workers: alloc[i], Jobs: jobs}
	}
	summary := wp.RunGroups(ctx, groups)
	wp.CloseBrowser() // tear down any reused browser sessions before the next run
	return summary
}

// targetMode describes how many requests a target will fire, for the run banner.
func targetMode(t cfgdb.Target, rc cfgdb.Config, hasProxies bool) string {
	if t.Requests > 0 {
		return fmt.Sprintf("%d req", t.Requests)
	}
	// Browser targets are always finite (no "until blocked" — a browser can fall
	// back to a direct run, so there is no proxy-exhaustion stop condition).
	if hasProxies && !isBrowserMode(t.Mode) {
		return "until blocked"
	}
	n := rc.Requests
	if n <= 0 {
		n = 100
	}
	return fmt.Sprintf("%d req (default)", n)
}

// isBrowserMode reports whether a target runs through the browser executor.
func isBrowserMode(mode string) bool {
	return strings.EqualFold(strings.TrimSpace(mode), "browser")
}

// produceTarget feeds one target's jobs into its channel: a fixed per-URL count,
// or — when the count is 0 and proxies exist — until no proxy is usable for the
// target. With no proxies (direct) an unset count falls back to the global
// default. Closes the channel when done so the target's workers exit.
func produceTarget(ctx context.Context, wp *pool.Pool, t cfgdb.Target, rc cfgdb.Config, hasProxies bool, jobs chan<- pool.Job) {
	defer close(jobs)
	mk := func() pool.Job {
		return pool.Job{URL: t.URL, Mode: t.Mode, Method: t.Method, Params: t.Params}
	}

	// "Until blocked" (requests=0 with proxies) applies to http targets only.
	// Browser targets are always finite: they can fall back to a direct run, so
	// there is no proxy-exhaustion stop condition and an unbounded loop would just
	// launch browsers forever. A browser target with requests<=0 uses the global
	// default count instead.
	if t.Requests <= 0 && hasProxies && !isBrowserMode(t.Mode) {
		domain := []string{t.URL}
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if !wp.AnyUsable(ctx, domain, false) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case jobs <- mk():
			}
		}
	}

	n := t.Requests
	if n <= 0 {
		n = rc.Requests
		if n <= 0 {
			n = 100
		}
	}
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			return
		case jobs <- mk():
		}
	}
}

// allocateWorkers partitions total workers across the given per-target weights
// (dedicated split). Weights are relative and normalized; every target gets at
// least one worker (so total is raised to len(weights) when there are more
// targets than workers), and any remainder from flooring goes to the largest
// fractional shares.
func allocateWorkers(weights []float64, total int) []int {
	n := len(weights)
	if n == 0 {
		return nil
	}
	if total < n {
		total = n // guarantee at least one worker per target
	}

	sum := 0.0
	for _, w := range weights {
		if w > 0 {
			sum += w
		}
	}
	norm := make([]float64, n)
	copy(norm, weights)
	if sum <= 0 { // all non-positive: equal split
		for i := range norm {
			norm[i] = 1
		}
		sum = float64(n)
	}

	alloc := make([]int, n)
	type frac struct {
		i int
		f float64
	}
	fracs := make([]frac, n)
	used := 0
	for i, w := range norm {
		if w < 0 {
			w = 0
		}
		exact := w / sum * float64(total)
		base := int(math.Floor(exact))
		if base < 1 {
			base = 1
		}
		alloc[i] = base
		used += base
		fracs[i] = frac{i, exact - math.Floor(exact)}
	}
	if used < total {
		sort.Slice(fracs, func(a, b int) bool { return fracs[a].f > fracs[b].f })
		for k := 0; used < total; k++ {
			alloc[fracs[k%n].i]++
			used++
		}
	}
	return alloc
}

func parseFlags(args []string, out io.Writer) (config, error) {
	fs := flag.NewFlagSet("dd", flag.ContinueOnError)
	fs.SetOutput(out)

	workers := fs.Int("workers", 10, "seed: number of concurrent workers (partitioned across targets by weight)")
	urls := fs.String("urls", "https://example.com", "seed: comma-separated target URLs inserted into the urls table on first run")
	requests := fs.Int("requests", 100, "seed: default requests per URL (0 = until all proxies blocked); per-URL value lives in the urls table")
	dsn := fs.String("dsn", os.Getenv("DATABASE_DSN"), "Postgres DSN (or DATABASE_DSN env); required")
	seed := fs.String("proxies", "", "comma-separated proxy URLs to insert into the DB before running")
	timeout := fs.Duration("timeout", 30*time.Second, "seed: per-request timeout")
	insecure := fs.Bool("insecure", false, "seed: skip TLS verification")
	verbose := fs.Bool("verbose", false, "print each request result")
	human := fs.Bool("human", true, "seed: send realistic browser headers (a stable browser identity per proxy)")
	userAgent := fs.String("user-agent", "", "seed: override the User-Agent for all requests (disables -human header rotation)")
	blockAfter := fs.Int("block-after", 5, "blocked responses per domain before a proxy is quarantined")
	blockFor := fs.Duration("block-for", 30*time.Minute, "how long a quarantined proxy is skipped for a domain")
	blockTTL := fs.Duration("block-ttl", 5*time.Second, "how long a domain's blocked-proxy set is cached before reloading")
	blockCacheMax := fs.Int("block-cache-max", 1024, "max domains held in the block cache (LRU-evicted; bounds memory)")
	blockPrune := fs.Duration("block-prune", 24*time.Hour, "at startup, delete partial-failure rows older than this (0 disables)")
	retries := fs.Int("retries", 2, "seed: extra attempts through another proxy when blocked or failed")
	rps := fs.Float64("rps", 0, "seed: global request rate cap (requests/sec); 0 = unlimited")
	proxyFails := fs.Int("proxy-fail-limit", 3, "consecutive transport failures before a proxy cools down")
	proxyCool := fs.Duration("proxy-cooldown", time.Minute, "how long an unhealthy proxy is skipped")
	outFile := fs.String("out", "", "write per-request results to this .csv or .jsonl file")
	errorLog := fs.String("error-log", "", "append error logs (failed requests + warnings) to this file")
	cacheBust := fs.Bool("cache-bust", false, "seed: append a unique query param to each request to bypass caches (nginx/CDN)")
	cacheBustParam := fs.String("cache-bust-param", "_", "seed: query param name used for cache busting")
	reportInterval := fs.Duration("report-interval", 30*time.Second, "how often to upsert per-URL success/fail into the `report` table (<=0 uses 30s)")
	skipReport := fs.Bool("skip-report", false, "disable the `report` table entirely (no schema, no periodic upserts); the end-of-run summary still prints")
	dbMaxConns := fs.Int("db-max-conns", 10, "max Postgres connections dd opens (0 = unbounded); keeps dd from exhausting a shared server's connection slots under load")
	watchPoll := fs.Duration("watch-poll", 10*time.Second, "also poll the config + urls tables this often and restart on a change, for when LISTEN/NOTIFY is lost (pooler, dropped connection); 0 = NOTIFY only")
	headless := fs.Bool("headless", true, "run browser-mode (mode='browser') targets in headless Chromium; set false to watch")
	browserDebug := fs.Bool("browser-debug", false, "seed: log each browser navigation/step during the run (toggle live via config.browser_debug)")
	browserMax := fs.Int("browser-max", 2, "seed: max concurrent browser (Chromium) flows across all workers, 0=unlimited (tune live via config.browser_max)")
	browserReuse := fs.Bool("browser-reuse", true, "seed: reuse one browser across direct flow runs instead of relaunching per request (toggle live via config.browser_reuse)")
	browserBlock := fs.Bool("browser-block", true, "seed: block web fonts, media and third-party analytics/ad scripts in browser flows (toggle live via config.browser_block)")
	browserTest := fs.String("browser-test", "", "run the browser flow for this urls.url once and exit (manual confirm); pairs with -headless=false")
	browserTestProxy := fs.String("browser-test-proxy", "", "proxy URL to route the -browser-test run through (default direct)")
	browserTestHold := fs.Duration("browser-test-hold", 0, "keep the browser open this long after a -browser-test run (e.g. 30s) to inspect it")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	return config{
		workers:          *workers,
		urls:             splitList(*urls),
		requests:         *requests,
		dsn:              *dsn,
		seed:             splitList(*seed),
		timeout:          *timeout,
		insecure:         *insecure,
		verbose:          *verbose,
		human:            *human,
		userAgent:        *userAgent,
		blockAfter:       *blockAfter,
		blockFor:         *blockFor,
		blockTTL:         *blockTTL,
		blockCacheMax:    *blockCacheMax,
		blockPrune:       *blockPrune,
		retries:          *retries,
		rps:              *rps,
		proxyFails:       *proxyFails,
		proxyCool:        *proxyCool,
		out:              *outFile,
		errorLog:         *errorLog,
		cacheBust:        *cacheBust,
		cacheBustParam:   *cacheBustParam,
		reportInterval:   *reportInterval,
		skipReport:       *skipReport,
		dbMaxConns:       *dbMaxConns,
		watchPoll:        *watchPoll,
		headless:         *headless,
		browserDebug:     *browserDebug,
		browserMax:       *browserMax,
		browserReuse:     *browserReuse,
		browserBlock:     *browserBlock,
		browserTest:      *browserTest,
		browserTestProxy: *browserTestProxy,
		browserTestHold:  *browserTestHold,
	}, nil
}

// loadProxyState ensures the proxy/block schemas, seeds any -proxies, prunes
// stale block rows, and returns the proxy endpoints plus the (lazy) block cache.
// The database handle is owned by the caller.
func loadProxyState(ctx context.Context, database *bun.DB, cfg config, out io.Writer) ([]httpclient.ProxyEndpoint, *proxy.BlockCache, error) {
	proxyRepo := proxy.NewRepo(database)
	blockRepo := proxy.NewBlockRepo(database, cfg.blockAfter, cfg.blockFor)
	if err := proxyRepo.EnsureSchema(ctx); err != nil {
		return nil, nil, fmt.Errorf("ensure proxies schema: %w", err)
	}
	if err := blockRepo.EnsureSchema(ctx); err != nil {
		return nil, nil, fmt.Errorf("ensure proxies_blocked schema: %w", err)
	}

	for _, purl := range cfg.seed {
		if err := proxyRepo.Add(ctx, purl); err != nil {
			return nil, nil, fmt.Errorf("seed proxy %q: %w", purl, err)
		}
	}

	if n, err := blockRepo.Prune(ctx, cfg.blockPrune); err != nil {
		fmt.Fprintln(out, "block-table prune warning:", err)
	} else if n > 0 {
		fmt.Fprintf(out, "pruned %d stale block rows\n", n)
	}

	proxies, err := proxyRepo.LoadActive(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("load proxies: %w", err)
	}

	cache := proxy.NewBlockCache(blockRepo, cfg.blockTTL, cfg.blockCacheMax)

	if len(proxies) == 0 {
		fmt.Fprintln(out, "no active proxies in db: running direct")
	} else {
		fmt.Fprintf(out, "loaded %d proxies from db (block-after=%d, block-for=%s)\n",
			len(proxies), cfg.blockAfter, cfg.blockFor)
	}

	endpoints := make([]httpclient.ProxyEndpoint, len(proxies))
	for i, p := range proxies {
		endpoints[i] = httpclient.ProxyEndpoint{ID: p.ID, URL: p.URL}
	}
	return endpoints, cache, nil
}

func verbosePrinter(out io.Writer) func(pool.Result) {
	return func(r pool.Result) {
		via := "direct"
		if r.ProxyURL != "" {
			via = r.ProxyURL
		}
		if r.Err != nil {
			fmt.Fprintf(out, "  ERR   %-30s via %-25s (%d try) %v\n", r.URL, via, r.Attempts, r.Err)
			return
		}
		tag := ""
		if r.Blocked {
			tag = " BLOCKED"
		}
		fmt.Fprintf(out, "  %-5d %-30s via %-25s (%d try) %s%s\n",
			r.StatusCode, r.URL, via, r.Attempts, r.Latency.Round(time.Millisecond), tag)
	}
}

// errorLogHandler logs only results that errored: transport failures and skips
// (no usable proxy). Blocked/non-2xx responses are not errors — they belong in
// the summary and the -out export, not the error log. Safe for concurrent use
// (log.Logger locks internally).
func errorLogHandler(l *log.Logger) func(pool.Result) {
	return func(r pool.Result) {
		if r.Err == nil {
			return
		}
		if errors.Is(r.Err, pool.ErrAllBlocked) {
			l.Printf("skipped url=%q domain=%q: no usable proxy", r.URL, r.Domain)
			return
		}
		via := "direct"
		if r.ProxyURL != "" {
			via = r.ProxyURL
		}
		l.Printf("request error url=%q via=%s attempts=%d: %v", r.URL, via, r.Attempts, r.Err)
	}
}

// metricsHandler records each result into the in-memory per-URL tally. A result
// counts as success only when it completed, was not detected as a block, and had
// a status < 400; everything else (transport error, skip, block, non-2xx) is a fail.
func metricsHandler(m *metrics.Metrics) func(pool.Result) {
	return func(r pool.Result) {
		success := r.Err == nil && !r.Blocked && r.StatusCode < 400
		m.Record(r.URL, success)
	}
}

func exportHandler(w *report.Writer) func(pool.Result) {
	return func(r pool.Result) {
		rec := report.Record{
			Time:     time.Now(),
			URL:      r.URL,
			Domain:   r.Domain,
			Proxy:    r.ProxyURL,
			Status:   r.StatusCode,
			Blocked:  r.Blocked,
			Attempts: r.Attempts,
			Latency:  r.Latency,
		}
		if r.Err != nil {
			rec.Err = r.Err.Error()
		}
		_ = w.Write(rec)
	}
}

func printSummary(out io.Writer, s *pool.Summary) {
	fmt.Fprintln(out, "\n─── summary ───")
	fmt.Fprintf(out, "total:    %d\n", s.Total)
	fmt.Fprintf(out, "success:  %d\n", s.Success)
	fmt.Fprintf(out, "blocked:  %d\n", s.Blocked)
	fmt.Fprintf(out, "non-2xx:  %d\n", s.Non2xx)
	fmt.Fprintf(out, "failed:   %d\n", s.Failed)
	fmt.Fprintf(out, "skipped:  %d (no usable proxy)\n", s.Skipped)
	fmt.Fprintf(out, "retries:  %d (extra attempts)\n", s.Retries)
	fmt.Fprintf(out, "duration: %s\n", s.Duration.Round(time.Millisecond))
	if s.Duration > 0 {
		fmt.Fprintf(out, "rps:      %.1f\n", float64(s.Total)/s.Duration.Seconds())
	}

	byStatus := s.ByStatus()
	if len(byStatus) > 0 {
		codes := make([]int, 0, len(byStatus))
		for c := range byStatus {
			codes = append(codes, c)
		}
		sort.Ints(codes)
		fmt.Fprintln(out, "status codes:")
		for _, c := range codes {
			fmt.Fprintf(out, "  %d: %d\n", c, byStatus[c])
		}
	}
}

func rpsLabel(rps float64) string {
	if rps <= 0 {
		return "unlimited"
	}
	return fmt.Sprintf("%.0f", rps)
}

// splitList splits a comma-separated list, trimming spaces and dropping empties.
func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
