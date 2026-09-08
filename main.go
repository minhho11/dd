// Command dd fires a large volume of HTTP requests across a worker pool,
// optionally routing them through proxies loaded from Postgres. Proxies that a
// domain repeatedly blocks are quarantined (see internal/proxy.ProxyBlock), dead
// proxies are cooled down, blocked requests retry through another proxy, and the
// request rate can be capped.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/uptrace/bun"

	cfgdb "github.com/minhho11/dd/internal/config"
	"github.com/minhho11/dd/internal/db"
	"github.com/minhho11/dd/internal/httpclient"
	"github.com/minhho11/dd/internal/metrics"
	"github.com/minhho11/dd/internal/pool"
	"github.com/minhho11/dd/internal/proxy"
	"github.com/minhho11/dd/internal/report"
)

// config holds the resolved command-line options.
type config struct {
	workers        int
	urls           []string
	requests       int
	requestsSet    bool // -requests was passed explicitly
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
	watchConfig    bool
	reportInterval time.Duration
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
	if len(cfg.urls) == 0 && !cfg.watchConfig {
		return fmt.Errorf("no urls provided")
	}

	// Derived context so the job producer goroutine stops when the run finishes,
	// not only on signal.
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
		// Record a fatal run error (named return) on the way out.
		defer func() {
			if err != nil {
				errLog.Printf("fatal: %v", err)
			}
		}()
	}

	// Open the database once (shared by proxy state and, in watch mode, config).
	var database *bun.DB
	var endpoints []httpclient.ProxyEndpoint
	var blocks pool.BlockStore
	if cfg.dsn != "" {
		database, err = db.Open(ctx, cfg.dsn, cfg.verbose)
		if err != nil {
			return fmt.Errorf("connect db: %w", err)
		}
		defer database.Close()

		var cache *proxy.BlockCache
		endpoints, cache, err = loadProxyState(ctx, database, cfg, out)
		if err != nil {
			return err
		}
		if cache != nil {
			blocks = cache
		}
	} else {
		fmt.Fprintln(out, "no -dsn/DATABASE_DSN set: running without proxies (direct)")
	}
	if cfg.watchConfig && database == nil {
		return fmt.Errorf("-watch-config requires -dsn/DATABASE_DSN")
	}

	// Metrics: in-memory per-URL success/fail, flushed to the report table every
	// -report-interval. Its stop+final-flush must run before the DB is closed, so
	// register it after the database defer (defers run LIFO).
	var mtx *metrics.Metrics
	if database != nil && cfg.reportInterval > 0 {
		reportRepo := metrics.NewRepo(database)
		if err := reportRepo.EnsureSchema(ctx); err != nil {
			return fmt.Errorf("ensure report schema: %w", err)
		}
		mtx = metrics.New()
		reporterCtx, reporterCancel := context.WithCancel(ctx)
		reporterDone := make(chan struct{})
		go func() {
			defer close(reporterDone)
			mtx.Run(reporterCtx, reportRepo, cfg.reportInterval, out)
		}()
		defer func() { reporterCancel(); <-reporterDone }()
		fmt.Fprintf(out, "reporting per-URL success/fail to `report` table every %s\n", cfg.reportInterval)
	}

	onResult, closeHandlers, err := buildResultHandler(cfg, out, errLog, mtx)
	if err != nil {
		return err
	}
	defer closeHandlers()

	tuning := poolTuning{failLimit: cfg.proxyFails, cooldown: cfg.proxyCool}

	if cfg.watchConfig {
		return runWatched(ctx, database, cfg, out, endpoints, blocks, tuning, onResult)
	}

	rc := cliRunConfig(cfg)
	untilBlocked := !cfg.requestsSet && len(endpoints) > 0
	summary := executeOneRun(ctx, out, rc, endpoints, blocks, tuning, onResult, untilBlocked)
	printSummary(out, summary)
	return nil
}

// poolTuning holds the operational proxy-health knobs that stay on the CLI (not
// in the DB config), so both run paths pass them through unchanged.
type poolTuning struct {
	failLimit int
	cooldown  time.Duration
}

// cliRunConfig maps CLI flags to a cfgdb.Config for the single-run path, so both
// the CLI and the DB-watch paths drive the same executeOneRun.
func cliRunConfig(cfg config) cfgdb.Config {
	return cfgdb.Config{
		URLs:           strings.Join(cfg.urls, ","),
		Workers:        cfg.workers,
		Requests:       cfg.requests,
		Retries:        cfg.retries,
		RPS:            cfg.rps,
		TimeoutSeconds: int(cfg.timeout / time.Second),
		CacheBust:      cfg.cacheBust,
		CacheBustParam: cfg.cacheBustParam,
		Human:          cfg.human,
		UserAgent:      cfg.userAgent,
		Insecure:       cfg.insecure,
	}
}

// buildResultHandler composes the per-result callback (verbose print, export,
// error log) shared by both run paths, plus a cleanup that closes the exporter.
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

// executeOneRun builds the clients and pool for one config, dispatches the jobs
// (fixed count or until-blocked), and returns the summary. It returns when the
// run completes or ctx is cancelled.
func executeOneRun(ctx context.Context, out io.Writer, rc cfgdb.Config, endpoints []httpclient.ProxyEndpoint, blocks pool.BlockStore, tuning poolTuning, onResult func(pool.Result), untilBlocked bool) *pool.Summary {
	clients := httpclient.New(httpclient.Config{
		Timeout:         rc.Timeout(),
		InsecureTLS:     rc.Insecure,
		FollowRedirects: true,
		Human:           rc.Human,
		UserAgent:       rc.UserAgent,
	}, endpoints)

	wp := pool.New(clients, blocks, pool.Options{
		Workers:        rc.Workers,
		Retries:        rc.Retries,
		RPS:            rc.RPS,
		ProxyFailLimit: tuning.failLimit,
		ProxyCooldown:  tuning.cooldown,
		CacheBust:      rc.CacheBust,
		CacheBustParam: rc.CacheBustParam,
	})
	wp.OnResult = onResult

	urls := rc.URLList()
	if untilBlocked {
		fmt.Fprintf(out, "firing until all proxies blocked: %d urls, workers=%d, clients=%d, retries=%d, rps=%s\n",
			len(urls), rc.Workers, clients.Size(), rc.Retries, rpsLabel(rc.RPS))
	} else {
		fmt.Fprintf(out, "firing %d requests: %d urls x %d, workers=%d, clients=%d, retries=%d, rps=%s\n",
			rc.Requests*len(urls), len(urls), rc.Requests, rc.Workers, clients.Size(), rc.Retries, rpsLabel(rc.RPS))
	}

	jobs := make(chan pool.Job, rc.Workers)
	go func() {
		defer close(jobs)
		if untilBlocked {
			produceUntilBlocked(ctx, wp, urls, jobs)
			return
		}
		for i := 0; i < rc.Requests; i++ {
			for _, u := range urls {
				select {
				case <-ctx.Done():
					return
				case jobs <- pool.Job{URL: u}:
				}
			}
		}
	}()

	return wp.Run(ctx, jobs)
}

// runWatched loads the config from the DB, runs it, and restarts the run whenever
// the config row changes (via LISTEN/NOTIFY). A finished run idles until the next
// change. Returns when ctx is cancelled (SIGINT/SIGTERM).
func runWatched(ctx context.Context, database *bun.DB, cfg config, out io.Writer, endpoints []httpclient.ProxyEndpoint, blocks pool.BlockStore, tuning poolTuning, onResult func(pool.Result)) error {
	cfgRepo := cfgdb.NewRepo(database)
	if err := cfgRepo.EnsureSchema(ctx); err != nil {
		return fmt.Errorf("ensure config schema: %w", err)
	}
	// Seed the row from the CLI defaults on first run; an existing row is kept.
	if err := cfgRepo.EnsureDefault(ctx, cliRunConfig(cfg)); err != nil {
		return fmt.Errorf("seed config: %w", err)
	}

	changes, err := cfgRepo.Watch(ctx)
	if err != nil {
		return fmt.Errorf("watch config: %w", err)
	}
	fmt.Fprintln(out, "watching config table for changes (LISTEN config_changed)")

	for {
		rc, err := cfgRepo.Load(ctx)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
		fmt.Fprintf(out, "── config v%s: urls=%q workers=%d requests=%d retries=%d rps=%s cache-bust=%t\n",
			rc.UpdatedAt.Format(time.RFC3339), rc.URLs, rc.Workers, rc.Requests, rc.Retries, rpsLabel(rc.RPS), rc.CacheBust)

		untilBlocked := rc.Requests <= 0 && len(endpoints) > 0

		runCtx, runCancel := context.WithCancel(ctx)
		done := make(chan *pool.Summary, 1)
		go func() { done <- executeOneRun(runCtx, out, rc, endpoints, blocks, tuning, onResult, untilBlocked) }()

		select {
		case <-ctx.Done():
			runCancel()
			<-done
			return nil
		case <-changes:
			// Config changed: stop the current run and loop to reload.
			fmt.Fprintln(out, "config changed — restarting run")
			runCancel()
			<-done
			continue
		case summary := <-done:
			// Run finished on its own; idle until the next change.
			runCancel()
			printSummary(out, summary)
			fmt.Fprintln(out, "run complete — waiting for next config change")
			select {
			case <-ctx.Done():
				return nil
			case <-changes:
				continue
			}
		}
	}
}

func parseFlags(args []string, out io.Writer) (config, error) {
	fs := flag.NewFlagSet("dd", flag.ContinueOnError)
	fs.SetOutput(out)

	workers := fs.Int("workers", 10, "number of concurrent workers")
	urls := fs.String("urls", "https://example.com", "comma-separated target URLs, e.g. aaa.com,xxx.vn")
	requests := fs.Int("requests", 100, "requests per URL; if omitted with proxies, run until all proxies are blocked")
	dsn := fs.String("dsn", os.Getenv("DATABASE_DSN"), "Postgres DSN for proxies (or DATABASE_DSN env)")
	seed := fs.String("proxies", "", "comma-separated proxy URLs to insert into the DB before running")
	timeout := fs.Duration("timeout", 30*time.Second, "per-request timeout")
	insecure := fs.Bool("insecure", false, "skip TLS verification")
	verbose := fs.Bool("verbose", false, "print each request result")
	human := fs.Bool("human", true, "send realistic browser headers (a stable browser identity per proxy)")
	userAgent := fs.String("user-agent", "", "override the User-Agent for all requests (disables -human header rotation)")
	blockAfter := fs.Int("block-after", 5, "blocked responses per domain before a proxy is quarantined")
	blockFor := fs.Duration("block-for", 30*time.Minute, "how long a quarantined proxy is skipped for a domain")
	blockTTL := fs.Duration("block-ttl", 5*time.Second, "how long a domain's blocked-proxy set is cached before reloading")
	blockCacheMax := fs.Int("block-cache-max", 1024, "max domains held in the block cache (LRU-evicted; bounds memory)")
	blockPrune := fs.Duration("block-prune", 24*time.Hour, "at startup, delete partial-failure rows older than this (0 disables)")
	retries := fs.Int("retries", 2, "extra attempts through another proxy when blocked or failed")
	rps := fs.Float64("rps", 0, "global request rate cap (requests/sec); 0 = unlimited")
	proxyFails := fs.Int("proxy-fail-limit", 3, "consecutive transport failures before a proxy cools down")
	proxyCool := fs.Duration("proxy-cooldown", time.Minute, "how long an unhealthy proxy is skipped")
	outFile := fs.String("out", "", "write per-request results to this .csv or .jsonl file")
	errorLog := fs.String("error-log", "", "append error logs (failed requests + warnings) to this file")
	cacheBust := fs.Bool("cache-bust", false, "append a unique query param to each request to bypass caches (nginx/CDN)")
	cacheBustParam := fs.String("cache-bust-param", "_", "query param name used for cache busting")
	watchConfig := fs.Bool("watch-config", false, "load run params from the DB `config` table and restart the run when it changes")
	reportInterval := fs.Duration("report-interval", 30*time.Second, "insert per-URL success/fail counts into the DB `report` table this often (0 disables; needs -dsn)")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	requestsSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "requests" {
			requestsSet = true
		}
	})

	return config{
		workers:        *workers,
		urls:           splitList(*urls),
		requests:       *requests,
		requestsSet:    requestsSet,
		dsn:            *dsn,
		seed:           splitList(*seed),
		timeout:        *timeout,
		insecure:       *insecure,
		verbose:        *verbose,
		human:          *human,
		userAgent:      *userAgent,
		blockAfter:     *blockAfter,
		blockFor:       *blockFor,
		blockTTL:       *blockTTL,
		blockCacheMax:  *blockCacheMax,
		blockPrune:     *blockPrune,
		retries:        *retries,
		rps:            *rps,
		proxyFails:     *proxyFails,
		proxyCool:      *proxyCool,
		out:            *outFile,
		errorLog:       *errorLog,
		cacheBust:      *cacheBust,
		cacheBustParam: *cacheBustParam,
		watchConfig:    *watchConfig,
		reportInterval: *reportInterval,
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

// produceUntilBlocked feeds jobs round-robin over the URLs, stopping once no
// proxy is usable for any target (all quarantined or cooled down). Workers update
// the block/health state as they process, so AnyUsable eventually returns false.
// If the targets never block, this runs until the context is cancelled (Ctrl-C).
func produceUntilBlocked(ctx context.Context, wp *pool.Pool, urls []string, jobs chan<- pool.Job) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !wp.AnyUsable(ctx, urls) {
			return
		}
		for _, u := range urls {
			select {
			case <-ctx.Done():
				return
			case jobs <- pool.Job{URL: u}:
			}
		}
	}
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
