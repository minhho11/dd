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

	"github.com/minhho11/dd/internal/db"
	"github.com/minhho11/dd/internal/httpclient"
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
	if len(cfg.urls) == 0 {
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

	// Proxies and the block store live in Postgres. Without a DSN we run direct.
	var endpoints []httpclient.ProxyEndpoint
	var blocks pool.BlockStore
	if cfg.dsn != "" {
		var cache *proxy.BlockCache
		var cleanup func()
		endpoints, cache, cleanup, err = openProxyState(ctx, cfg, out)
		if err != nil {
			return err
		}
		defer cleanup()
		if cache != nil {
			blocks = cache
		}
	} else {
		fmt.Fprintln(out, "no -dsn/DATABASE_DSN set: running without proxies (direct)")
	}

	clients := httpclient.New(httpclient.Config{
		Timeout:         cfg.timeout,
		InsecureTLS:     cfg.insecure,
		FollowRedirects: true,
		Human:           cfg.human,
		UserAgent:       cfg.userAgent,
	}, endpoints)

	wp := pool.New(clients, blocks, pool.Options{
		Workers:        cfg.workers,
		Retries:        cfg.retries,
		RPS:            cfg.rps,
		ProxyFailLimit: cfg.proxyFails,
		ProxyCooldown:  cfg.proxyCool,
		CacheBust:      cfg.cacheBust,
		CacheBustParam: cfg.cacheBustParam,
	})

	// Compose the per-result handler from optional verbose printing and export.
	var handlers []func(pool.Result)
	if cfg.verbose {
		handlers = append(handlers, verbosePrinter(out))
	}
	if cfg.out != "" {
		w, err := report.New(cfg.out)
		if err != nil {
			return fmt.Errorf("open report %q: %w", cfg.out, err)
		}
		defer func() {
			if cerr := w.Close(); cerr != nil {
				fmt.Fprintln(out, "report close error:", cerr)
				if errLog != nil {
					errLog.Printf("report close error: %v", cerr)
				}
			}
		}()
		handlers = append(handlers, exportHandler(w))
		fmt.Fprintf(out, "writing results to %s\n", cfg.out)
	}
	if errLog != nil {
		handlers = append(handlers, errorLogHandler(errLog))
	}
	if len(handlers) > 0 {
		wp.OnResult = func(r pool.Result) {
			for _, h := range handlers {
				h(r)
			}
		}
	}

	// "Until blocked" mode: -requests omitted AND proxies present -> keep firing
	// until no proxy is usable for any target. In direct mode there is nothing to
	// exhaust, so fall back to the fixed count.
	untilBlocked := !cfg.requestsSet && len(endpoints) > 0

	if untilBlocked {
		fmt.Fprintf(out, "firing until all proxies blocked: %d urls, workers=%d, clients=%d, retries=%d, rps=%s\n",
			len(cfg.urls), cfg.workers, clients.Size(), cfg.retries, rpsLabel(cfg.rps))
	} else {
		total := cfg.requests * len(cfg.urls)
		fmt.Fprintf(out, "firing %d requests: %d urls x %d, workers=%d, clients=%d, retries=%d, rps=%s\n",
			total, len(cfg.urls), cfg.requests, cfg.workers, clients.Size(), cfg.retries, rpsLabel(cfg.rps))
	}

	jobs := make(chan pool.Job, cfg.workers)
	go func() {
		defer close(jobs)
		if untilBlocked {
			produceUntilBlocked(ctx, wp, cfg.urls, jobs)
			return
		}
		for i := 0; i < cfg.requests; i++ {
			for _, u := range cfg.urls {
				select {
				case <-ctx.Done():
					return
				case jobs <- pool.Job{URL: u}:
				}
			}
		}
	}()

	summary := wp.Run(ctx, jobs)
	printSummary(out, summary)
	return nil
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
	}, nil
}

// openProxyState connects to Postgres, ensures both schemas, seeds any proxies
// passed via -proxies, prunes stale block rows, and returns the proxy endpoints
// plus the (lazy) block cache. The returned cleanup closes the DB.
func openProxyState(ctx context.Context, cfg config, out io.Writer) ([]httpclient.ProxyEndpoint, *proxy.BlockCache, func(), error) {
	database, err := db.Open(ctx, cfg.dsn, cfg.verbose)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("connect db: %w", err)
	}
	cleanup := func() { _ = database.Close() }

	proxyRepo := proxy.NewRepo(database)
	blockRepo := proxy.NewBlockRepo(database, cfg.blockAfter, cfg.blockFor)
	if err := proxyRepo.EnsureSchema(ctx); err != nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("ensure proxies schema: %w", err)
	}
	if err := blockRepo.EnsureSchema(ctx); err != nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("ensure proxies_blocked schema: %w", err)
	}

	for _, purl := range cfg.seed {
		if err := proxyRepo.Add(ctx, purl); err != nil {
			cleanup()
			return nil, nil, nil, fmt.Errorf("seed proxy %q: %w", purl, err)
		}
	}

	if n, err := blockRepo.Prune(ctx, cfg.blockPrune); err != nil {
		fmt.Fprintln(out, "block-table prune warning:", err)
	} else if n > 0 {
		fmt.Fprintf(out, "pruned %d stale block rows\n", n)
	}

	proxies, err := proxyRepo.LoadActive(ctx)
	if err != nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("load proxies: %w", err)
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
	return endpoints, cache, cleanup, nil
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
