// Package pool implements the fixed-size worker pool that fires HTTP requests
// concurrently, rotates proxies, retries through healthy ones, and aggregates
// the results.
package pool

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	"golang.org/x/time/rate"

	"github.com/minhho11/dd/internal/browser"
	"github.com/minhho11/dd/internal/httpclient"
	"github.com/minhho11/dd/internal/tmpl"
)

// ErrAllBlocked is returned as a Result.Err when every proxy is unavailable
// (quarantined or unhealthy) for the job's domain, so the request was skipped.
var ErrAllBlocked = errors.New("no usable proxy for domain")

// blockedStatuses are HTTP status codes that mean "the target blocked this
// proxy" (as opposed to a transport/proxy failure).
var blockedStatuses = map[int]bool{403: true, 429: true, 503: true}

// blockMarkers are substrings that betray an anti-bot challenge / block page even
// when it is served with a 200. Matched case-insensitively against the body.
var blockMarkers = []string{
	"captcha",
	"are you human",
	"verify you are human",
	"access denied",
	"attention required",
	"just a moment", // Cloudflare interstitial
	"cf-chl",        // Cloudflare challenge assets
	"request blocked",
	"unusual traffic",
}

// urlMarkers are path fragments a block redirect commonly lands on.
var urlMarkers = []string{"/blocked", "/captcha", "/challenge"}

// BlockStore records and evaluates per-(proxy, domain) blocks. Satisfied by
// *proxy.BlockCache; the pool depends only on this narrow interface so it stays
// testable and DB-free when nil.
type BlockStore interface {
	Available(ctx context.Context, proxyID int64, domain string) (bool, error)
	OnBlocked(ctx context.Context, proxyID int64, domain string) error
	OnSuccess(ctx context.Context, proxyID int64, domain string) error
}

// Job is a single unit of work. Mode selects the executor:
//
//   - "" or "http":  one HTTP request against URL. Method defaults to GET; Params
//     is a JSON object sent as the query string for GET/HEAD/DELETE and as the
//     request body for POST/PUT/PATCH.
//   - "browser":     a scripted Chromium flow against URL. Params is a
//     {"steps":[...]} JSON flow (see internal/browser); Method is ignored.
type Job struct {
	URL    string
	Mode   string
	Method string
	Params string
}

// Result records the final outcome of one Job (after any retries).
type Result struct {
	URL        string
	Domain     string
	ProxyURL   string // "" for a direct request
	StatusCode int
	Blocked    bool // detected as a block (status or challenge page)
	Attempts   int  // HTTP requests made for this job (1 + retries)
	Latency    time.Duration
	Err        error

	// retry reports whether this attempt is worth retrying through another proxy.
	// Set by fire/fireBrowser; consumed by do via retryable. Unexported: it is an
	// internal control signal, not part of the reported result.
	retry bool
}

// Options configures a Pool.
type Options struct {
	Workers        int
	Retries        int           // extra attempts through other proxies on block/error
	RPS            float64       // global request rate cap; <=0 means unlimited
	ProxyFailLimit int           // consecutive transport failures before a proxy cools down
	ProxyCooldown  time.Duration // how long an unhealthy proxy is skipped
	CacheBust      bool          // append a unique query param to each request
	CacheBustParam string        // the param name (default "_")

	// Browser-mode options (mode='browser' jobs), forwarded to internal/browser.
	Headless       bool          // run Chromium headless
	BrowserTimeout time.Duration // per-step/navigation timeout for browser flows
	Insecure       bool          // ignore TLS cert errors in the browser
	UserAgent      string        // override the browser User-Agent when non-empty

	// BrowserDebug, when true and BrowserLog is set, logs each browser navigation/
	// step (ok/FAIL) during the run. Toggled from the config table (browser_debug).
	BrowserDebug bool
	BrowserLog   func(format string, args ...any)

	// BrowserMax caps how many browser (Chromium) flows run concurrently across all
	// workers; <=0 = unlimited. Each browser flow launches a Chromium, which is
	// CPU-heavy, so this bounds CPU independently of the worker count.
	BrowserMax int

	// BrowserReuse reuses one browser across many direct flow runs (cookies cleared
	// between runs) instead of launching a fresh Chromium per request — far cheaper
	// on CPU. Proxied browser jobs always use a fresh browser (proxy is a
	// browser-level setting).
	BrowserReuse bool
}

// Summary aggregates results after a run.
type Summary struct {
	Total    int
	Success  int // non-blocked responses with status < 400
	Blocked  int // detected blocks (status or challenge page)
	Non2xx   int // completed, not blocked, status >= 400
	Failed   int // transport errors
	Skipped  int // no usable proxy
	Retries  int // total extra attempts across all jobs
	Duration time.Duration
	byStatus map[int]int
	mu       sync.Mutex
}

// ByStatus returns a copy of the status-code histogram.
func (s *Summary) ByStatus() map[int]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int]int, len(s.byStatus))
	for k, v := range s.byStatus {
		out[k] = v
	}
	return out
}

func (s *Summary) record(r Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Total++
	if r.Attempts > 1 {
		s.Retries += r.Attempts - 1
	}
	if r.Err != nil {
		if errors.Is(r.Err, ErrAllBlocked) {
			s.Skipped++
		} else {
			s.Failed++
		}
		return
	}
	if s.byStatus == nil {
		s.byStatus = make(map[int]int)
	}
	s.byStatus[r.StatusCode]++
	switch {
	case r.Blocked:
		s.Blocked++
	case r.StatusCode >= 400:
		s.Non2xx++
	default:
		s.Success++
	}
}

// Pool runs jobs across a fixed number of workers using a shared client pool.
type Pool struct {
	workers      int
	retries      int
	clients      *httpclient.Pool
	blocks       BlockStore    // optional; nil disables the domain-block feedback loop
	limiter      *rate.Limiter // optional; nil means unlimited
	health       *health       // proxy transport-failure circuit breaker
	cacheBust    bool
	cbParam      string
	browser      browser.Options // browser-mode execution options
	browserDebug bool
	browserLog   func(format string, args ...any)
	browserSem   chan struct{} // bounds concurrent browser flows; nil = unlimited
	browserReuse bool
	runnerOnce   sync.Once
	runner       *browser.Runner // reusable direct-browser sessions; lazily created
	// OnResult, if set, is called for every final result (concurrently). It must
	// be safe for concurrent use.
	OnResult func(Result)
}

// New creates a worker pool. blocks may be nil to run without the proxy-block
// feedback loop.
func New(clients *httpclient.Pool, blocks BlockStore, opts Options) *Pool {
	if opts.Workers < 1 {
		opts.Workers = 1
	}
	var limiter *rate.Limiter
	if opts.RPS > 0 {
		burst := int(opts.RPS)
		if burst < 1 {
			burst = 1
		}
		limiter = rate.NewLimiter(rate.Limit(opts.RPS), burst)
	}
	return &Pool{
		workers:   opts.Workers,
		retries:   opts.Retries,
		clients:   clients,
		blocks:    blocks,
		limiter:   limiter,
		health:    newHealth(opts.ProxyFailLimit, opts.ProxyCooldown),
		cacheBust: opts.CacheBust,
		cbParam:   opts.CacheBustParam,
		browser: browser.Options{
			Headless:  opts.Headless,
			Timeout:   opts.BrowserTimeout,
			Insecure:  opts.Insecure,
			UserAgent: opts.UserAgent,
		},
		browserDebug: opts.BrowserDebug,
		browserLog:   opts.BrowserLog,
		browserSem:   newSem(opts.BrowserMax),
		browserReuse: opts.BrowserReuse,
	}
}

// newSem returns a buffered channel of size n used as a counting semaphore, or nil
// when n<=0 (unlimited).
func newSem(n int) chan struct{} {
	if n <= 0 {
		return nil
	}
	return make(chan struct{}, n)
}

// Run consumes jobs until the channel is closed or ctx is cancelled and returns
// the aggregated Summary. It runs a single group of p.workers workers over one
// job channel; RunGroups is the multi-target (dedicated-split) form.
func (p *Pool) Run(ctx context.Context, jobs <-chan Job) *Summary {
	return p.RunGroups(ctx, []Group{{Workers: p.workers, Jobs: jobs}})
}

// Group is a dedicated set of workers bound to one job channel. Each group's
// workers only process that group's jobs, so weighting the worker count per
// target partitions the pool across targets (dedicated split).
type Group struct {
	Workers int
	Jobs    <-chan Job
}

// RunGroups starts every group's workers over its own job channel, all sharing
// the same client pool and one aggregated Summary. It returns when every job
// channel is drained/closed or ctx is cancelled.
func (p *Pool) RunGroups(ctx context.Context, groups []Group) *Summary {
	summary := &Summary{}
	start := time.Now()

	var wg sync.WaitGroup
	for _, g := range groups {
		n := g.Workers
		if n < 1 {
			n = 1
		}
		jobs := g.Jobs
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p.worker(ctx, jobs, summary)
			}()
		}
	}
	wg.Wait()

	summary.Duration = time.Since(start)
	return summary
}

func (p *Pool) worker(ctx context.Context, jobs <-chan Job, summary *Summary) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			res := p.do(ctx, job)
			summary.record(res)
			if p.OnResult != nil {
				p.OnResult(res)
			}
		}
	}
}

// do runs one job, retrying through a different proxy when a request is blocked
// or fails at the transport layer, up to Retries extra attempts.
func (p *Pool) do(ctx context.Context, job Job) Result {
	domain := domainOf(job.URL)
	exclude := map[int64]bool{}
	var last Result
	attempts := 0

	for {
		select {
		case <-ctx.Done():
			if attempts == 0 {
				return Result{URL: job.URL, Domain: domain, Err: ctx.Err()}
			}
			last.Attempts = attempts
			return last
		default:
		}

		client := p.pick(ctx, domain, exclude, isBrowser(job.Mode))
		if client == nil {
			// A browser job can still run without a proxy (e.g. only SOCKS proxies
			// exist, which Chromium can't use) — fall back to a direct run, once.
			if isBrowser(job.Mode) && !exclude[0] {
				client = &httpclient.Client{} // synthetic direct client (ProxyID 0)
			} else if attempts == 0 {
				return Result{URL: job.URL, Domain: domain, Err: ErrAllBlocked}
			} else {
				last.Attempts = attempts
				return last
			}
		}
		exclude[client.ProxyID] = true

		if isBrowser(job.Mode) {
			last = p.fireBrowser(ctx, client, job, domain)
		} else {
			last = p.fire(ctx, client, job, domain)
		}
		attempts++

		if !retryable(last) || attempts > p.retries {
			last.Attempts = attempts
			return last
		}
	}
}

// fire sends a single request through the given client and feeds the outcome back
// into the health and block stores.
func (p *Pool) fire(ctx context.Context, client *httpclient.Client, job Job, domain string) Result {
	res := Result{URL: job.URL, Domain: domain, ProxyURL: client.ProxyURL}

	if p.limiter != nil {
		if err := p.limiter.Wait(ctx); err != nil {
			res.Err = err
			return res
		}
	}

	// Cache busting: a unique query param per request so a caching layer forwards
	// each one to origin instead of serving one cached copy. Result.URL keeps the
	// clean URL so the summary/export group logically.
	target := job.URL
	if p.cacheBust {
		target = addCacheBuster(job.URL, p.cbParam)
	}

	// Method + params: POST/PUT/PATCH send the JSON params as the body; the rest
	// merge them into the query string.
	method := normalizeMethod(job.Method)
	req := client.RC.R().SetContext(ctx)
	if job.Params != "" {
		// Expand {{...}} generators fresh for this request so each one gets unique
		// data (random email/string/number/uuid/…).
		params := tmpl.Expand(job.Params)
		if bodyMethods[method] {
			req.SetHeader("Content-Type", "application/json").SetBody(params)
		} else if q, qerr := jsonToQuery(params); qerr == nil {
			for k, v := range q {
				req.SetQueryParam(k, v)
			}
		}
	}

	start := time.Now()
	resp, err := req.Execute(method, target)
	res.Latency = time.Since(start)
	res.Err = err
	if err == nil {
		res.StatusCode = resp.StatusCode()
		res.Blocked = looksBlocked(resp)
	}
	res.retry = res.Err != nil || res.Blocked

	// Transport health: only a transport error marks a proxy as failing; any HTTP
	// response (even a block) proves the proxy is alive.
	if !client.Direct() {
		if err != nil {
			p.health.onFail(client.ProxyID)
		} else {
			p.health.onOK(client.ProxyID)
		}
	}
	p.updateBlocks(ctx, client, domain, res)
	return res
}

// isBrowser reports whether a job runs through the browser (Chromium) executor.
func isBrowser(mode string) bool {
	return strings.EqualFold(strings.TrimSpace(mode), "browser")
}

// isSOCKS reports whether a proxy URL uses a SOCKS scheme (socks/socks4/socks5),
// which Chromium cannot authenticate — skipped for browser jobs.
func isSOCKS(proxyURL string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(proxyURL)), "socks")
}

// browserRunner lazily creates the reusable-browser runner, parented to ctx (the
// run context) so its browsers are torn down when the run ends. CloseBrowser also
// tears them down explicitly after the workers stop.
func (p *Pool) browserRunner(ctx context.Context) *browser.Runner {
	p.runnerOnce.Do(func() {
		p.runner = browser.NewRunner(ctx, p.browser)
	})
	return p.runner
}

// CloseBrowser tears down any reused browser sessions. Call it after the run's
// workers have stopped (RunGroups returned).
func (p *Pool) CloseBrowser() {
	if p.runner != nil {
		p.runner.Close()
	}
}

// fireBrowser runs a browser-mode job: it executes the flow (params) against the
// target through the picked proxy in a headless Chromium, then classifies and
// feeds the outcome back into health and the block store like fire does. A blocked
// or connection-level failure is retryable through another proxy; a page/assertion
// failure is not.
func (p *Pool) fireBrowser(ctx context.Context, client *httpclient.Client, job Job, domain string) Result {
	res := Result{URL: job.URL, Domain: domain, ProxyURL: client.ProxyURL}

	if p.limiter != nil {
		if err := p.limiter.Wait(ctx); err != nil {
			res.Err = err
			return res
		}
	}

	flow, err := browser.ParseFlow(job.Params)
	if err != nil {
		res.Err = err // a malformed flow is a config error, not worth retrying
		return res
	}

	// Cap concurrent Chromium instances (each is CPU-heavy) independently of the
	// worker count. Workers over the cap block here until a slot frees up.
	if p.browserSem != nil {
		select {
		case <-ctx.Done():
			res.Err = ctx.Err()
			return res
		case p.browserSem <- struct{}{}:
			defer func() { <-p.browserSem }()
		}
	}

	var logf func(string, ...any)
	if p.browserDebug && p.browserLog != nil {
		via := "direct"
		if client.ProxyURL != "" {
			via = client.ProxyURL
		}
		prefix := fmt.Sprintf("[browser %s via %s]", job.URL, via)
		logf = func(f string, a ...any) { p.browserLog(prefix+" "+f, a...) }
	}

	start := time.Now()
	var oc browser.Outcome
	if client.Direct() && p.browserReuse {
		// Reuse one browser across direct runs (cookies cleared each run) instead of
		// launching a fresh Chromium per request.
		oc = p.browserRunner(ctx).Run(job.URL, flow, logf)
	} else {
		opts := p.browser
		opts.Logf = logf
		oc = browser.Execute(ctx, job.URL, flow, client.ProxyURL, opts)
	}
	res.Latency = time.Since(start)
	res.StatusCode = oc.Status
	res.Err = oc.Err
	res.Blocked = blockedStatuses[oc.Status] || looksBlockedText(oc.BodyText) || looksBlockedURL(oc.FinalURL)
	res.retry = res.Blocked || oc.Transport

	// Transport health: only a connection-level failure marks the proxy as failing;
	// a completed flow (even a blocked page or a failed assertion) proves it is alive.
	if !client.Direct() {
		if oc.Transport {
			p.health.onFail(client.ProxyID)
		} else {
			p.health.onOK(client.ProxyID)
		}
	}
	p.updateBlocks(ctx, client, domain, res)
	return res
}

// AnyUsable reports whether at least one proxy is currently usable for at least
// one of the given domains (not quarantined and not health-cooled-down). The
// "run until all proxies blocked" mode polls this to decide when to stop. It is
// read-only. Note: a pool with a direct client (no proxies) is always usable.
func (p *Pool) AnyUsable(ctx context.Context, domains []string, browserMode bool) bool {
	for _, d := range domains {
		if p.pick(ctx, domainOf(d), nil, browserMode) != nil {
			return true
		}
	}
	return false
}

// pick chooses a random usable client for the domain: not in exclude, not cooled
// down by the health tracker, and not quarantined for the domain. rand.Perm gives
// a random visit order. Returns nil when nothing is usable. A direct client
// (ProxyID 0) bypasses the health/block checks. For browserMode jobs, SOCKS
// proxies are skipped — Chromium cannot authenticate SOCKS5 proxies, so routing a
// browser flow through one always fails.
func (p *Pool) pick(ctx context.Context, domain string, exclude map[int64]bool, browserMode bool) *httpclient.Client {
	clients := p.clients.Clients()

	for _, idx := range rand.Perm(len(clients)) {
		c := clients[idx]
		if exclude[c.ProxyID] {
			continue
		}
		if c.Direct() {
			return c
		}
		if browserMode && isSOCKS(c.ProxyURL) {
			continue
		}
		if !p.health.usable(c.ProxyID) {
			continue
		}
		if p.blocks != nil {
			ok, err := p.blocks.Available(ctx, c.ProxyID, domain)
			if err == nil && !ok {
				continue
			}
		}
		return c
	}
	return nil
}

// updateBlocks feeds the request outcome into the BlockStore: a detected block
// increments the proxy's counter for the domain; any other HTTP response clears
// it (the proxy reached the origin). Transport errors are left to the health
// tracker.
func (p *Pool) updateBlocks(ctx context.Context, c *httpclient.Client, domain string, res Result) {
	if p.blocks == nil || c.Direct() {
		return
	}
	if res.Blocked {
		_ = p.blocks.OnBlocked(ctx, c.ProxyID, domain)
		return
	}
	if res.Err != nil {
		return // transport/logic failure with no block signal: leave to the health tracker
	}
	_ = p.blocks.OnSuccess(ctx, c.ProxyID, domain)
}

// retryable reports whether a result is worth retrying through another proxy.
func retryable(r Result) bool {
	return r.retry
}

// looksBlocked classifies a response as a block by status code, body challenge
// markers, or a block-page redirect target.
func looksBlocked(resp *resty.Response) bool {
	if resp == nil {
		return false
	}
	if blockedStatuses[resp.StatusCode()] {
		return true
	}
	if looksBlockedText(string(resp.Body())) {
		return true
	}
	if resp.RawResponse != nil && resp.RawResponse.Request != nil {
		return looksBlockedURL(resp.RawResponse.Request.URL.String())
	}
	return false
}

// looksBlockedText reports whether a page body contains an anti-bot challenge /
// block marker. Only the head is scanned (challenge pages announce early). Shared
// by the HTTP and browser paths.
func looksBlockedText(body string) bool {
	if body == "" {
		return false
	}
	b := strings.ToLower(body)
	if len(b) > 8192 {
		b = b[:8192]
	}
	for _, m := range blockMarkers {
		if strings.Contains(b, m) {
			return true
		}
	}
	return false
}

// looksBlockedURL reports whether a (final) URL landed on a common block/challenge
// path.
func looksBlockedURL(finalURL string) bool {
	if finalURL == "" {
		return false
	}
	u := strings.ToLower(finalURL)
	for _, m := range urlMarkers {
		if strings.Contains(u, m) {
			return true
		}
	}
	return false
}

// domainOf extracts the host from a URL, falling back to the raw string.
func domainOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return rawURL
	}
	return u.Hostname()
}
