// Package pool implements the fixed-size worker pool that fires HTTP requests
// concurrently, rotates proxies, retries through healthy ones, and aggregates
// the results.
package pool

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	"golang.org/x/time/rate"

	"github.com/minhho11/dd/internal/httpclient"
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

// Job is a single unit of work: one HTTP GET against URL.
type Job struct {
	URL string
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
	workers   int
	retries   int
	clients   *httpclient.Pool
	blocks    BlockStore    // optional; nil disables the domain-block feedback loop
	limiter   *rate.Limiter // optional; nil means unlimited
	health    *health       // proxy transport-failure circuit breaker
	cacheBust bool
	cbParam   string
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
	}
}

// Run consumes jobs until the channel is closed or ctx is cancelled and returns
// the aggregated Summary.
func (p *Pool) Run(ctx context.Context, jobs <-chan Job) *Summary {
	summary := &Summary{}
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.worker(ctx, jobs, summary)
		}()
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

		client := p.pick(ctx, domain, exclude)
		if client == nil {
			if attempts == 0 {
				return Result{URL: job.URL, Domain: domain, Err: ErrAllBlocked}
			}
			last.Attempts = attempts
			return last
		}
		exclude[client.ProxyID] = true

		last = p.fire(ctx, client, job, domain)
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

	start := time.Now()
	resp, err := client.RC.R().SetContext(ctx).Get(target)
	res.Latency = time.Since(start)
	res.Err = err
	if err == nil {
		res.StatusCode = resp.StatusCode()
		res.Blocked = looksBlocked(resp)
	}

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

// AnyUsable reports whether at least one proxy is currently usable for at least
// one of the given domains (not quarantined and not health-cooled-down). The
// "run until all proxies blocked" mode polls this to decide when to stop. It is
// read-only. Note: a pool with a direct client (no proxies) is always usable.
func (p *Pool) AnyUsable(ctx context.Context, domains []string) bool {
	for _, d := range domains {
		if p.pick(ctx, d, nil) != nil {
			return true
		}
	}
	return false
}

// pick chooses a random usable client for the domain: not in exclude, not cooled
// down by the health tracker, and not quarantined for the domain. rand.Perm gives
// a random visit order. Returns nil when nothing is usable. A direct client
// (ProxyID 0) bypasses the health/block checks.
func (p *Pool) pick(ctx context.Context, domain string, exclude map[int64]bool) *httpclient.Client {
	clients := p.clients.Clients()

	for _, idx := range rand.Perm(len(clients)) {
		c := clients[idx]
		if exclude[c.ProxyID] {
			continue
		}
		if c.Direct() {
			return c
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
	if p.blocks == nil || c.Direct() || res.Err != nil {
		return
	}
	if res.Blocked {
		_ = p.blocks.OnBlocked(ctx, c.ProxyID, domain)
		return
	}
	_ = p.blocks.OnSuccess(ctx, c.ProxyID, domain)
}

// retryable reports whether a result is worth retrying through another proxy.
func retryable(r Result) bool {
	return r.Err != nil || r.Blocked
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
	if body := resp.Body(); len(body) > 0 {
		b := strings.ToLower(string(body))
		if len(b) > 8192 { // only scan the head; challenge pages announce early
			b = b[:8192]
		}
		for _, m := range blockMarkers {
			if strings.Contains(b, m) {
				return true
			}
		}
	}
	if resp.RawResponse != nil && resp.RawResponse.Request != nil {
		finalURL := strings.ToLower(resp.RawResponse.Request.URL.String())
		for _, m := range urlMarkers {
			if strings.Contains(finalURL, m) {
				return true
			}
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
