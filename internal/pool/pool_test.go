package pool

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minhho11/dd/internal/httpclient"
)

func TestPoolRun(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	clients := httpclient.New(httpclient.DefaultConfig(), nil)
	p := New(clients, nil, Options{Workers: 4})

	const n = 50
	jobs := make(chan Job, n)
	for i := 0; i < n; i++ {
		jobs <- Job{URL: srv.URL}
	}
	close(jobs)

	summary := p.Run(context.Background(), jobs)

	if summary.Total != n {
		t.Errorf("Total = %d, want %d", summary.Total, n)
	}
	if summary.Success != n {
		t.Errorf("Success = %d, want %d", summary.Success, n)
	}
	if got := hits.Load(); got != n {
		t.Errorf("server hits = %d, want %d", got, n)
	}
}

func TestPoolRunNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	p := New(httpclient.New(httpclient.DefaultConfig(), nil), nil, Options{Workers: 2})

	jobs := make(chan Job, 3)
	for i := 0; i < 3; i++ {
		jobs <- Job{URL: srv.URL}
	}
	close(jobs)

	summary := p.Run(context.Background(), jobs)
	if summary.Non2xx != 3 {
		t.Errorf("Non2xx = %d, want 3", summary.Non2xx)
	}
	if summary.ByStatus()[404] != 3 {
		t.Errorf("ByStatus[404] = %d, want 3", summary.ByStatus()[404])
	}
}

func TestDomainOf(t *testing.T) {
	cases := map[string]string{
		"https://aaa.com/path?q=1": "aaa.com",
		"http://xxx.vn:8080/":      "xxx.vn",
		"not a url":                "not a url",
	}
	for in, want := range cases {
		if got := domainOf(in); got != want {
			t.Errorf("domainOf(%q) = %q, want %q", in, got, want)
		}
	}
}

// fakeBlocks is an in-memory BlockStore for exercising selection without a DB.
type fakeBlocks struct {
	mu        sync.Mutex
	blocked   map[string]bool // key: proxyID|domain -> quarantined
	onBlocked int
	onSuccess int
}

func key(id int64, domain string) string { return fmt.Sprintf("%d|%s", id, domain) }

func (f *fakeBlocks) Available(_ context.Context, id int64, domain string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.blocked[key(id, domain)], nil
}

func (f *fakeBlocks) OnBlocked(_ context.Context, id int64, domain string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onBlocked++
	f.blocked[key(id, domain)] = true
	return nil
}

var _ BlockStore = (*fakeBlocks)(nil)

func (f *fakeBlocks) OnSuccess(_ context.Context, id int64, domain string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onSuccess++
	delete(f.blocked, key(id, domain))
	return nil
}

// TestPickSkipsBlockedProxy verifies pick() never returns a quarantined proxy
// and returns nil only when all proxies are blocked for the domain.
func TestPickSkipsBlockedProxy(t *testing.T) {
	clients := httpclient.New(httpclient.DefaultConfig(), []httpclient.ProxyEndpoint{
		{ID: 1, URL: "http://127.0.0.1:1"},
		{ID: 2, URL: "http://127.0.0.1:2"},
	})
	fb := &fakeBlocks{blocked: map[string]bool{}}
	p := New(clients, fb, Options{Workers: 1})

	const domain = "a.com"
	fb.blocked[key(1, domain)] = true // quarantine proxy 1

	// Every pick must avoid proxy 1.
	for i := 0; i < 10; i++ {
		c := p.pick(context.Background(), domain, nil)
		if c == nil {
			t.Fatalf("pick returned nil while proxy 2 is available")
		}
		if c.ProxyID == 1 {
			t.Fatalf("pick returned quarantined proxy 1")
		}
	}

	// Block the second proxy too: no proxy left -> nil.
	fb.blocked[key(2, domain)] = true
	if c := p.pick(context.Background(), domain, nil); c != nil {
		t.Fatalf("pick = proxy %d, want nil when all blocked", c.ProxyID)
	}
}

// TestPickIsRandom checks that selection spreads across the whole pool rather
// than following a fixed order: over many picks every proxy should appear.
func TestPickIsRandom(t *testing.T) {
	var eps []httpclient.ProxyEndpoint
	for id := int64(1); id <= 5; id++ {
		eps = append(eps, httpclient.ProxyEndpoint{ID: id, URL: fmt.Sprintf("http://127.0.0.1:%d", id)})
	}
	p := New(httpclient.New(httpclient.DefaultConfig(), eps), nil, Options{Workers: 1})

	seen := map[int64]int{}
	for i := 0; i < 500; i++ {
		c := p.pick(context.Background(), "a.com", nil)
		seen[c.ProxyID]++
	}
	if len(seen) != 5 {
		t.Fatalf("expected all 5 proxies to be picked, saw %d distinct: %v", len(seen), seen)
	}
	// No proxy should dominate: with 500 draws over 5, each ~100. Guard against a
	// fixed-order regression where one is picked far more than the rest.
	for id, n := range seen {
		if n < 40 {
			t.Errorf("proxy %d picked only %d/500 times; distribution looks non-random: %v", id, n, seen)
		}
	}
}

// TestRetryThroughProxies verifies a blocked request is retried through other
// proxies (excluding already-tried ones) up to Retries extra attempts. All three
// proxies route to a 403 server, so the job exhausts its 1+2 attempts.
func TestRetryThroughProxies(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	eps := []httpclient.ProxyEndpoint{
		{ID: 1, URL: srv.URL},
		{ID: 2, URL: srv.URL},
		{ID: 3, URL: srv.URL},
	}
	fb := &fakeBlocks{blocked: map[string]bool{}}
	p := New(httpclient.New(httpclient.DefaultConfig(), eps), fb, Options{Workers: 1, Retries: 2})

	res := p.do(context.Background(), Job{URL: "http://blocked.test/"})

	if res.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3 (1 + 2 retries)", res.Attempts)
	}
	if !res.Blocked || res.StatusCode != 403 {
		t.Errorf("want blocked 403, got blocked=%v status=%d", res.Blocked, res.StatusCode)
	}
	if fb.onBlocked != 3 {
		t.Errorf("onBlocked = %d, want 3 (one per attempt)", fb.onBlocked)
	}
}

// TestRetryRecoversOnGoodProxy verifies retry stops as soon as a proxy succeeds.
func TestRetryRecoversOnGoodProxy(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer good.Close()

	// Two proxies: one always blocks, one always succeeds. With retries the job
	// must end 200 regardless of which is picked first.
	eps := []httpclient.ProxyEndpoint{{ID: 1, URL: bad.URL}, {ID: 2, URL: good.URL}}
	fb := &fakeBlocks{blocked: map[string]bool{}}
	p := New(httpclient.New(httpclient.DefaultConfig(), eps), fb, Options{Workers: 1, Retries: 3})

	for i := 0; i < 20; i++ {
		res := p.do(context.Background(), Job{URL: "http://target.test/"})
		if res.StatusCode != 200 || res.Blocked {
			t.Fatalf("iter %d: want final 200, got status=%d blocked=%v attempts=%d", i, res.StatusCode, res.Blocked, res.Attempts)
		}
	}
}

func TestHealthCooldown(t *testing.T) {
	h := newHealth(3, 50*time.Millisecond)
	const id = int64(9)

	if !h.usable(id) {
		t.Fatal("fresh proxy should be usable")
	}
	h.onFail(id)
	h.onFail(id)
	if !h.usable(id) {
		t.Fatal("2 failures (< limit 3) should not cool down")
	}
	h.onFail(id) // 3rd failure trips cooldown
	if h.usable(id) {
		t.Fatal("proxy should be cooling down after hitting the limit")
	}
	time.Sleep(70 * time.Millisecond)
	if !h.usable(id) {
		t.Fatal("proxy should be usable again after cooldown")
	}

	// A success mid-way resets the counter.
	h.onFail(id)
	h.onFail(id)
	h.onOK(id)
	h.onFail(id)
	h.onFail(id)
	if !h.usable(id) {
		t.Fatal("counter should have reset after onOK; not yet at limit")
	}
}

func TestLooksBlocked(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"ok", 200, "<html>welcome</html>", false},
		{"forbidden", 403, "nope", true},
		{"rate limited", 429, "slow down", true},
		{"challenge 200", 200, "<title>Just a moment...</title>", true},
		{"captcha 200", 200, "Please complete the CAPTCHA to continue", true},
		{"not found", 404, "missing", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			resp, err := httpclient.New(httpclient.DefaultConfig(), nil).
				Clients()[0].RC.R().Get(srv.URL)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if got := looksBlocked(resp); got != tc.want {
				t.Errorf("looksBlocked = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestAnyUsable drives the stop condition of "until all proxies blocked" mode.
func TestAnyUsable(t *testing.T) {
	clients := httpclient.New(httpclient.DefaultConfig(), []httpclient.ProxyEndpoint{
		{ID: 1, URL: "http://127.0.0.1:1"},
		{ID: 2, URL: "http://127.0.0.1:2"},
	})
	fb := &fakeBlocks{blocked: map[string]bool{}}
	p := New(clients, fb, Options{Workers: 1})
	ctx := context.Background()
	domains := []string{"a.com", "b.com"}

	if !p.AnyUsable(ctx, domains) {
		t.Fatal("fresh pool should have usable proxies")
	}

	// Block everything for a.com; b.com still open -> still usable.
	fb.blocked[key(1, "a.com")] = true
	fb.blocked[key(2, "a.com")] = true
	if !p.AnyUsable(ctx, domains) {
		t.Fatal("b.com still has usable proxies")
	}

	// Block everything for b.com too -> nothing usable anywhere.
	fb.blocked[key(1, "b.com")] = true
	fb.blocked[key(2, "b.com")] = true
	if p.AnyUsable(ctx, domains) {
		t.Fatal("all proxies blocked for all domains; AnyUsable should be false")
	}
}

// TestUpdateBlocksClassification checks block/success feedback on status codes.
func TestUpdateBlocksClassification(t *testing.T) {
	clients := httpclient.New(httpclient.DefaultConfig(), []httpclient.ProxyEndpoint{
		{ID: 7, URL: "http://127.0.0.1:1"},
	})
	fb := &fakeBlocks{blocked: map[string]bool{}}
	p := New(clients, fb, Options{Workers: 1})
	c := clients.Clients()[0]

	p.updateBlocks(context.Background(), c, "a.com", Result{StatusCode: 429, Blocked: true})
	if fb.onBlocked != 1 {
		t.Errorf("blocked result should record a block, onBlocked=%d", fb.onBlocked)
	}
	p.updateBlocks(context.Background(), c, "a.com", Result{StatusCode: 200})
	if fb.onSuccess != 1 {
		t.Errorf("200 should record a success, onSuccess=%d", fb.onSuccess)
	}
	// A transport error is neither a block nor a success.
	p.updateBlocks(context.Background(), c, "a.com", Result{Err: context.DeadlineExceeded})
	if fb.onBlocked != 1 || fb.onSuccess != 1 {
		t.Errorf("transport error must not change counters: b=%d s=%d", fb.onBlocked, fb.onSuccess)
	}
}
