package proxy_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/minhho11/dd/internal/proxy"
)

// fakeSource is an in-memory BlockSource that counts LoadDomain calls so tests
// can assert cache hits/misses without a database.
type fakeSource struct {
	mu           sync.Mutex
	rows         map[string][]proxy.ProxyBlock // domain -> active blocks
	loads        map[string]int                // domain -> LoadDomain call count
	successCalls int                           // OnSuccess DB writes actually issued
}

func (f *fakeSource) successCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.successCalls
}

func newFakeSource() *fakeSource {
	return &fakeSource{rows: map[string][]proxy.ProxyBlock{}, loads: map[string]int{}}
}

func (f *fakeSource) LoadDomain(_ context.Context, domain string) ([]proxy.ProxyBlock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads[domain]++
	return append([]proxy.ProxyBlock(nil), f.rows[domain]...), nil
}

func (f *fakeSource) OnBlocked(_ context.Context, proxyID int64, domain string) (time.Time, error) {
	until := time.Now().Add(30 * time.Minute)
	f.mu.Lock()
	f.rows[domain] = append(f.rows[domain], proxy.ProxyBlock{ProxyID: proxyID, Domain: domain, BlockedUntil: until})
	f.mu.Unlock()
	return until, nil
}

func (f *fakeSource) OnSuccess(_ context.Context, proxyID int64, domain string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.successCalls++
	kept := f.rows[domain][:0]
	for _, r := range f.rows[domain] {
		if r.ProxyID != proxyID {
			kept = append(kept, r)
		}
	}
	f.rows[domain] = kept
	return nil
}

func (f *fakeSource) loadCount(domain string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loads[domain]
}

var _ proxy.BlockSource = (*fakeSource)(nil)

func TestBlockCacheServesFromMemory(t *testing.T) {
	src := newFakeSource()
	src.rows["a.com"] = []proxy.ProxyBlock{{ProxyID: 1, Domain: "a.com", BlockedUntil: time.Now().Add(time.Hour)}}
	c := proxy.NewBlockCache(src, time.Minute, 10)
	ctx := context.Background()

	// Proxy 1 blocked, proxy 2 free.
	if ok, _ := c.Available(ctx, 1, "a.com"); ok {
		t.Error("proxy 1 should be unavailable for a.com")
	}
	if ok, _ := c.Available(ctx, 2, "a.com"); !ok {
		t.Error("proxy 2 should be available for a.com")
	}
	// Many further reads within the TTL must not hit the source again.
	for i := 0; i < 100; i++ {
		_, _ = c.Available(ctx, int64(i), "a.com")
	}
	if n := src.loadCount("a.com"); n != 1 {
		t.Errorf("LoadDomain called %d times, want 1 (TTL caching)", n)
	}
}

func TestBlockCacheTTLReload(t *testing.T) {
	src := newFakeSource()
	c := proxy.NewBlockCache(src, 30*time.Millisecond, 10)
	ctx := context.Background()

	_, _ = c.Available(ctx, 1, "a.com") // load 1
	_, _ = c.Available(ctx, 1, "a.com") // cached
	if n := src.loadCount("a.com"); n != 1 {
		t.Fatalf("loads=%d, want 1 before TTL", n)
	}
	time.Sleep(45 * time.Millisecond)
	_, _ = c.Available(ctx, 1, "a.com") // stale -> reload
	if n := src.loadCount("a.com"); n != 2 {
		t.Errorf("loads=%d, want 2 after TTL", n)
	}
}

func TestBlockCacheWriteThrough(t *testing.T) {
	src := newFakeSource()
	c := proxy.NewBlockCache(src, time.Minute, 10)
	ctx := context.Background()

	// Warm the domain (empty), then block proxy 5 via the cache.
	if ok, _ := c.Available(ctx, 5, "a.com"); !ok {
		t.Fatal("proxy 5 should start available")
	}
	if err := c.OnBlocked(ctx, 5, "a.com"); err != nil {
		t.Fatalf("OnBlocked: %v", err)
	}
	// Immediately reflected without waiting for a reload.
	if ok, _ := c.Available(ctx, 5, "a.com"); ok {
		t.Error("proxy 5 should be unavailable right after OnBlocked")
	}
	// Success clears it.
	if err := c.OnSuccess(ctx, 5, "a.com"); err != nil {
		t.Fatalf("OnSuccess: %v", err)
	}
	if ok, _ := c.Available(ctx, 5, "a.com"); !ok {
		t.Error("proxy 5 should be available again after OnSuccess")
	}
}

// TestBlockCacheOnSuccessSkipsNoop verifies that a success for a proxy with no
// block row on a cached domain issues no DB write (the per-request hot path), while
// a success that actually clears a block does write.
func TestBlockCacheOnSuccessSkipsNoop(t *testing.T) {
	src := newFakeSource()
	c := proxy.NewBlockCache(src, time.Minute, 10)
	ctx := context.Background()

	// Warm the domain (empty). Now repeated successes for an unblocked proxy must
	// not touch the source.
	if ok, _ := c.Available(ctx, 7, "a.com"); !ok {
		t.Fatal("proxy 7 should start available")
	}
	for i := 0; i < 100; i++ {
		if err := c.OnSuccess(ctx, 7, "a.com"); err != nil {
			t.Fatalf("OnSuccess: %v", err)
		}
	}
	if n := src.successCount(); n != 0 {
		t.Errorf("no-op successes issued %d DB writes, want 0", n)
	}

	// A success that clears a real block must write exactly once, then go quiet.
	if err := c.OnBlocked(ctx, 7, "a.com"); err != nil {
		t.Fatalf("OnBlocked: %v", err)
	}
	if err := c.OnSuccess(ctx, 7, "a.com"); err != nil {
		t.Fatalf("OnSuccess: %v", err)
	}
	if n := src.successCount(); n != 1 {
		t.Errorf("clearing success issued %d DB writes, want 1", n)
	}
	for i := 0; i < 10; i++ {
		_ = c.OnSuccess(ctx, 7, "a.com")
	}
	if n := src.successCount(); n != 1 {
		t.Errorf("after clearing, further no-op successes issued writes: got %d, want 1", n)
	}

	// A domain that was never cached falls through to a write (correctness over the
	// optimization when we can't prove there's nothing to clear).
	if err := c.OnSuccess(ctx, 9, "uncached.com"); err != nil {
		t.Fatalf("OnSuccess uncached: %v", err)
	}
	if n := src.successCount(); n != 2 {
		t.Errorf("uncached-domain success should write through: got %d, want 2", n)
	}
}

func TestBlockCacheLRUEviction(t *testing.T) {
	src := newFakeSource()
	const max = 3
	c := proxy.NewBlockCache(src, time.Hour, max) // long TTL so only eviction forces reloads
	ctx := context.Background()

	// Touch 5 distinct domains; only the last `max` may remain cached.
	for i := 0; i < 5; i++ {
		_, _ = c.Available(ctx, 1, fmt.Sprintf("d%d.com", i))
	}
	// d0 and d1 were evicted -> re-reading them reloads (miss).
	_, _ = c.Available(ctx, 1, "d0.com")
	if n := src.loadCount("d0.com"); n != 2 {
		t.Errorf("d0.com loads=%d, want 2 (evicted then reloaded)", n)
	}
	// d4 (most recent) stays cached -> no extra load.
	_, _ = c.Available(ctx, 1, "d4.com")
	if n := src.loadCount("d4.com"); n != 1 {
		t.Errorf("d4.com loads=%d, want 1 (still cached)", n)
	}
}
