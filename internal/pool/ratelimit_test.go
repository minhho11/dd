package pool

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestProxyRateLimiterDisabled(t *testing.T) {
	if newProxyRateLimiter(0, time.Minute) != nil {
		t.Fatal("limit 0 should disable (nil) the limiter")
	}
	if newProxyRateLimiter(5, 0) != nil {
		t.Fatal("window 0 should disable (nil) the limiter")
	}
}

// Within the window, the first `limit` requests are admitted immediately and the
// next one blocks until the oldest ages out.
func TestProxyRateLimiterBlocksWhenFull(t *testing.T) {
	l := newProxyRateLimiter(3, 100*time.Millisecond)
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 3; i++ {
		if err := l.wait(ctx, 7); err != nil {
			t.Fatalf("admit %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("first %d requests should be immediate, took %s", 3, elapsed)
	}

	// 4th must wait ~the window (until the first timestamp falls out).
	before := time.Now()
	if err := l.wait(ctx, 7); err != nil {
		t.Fatalf("admit 4th: %v", err)
	}
	if waited := time.Since(before); waited < 70*time.Millisecond {
		t.Fatalf("4th request should have blocked ~window, only waited %s", waited)
	}
}

// Each proxy has its own independent budget.
func TestProxyRateLimiterPerProxy(t *testing.T) {
	l := newProxyRateLimiter(1, time.Second)
	ctx := context.Background()

	if err := l.wait(ctx, 1); err != nil {
		t.Fatal(err)
	}
	// A different proxy still has its full allowance — must not block.
	done := make(chan struct{})
	go func() { _ = l.wait(ctx, 2); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("proxy 2 blocked on proxy 1's budget")
	}
}

// A cancelled context unblocks a waiter instead of sleeping the whole window.
func TestProxyRateLimiterContextCancel(t *testing.T) {
	l := newProxyRateLimiter(1, time.Hour)
	if err := l.wait(context.Background(), 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- l.wait(ctx, 1) }()
	cancel()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("expected context error, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after context cancel")
	}
}

// Concurrent workers sharing a proxy never exceed the limit within the window.
func TestProxyRateLimiterConcurrent(t *testing.T) {
	const limit = 5
	l := newProxyRateLimiter(limit, 200*time.Millisecond)
	ctx := context.Background()

	var mu sync.Mutex
	var admits []time.Time
	var wg sync.WaitGroup
	for i := 0; i < limit*3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.wait(ctx, 9); err != nil {
				t.Errorf("wait: %v", err)
				return
			}
			mu.Lock()
			admits = append(admits, time.Now())
			mu.Unlock()
		}()
	}
	wg.Wait()

	// No window of `window` width may contain more than `limit` admissions.
	mu.Lock()
	defer mu.Unlock()
	for i := range admits {
		count := 0
		for j := range admits {
			if d := admits[i].Sub(admits[j]); d >= 0 && d < 200*time.Millisecond {
				count++
			}
		}
		if count > limit {
			t.Fatalf("found %d admissions within one window (limit %d)", count, limit)
		}
	}
}
