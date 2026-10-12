package pool

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestWindowLimiterUnlimited(t *testing.T) {
	l := newWindowLimiter()
	ctx := context.Background()
	// limit<=0 or window<=0 means no cap: every call returns immediately.
	for i := 0; i < 100; i++ {
		if err := l.wait(ctx, "u", 0, time.Minute); err != nil {
			t.Fatalf("limit 0 should be unlimited: %v", err)
		}
		if err := l.wait(ctx, "u", 5, 0); err != nil {
			t.Fatalf("window 0 should be unlimited: %v", err)
		}
	}
}

// Within the window, the first `limit` requests are admitted immediately and the
// next one blocks until the oldest ages out.
func TestWindowLimiterBlocksWhenFull(t *testing.T) {
	l := newWindowLimiter()
	ctx := context.Background()
	const key = "https://shop-a.test/"

	start := time.Now()
	for i := 0; i < 3; i++ {
		if err := l.wait(ctx, key, 3, 100*time.Millisecond); err != nil {
			t.Fatalf("admit %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("first 3 requests should be immediate, took %s", elapsed)
	}

	before := time.Now()
	if err := l.wait(ctx, key, 3, 100*time.Millisecond); err != nil {
		t.Fatalf("admit 4th: %v", err)
	}
	if waited := time.Since(before); waited < 70*time.Millisecond {
		t.Fatalf("4th request should have blocked ~window, only waited %s", waited)
	}
}

// Each key (URL/store) has its own independent budget.
func TestWindowLimiterPerKey(t *testing.T) {
	l := newWindowLimiter()
	ctx := context.Background()

	if err := l.wait(ctx, "https://shop-a.test/", 1, time.Second); err != nil {
		t.Fatal(err)
	}
	// A different URL still has its full allowance — must not block.
	done := make(chan struct{})
	go func() { _ = l.wait(ctx, "https://shop-b.test/", 1, time.Second); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("shop-b blocked on shop-a's budget")
	}
}

// Different keys may carry different caps.
func TestWindowLimiterPerKeyDifferentLimits(t *testing.T) {
	l := newWindowLimiter()
	ctx := context.Background()

	// "a" allows 2/window, "b" allows 1/window.
	if err := l.wait(ctx, "a", 2, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := l.wait(ctx, "a", 2, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := l.wait(ctx, "b", 1, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// "a"'s 3rd and "b"'s 2nd both block; just confirm "a" is independent by timing
	// its 3rd against its own window.
	before := time.Now()
	if err := l.wait(ctx, "a", 2, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(before); waited < 70*time.Millisecond {
		t.Fatalf("a's 3rd should have blocked ~window, waited %s", waited)
	}
}

// A cancelled context unblocks a waiter instead of sleeping the whole window.
func TestWindowLimiterContextCancel(t *testing.T) {
	l := newWindowLimiter()
	if err := l.wait(context.Background(), "k", 1, time.Hour); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- l.wait(ctx, "k", 1, time.Hour) }()
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

// Concurrent workers sharing a key never exceed the limit within the window.
func TestWindowLimiterConcurrent(t *testing.T) {
	const limit = 5
	const window = 200 * time.Millisecond
	l := newWindowLimiter()
	ctx := context.Background()

	var mu sync.Mutex
	var admits []time.Time
	var wg sync.WaitGroup
	for i := 0; i < limit*3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.wait(ctx, "k", limit, window); err != nil {
				t.Errorf("wait: %v", err)
				return
			}
			mu.Lock()
			admits = append(admits, time.Now())
			mu.Unlock()
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	for i := range admits {
		count := 0
		for j := range admits {
			if d := admits[i].Sub(admits[j]); d >= 0 && d < window {
				count++
			}
		}
		if count > limit {
			t.Fatalf("found %d admissions within one window (limit %d)", count, limit)
		}
	}
}
