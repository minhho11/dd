package pool

import (
	"context"
	"sync"
	"time"
)

// proxyRateLimiter enforces a per-proxy sliding-window request cap: for a given
// proxy, at most `limit` requests may start within any trailing `window`. When a
// proxy has already spent its allowance, wait blocks until the oldest request in
// the window ages out (or ctx is cancelled), then admits and records the new one.
//
// This keeps each proxy within a third-party per-identity rate limit on its own
// (e.g. a Shopify/Recharge API token capped at N requests/minute) — it throttles
// each proxy to stay compliant, it does not defeat the limit. It is keyed by proxy
// ID (0 == the direct client), so a direct run is throttled too.
//
// Distinct from Options.RPS, which is a single global request/second cap shared by
// every proxy; this is a separate budget per proxy.
type proxyRateLimiter struct {
	limit  int
	window time.Duration

	mu   sync.Mutex
	hits map[int64][]time.Time // request start times still inside the window, per proxy
}

// newProxyRateLimiter returns a limiter capping each proxy to limit requests per
// window, or nil (unlimited) when either bound is non-positive.
func newProxyRateLimiter(limit int, window time.Duration) *proxyRateLimiter {
	if limit <= 0 || window <= 0 {
		return nil
	}
	return &proxyRateLimiter{
		limit:  limit,
		window: window,
		hits:   make(map[int64][]time.Time),
	}
}

// wait blocks until proxyID may make another request without exceeding limit
// within the trailing window, then records it. It returns ctx.Err() if ctx is
// cancelled while waiting. Safe for concurrent use by many workers sharing a proxy.
func (l *proxyRateLimiter) wait(ctx context.Context, proxyID int64) error {
	for {
		l.mu.Lock()
		now := time.Now()
		times := l.prune(proxyID, now)
		if len(times) < l.limit {
			l.hits[proxyID] = append(times, now)
			l.mu.Unlock()
			return nil
		}
		// Allowance spent: the earliest request must leave the window before another
		// may start. Sleep until then, release the lock meanwhile, and re-check (other
		// workers on this proxy may have consumed the freed slot first).
		sleep := l.window - now.Sub(times[0])
		l.mu.Unlock()

		if sleep <= 0 {
			continue
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// prune drops timestamps older than the window for proxyID and returns the
// remaining (in-window) slice. The caller holds l.mu. Timestamps are appended in
// time order, so the stale ones are always a prefix.
func (l *proxyRateLimiter) prune(proxyID int64, now time.Time) []time.Time {
	cutoff := now.Add(-l.window)
	times := l.hits[proxyID]
	drop := 0
	for drop < len(times) && !times[drop].After(cutoff) {
		drop++
	}
	if drop == 0 {
		return times
	}
	if drop == len(times) {
		delete(l.hits, proxyID) // nothing in the window; don't retain an empty slice
		return nil
	}
	times = times[drop:]
	l.hits[proxyID] = times
	return times
}
