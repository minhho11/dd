package pool

import (
	"context"
	"strconv"
	"sync"
	"time"
)

// windowLimiter enforces an independent sliding-window request cap per key: for a
// given key, at most `limit` requests may start within any trailing `window`. When
// a key's allowance is spent, wait blocks until that key's oldest in-window request
// ages out, then admits and records the new one.
//
// The pool keys it per (proxy, URL) — see rateKey — so each proxy carries its own
// request count and sleep for each target URL, independently of the other proxies
// and the other URLs. limit and window are supplied per call (from the urls row),
// so different URLs can carry different caps.
type windowLimiter struct {
	mu   sync.Mutex
	hits map[string][]time.Time // request start times still inside the window, per key
}

// rateKey builds the per-(proxy, URL) limiter key: a given proxy's budget for a
// given URL is tracked separately from every other (proxy, URL) pair. ProxyID 0 is
// the direct client.
func rateKey(proxyID int64, url string) string {
	return strconv.FormatInt(proxyID, 10) + "\x00" + url
}

func newWindowLimiter() *windowLimiter {
	return &windowLimiter{hits: make(map[string][]time.Time)}
}

// wait blocks until key may make another request without exceeding limit within the
// trailing window, then records it. A non-positive limit or window means the key is
// unlimited and wait returns immediately. It returns ctx.Err() if ctx is cancelled
// while waiting. Safe for concurrent use by many workers sharing a key.
func (l *windowLimiter) wait(ctx context.Context, key string, limit int, window time.Duration) error {
	if limit <= 0 || window <= 0 {
		return nil
	}
	for {
		l.mu.Lock()
		now := time.Now()
		times := l.prune(key, window, now)
		if len(times) < limit {
			l.hits[key] = append(times, now)
			l.mu.Unlock()
			return nil
		}
		// Allowance spent: the earliest request must leave the window before another
		// may start. Sleep until then, release the lock meanwhile, and re-check (other
		// workers on this key may have consumed the freed slot first).
		sleep := window - now.Sub(times[0])
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

// prune drops timestamps older than the window for key and returns the remaining
// (in-window) slice. The caller holds l.mu. Timestamps are appended in time order,
// so the stale ones are always a prefix.
func (l *windowLimiter) prune(key string, window time.Duration, now time.Time) []time.Time {
	cutoff := now.Add(-window)
	times := l.hits[key]
	drop := 0
	for drop < len(times) && !times[drop].After(cutoff) {
		drop++
	}
	if drop == 0 {
		return times
	}
	if drop == len(times) {
		delete(l.hits, key) // nothing in the window; don't retain an empty slice
		return nil
	}
	times = times[drop:]
	l.hits[key] = times
	return times
}
