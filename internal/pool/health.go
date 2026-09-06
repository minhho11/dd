package pool

import (
	"sync"
	"time"
)

// health is an in-memory circuit breaker per proxy. Consecutive transport
// failures (dead/unreachable proxy) trip a cooldown during which the proxy is
// skipped; any successful transport resets it. This is proxy-wide and separate
// from the per-domain block store.
type health struct {
	mu       sync.Mutex
	fails    map[int64]int
	until    map[int64]time.Time
	limit    int
	cooldown time.Duration
}

// newHealth builds a tracker. limit<1 falls back to 3, cooldown<=0 to 1m.
func newHealth(limit int, cooldown time.Duration) *health {
	if limit < 1 {
		limit = 3
	}
	if cooldown <= 0 {
		cooldown = time.Minute
	}
	return &health{
		fails:    map[int64]int{},
		until:    map[int64]time.Time{},
		limit:    limit,
		cooldown: cooldown,
	}
}

// usable reports whether the proxy is outside its cooldown window.
func (h *health) usable(proxyID int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	until, ok := h.until[proxyID]
	return !ok || !until.After(time.Now())
}

// onFail records a transport failure and trips the cooldown at the limit.
func (h *health) onFail(proxyID int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fails[proxyID]++
	if h.fails[proxyID] >= h.limit {
		h.until[proxyID] = time.Now().Add(h.cooldown)
		h.fails[proxyID] = 0
	}
}

// onOK clears failure state after any successful transport.
func (h *health) onOK(proxyID int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.fails, proxyID)
	delete(h.until, proxyID)
}
