package browser

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// session is a reusable browser: one Chromium process and one tab, run repeatedly.
// It is used only for direct (no-proxy) flows — proxy is a browser-level setting, so
// a per-proxy browser would defeat rotation; proxied browser jobs stay per-request.
type session struct {
	allocCancel context.CancelFunc
	taskCancel  context.CancelFunc
	taskCtx     context.Context
	status      *statusHolder
}

// newSession launches a direct browser parented to ctx (so it dies when the run
// ends) and enables the domains once, ready to run flows.
func newSession(ctx context.Context, opts Options) (*session, error) {
	allocOpts, _, _ := buildAllocOptions(opts, "") // reuse is direct-only
	allocCtx, allocCancel := chromedp.NewExecAllocator(ctx, allocOpts...)
	taskCtx, taskCancel := chromedp.NewContext(allocCtx)
	status := newStatusHolder()
	listen(taskCtx, status, "", "")
	if err := enableDomains(taskCtx, false, opts.BlockResources); err != nil {
		taskCancel()
		allocCancel()
		return nil, err
	}
	return &session{allocCancel: allocCancel, taskCancel: taskCancel, taskCtx: taskCtx, status: status}, nil
}

// run executes one flow in the session, clearing cookies first so a prior success
// (e.g. a login cookie set after a registration) doesn't leak into the next run.
func (s *session) run(entryURL string, flow Flow, opts Options, logf func(string, ...any)) Outcome {
	stepTimeout := normalizeTimeout(opts.Timeout)
	_ = chromedp.Run(s.taskCtx, network.ClearBrowserCookies())
	s.status.reset()

	overall := stepTimeout * time.Duration(len(flow.Steps)+2)
	runCtx, cancel := context.WithTimeout(s.taskCtx, overall)
	defer cancel()

	out := runFlow(runCtx, entryURL, flow, stepTimeout, logfOrNoop(logf))
	out.Status = s.status.get()
	return out
}

// alive reports whether the session's browser is still usable.
func (s *session) alive() bool { return s.taskCtx.Err() == nil }

func (s *session) close() {
	if s.taskCancel != nil {
		s.taskCancel()
	}
	if s.allocCancel != nil {
		s.allocCancel()
	}
}

// Runner reuses direct browser sessions across flow runs, so a burst of direct
// browser jobs shares a handful of long-lived Chromium processes instead of
// launching one per request (the dominant CPU cost). It is safe for concurrent use;
// the number of live browsers tracks peak concurrency, which the caller bounds (the
// pool's browser semaphore / browser_max). Call Close when the run ends.
type Runner struct {
	parent context.Context
	opts   Options
	mu     sync.Mutex
	idle   []*session
	all    []*session
	closed bool
}

// NewRunner returns a Runner whose sessions are parented to ctx (they are torn down
// when ctx is cancelled, and by Close).
func NewRunner(ctx context.Context, opts Options) *Runner {
	return &Runner{parent: ctx, opts: opts}
}

// Run executes the flow in a reused direct session, launching a new browser only
// when none is idle. A dead session (crashed/cancelled) is discarded rather than
// returned to the pool.
func (r *Runner) Run(entryURL string, flow Flow, logf func(string, ...any)) Outcome {
	s, err := r.acquire()
	if err != nil {
		return Outcome{Err: fmt.Errorf("browser session: %w", err), Transport: true}
	}
	out := s.run(entryURL, flow, r.opts, logf)
	if s.alive() {
		r.release(s)
	} else {
		r.discard(s)
	}
	return out
}

func (r *Runner) acquire() (*session, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, fmt.Errorf("runner closed")
	}
	if n := len(r.idle); n > 0 {
		s := r.idle[n-1]
		r.idle = r.idle[:n-1]
		r.mu.Unlock()
		return s, nil
	}
	r.mu.Unlock()

	// Launch outside the lock (a browser start is slow).
	s, err := newSession(r.parent, r.opts)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		s.close()
		return nil, fmt.Errorf("runner closed")
	}
	r.all = append(r.all, s)
	r.mu.Unlock()
	return s, nil
}

func (r *Runner) release(s *session) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		s.close()
		return
	}
	r.idle = append(r.idle, s)
	r.mu.Unlock()
}

func (r *Runner) discard(s *session) {
	r.mu.Lock()
	for i, x := range r.all {
		if x == s {
			r.all = append(r.all[:i], r.all[i+1:]...)
			break
		}
	}
	r.mu.Unlock()
	s.close()
}

// Close tears down every session. Safe to call once the run's workers have stopped.
func (r *Runner) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	all := r.all
	r.all, r.idle = nil, nil
	r.mu.Unlock()
	for _, s := range all {
		s.close()
	}
}
