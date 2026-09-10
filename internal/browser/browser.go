package browser

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"

	"github.com/minhho11/dd/internal/tmpl"
)

// Options tunes one flow execution.
type Options struct {
	Headless  bool          // run Chromium headless (true) or with a visible window
	Timeout   time.Duration // default per-step (and navigation) timeout; <=0 uses 30s
	Insecure  bool          // ignore TLS certificate errors
	UserAgent string        // override the browser User-Agent when non-empty

	// Logf, when set, is called with a human-readable line per navigation/step for
	// interactive debugging (the one-shot browser-test mode). nil in the load path.
	Logf func(format string, args ...any)
	// HoldOpen keeps the browser open this long after the flow finishes, so a visible
	// (headless=false) run can be inspected. Ignored when <=0.
	HoldOpen time.Duration
}

// Outcome is the result of running one flow.
type Outcome struct {
	Status    int    // main-document HTTP status (0 if not observed)
	FinalURL  string // URL after the flow finished
	BodyText  string // body innerText at the end (truncated), for block detection
	Steps     int    // steps that ran successfully (including the entry navigation)
	Err       error  // execution or assertion failure; nil means the flow passed
	Transport bool   // Err was a navigation/connection failure (worth retrying via
	// another proxy), not a page/assertion logic failure
}

// Execute runs flow against entryURL through the given proxy (empty = direct) in a
// fresh headless Chromium and returns the outcome. A new browser process is used
// per call so each run is isolated (fresh cookies/storage) and can use its own
// proxy — the proxy is a browser-level setting, so proxy rotation needs a new
// process. Any step error (including a failed assertion) stops the flow and is
// returned in Outcome.Err; the browser is always torn down before returning. For
// repeated direct runs, a Runner reuses one browser instead (see runner.go).
func Execute(ctx context.Context, entryURL string, flow Flow, proxyURL string, opts Options) Outcome {
	stepTimeout := normalizeTimeout(opts.Timeout)

	allocOpts, user, pass := buildAllocOptions(opts, proxyURL)
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(ctx, allocOpts...)
	defer cancelAlloc()
	taskCtx, cancelTask := chromedp.NewContext(allocCtx)
	defer cancelTask()

	status := newStatusHolder()
	listen(taskCtx, status, user, pass)
	if err := enableDomains(taskCtx, user != ""); err != nil {
		return Outcome{Err: fmt.Errorf("start browser: %w", err), Transport: true}
	}

	// Overall guard so a wedged browser can't hang the worker forever.
	overall := stepTimeout * time.Duration(len(flow.Steps)+2)
	runCtx, cancelRun := context.WithTimeout(taskCtx, overall)
	defer cancelRun()

	logf := logfOrNoop(opts.Logf)
	out := runFlow(runCtx, entryURL, flow, stepTimeout, logf)
	out.Status = status.get()
	holdOpen(runCtx, opts.HoldOpen, logf)
	return out
}

// normalizeTimeout returns the per-step timeout, defaulting to 30s.
func normalizeTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return 30 * time.Second
	}
	return d
}

func logfOrNoop(f func(string, ...any)) func(string, ...any) {
	if f == nil {
		return func(string, ...any) {}
	}
	return f
}

// buildAllocOptions assembles the chromedp exec-allocator options for opts and an
// optional proxy, and returns the proxy credentials (empty when none) so the caller
// can answer a proxy auth challenge.
func buildAllocOptions(opts Options, proxyURL string) (allocOpts []chromedp.ExecAllocatorOption, user, pass string) {
	// Copy the default options (never append into the package-global slice).
	allocOpts = append([]chromedp.ExecAllocatorOption{}, chromedp.DefaultExecAllocatorOptions[:]...)
	allocOpts = append(allocOpts, chromedp.Flag("headless", opts.Headless))
	// CPU/memory-reducing flags: forms don't need images, GPU, audio, or Chrome's
	// background machinery, and these cut the cost of each launch substantially.
	allocOpts = append(allocOpts,
		chromedp.Flag("blink-settings", "imagesEnabled=false"),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("disable-software-rasterizer", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("mute-audio", true),
		chromedp.Flag("disable-background-networking", true),
		chromedp.Flag("disable-background-timer-throttling", true),
		chromedp.Flag("disable-renderer-backgrounding", true),
		chromedp.Flag("disable-extensions", true),
	)
	if opts.Insecure {
		allocOpts = append(allocOpts, chromedp.IgnoreCertErrors)
	}
	if opts.UserAgent != "" {
		allocOpts = append(allocOpts, chromedp.UserAgent(opts.UserAgent))
	}
	proxyAddr, u, p := splitProxy(proxyURL)
	if proxyAddr != "" {
		allocOpts = append(allocOpts, chromedp.ProxyServer(proxyAddr))
	}
	return allocOpts, u, p
}

// statusHolder records the latest main-document HTTP status, safe for concurrent
// updates from the CDP event listener.
type statusHolder struct {
	mu     sync.Mutex
	status int
}

func newStatusHolder() *statusHolder { return &statusHolder{} }

func (h *statusHolder) set(s int) { h.mu.Lock(); h.status = s; h.mu.Unlock() }
func (h *statusHolder) get() int  { h.mu.Lock(); defer h.mu.Unlock(); return h.status }
func (h *statusHolder) reset()    { h.set(0) }

// listen wires the CDP event handlers on taskCtx: capture the main document's HTTP
// status, and answer proxy auth challenges (Chrome's --proxy-server takes no
// credentials, so they go over the Fetch domain).
func listen(taskCtx context.Context, status *statusHolder, user, pass string) {
	chromedp.ListenTarget(taskCtx, func(ev interface{}) {
		switch e := ev.(type) {
		case *network.EventResponseReceived:
			if e.Type == network.ResourceTypeDocument && e.Response != nil {
				status.set(int(e.Response.Status))
			}
		case *fetch.EventAuthRequired:
			resp := &fetch.AuthChallengeResponse{
				Response: fetch.AuthChallengeResponseResponseProvideCredentials,
				Username: user,
				Password: pass,
			}
			go func() { _ = chromedp.Run(taskCtx, fetch.ContinueWithAuth(e.RequestID, resp)) }()
		case *fetch.EventRequestPaused:
			// Fetch is only enabled for proxy auth; every paused request must be
			// continued or the page stalls.
			go func() { _ = chromedp.Run(taskCtx, fetch.ContinueRequest(e.RequestID)) }()
		}
	})
}

// enableDomains turns on the CDP Network domain (for status capture) and, when the
// proxy needs auth, the Fetch domain with auth handling.
func enableDomains(taskCtx context.Context, needAuth bool) error {
	enable := []chromedp.Action{network.Enable()}
	if needAuth {
		enable = append(enable, fetch.Enable().WithHandleAuthRequests(true))
	}
	return chromedp.Run(taskCtx, enable...)
}

// runFlow navigates the entry URL and runs each step on the given chromedp context
// (which must already have a browser + enabled domains). It fills every Outcome
// field except Status (the caller sets that from its statusHolder). Shared by
// Execute (fresh browser) and a reused Runner session.
func runFlow(runCtx context.Context, entryURL string, flow Flow, stepTimeout time.Duration, logf func(string, ...any)) Outcome {
	out := Outcome{}

	// Resolve flow variables once for this run so fields that reference the same
	// {{name}} (e.g. password + confirm) get one shared value.
	vars := resolveVars(flow.Vars)

	// Navigate the entry URL first, then run each step in order.
	if entryURL != "" {
		if err := runAction(runCtx, stepTimeout, chromedp.Navigate(tmpl.Expand(entryURL))); err != nil {
			out.Err = fmt.Errorf("navigate %s: %w", entryURL, err)
			out.Transport = true // failing to load the entry page is a connection issue
			logf("navigate %s → FAIL: %v", entryURL, err)
			captureState(runCtx, &out)
			return out
		}
		out.Steps++
		logf("navigate %s → ok", entryURL)
	}

	for i, s := range flow.Steps {
		act, err := stepAction(s, vars)
		if err != nil {
			out.Err = fmt.Errorf("step %d: %w", i+1, err)
			logf("step %d %s → FAIL: %v", i+1, s.Action, err)
			break
		}
		d := stepTimeout
		if s.Timeout != "" {
			if pd := parseDur(s.Timeout); pd > 0 {
				d = pd
			}
		}
		if err := runAction(runCtx, d, act); err != nil {
			out.Err = fmt.Errorf("step %d (%s): %w", i+1, s.Action, err)
			// A failed navigation is a connection issue (retry via another proxy);
			// a failed fill/click/wait/assert is a page/logic failure (don't retry).
			out.Transport = isNavStep(s.Action)
			logf("step %d %s %s → FAIL: %v", i+1, s.Action, s.Selector, err)
			break
		}
		out.Steps++
		logf("step %d %s %s → ok", i+1, s.Action, s.Selector)
	}

	captureState(runCtx, &out)
	return out
}

// holdOpen keeps the browser process alive for d (so a visible run can be
// inspected), returning early if the context is cancelled.
func holdOpen(ctx context.Context, d time.Duration, logf func(string, ...any)) {
	if d <= 0 {
		return
	}
	logf("holding browser open for %s (Ctrl-C to stop)…", d)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// runAction runs a single chromedp action under its own timeout.
func runAction(ctx context.Context, d time.Duration, a chromedp.Action) error {
	c, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	return chromedp.Run(c, a)
}

// stepAction maps one Step to a chromedp action. Values and URLs are resolved
// against the flow vars and then expanded through internal/tmpl, so each run gets
// fresh random data and repeated {{name}} references share one value.
func stepAction(s Step, vars map[string]string) (chromedp.Action, error) {
	sel := s.Selector
	val := expandValue(s.Value, vars)
	switch strings.ToLower(strings.TrimSpace(s.Action)) {
	case "navigate", "goto":
		u := expandValue(s.URL, vars)
		if u == "" {
			u = val
		}
		if u == "" {
			return nil, fmt.Errorf("navigate: no url")
		}
		return chromedp.Navigate(u), nil
	case "fill", "type", "sendkeys":
		return chromedp.SendKeys(sel, val, chromedp.ByQuery), nil
	case "setvalue", "select":
		return chromedp.SetValue(sel, val, chromedp.ByQuery), nil
	case "click":
		return chromedp.Click(sel, chromedp.ByQuery), nil
	case "submit":
		return chromedp.Submit(sel, chromedp.ByQuery), nil
	case "waitvisible", "assertvisible":
		// assertVisible is the same wait; it fails the run if the element does not
		// appear within the step timeout.
		return chromedp.WaitVisible(sel, chromedp.ByQuery), nil
	case "waitready":
		return chromedp.WaitReady(sel, chromedp.ByQuery), nil
	case "sleep", "wait":
		return chromedp.Sleep(parseDur(s.Value)), nil
	case "asserttext":
		// contains is matched literally, but may reference a var so an assertion can
		// check the value a field was filled with; no generator expansion.
		return assertTextAction(sel, substituteVars(s.Contains, vars)), nil
	default:
		return nil, fmt.Errorf("unknown action %q", s.Action)
	}
}

// resolveVars expands each flow variable's value once (through internal/tmpl), so
// {"pw":"{{randString:12}}"} yields one random string reused for the whole run.
func resolveVars(vars map[string]string) map[string]string {
	if len(vars) == 0 {
		return nil
	}
	out := make(map[string]string, len(vars))
	for name, raw := range vars {
		out[name] = tmpl.Expand(raw)
	}
	return out
}

// expandValue substitutes {{name}} var references, then expands any remaining
// {{...}} generators. Var substitution runs first so a var value is used verbatim
// (and generator names never shadow a var).
func expandValue(s string, vars map[string]string) string {
	return tmpl.Expand(substituteVars(s, vars))
}

// substituteVars replaces {{name}} with the resolved value for each var name.
// Unknown tokens (generators, typos) are left for tmpl.Expand / visibility.
func substituteVars(s string, vars map[string]string) string {
	if len(vars) == 0 || !strings.Contains(s, "{{") {
		return s
	}
	for name, val := range vars {
		s = strings.ReplaceAll(s, "{{"+name+"}}", val)
	}
	return s
}

// isNavStep reports whether an action loads a page (so a failure is a connection
// issue worth retrying via another proxy).
func isNavStep(action string) bool {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "navigate", "goto":
		return true
	default:
		return false
	}
}

// assertTextAction reads the element's text and fails unless it contains the
// (literal) wanted substring.
func assertTextAction(sel, want string) chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		var got string
		if err := chromedp.Run(ctx, chromedp.Text(sel, &got, chromedp.ByQuery)); err != nil {
			return fmt.Errorf("assertText %q: %w", sel, err)
		}
		if !strings.Contains(got, want) {
			return fmt.Errorf("assertText: %q not found in element text", want)
		}
		return nil
	})
}

// captureState records the final URL and a (truncated) body innerText, best-effort
// — used for block-page detection and reporting. Failures are ignored.
func captureState(ctx context.Context, out *Outcome) {
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var body, loc string
	_ = chromedp.Run(c,
		chromedp.Location(&loc),
		chromedp.Evaluate(`document.body ? document.body.innerText : ""`, &body),
	)
	out.FinalURL = loc
	if len(body) > 8192 {
		body = body[:8192]
	}
	out.BodyText = body
}

// parseDur parses a duration string ("500ms", "2s"); a bare integer is treated as
// milliseconds. Invalid input yields 0.
func parseDur(s string) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	if n, err := strconv.Atoi(s); err == nil {
		return time.Duration(n) * time.Millisecond
	}
	return 0
}

// splitProxy splits a proxy URL into the address chromedp wants (scheme://host:port,
// no credentials) and the username/password to answer a proxy auth challenge with.
// An empty or unparseable input yields an empty address (direct).
func splitProxy(proxyURL string) (addr, user, pass string) {
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" {
		return "", "", ""
	}
	u, err := url.Parse(proxyURL)
	if err != nil || u.Host == "" {
		return "", "", ""
	}
	if u.User != nil {
		user = u.User.Username()
		pass, _ = u.User.Password()
	}
	scheme := u.Scheme
	if scheme == "" {
		scheme = "http"
	}
	return scheme + "://" + u.Host, user, pass
}
