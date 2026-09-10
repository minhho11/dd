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
// returned in Outcome.Err; the browser is always torn down before returning.
func Execute(ctx context.Context, entryURL string, flow Flow, proxyURL string, opts Options) Outcome {
	stepTimeout := opts.Timeout
	if stepTimeout <= 0 {
		stepTimeout = 30 * time.Second
	}

	// Copy the default options (never append into the package-global slice) and add
	// ours: headless toggle, proxy, TLS, user-agent.
	allocOpts := append([]chromedp.ExecAllocatorOption{}, chromedp.DefaultExecAllocatorOptions[:]...)
	allocOpts = append(allocOpts, chromedp.Flag("headless", opts.Headless))
	if opts.Insecure {
		allocOpts = append(allocOpts, chromedp.IgnoreCertErrors)
	}
	if opts.UserAgent != "" {
		allocOpts = append(allocOpts, chromedp.UserAgent(opts.UserAgent))
	}
	proxyAddr, proxyUser, proxyPass := splitProxy(proxyURL)
	if proxyAddr != "" {
		allocOpts = append(allocOpts, chromedp.ProxyServer(proxyAddr))
	}

	allocCtx, cancelAlloc := chromedp.NewExecAllocator(ctx, allocOpts...)
	defer cancelAlloc()
	taskCtx, cancelTask := chromedp.NewContext(allocCtx)
	defer cancelTask()

	// Overall guard so a wedged browser can't hang the worker forever.
	overall := stepTimeout * time.Duration(len(flow.Steps)+2)
	runCtx, cancelRun := context.WithTimeout(taskCtx, overall)
	defer cancelRun()

	// Observe the main document's HTTP status, and — when the proxy needs auth —
	// answer the proxy auth challenge (Chrome's --proxy-server takes no credentials,
	// so they must be provided over the Fetch domain).
	var (
		mu        sync.Mutex
		docStatus int
	)
	chromedp.ListenTarget(taskCtx, func(ev interface{}) {
		switch e := ev.(type) {
		case *network.EventResponseReceived:
			if e.Type == network.ResourceTypeDocument && e.Response != nil {
				mu.Lock()
				docStatus = int(e.Response.Status)
				mu.Unlock()
			}
		case *fetch.EventAuthRequired:
			resp := &fetch.AuthChallengeResponse{
				Response: fetch.AuthChallengeResponseResponseProvideCredentials,
				Username: proxyUser,
				Password: proxyPass,
			}
			go func() { _ = chromedp.Run(taskCtx, fetch.ContinueWithAuth(e.RequestID, resp)) }()
		case *fetch.EventRequestPaused:
			// Fetch is only enabled for proxy auth; every paused request must be
			// continued or the page stalls.
			go func() { _ = chromedp.Run(taskCtx, fetch.ContinueRequest(e.RequestID)) }()
		}
	})

	enable := []chromedp.Action{network.Enable()}
	if proxyUser != "" {
		enable = append(enable, fetch.Enable().WithHandleAuthRequests(true))
	}
	if err := chromedp.Run(taskCtx, enable...); err != nil {
		return Outcome{Err: fmt.Errorf("start browser: %w", err)}
	}

	out := Outcome{}

	// Navigate the entry URL first, then run each step in order.
	if entryURL != "" {
		if err := runAction(runCtx, stepTimeout, chromedp.Navigate(tmpl.Expand(entryURL))); err != nil {
			out.Err = fmt.Errorf("navigate %s: %w", entryURL, err)
			out.Transport = true // failing to load the entry page is a connection issue
			captureState(runCtx, &out)
			out.Status = readStatus(&mu, &docStatus)
			return out
		}
		out.Steps++
	}

	for i, s := range flow.Steps {
		act, err := stepAction(s)
		if err != nil {
			out.Err = fmt.Errorf("step %d: %w", i+1, err)
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
			break
		}
		out.Steps++
	}

	captureState(runCtx, &out)
	out.Status = readStatus(&mu, &docStatus)
	return out
}

func readStatus(mu *sync.Mutex, status *int) int {
	mu.Lock()
	defer mu.Unlock()
	return *status
}

// runAction runs a single chromedp action under its own timeout.
func runAction(ctx context.Context, d time.Duration, a chromedp.Action) error {
	c, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	return chromedp.Run(c, a)
}

// stepAction maps one Step to a chromedp action. Values and URLs are expanded
// through internal/tmpl so each run gets fresh random data.
func stepAction(s Step) (chromedp.Action, error) {
	sel := s.Selector
	val := tmpl.Expand(s.Value)
	switch strings.ToLower(strings.TrimSpace(s.Action)) {
	case "navigate", "goto":
		u := tmpl.Expand(s.URL)
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
		return assertTextAction(sel, s.Contains), nil
	default:
		return nil, fmt.Errorf("unknown action %q", s.Action)
	}
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
