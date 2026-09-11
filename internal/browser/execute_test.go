package browser

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// These tests drive a real Chromium, so they run only when DD_BROWSER_TEST is set.
func needChrome(t *testing.T) {
	t.Helper()
	if os.Getenv("DD_BROWSER_TEST") == "" {
		t.Skip("set DD_BROWSER_TEST=1 to run tests that launch Chromium")
	}
}

// testSite serves a form page with a web font and a slow async script, and counts
// font requests so a test can assert resource blocking.
type testSite struct {
	*httptest.Server
	fontHits atomic.Int64
}

func newTestSite(t *testing.T) *testSite {
	s := &testSite{}
	mux := http.NewServeMux()
	// DOMContentLoaded fires at once; the load event waits for the 3s async script.
	mux.HandleFunc("/form", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `<html><head>
			<style>@font-face{font-family:X;src:url(/font.woff2?v=1)} body{font-family:X}</style>
			<script async src="/slow.js"></script></head>
			<body><input id="email"><div id="ok">Welcome</div></body></html>`)
	})
	mux.HandleFunc("/font.woff2", func(w http.ResponseWriter, r *http.Request) {
		s.fontHits.Add(1)
		w.Write(make([]byte, 64))
	})
	mux.HandleFunc("/slow.js", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(3 * time.Second):
		case <-r.Context().Done():
		}
		io.WriteString(w, "//")
	})
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func directOpts(block bool) Options {
	return Options{Headless: true, Timeout: 10 * time.Second, BlockResources: block}
}

// TestNavigateReturnsAtDOMContentLoaded checks the entry navigation returns at
// DOMContentLoaded rather than waiting for the page's slow (3s) async script.
func TestNavigateReturnsAtDOMContentLoaded(t *testing.T) {
	needChrome(t)
	site := newTestSite(t)
	flow := Flow{Steps: []Step{{Action: "assertText", Selector: "#ok", Contains: "Welcome"}}}
	start := time.Now()
	oc := Execute(context.Background(), site.URL+"/form", flow, "", directOpts(false))
	if oc.Err != nil {
		t.Fatalf("run: %v", oc.Err)
	}
	// The 3s slow.js must not gate the flow; allow generous headroom for launch.
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("run took %s; navigation appears to wait for the full load event", d)
	}
}

// TestBlockResources checks BlockResources drops the web font (and leaves the flow
// working), and that it is off by default.
func TestBlockResources(t *testing.T) {
	needChrome(t)
	flow := Flow{Steps: []Step{{Action: "assertText", Selector: "#ok", Contains: "Welcome"}, {Action: "sleep", Value: "300ms"}}}
	for _, block := range []bool{false, true} {
		site := newTestSite(t)
		oc := Execute(context.Background(), site.URL+"/form", flow, "", directOpts(block))
		if oc.Err != nil {
			t.Fatalf("block=%t: %v", block, oc.Err)
		}
		if hits := site.fontHits.Load(); (hits > 0) == block {
			t.Errorf("block=%t: font requests = %d", block, hits)
		}
	}
}
