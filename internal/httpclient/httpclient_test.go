package httpclient

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// captureHeaders spins up a server that records the headers of the last request.
func captureHeaders(t *testing.T) (*httptest.Server, func() http.Header) {
	t.Helper()
	var last http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		last = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, func() http.Header { return last }
}

func TestHumanHeaders(t *testing.T) {
	srv, got := captureHeaders(t)
	pool := New(Config{FollowRedirects: true, Human: true}, nil)

	if _, err := pool.Clients()[0].RC.R().Get(srv.URL); err != nil {
		t.Fatalf("get: %v", err)
	}
	h := got()

	ua := h.Get("User-Agent")
	if !strings.Contains(ua, "Mozilla/5.0") {
		t.Errorf("User-Agent %q does not look like a browser", ua)
	}
	if h.Get("Accept") == "" || h.Get("Accept-Language") == "" {
		t.Errorf("missing Accept/Accept-Language: %v", h)
	}
	if strings.Contains(ua, "dd/") {
		t.Errorf("human mode should not send the tool UA, got %q", ua)
	}
}

func TestUserAgentOverride(t *testing.T) {
	srv, got := captureHeaders(t)
	pool := New(Config{Human: true, UserAgent: "my-agent/9"}, nil)

	if _, err := pool.Clients()[0].RC.R().Get(srv.URL); err != nil {
		t.Fatalf("get: %v", err)
	}
	if ua := got().Get("User-Agent"); ua != "my-agent/9" {
		t.Errorf("User-Agent = %q, want my-agent/9 (override wins over -human)", ua)
	}
}

func TestDefaultToolUA(t *testing.T) {
	srv, got := captureHeaders(t)
	pool := New(Config{Human: false}, nil) // neither human nor override

	if _, err := pool.Clients()[0].RC.R().Get(srv.URL); err != nil {
		t.Fatalf("get: %v", err)
	}
	if ua := got().Get("User-Agent"); ua != "dd/1.0" {
		t.Errorf("User-Agent = %q, want dd/1.0", ua)
	}
}

func TestProfilesAreCoherent(t *testing.T) {
	for _, p := range browserProfiles {
		ua := p.Headers["User-Agent"]
		if ua == "" {
			t.Errorf("profile %s missing User-Agent", p.Name)
		}
		// Chromium-family profiles must carry matching client hints.
		if strings.Contains(ua, "Chrome/") && !strings.Contains(ua, "Firefox") {
			if p.Headers["Sec-Ch-Ua"] == "" || p.Headers["Sec-Ch-Ua-Platform"] == "" {
				t.Errorf("profile %s is Chromium but missing Sec-Ch-Ua hints", p.Name)
			}
		}
	}
}
