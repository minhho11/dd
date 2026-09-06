package pool

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/minhho11/dd/internal/httpclient"
)

func TestAddCacheBuster(t *testing.T) {
	// No existing query.
	got := addCacheBuster("http://a.com/path", "_")
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.Query().Get("_") == "" {
		t.Errorf("missing buster param in %q", got)
	}

	// Existing query is preserved and order kept.
	got = addCacheBuster("http://a.com/p?x=1&y=2", "cb")
	if !strings.HasPrefix(got, "http://a.com/p?x=1&y=2&cb=") {
		t.Errorf("existing query not preserved/appended: %q", got)
	}

	// Fragment is preserved.
	got = addCacheBuster("http://a.com/p?x=1#frag", "_")
	if !strings.Contains(got, "#frag") || !strings.Contains(got, "_=") {
		t.Errorf("fragment/param handling wrong: %q", got)
	}

	// Consecutive values are unique.
	a := addCacheBuster("http://a.com/", "_")
	b := addCacheBuster("http://a.com/", "_")
	if a == b {
		t.Errorf("cache-buster values not unique: %q == %q", a, b)
	}
}

// TestCacheBustDistinctURLs verifies every request the server receives carries a
// distinct query string, so a URL-keyed cache would forward each to origin.
func TestCacheBustDistinctURLs(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.RequestURI()]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := New(httpclient.New(httpclient.DefaultConfig(), nil), nil, Options{
		Workers:        4,
		CacheBust:      true,
		CacheBustParam: "_",
	})

	const n = 40
	jobs := make(chan Job, n)
	for i := 0; i < n; i++ {
		jobs <- Job{URL: srv.URL + "/page"}
	}
	close(jobs)
	summary := p.Run(context.Background(), jobs)

	if summary.Total != n {
		t.Fatalf("Total = %d, want %d", summary.Total, n)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Errorf("server saw %d distinct URIs, want %d (each request must be unique)", len(seen), n)
	}
	for uri := range seen {
		if !strings.Contains(uri, "_=") {
			t.Errorf("URI %q missing cache-buster param", uri)
		}
	}
}
