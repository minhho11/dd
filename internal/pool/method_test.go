package pool

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/minhho11/dd/internal/httpclient"
)

// TestFireMethodAndParams checks that a GET merges JSON params into the query and
// a POST sends them as the JSON body, with the right HTTP method reaching origin.
func TestFireMethodAndParams(t *testing.T) {
	type capture struct {
		method string
		query  string
		body   string
	}
	var mu sync.Mutex
	var got capture
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = capture{method: r.Method, query: r.URL.RawQuery, body: string(b)}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := New(httpclient.New(httpclient.DefaultConfig(), nil), nil, Options{Workers: 1})

	// GET with params -> query string.
	res := p.do(context.Background(), Job{URL: srv.URL, Method: "GET", Params: `{"q":"hi"}`})
	if res.Err != nil || res.StatusCode != 200 {
		t.Fatalf("GET result: status=%d err=%v", res.StatusCode, res.Err)
	}
	mu.Lock()
	g := got
	mu.Unlock()
	if g.method != "GET" || g.query != "q=hi" {
		t.Errorf("GET: method=%q query=%q, want GET q=hi", g.method, g.query)
	}

	// POST with params -> JSON body.
	res = p.do(context.Background(), Job{URL: srv.URL, Method: "POST", Params: `{"a":1}`})
	if res.Err != nil || res.StatusCode != 200 {
		t.Fatalf("POST result: status=%d err=%v", res.StatusCode, res.Err)
	}
	mu.Lock()
	g = got
	mu.Unlock()
	if g.method != "POST" || g.body != `{"a":1}` {
		t.Errorf("POST: method=%q body=%q, want POST {\"a\":1}", g.method, g.body)
	}
}

// TestRunGroupsDedicatedSplit checks that each worker group only hits its own
// target and that all groups' jobs are processed under one summary.
func TestRunGroupsDedicatedSplit(t *testing.T) {
	var muA, muB sync.Mutex
	hitsA, hitsB := 0, 0
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		muA.Lock()
		hitsA++
		muA.Unlock()
	}))
	defer srvA.Close()
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		muB.Lock()
		hitsB++
		muB.Unlock()
	}))
	defer srvB.Close()

	p := New(httpclient.New(httpclient.DefaultConfig(), nil), nil, Options{Workers: 4})

	jobsA := make(chan Job, 6)
	for i := 0; i < 6; i++ {
		jobsA <- Job{URL: srvA.URL}
	}
	close(jobsA)
	jobsB := make(chan Job, 4)
	for i := 0; i < 4; i++ {
		jobsB <- Job{URL: srvB.URL}
	}
	close(jobsB)

	s := p.RunGroups(context.Background(), []Group{
		{Workers: 3, Jobs: jobsA},
		{Workers: 1, Jobs: jobsB},
	})

	if s.Total != 10 || s.Success != 10 {
		t.Errorf("summary total=%d success=%d, want 10/10", s.Total, s.Success)
	}
	if hitsA != 6 {
		t.Errorf("target A hits = %d, want 6", hitsA)
	}
	if hitsB != 4 {
		t.Errorf("target B hits = %d, want 4", hitsB)
	}
}
