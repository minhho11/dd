package main

import (
	"bytes"
	"errors"
	"log"
	"reflect"
	"strings"
	"testing"

	"github.com/minhho11/dd/internal/pool"
)

func TestSplitList(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"aaa.com,xxx.vn", []string{"aaa.com", "xxx.vn"}},
		{" a , b ,, c ", []string{"a", "b", "c"}},
		{"", nil},
		{"only.one", []string{"only.one"}},
	}
	for _, tt := range tests {
		got := splitList(tt.in)
		if len(got) == 0 && len(tt.want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("splitList(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestParseFlagsDefaults(t *testing.T) {
	cfg, err := parseFlags(nil, nil)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.workers != 10 {
		t.Errorf("workers = %d, want 10", cfg.workers)
	}
	if cfg.requests != 100 {
		t.Errorf("requests = %d, want 100", cfg.requests)
	}
	if !reflect.DeepEqual(cfg.urls, []string{"https://example.com"}) {
		t.Errorf("urls = %v, want [https://example.com]", cfg.urls)
	}
}

func TestErrorLogHandler(t *testing.T) {
	var buf bytes.Buffer
	h := errorLogHandler(log.New(&buf, "", 0))

	// Successful and blocked results are not errors -> nothing logged.
	h(pool.Result{URL: "http://a.com", StatusCode: 200})
	h(pool.Result{URL: "http://a.com", StatusCode: 403, Blocked: true})
	if buf.Len() != 0 {
		t.Fatalf("non-error results should not be logged, got: %q", buf.String())
	}

	// Transport error and skip are logged.
	h(pool.Result{URL: "http://a.com", ProxyURL: "http://p:1", Attempts: 3, Err: errors.New("connection refused")})
	h(pool.Result{URL: "http://b.vn", Domain: "b.vn", Err: pool.ErrAllBlocked})

	out := buf.String()
	if !strings.Contains(out, "request error") || !strings.Contains(out, "connection refused") {
		t.Errorf("missing transport error line: %q", out)
	}
	if !strings.Contains(out, "skipped") || !strings.Contains(out, "b.vn") {
		t.Errorf("missing skip line: %q", out)
	}
	if lines := strings.Count(strings.TrimSpace(out), "\n"); lines != 1 { // 2 lines -> 1 newline between
		t.Errorf("expected exactly 2 logged lines, got %d newlines: %q", lines, out)
	}
}
