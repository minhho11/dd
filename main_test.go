package main

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/minhho11/dd/internal/pool"
)

func sum(xs []int) int {
	t := 0
	for _, x := range xs {
		t += x
	}
	return t
}

func TestAllocateWorkers(t *testing.T) {
	t.Run("proportional split", func(t *testing.T) {
		got := allocateWorkers([]float64{50, 30, 20}, 10)
		want := []int{5, 3, 2}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("alloc = %v, want %v", got, want)
			}
		}
		if sum(got) != 10 {
			t.Errorf("total = %d, want 10", sum(got))
		}
	})

	t.Run("relative weights normalize", func(t *testing.T) {
		// 3:1 over 8 workers -> 6:2.
		got := allocateWorkers([]float64{3, 1}, 8)
		if got[0] != 6 || got[1] != 2 {
			t.Errorf("alloc = %v, want [6 2]", got)
		}
	})

	t.Run("every target gets at least one worker", func(t *testing.T) {
		got := allocateWorkers([]float64{1, 1, 1, 1}, 2) // fewer workers than targets
		if len(got) != 4 || sum(got) != 4 {
			t.Fatalf("alloc = %v, want four 1s", got)
		}
		for i, w := range got {
			if w < 1 {
				t.Errorf("target %d got %d workers, want >=1", i, w)
			}
		}
	})

	t.Run("remainder distributed, total preserved", func(t *testing.T) {
		got := allocateWorkers([]float64{1, 1, 1}, 10)
		if sum(got) != 10 {
			t.Errorf("total = %d, want 10 (got %v)", sum(got), got)
		}
	})

	t.Run("zero weights fall back to equal split", func(t *testing.T) {
		got := allocateWorkers([]float64{0, 0}, 4)
		if got[0] != 2 || got[1] != 2 {
			t.Errorf("alloc = %v, want [2 2]", got)
		}
	})

	t.Run("empty", func(t *testing.T) {
		if got := allocateWorkers(nil, 10); got != nil {
			t.Errorf("alloc = %v, want nil", got)
		}
	})
}

func TestErrorLogHandlerCoalescesSkips(t *testing.T) {
	var buf bytes.Buffer
	l := log.New(&buf, "", 0)
	h := errorLogHandler(l, time.Minute) // long interval: no mid-window summary

	skip := pool.Result{URL: "https://a.ae/x", Domain: "a.ae", Err: pool.ErrAllBlocked}
	// Many skips for the same domain → exactly one "no usable proxy" line.
	for i := 0; i < 100; i++ {
		h(skip)
	}
	if n := strings.Count(buf.String(), "no usable proxy for domain=\"a.ae\""); n != 1 {
		t.Fatalf("want 1 initial skip line, got %d\n%s", n, buf.String())
	}

	// A usable result for the domain → one recovery line.
	h(pool.Result{URL: "https://a.ae/x", Domain: "a.ae", StatusCode: 200})
	if !strings.Contains(buf.String(), "usable proxy recovered for domain=\"a.ae\"") {
		t.Fatalf("want recovery line, got:\n%s", buf.String())
	}

	// After recovery, skipping again logs a fresh initial line (2 total now).
	h(skip)
	if n := strings.Count(buf.String(), "no usable proxy for domain=\"a.ae\""); n != 2 {
		t.Fatalf("want 2 initial skip lines after re-entry, got %d\n%s", n, buf.String())
	}
}

func TestErrorLogHandlerSkipIntervalZeroLogsEach(t *testing.T) {
	var buf bytes.Buffer
	l := log.New(&buf, "", 0)
	h := errorLogHandler(l, 0) // disabled: every skip logs

	skip := pool.Result{URL: "https://a.ae/x", Domain: "a.ae", Err: pool.ErrAllBlocked}
	for i := 0; i < 5; i++ {
		h(skip)
	}
	if n := strings.Count(buf.String(), "no usable proxy"); n != 5 {
		t.Fatalf("want 5 skip lines with interval=0, got %d\n%s", n, buf.String())
	}
}

func TestErrorLogHandlerStillLogsRequestErrors(t *testing.T) {
	var buf bytes.Buffer
	l := log.New(&buf, "", 0)
	h := errorLogHandler(l, time.Minute)

	h(pool.Result{URL: "https://a.ae/x", Domain: "a.ae", ProxyURL: "http://p:1", Attempts: 3, Err: errors.New("dial timeout")})
	if !strings.Contains(buf.String(), "request error") || !strings.Contains(buf.String(), "dial timeout") {
		t.Fatalf("want request error line, got:\n%s", buf.String())
	}
}
