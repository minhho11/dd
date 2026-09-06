// Package report streams per-request results to a CSV or JSONL file.
package report

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Record is one exported request outcome.
type Record struct {
	Time      time.Time     `json:"time"`
	URL       string        `json:"url"`
	Domain    string        `json:"domain"`
	Proxy     string        `json:"proxy"` // "" == direct
	Status    int           `json:"status"`
	Blocked   bool          `json:"blocked"`
	Attempts  int           `json:"attempts"`
	Latency   time.Duration `json:"-"`
	LatencyMS int64         `json:"latency_ms"`
	Err       string        `json:"error,omitempty"`
}

// Writer streams Records to a file. It is safe for concurrent use. Format is
// chosen by extension: .json/.jsonl write one JSON object per line, anything else
// writes CSV.
type Writer struct {
	mu    sync.Mutex
	f     *os.File
	csv   *csv.Writer
	jsonl bool
}

// New opens path for writing and, for CSV, writes the header row.
func New(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := &Writer{f: f}

	switch strings.ToLower(filepath.Ext(path)) {
	case ".json", ".jsonl":
		w.jsonl = true
	default:
		w.csv = csv.NewWriter(f)
		if err := w.csv.Write([]string{
			"time", "url", "domain", "proxy", "status", "blocked", "attempts", "latency_ms", "error",
		}); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return w, nil
}

// Write appends one record.
func (w *Writer) Write(r Record) error {
	r.LatencyMS = r.Latency.Milliseconds()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.jsonl {
		b, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if _, err := w.f.Write(append(b, '\n')); err != nil {
			return err
		}
		return nil
	}

	return w.csv.Write([]string{
		r.Time.Format(time.RFC3339),
		r.URL,
		r.Domain,
		r.Proxy,
		strconv.Itoa(r.Status),
		strconv.FormatBool(r.Blocked),
		strconv.Itoa(r.Attempts),
		fmt.Sprintf("%d", r.LatencyMS),
		r.Err,
	})
}

// Close flushes and closes the underlying file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.csv != nil {
		w.csv.Flush()
		if err := w.csv.Error(); err != nil {
			_ = w.f.Close()
			return err
		}
	}
	return w.f.Close()
}
