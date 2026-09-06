package report

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCSVWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	w, err := New(path)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := w.Write(Record{
		Time: time.Now(), URL: "http://a.com", Domain: "a.com",
		Proxy: "http://p:1", Status: 200, Attempts: 1, Latency: 150 * time.Millisecond,
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Write(Record{
		Time: time.Now(), URL: "http://b.vn", Domain: "b.vn",
		Status: 403, Blocked: true, Attempts: 3, Latency: 2 * time.Second, Err: "boom",
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(rows) != 3 { // header + 2 records
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if rows[0][0] != "time" || rows[0][7] != "latency_ms" {
		t.Errorf("unexpected header: %v", rows[0])
	}
	if rows[1][4] != "200" || rows[1][7] != "150" {
		t.Errorf("record 1 wrong: %v", rows[1])
	}
	if rows[2][5] != "true" || rows[2][6] != "3" || rows[2][8] != "boom" {
		t.Errorf("record 2 wrong: %v", rows[2])
	}
}

func TestJSONLWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	w, err := New(path)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := w.Write(Record{URL: "http://a.com", Status: 200, Latency: 1200 * time.Millisecond}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		t.Fatal("expected one JSON line")
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(sc.Text())), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rec["url"] != "http://a.com" || rec["latency_ms"].(float64) != 1200 {
		t.Errorf("unexpected record: %v", rec)
	}
}
