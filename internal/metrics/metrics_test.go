package metrics

import (
	"testing"
	"time"
)

func TestRecordAndTotals(t *testing.T) {
	m := New()
	m.Record("a.com", true)
	m.Record("a.com", true)
	m.Record("a.com", false)
	m.Record("b.vn", false)

	tot := m.Totals()
	if got := tot["a.com"]; got != [2]int64{2, 1} {
		t.Errorf("a.com totals = %v, want [2 1]", got)
	}
	if got := tot["b.vn"]; got != [2]int64{0, 1} {
		t.Errorf("b.vn totals = %v, want [0 1]", got)
	}
}

func TestSnapshotEmitsCumulative(t *testing.T) {
	m := New()
	m.Record("a.com", true)
	m.Record("a.com", false)

	// First flush: the snapshot reflects the cumulative totals so far.
	rows := m.snapshot(time.Now())
	got := map[string][2]int64{}
	for _, r := range rows {
		got[r.URL] = [2]int64{r.Success, r.Fail}
	}
	if got["a.com"] != [2]int64{1, 1} {
		t.Fatalf("first snapshot a.com = %v, want [1 1]", got["a.com"])
	}

	// No new activity -> nothing to re-write.
	if rows := m.snapshot(time.Now()); len(rows) != 0 {
		t.Errorf("expected no rows with no new activity, got %d", len(rows))
	}

	// New activity -> the row carries the new cumulative totals, not the delta.
	m.Record("a.com", true)
	m.Record("a.com", true)
	rows = m.snapshot(time.Now())
	if len(rows) != 1 || rows[0].Success != 3 || rows[0].Fail != 1 {
		t.Errorf("second snapshot = %+v, want success=3 fail=1", rows)
	}

	// Cumulative totals remain intact in the in-memory variable.
	if tot := m.Totals()["a.com"]; tot != [2]int64{3, 1} {
		t.Errorf("cumulative a.com = %v, want [3 1]", tot)
	}
}
