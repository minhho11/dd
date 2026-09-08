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

func TestDeltaResetsPerInterval(t *testing.T) {
	m := New()
	m.Record("a.com", true)
	m.Record("a.com", false)

	// First interval: the delta reflects everything so far.
	rows := m.delta(time.Now())
	got := map[string][2]int64{}
	for _, r := range rows {
		got[r.URL] = [2]int64{r.Success, r.Fail}
	}
	if got["a.com"] != [2]int64{1, 1} {
		t.Fatalf("first delta a.com = %v, want [1 1]", got["a.com"])
	}

	// No new activity -> no rows.
	if rows := m.delta(time.Now()); len(rows) != 0 {
		t.Errorf("expected no rows with no new activity, got %d", len(rows))
	}

	// New activity -> only the new delta, not cumulative.
	m.Record("a.com", true)
	m.Record("a.com", true)
	rows = m.delta(time.Now())
	if len(rows) != 1 || rows[0].Success != 2 || rows[0].Fail != 0 {
		t.Errorf("second delta = %+v, want success=2 fail=0", rows)
	}

	// Cumulative totals remain intact in the in-memory variable.
	if tot := m.Totals()["a.com"]; tot != [2]int64{3, 1} {
		t.Errorf("cumulative a.com = %v, want [3 1]", tot)
	}
}
