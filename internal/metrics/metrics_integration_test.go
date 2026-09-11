package metrics_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/minhho11/dd/internal/db"
	"github.com/minhho11/dd/internal/metrics"
)

// TestReportFlush runs the reporter against real Postgres and checks that each
// URL ends up as a single row holding its cumulative totals, updated in place
// across ticks (upsert). Runs only with DD_TEST_DSN.
func TestReportFlush(t *testing.T) {
	dsn := os.Getenv("DD_TEST_DSN")
	if dsn == "" {
		t.Skip("set DD_TEST_DSN to run the Postgres integration test")
	}
	ctx := context.Background()

	database, err := db.Open(ctx, dsn, false, 5)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer database.Close()

	if _, err := database.NewDropTable().Model((*metrics.Report)(nil)).IfExists().Exec(ctx); err != nil {
		t.Fatalf("drop: %v", err)
	}
	repo := metrics.NewRepo(database)
	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}

	m := metrics.New()
	for i := 0; i < 3; i++ {
		m.Record("a.com", true)
	}
	m.Record("a.com", false)
	m.Record("b.vn", false)

	// A short-interval reporter: run it, let one tick fire, then cancel (final flush).
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.Run(rctx, repo, func() time.Duration { return 40 * time.Millisecond }, os.Stderr)
	}()
	time.Sleep(120 * time.Millisecond)
	cancel()
	<-done

	var rows []metrics.Report
	if err := database.NewSelect().Model(&rows).Order("url").Scan(ctx); err != nil {
		t.Fatalf("scan: %v", err)
	}

	// One row per URL (upsert), each holding cumulative totals.
	byURL := map[string][2]int64{}
	for _, r := range rows {
		if _, seen := byURL[r.URL]; seen {
			t.Errorf("url %q has more than one row; want exactly one", r.URL)
		}
		byURL[r.URL] = [2]int64{r.Success, r.Fail}
	}
	if len(rows) != 2 {
		t.Errorf("got %d report rows, want 2 (one per URL)", len(rows))
	}
	if byURL["a.com"] != [2]int64{3, 1} {
		t.Errorf("a.com report row = %v, want [3 1]", byURL["a.com"])
	}
	if byURL["b.vn"] != [2]int64{0, 1} {
		t.Errorf("b.vn report row = %v, want [0 1]", byURL["b.vn"])
	}
}
