package metrics_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/minhho11/dd/internal/db"
	"github.com/minhho11/dd/internal/metrics"
)

// TestReportFlush runs the reporter against real Postgres and checks that an
// interval's per-URL deltas land in the report table. Runs only with DD_TEST_DSN.
func TestReportFlush(t *testing.T) {
	dsn := os.Getenv("DD_TEST_DSN")
	if dsn == "" {
		t.Skip("set DD_TEST_DSN to run the Postgres integration test")
	}
	ctx := context.Background()

	database, err := db.Open(ctx, dsn, false)
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
	go func() { defer close(done); m.Run(rctx, repo, 40*time.Millisecond, os.Stderr) }()
	time.Sleep(120 * time.Millisecond)
	cancel()
	<-done

	var rows []metrics.Report
	if err := database.NewSelect().Model(&rows).Order("url").Scan(ctx); err != nil {
		t.Fatalf("scan: %v", err)
	}

	// Sum per URL across however many interval rows were written.
	sum := map[string][2]int64{}
	for _, r := range rows {
		s := sum[r.URL]
		s[0] += r.Success
		s[1] += r.Fail
		sum[r.URL] = s
	}
	if sum["a.com"] != [2]int64{3, 1} {
		t.Errorf("a.com report sum = %v, want [3 1]", sum["a.com"])
	}
	if sum["b.vn"] != [2]int64{0, 1} {
		t.Errorf("b.vn report sum = %v, want [0 1]", sum["b.vn"])
	}
}
