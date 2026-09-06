package proxy_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/minhho11/dd/internal/db"
	"github.com/minhho11/dd/internal/proxy"
)

// TestBlockLifecycle exercises the full quarantine cycle against real Postgres.
// It runs only when DD_TEST_DSN is set, e.g.
//
//	DD_TEST_DSN="postgres://postgres:pw@localhost:55434/dd_test?sslmode=disable" go test ./internal/proxy/
func TestBlockLifecycle(t *testing.T) {
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

	// Clean slate.
	if _, err := database.NewDropTable().Model((*proxy.ProxyBlock)(nil)).IfExists().Exec(ctx); err != nil {
		t.Fatalf("drop: %v", err)
	}

	repo := proxy.NewBlockRepo(database, 5, 30*time.Minute)
	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}

	const (
		proxyID = int64(42)
		domain  = "aaa.com"
	)

	mustAvailable := func(want bool) {
		t.Helper()
		got, err := repo.Available(ctx, proxyID, domain)
		if err != nil {
			t.Fatalf("available: %v", err)
		}
		if got != want {
			t.Fatalf("Available = %v, want %v", got, want)
		}
	}

	// 4 blocks: still available (threshold is 5).
	for i := 0; i < 4; i++ {
		if _, err := repo.OnBlocked(ctx, proxyID, domain); err != nil {
			t.Fatalf("onBlocked %d: %v", i, err)
		}
	}
	mustAvailable(true)

	// 5th block: quarantined.
	if _, err := repo.OnBlocked(ctx, proxyID, domain); err != nil {
		t.Fatalf("onBlocked 5: %v", err)
	}
	mustAvailable(false)

	// Simulate the 30-min window elapsing by back-dating blocked_until.
	if _, err := database.NewUpdate().
		Model((*proxy.ProxyBlock)(nil)).
		Set("blocked_until = ?", time.Now().Add(-time.Minute)).
		Where("proxy_id = ? AND domain = ?", proxyID, domain).
		Exec(ctx); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	mustAvailable(true) // probation: eligible again

	// A success clears the record entirely.
	if err := repo.OnSuccess(ctx, proxyID, domain); err != nil {
		t.Fatalf("onSuccess: %v", err)
	}
	mustAvailable(true)

	count, err := database.NewSelect().Model((*proxy.ProxyBlock)(nil)).
		Where("proxy_id = ? AND domain = ?", proxyID, domain).Count(ctx)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("row count after success = %d, want 0 (cleared)", count)
	}

	// A different domain is independent.
	if _, err := repo.OnBlocked(ctx, proxyID, "other.com"); err != nil {
		t.Fatalf("onBlocked other: %v", err)
	}
	if ok, _ := repo.Available(ctx, proxyID, "other.com"); !ok {
		t.Fatal("one block on other.com should not quarantine it")
	}
}
