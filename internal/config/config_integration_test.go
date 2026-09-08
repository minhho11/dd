package config_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/minhho11/dd/internal/config"
	"github.com/minhho11/dd/internal/db"
)

// TestConfigLoadAndWatch exercises schema creation, seeding, loading, and the
// LISTEN/NOTIFY watcher against real Postgres. Runs only when DD_TEST_DSN is set.
func TestConfigLoadAndWatch(t *testing.T) {
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
	if _, err := database.NewDropTable().Model((*config.Config)(nil)).IfExists().Exec(ctx); err != nil {
		t.Fatalf("drop: %v", err)
	}

	repo := config.NewRepo(database)
	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}

	// Seed a default; it must be loadable.
	seed := config.Config{URLs: "a.com,b.vn", Workers: 7, Requests: 50, Retries: 1, CacheBustParam: "_"}
	if err := repo.EnsureDefault(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := repo.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Workers != 7 || got.URLs != "a.com,b.vn" {
		t.Fatalf("loaded config wrong: %+v", got)
	}
	if urls := got.URLList(); len(urls) != 2 || urls[0] != "a.com" {
		t.Errorf("URLList = %v", urls)
	}

	// EnsureDefault must NOT overwrite an existing row.
	if err := repo.EnsureDefault(ctx, config.Config{URLs: "x", Workers: 99}); err != nil {
		t.Fatalf("re-seed: %v", err)
	}
	if again, _ := repo.Load(ctx); again.Workers != 7 {
		t.Errorf("EnsureDefault overwrote existing row: workers=%d", again.Workers)
	}

	// Watch, then Save a change: the watcher must fire via NOTIFY.
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	changes, err := repo.Watch(watchCtx)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // let LISTEN establish

	seed.Workers = 20
	if err := repo.Save(ctx, seed); err != nil {
		t.Fatalf("save: %v", err)
	}

	select {
	case <-changes:
		// good
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for config-change notification")
	}

	if got, _ := repo.Load(ctx); got.Workers != 20 {
		t.Errorf("after Save, workers = %d, want 20", got.Workers)
	}
}
