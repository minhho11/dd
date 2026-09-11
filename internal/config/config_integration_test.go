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

	database, err := db.Open(ctx, dsn, false, 5)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer database.Close()

	// Clean slate.
	if _, err := database.NewDropTable().Model((*config.Setting)(nil)).IfExists().Exec(ctx); err != nil {
		t.Fatalf("drop: %v", err)
	}

	repo := config.NewRepo(database)
	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}

	// Seed a default; it must be loadable.
	seed := config.Config{Workers: 7, Requests: 50, Retries: 1, CacheBustParam: "_"}
	if err := repo.EnsureDefault(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := repo.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Workers != 7 || got.Requests != 50 || got.CacheBustParam != "_" {
		t.Fatalf("loaded config wrong: %+v", got)
	}

	// EnsureDefault must NOT overwrite an existing (non-empty) config.
	if err := repo.EnsureDefault(ctx, config.Config{Workers: 99}); err != nil {
		t.Fatalf("re-seed: %v", err)
	}
	if again, _ := repo.Load(ctx); again.Workers != 7 {
		t.Errorf("EnsureDefault overwrote existing config: workers=%d", again.Workers)
	}

	// Watch, then Save a change: the watcher must fire via NOTIFY.
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	changes, err := repo.Watch(watchCtx, config.WatchOptions{})
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

// TestWatchPollFallback checks that a change whose NOTIFY never arrives (trigger
// disabled here; in the wild a transaction-mode pooler or a dropped listener
// connection) is still picked up by polling, and that a NOTIFY-delivered change
// does not fire a second time from the poller. Runs only when DD_TEST_DSN is set.
func TestWatchPollFallback(t *testing.T) {
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

	repo := config.NewRepo(database)
	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("config schema: %v", err)
	}
	tr := config.NewTargetRepo(database)
	if err := tr.EnsureSchema(ctx); err != nil {
		t.Fatalf("urls schema: %v", err)
	}
	// Fingerprint (used by the poller) also reads the proxies table.
	if _, err := database.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS proxies (id bigserial PRIMARY KEY, url text NOT NULL, active boolean NOT NULL DEFAULT true)`); err != nil {
		t.Fatalf("proxies table: %v", err)
	}
	seed := config.Config{Workers: 3, CacheBustParam: "_"}
	if err := repo.Save(ctx, seed); err != nil {
		t.Fatalf("save: %v", err)
	}

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	changes, err := repo.Watch(watchCtx, config.WatchOptions{Poll: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// A NOTIFY-delivered change fires exactly once (the poller re-baselines).
	seed.Workers = 4
	if err := repo.Save(ctx, seed); err != nil {
		t.Fatalf("save: %v", err)
	}
	select {
	case <-changes:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for NOTIFY-delivered change")
	}
	select {
	case <-changes:
		t.Fatal("poller fired a duplicate event for a change NOTIFY already delivered")
	case <-time.After(700 * time.Millisecond):
	}

	// With the trigger off no NOTIFY is sent; polling must still catch the change.
	if _, err := database.ExecContext(ctx, `ALTER TABLE config DISABLE TRIGGER dd_config_notify_trg`); err != nil {
		t.Fatalf("disable trigger: %v", err)
	}
	defer database.ExecContext(ctx, `ALTER TABLE config ENABLE TRIGGER dd_config_notify_trg`)

	seed.Workers = 5
	if err := repo.Save(ctx, seed); err != nil {
		t.Fatalf("save: %v", err)
	}
	select {
	case <-changes:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for poll-detected change")
	}
}

// TestTargetRepo exercises the urls table: schema, seeding (idempotent), and
// loading enabled rows. Runs only when DD_TEST_DSN is set.
func TestTargetRepo(t *testing.T) {
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

	if _, err := database.NewDropTable().Model((*config.Target)(nil)).IfExists().Exec(ctx); err != nil {
		t.Fatalf("drop: %v", err)
	}
	// config schema installs the shared notify function the urls trigger needs.
	if err := config.NewRepo(database).EnsureSchema(ctx); err != nil {
		t.Fatalf("config schema: %v", err)
	}
	tr := config.NewTargetRepo(database)
	if err := tr.EnsureSchema(ctx); err != nil {
		t.Fatalf("urls schema: %v", err)
	}

	if err := tr.Seed(ctx, []string{"https://a.com", "https://b.vn"}, 25); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Seeding again is a no-op for existing URLs.
	if err := tr.Seed(ctx, []string{"https://a.com"}, 999); err != nil {
		t.Fatalf("re-seed: %v", err)
	}

	ts, err := tr.LoadEnabled(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(ts) != 2 {
		t.Fatalf("got %d targets, want 2", len(ts))
	}
	if ts[0].URL != "https://a.com" || ts[0].Method != "GET" || ts[0].Weight != 1 || ts[0].Requests != 25 {
		t.Errorf("first target wrong: %+v", ts[0])
	}
}
