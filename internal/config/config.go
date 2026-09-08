// Package config stores the tool's run parameters in Postgres so they can be
// changed at runtime. A single-row `config` table holds the parameters; a
// trigger fires NOTIFY on every change and Watch turns those into events, so a
// running process can reload and restart with the new settings.
package config

import (
	"context"
	"strings"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/pgdriver"
)

// NotifyChannel is the Postgres NOTIFY channel the config trigger fires on.
const NotifyChannel = "config_changed"

// Config is the single-row (id=1) set of run parameters. Fields mirror the CLI
// flags that control *what to hit and how hard*; operational plumbing (dsn,
// block/proxy internals, output files) stays on the CLI.
type Config struct {
	bun.BaseModel `bun:"table:config,alias:cfg"`

	ID             int64     `bun:"id,pk" json:"id"`
	URLs           string    `bun:"urls,notnull" json:"urls"` // comma-separated
	Workers        int       `bun:"workers,notnull" json:"workers"`
	Requests       int       `bun:"requests,notnull" json:"requests"` // 0 = run until all proxies blocked
	Retries        int       `bun:"retries,notnull" json:"retries"`
	RPS            float64   `bun:"rps,notnull" json:"rps"`
	TimeoutSeconds int       `bun:"timeout_seconds,notnull" json:"timeout_seconds"`
	CacheBust      bool      `bun:"cache_bust,notnull" json:"cache_bust"`
	CacheBustParam string    `bun:"cache_bust_param,notnull" json:"cache_bust_param"`
	Human          bool      `bun:"human,notnull" json:"human"`
	UserAgent      string    `bun:"user_agent,notnull" json:"user_agent"`
	Insecure       bool      `bun:"insecure,notnull" json:"insecure"`
	UpdatedAt      time.Time `bun:"updated_at,notnull,default:current_timestamp" json:"updated_at"`
}

// Timeout returns the per-request timeout.
func (c Config) Timeout() time.Duration { return time.Duration(c.TimeoutSeconds) * time.Second }

// URLList splits URLs into a trimmed, non-empty slice.
func (c Config) URLList() []string {
	var out []string
	for _, p := range strings.Split(c.URLs, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Repo reads/writes the config row and watches for changes.
type Repo struct {
	db *bun.DB
}

// NewRepo returns a Repo backed by the given bun handle.
func NewRepo(db *bun.DB) *Repo { return &Repo{db: db} }

// EnsureSchema creates the config table and the NOTIFY trigger (idempotent).
func (r *Repo) EnsureSchema(ctx context.Context) error {
	if _, err := r.db.NewCreateTable().Model((*Config)(nil)).IfNotExists().Exec(ctx); err != nil {
		return err
	}
	stmts := []string{
		`CREATE OR REPLACE FUNCTION dd_config_notify() RETURNS trigger AS $$
		 BEGIN PERFORM pg_notify('` + NotifyChannel + `', ''); RETURN NEW; END;
		 $$ LANGUAGE plpgsql`,
		`DROP TRIGGER IF EXISTS dd_config_notify_trg ON config`,
		`CREATE TRIGGER dd_config_notify_trg AFTER INSERT OR UPDATE ON config
		 FOR EACH ROW EXECUTE FUNCTION dd_config_notify()`,
	}
	for _, s := range stmts {
		if _, err := r.db.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// EnsureDefault inserts the given config as row id=1 if no row exists yet. An
// existing row is left untouched (the DB is the source of truth once seeded).
func (r *Repo) EnsureDefault(ctx context.Context, def Config) error {
	def.ID = 1
	if def.UpdatedAt.IsZero() {
		def.UpdatedAt = time.Now()
	}
	_, err := r.db.NewInsert().
		Model(&def).
		On("CONFLICT (id) DO NOTHING").
		Exec(ctx)
	return err
}

// Load reads the config row.
func (r *Repo) Load(ctx context.Context) (Config, error) {
	c := Config{}
	err := r.db.NewSelect().Model(&c).Where("id = 1").Limit(1).Scan(ctx)
	return c, err
}

// Save upserts the config row (id=1) and bumps updated_at. The trigger fires a
// NOTIFY so any watcher reloads. Useful for tooling/tests.
func (r *Repo) Save(ctx context.Context, c Config) error {
	c.ID = 1
	c.UpdatedAt = time.Now()
	_, err := r.db.NewInsert().
		Model(&c).
		On("CONFLICT (id) DO UPDATE").
		Set("urls = EXCLUDED.urls").
		Set("workers = EXCLUDED.workers").
		Set("requests = EXCLUDED.requests").
		Set("retries = EXCLUDED.retries").
		Set("rps = EXCLUDED.rps").
		Set("timeout_seconds = EXCLUDED.timeout_seconds").
		Set("cache_bust = EXCLUDED.cache_bust").
		Set("cache_bust_param = EXCLUDED.cache_bust_param").
		Set("human = EXCLUDED.human").
		Set("user_agent = EXCLUDED.user_agent").
		Set("insecure = EXCLUDED.insecure").
		Set("updated_at = EXCLUDED.updated_at").
		Exec(ctx)
	return err
}

// Watch returns a channel that receives an event whenever the config row
// changes (coalesced: bursts collapse to one pending event). The channel closes
// when ctx is cancelled. It uses Postgres LISTEN/NOTIFY on a dedicated connection.
func (r *Repo) Watch(ctx context.Context) (<-chan struct{}, error) {
	ln := pgdriver.NewListener(r.db)
	if err := ln.Listen(ctx, NotifyChannel); err != nil {
		return nil, err
	}

	out := make(chan struct{}, 1)
	go func() {
		defer close(out)
		defer func() { _ = ln.Close() }()
		notes := ln.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-notes:
				if !ok {
					return
				}
				select {
				case out <- struct{}{}: // signal
				default: // already pending; coalesce
				}
			}
		}
	}()
	return out, nil
}
