// Package config stores the tool's run parameters in Postgres so they can be
// changed at runtime. The `config` table is a key/value metadata store holding
// the run-level parameters (how hard to hit); the targets — what to hit, and
// with what method/params/weight — live in the `urls` table (see target.go). A
// trigger fires NOTIFY on every change to either table and Watch turns those into
// events, so a running process can reload and restart with the new settings.
package config

import (
	"context"
	"strconv"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/pgdriver"
)

// NotifyChannel is the Postgres NOTIFY channel the config/urls triggers fire on.
const NotifyChannel = "config_changed"

// Setting is one key/value row of the `config` metadata table.
type Setting struct {
	bun.BaseModel `bun:"table:config,alias:cfg"`

	Key   string `bun:"key,pk" json:"key"`
	Value string `bun:"value,notnull" json:"value"`
}

// Config is the parsed set of run-level parameters, assembled from the key/value
// rows. Fields mirror the CLI flags that control *how hard to hit*; the targets
// (urls) and operational plumbing (dsn, block/proxy internals, output files) live
// elsewhere. Requests here is the global default used to seed new target rows and
// as the fallback for direct (no-proxy) runs.
type Config struct {
	Workers        int
	Requests       int
	Retries        int
	RPS            float64
	TimeoutSeconds int
	CacheBust      bool
	CacheBustParam string
	Human          bool
	UserAgent      string
	Insecure       bool
	BrowserDebug   bool
}

// config keys, stable across versions.
const (
	keyWorkers        = "workers"
	keyRequests       = "requests"
	keyRetries        = "retries"
	keyRPS            = "rps"
	keyTimeoutSeconds = "timeout_seconds"
	keyCacheBust      = "cache_bust"
	keyCacheBustParam = "cache_bust_param"
	keyHuman          = "human"
	keyUserAgent      = "user_agent"
	keyInsecure       = "insecure"
	keyBrowserDebug   = "browser_debug"
)

// Timeout returns the per-request timeout.
func (c Config) Timeout() time.Duration { return time.Duration(c.TimeoutSeconds) * time.Second }

// toSettings encodes a Config as the full key/value set.
func (c Config) toSettings() []Setting {
	return []Setting{
		{Key: keyWorkers, Value: strconv.Itoa(c.Workers)},
		{Key: keyRequests, Value: strconv.Itoa(c.Requests)},
		{Key: keyRetries, Value: strconv.Itoa(c.Retries)},
		{Key: keyRPS, Value: strconv.FormatFloat(c.RPS, 'g', -1, 64)},
		{Key: keyTimeoutSeconds, Value: strconv.Itoa(c.TimeoutSeconds)},
		{Key: keyCacheBust, Value: strconv.FormatBool(c.CacheBust)},
		{Key: keyCacheBustParam, Value: c.CacheBustParam},
		{Key: keyHuman, Value: strconv.FormatBool(c.Human)},
		{Key: keyUserAgent, Value: c.UserAgent},
		{Key: keyInsecure, Value: strconv.FormatBool(c.Insecure)},
		{Key: keyBrowserDebug, Value: strconv.FormatBool(c.BrowserDebug)},
	}
}

// configFromSettings decodes a key/value map into a Config, tolerating missing or
// malformed values by leaving the zero value in place.
func configFromSettings(kv map[string]string) Config {
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	atof := func(s string) float64 { f, _ := strconv.ParseFloat(s, 64); return f }
	atob := func(s string) bool { b, _ := strconv.ParseBool(s); return b }
	return Config{
		Workers:        atoi(kv[keyWorkers]),
		Requests:       atoi(kv[keyRequests]),
		Retries:        atoi(kv[keyRetries]),
		RPS:            atof(kv[keyRPS]),
		TimeoutSeconds: atoi(kv[keyTimeoutSeconds]),
		CacheBust:      atob(kv[keyCacheBust]),
		CacheBustParam: kv[keyCacheBustParam],
		Human:          atob(kv[keyHuman]),
		UserAgent:      kv[keyUserAgent],
		Insecure:       atob(kv[keyInsecure]),
		BrowserDebug:   atob(kv[keyBrowserDebug]),
	}
}

// Repo reads/writes the config key/value rows and watches for changes.
type Repo struct {
	db *bun.DB
}

// NewRepo returns a Repo backed by the given bun handle.
func NewRepo(db *bun.DB) *Repo { return &Repo{db: db} }

// EnsureSchema creates the config table and the NOTIFY trigger (idempotent). The
// notify function it installs is shared with the urls table (see TargetRepo).
func (r *Repo) EnsureSchema(ctx context.Context) error {
	if _, err := r.db.NewCreateTable().Model((*Setting)(nil)).IfNotExists().Exec(ctx); err != nil {
		return err
	}
	stmts := []string{
		`CREATE OR REPLACE FUNCTION dd_config_notify() RETURNS trigger AS $$
		 BEGIN PERFORM pg_notify('` + NotifyChannel + `', ''); RETURN NEW; END;
		 $$ LANGUAGE plpgsql`,
		`DROP TRIGGER IF EXISTS dd_config_notify_trg ON config`,
		`CREATE TRIGGER dd_config_notify_trg AFTER INSERT OR UPDATE OR DELETE ON config
		 FOR EACH ROW EXECUTE FUNCTION dd_config_notify()`,
	}
	for _, s := range stmts {
		if _, err := r.db.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// EnsureDefault seeds the key/value rows from def if the table is empty. An
// existing (non-empty) config is left untouched — the DB is the source of truth
// once seeded.
func (r *Repo) EnsureDefault(ctx context.Context, def Config) error {
	n, err := r.db.NewSelect().Model((*Setting)(nil)).Count(ctx)
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	rows := def.toSettings()
	_, err = r.db.NewInsert().Model(&rows).On("CONFLICT (key) DO NOTHING").Exec(ctx)
	return err
}

// Load reads all key/value rows into a Config.
func (r *Repo) Load(ctx context.Context) (Config, error) {
	var rows []Setting
	if err := r.db.NewSelect().Model(&rows).Scan(ctx); err != nil {
		return Config{}, err
	}
	kv := make(map[string]string, len(rows))
	for _, s := range rows {
		kv[s.Key] = s.Value
	}
	return configFromSettings(kv), nil
}

// Save upserts every key/value row. The trigger fires a NOTIFY so any watcher
// reloads. Useful for tooling/tests.
func (r *Repo) Save(ctx context.Context, c Config) error {
	rows := c.toSettings()
	_, err := r.db.NewInsert().
		Model(&rows).
		On("CONFLICT (key) DO UPDATE").
		Set("value = EXCLUDED.value").
		Exec(ctx)
	return err
}

// Watch returns a channel that receives an event whenever the config or urls
// tables change (coalesced: bursts collapse to one pending event). The channel
// closes when ctx is cancelled. It uses Postgres LISTEN/NOTIFY on a dedicated
// connection.
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
