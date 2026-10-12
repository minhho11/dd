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
	Workers               int
	Requests              int
	Retries               int
	RPS                   float64
	TimeoutSeconds        int
	CacheBust             bool
	CacheBustParam        string
	Human                 bool
	UserAgent             string
	Insecure              bool
	ReportIntervalSeconds int // how often the report table is upserted; <=0 uses 30s
	BrowserDebug          bool
	BrowserMax            int  // max concurrent browser (Chromium) flows; <=0 = unlimited
	BrowserReuse          bool // reuse one browser across direct flow runs instead of relaunching
	BrowserBlock          bool // block fonts/media/third-party trackers in browser flows
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
	keyReportInterval = "report_interval_seconds"
	keyBrowserDebug   = "browser_debug"
	keyBrowserMax     = "browser_max"
	keyBrowserReuse   = "browser_reuse"
	keyBrowserBlock   = "browser_block"
)

// Timeout returns the per-request timeout.
func (c Config) Timeout() time.Duration { return time.Duration(c.TimeoutSeconds) * time.Second }

// ReportInterval returns how often the report table is upserted; a non-positive
// stored value falls back to 30s.
func (c Config) ReportInterval() time.Duration {
	if c.ReportIntervalSeconds <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.ReportIntervalSeconds) * time.Second
}

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
		{Key: keyReportInterval, Value: strconv.Itoa(c.ReportIntervalSeconds)},
		{Key: keyBrowserDebug, Value: strconv.FormatBool(c.BrowserDebug)},
		{Key: keyBrowserMax, Value: strconv.Itoa(c.BrowserMax)},
		{Key: keyBrowserReuse, Value: strconv.FormatBool(c.BrowserReuse)},
		{Key: keyBrowserBlock, Value: strconv.FormatBool(c.BrowserBlock)},
	}
}

// configFromSettings decodes a key/value map into a Config, tolerating missing or
// malformed values by leaving the zero value in place.
func configFromSettings(kv map[string]string) Config {
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	atof := func(s string) float64 { f, _ := strconv.ParseFloat(s, 64); return f }
	atob := func(s string) bool { b, _ := strconv.ParseBool(s); return b }
	return Config{
		Workers:               atoi(kv[keyWorkers]),
		Requests:              atoi(kv[keyRequests]),
		Retries:               atoi(kv[keyRetries]),
		RPS:                   atof(kv[keyRPS]),
		TimeoutSeconds:        atoi(kv[keyTimeoutSeconds]),
		CacheBust:             atob(kv[keyCacheBust]),
		CacheBustParam:        kv[keyCacheBustParam],
		Human:                 atob(kv[keyHuman]),
		UserAgent:             kv[keyUserAgent],
		Insecure:              atob(kv[keyInsecure]),
		ReportIntervalSeconds: atoi(kv[keyReportInterval]),
		BrowserDebug:          atob(kv[keyBrowserDebug]),
		BrowserMax:            atoi(kv[keyBrowserMax]),
		BrowserReuse:          atob(kv[keyBrowserReuse]),
		BrowserBlock:          atob(kv[keyBrowserBlock]),
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

// EnsureDefault seeds the key/value rows from def, inserting only keys that are
// missing (ON CONFLICT DO NOTHING): the CLI defaults populate a fresh table on
// first run, and any *new* keys added in a later version get seeded on the next
// startup, while existing rows keep their values (the DB stays the source of truth
// for anything already set).
func (r *Repo) EnsureDefault(ctx context.Context, def Config) error {
	rows := def.toSettings()
	_, err := r.db.NewInsert().Model(&rows).On("CONFLICT (key) DO NOTHING").Exec(ctx)
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

// Fingerprint returns a hash of the full contents of the config, urls, and proxies
// tables. It changes whenever any row in any of them changes, so Watch can poll it
// to detect changes whose NOTIFY never arrived (proxies included, since adding a
// proxy also restarts the run).
func (r *Repo) Fingerprint(ctx context.Context) (string, error) {
	var fp string
	err := r.db.QueryRowContext(ctx, `SELECT md5(
		coalesce((SELECT string_agg(key || '=' || value, E'\n' ORDER BY key) FROM config), '')
		|| E'\n--urls--\n' ||
		coalesce((SELECT string_agg(u::text, E'\n' ORDER BY u.id) FROM urls u), '')
		|| E'\n--proxies--\n' ||
		coalesce((SELECT string_agg(p.id::text || ':' || p.url || ':' || p.active::text, E'\n' ORDER BY p.id) FROM proxies p), ''))`).Scan(&fp)
	return fp, err
}

// WatchOptions tunes Watch.
type WatchOptions struct {
	// Poll re-reads the tables' Fingerprint every Poll and signals when it changed:
	// the fallback for NOTIFYs that never arrive. A transaction-mode pooler
	// (PgBouncer, Supabase :6543, Neon -pooler) silently swallows LISTEN, and a
	// dropped listener connection misses every change until it reconnects. <=0
	// disables polling (NOTIFY only).
	Poll time.Duration
	// Logf, if set, receives watcher diagnostics (NOTIFY not delivered, a change
	// caught only by polling).
	Logf func(format string, args ...any)
}

// probePayload marks the self-test NOTIFY Watch sends at startup to check that
// notifications actually reach the listener. Trigger NOTIFYs carry an empty payload.
const probePayload = "dd:probe"

// probeTimeout is how long Watch waits for its startup probe before warning that
// LISTEN/NOTIFY is not being delivered.
const probeTimeout = 5 * time.Second

// Watch returns a channel that receives an event whenever the config or urls
// tables change (coalesced: bursts collapse to one pending event). The channel
// closes when ctx is cancelled. It uses Postgres LISTEN/NOTIFY on a dedicated
// connection, backed by polling the tables' Fingerprint (opts.Poll) so a change
// is still picked up when the NOTIFY is lost.
func (r *Repo) Watch(ctx context.Context, opts WatchOptions) (<-chan struct{}, error) {
	logf := opts.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	ln := pgdriver.NewListener(r.db)
	if err := ln.Listen(ctx, NotifyChannel); err != nil {
		return nil, err
	}
	notes := ln.Channel()

	// Baseline before returning, so a change committed after the caller's first
	// Load is always seen as a difference by the poller.
	var last string
	haveBaseline := false
	if opts.Poll > 0 {
		if fp, err := r.Fingerprint(ctx); err == nil {
			last, haveBaseline = fp, true
		}
	}

	// Self-test: if this never comes back, NOTIFY isn't reaching us.
	if _, err := r.db.ExecContext(ctx, "SELECT pg_notify(?, ?)", NotifyChannel, probePayload); err != nil {
		logf("watch: could not send NOTIFY probe: %v", err)
	}

	out := make(chan struct{}, 1)
	go func() {
		defer close(out)
		defer func() { _ = ln.Close() }()

		signal := func() {
			select {
			case out <- struct{}{}:
			default: // already pending; coalesce
			}
		}

		var tick <-chan time.Time
		if opts.Poll > 0 {
			t := time.NewTicker(opts.Poll)
			defer t.Stop()
			tick = t.C
		}
		probe := time.NewTimer(probeTimeout)
		defer probe.Stop()
		probeC := probe.C
		pollFailing := false

		for {
			select {
			case <-ctx.Done():
				return
			case n, ok := <-notes:
				if !ok {
					return
				}
				if n.Payload == probePayload {
					probeC = nil // delivered: LISTEN/NOTIFY works
					continue
				}
				// Re-baseline so the poller doesn't fire again for this same change.
				if opts.Poll > 0 {
					if fp, err := r.Fingerprint(ctx); err == nil {
						last, haveBaseline = fp, true
					}
				}
				signal()
			case <-probeC:
				probeC = nil
				if opts.Poll > 0 {
					logf("watch: warning: LISTEN/NOTIFY not delivered within %s (DSN through a transaction-mode pooler such as PgBouncer / Supabase :6543 / Neon -pooler?) — changes will be picked up by polling every %s", probeTimeout, opts.Poll)
				} else {
					logf("watch: warning: LISTEN/NOTIFY not delivered within %s (DSN through a transaction-mode pooler such as PgBouncer / Supabase :6543 / Neon -pooler?) and polling is off — config/urls changes will NOT restart the run", probeTimeout)
				}
			case <-tick:
				fp, err := r.Fingerprint(ctx)
				if err != nil {
					if !pollFailing && ctx.Err() == nil {
						logf("watch: poll failed: %v", err)
					}
					pollFailing = true
					continue
				}
				pollFailing = false
				if !haveBaseline {
					last, haveBaseline = fp, true
					continue
				}
				if fp != last {
					last = fp
					logf("watch: config/urls change detected by polling (its NOTIFY was not received)")
					signal()
				}
			}
		}
	}()
	return out, nil
}
