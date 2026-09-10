package config

import (
	"context"

	"github.com/uptrace/bun"
)

// Target is one row of the `urls` table: a URL to hit, its mode ('http' or
// 'browser'), its HTTP method, JSON params, a relative weight (how large a share
// of the worker pool this URL gets — normalized at runtime), and its own request
// count (0 = run until every proxy is blocked). Disabled rows are skipped.
//
// Params meaning depends on Mode:
//   - mode='http':    a JSON object of request params (query for GET/HEAD/DELETE,
//     request body for POST/PUT/PATCH), values may contain {{...}} generators
//     expanded per request (see internal/tmpl).
//   - mode='browser': a JSON flow {"steps":[...]} run in a headless Chromium
//     against URL — navigate, fill/submit a form, wait, assert (see
//     internal/browser); step values support the same {{...}} generators. Method
//     is ignored. A request count of 1 runs the flow once (a single automation
//     test); N repeats it under load.
type Target struct {
	bun.BaseModel `bun:"table:urls,alias:u"`

	ID       int64   `bun:"id,pk,autoincrement" json:"id"`
	URL      string  `bun:"url,notnull,unique" json:"url"`
	Mode     string  `bun:"mode,notnull,default:'http'" json:"mode"` // 'http' or 'browser'
	Method   string  `bun:"method,notnull,default:'GET'" json:"method"`
	Params   string  `bun:"params,notnull,default:''" json:"params"` // JSON object, may be empty
	Weight   float64 `bun:"weight,notnull,default:1" json:"weight"`
	Requests int     `bun:"requests,notnull,default:0" json:"requests"` // 0 = until blocked
	Enabled  bool    `bun:"enabled,notnull,default:true" json:"enabled"`
}

// TargetRepo reads/writes the `urls` table.
type TargetRepo struct {
	db *bun.DB
}

// NewTargetRepo returns a TargetRepo backed by the given bun handle.
func NewTargetRepo(db *bun.DB) *TargetRepo { return &TargetRepo{db: db} }

// EnsureSchema creates the urls table and a NOTIFY trigger that fires the shared
// config-change channel, so adding or editing a target restarts a watching run.
// It relies on the dd_config_notify() function installed by Repo.EnsureSchema,
// which must be called first; it re-creates that function defensively so it is
// safe on its own too.
func (r *TargetRepo) EnsureSchema(ctx context.Context) error {
	if _, err := r.db.NewCreateTable().Model((*Target)(nil)).IfNotExists().Exec(ctx); err != nil {
		return err
	}
	// Add columns introduced after the table's first creation (no migration tool);
	// idempotent so it is safe on every startup.
	if _, err := r.db.ExecContext(ctx,
		`ALTER TABLE urls ADD COLUMN IF NOT EXISTS mode text NOT NULL DEFAULT 'http'`); err != nil {
		return err
	}
	stmts := []string{
		`CREATE OR REPLACE FUNCTION dd_config_notify() RETURNS trigger AS $$
		 BEGIN PERFORM pg_notify('` + NotifyChannel + `', ''); RETURN NEW; END;
		 $$ LANGUAGE plpgsql`,
		`DROP TRIGGER IF EXISTS dd_urls_notify_trg ON urls`,
		`CREATE TRIGGER dd_urls_notify_trg AFTER INSERT OR UPDATE OR DELETE ON urls
		 FOR EACH ROW EXECUTE FUNCTION dd_config_notify()`,
	}
	for _, s := range stmts {
		if _, err := r.db.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// LoadEnabled returns every enabled target, ordered by id for stable allocation.
func (r *TargetRepo) LoadEnabled(ctx context.Context) ([]Target, error) {
	var ts []Target
	err := r.db.NewSelect().Model(&ts).Where("enabled = ?", true).Order("id ASC").Scan(ctx)
	if err != nil {
		return nil, err
	}
	return ts, nil
}

// Seed inserts a GET target for each URL that does not already exist, using
// defaultRequests as the per-URL count and weight 1. It bootstraps the urls table
// from the CLI -urls list on first run; existing rows are left untouched.
func (r *TargetRepo) Seed(ctx context.Context, urls []string, defaultRequests int) error {
	if len(urls) == 0 {
		return nil
	}
	rows := make([]Target, 0, len(urls))
	for _, u := range urls {
		rows = append(rows, Target{
			URL:      u,
			Mode:     "http",
			Method:   "GET",
			Params:   "",
			Weight:   1,
			Requests: defaultRequests,
			Enabled:  true,
		})
	}
	_, err := r.db.NewInsert().Model(&rows).On("CONFLICT (url) DO NOTHING").Exec(ctx)
	return err
}
