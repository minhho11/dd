// Package metrics keeps an in-memory per-URL success/fail tally and periodically
// upserts each URL's cumulative totals into a single per-URL row in the `report`
// table in Postgres, updating that one row every interval.
package metrics

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/uptrace/bun"
)

// Report is the single, live row for a single URL: its cumulative success/fail
// tally, updated in place each interval. URL is the primary key, so there is
// exactly one row per URL.
type Report struct {
	bun.BaseModel `bun:"table:report,alias:rpt"`

	URL       string    `bun:"url,pk" json:"url"`
	Success   int64     `bun:"success,notnull" json:"success"`
	Fail      int64     `bun:"fail,notnull" json:"fail"`
	UpdatedAt time.Time `bun:"updated_at,notnull" json:"updated_at"`
}

// Repo persists Report rows.
type Repo struct {
	db *bun.DB
}

// NewRepo returns a Repo backed by the given bun handle.
func NewRepo(db *bun.DB) *Repo { return &Repo{db: db} }

// EnsureSchema creates the report table if needed, and reconciles columns added
// after the table's first creation (no migration tool) with idempotent
// ALTER ... ADD COLUMN IF NOT EXISTS, so an older report table gains them on startup.
func (r *Repo) EnsureSchema(ctx context.Context) error {
	if _, err := r.db.NewCreateTable().Model((*Report)(nil)).IfNotExists().Exec(ctx); err != nil {
		return err
	}
	alters := []string{
		`ALTER TABLE report ADD COLUMN IF NOT EXISTS success bigint NOT NULL DEFAULT 0`,
		`ALTER TABLE report ADD COLUMN IF NOT EXISTS fail bigint NOT NULL DEFAULT 0`,
		`ALTER TABLE report ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now()`,
	}
	for _, a := range alters {
		if _, err := r.db.ExecContext(ctx, a); err != nil {
			return err
		}
	}
	// Upsert relies on a unique key on url (ON CONFLICT (url)). A table created by
	// an older version may lack it; add a unique index so the upsert works. This
	// self-heals only when url has no duplicate rows — if it does, drop the report
	// table (it is a disposable rolling tally) and let it be recreated.
	if _, err := r.db.ExecContext(ctx,
		`CREATE UNIQUE INDEX IF NOT EXISTS report_url_key ON report (url)`); err != nil {
		return fmt.Errorf("ensure report url unique index (drop the report table if it has duplicate url rows): %w", err)
	}
	return nil
}

// Upsert writes each URL's cumulative row, updating the existing row in place on
// a URL conflict, in one statement. This keeps one row per URL that advances over
// time rather than appending a new row per interval.
func (r *Repo) Upsert(ctx context.Context, rows []Report) error {
	if len(rows) == 0 {
		return nil
	}
	_, err := r.db.NewInsert().Model(&rows).
		On("CONFLICT (url) DO UPDATE").
		Set("success = EXCLUDED.success").
		Set("fail = EXCLUDED.fail").
		Set("updated_at = EXCLUDED.updated_at").
		Exec(ctx)
	return err
}

// counters holds the cumulative tallies plus the marks taken at the last flush,
// so each flush can skip URLs that have not changed since it last wrote them.
type counters struct {
	success, fail         int64
	lastSuccess, lastFail int64
}

// Metrics is the in-memory per-URL success/fail variable. Safe for concurrent use.
type Metrics struct {
	mu     sync.Mutex
	perURL map[string]*counters
}

// New returns an empty Metrics.
func New() *Metrics { return &Metrics{perURL: map[string]*counters{}} }

// Record increments the running success or fail count for a URL. Called on every
// request result — cheap, in-memory only.
func (m *Metrics) Record(url string, success bool) {
	m.mu.Lock()
	c := m.perURL[url]
	if c == nil {
		c = &counters{}
		m.perURL[url] = c
	}
	if success {
		c.success++
	} else {
		c.fail++
	}
	m.mu.Unlock()
}

// Totals returns a copy of the cumulative in-memory counts per URL (the live
// variable), as [success, fail].
func (m *Metrics) Totals() map[string][2]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][2]int64, len(m.perURL))
	for u, c := range m.perURL {
		out[u] = [2]int64{c.success, c.fail}
	}
	return out
}

// snapshot returns the cumulative Report row for each URL that has changed since
// the last flush and advances the marks. Emitting cumulative totals (not deltas)
// lets each URL keep a single row that is updated in place.
func (m *Metrics) snapshot(now time.Time) []Report {
	m.mu.Lock()
	defer m.mu.Unlock()
	var rows []Report
	for u, c := range m.perURL {
		if c.success == c.lastSuccess && c.fail == c.lastFail {
			continue
		}
		rows = append(rows, Report{URL: u, Success: c.success, Fail: c.fail, UpdatedAt: now})
		c.lastSuccess = c.success
		c.lastFail = c.fail
	}
	return rows
}

func (m *Metrics) flush(ctx context.Context, repo *Repo, out io.Writer) {
	rows := m.snapshot(time.Now())
	if len(rows) == 0 {
		return
	}
	if err := repo.Upsert(ctx, rows); err != nil {
		fmt.Fprintln(out, "report upsert error:", err)
	}
}

// Run upserts each URL's cumulative totals into its one report row every interval
// until ctx is cancelled, then does a final flush (with a fresh context so it
// still writes while shutting down).
//
// interval is a function so the cadence can be changed live (e.g. from the config
// table): it is re-read after every flush, and a changed value re-arms the ticker.
// A value <=0 is treated as the 30s default. The first value is read immediately;
// if it is <=0 the reporter still runs at 30s (it never disables itself here —
// disabling reporting is the caller's decision).
func (m *Metrics) Run(ctx context.Context, repo *Repo, interval func() time.Duration, out io.Writer) {
	const fallback = 30 * time.Second
	cur := interval()
	if cur <= 0 {
		cur = fallback
	}
	t := time.NewTicker(cur)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			m.flush(fctx, repo, out)
			cancel()
			return
		case <-t.C:
			m.flush(ctx, repo, out)
			next := interval()
			if next <= 0 {
				next = fallback
			}
			if next != cur {
				cur = next
				t.Reset(cur)
				fmt.Fprintf(out, "report interval changed to %s\n", cur)
			}
		}
	}
}
