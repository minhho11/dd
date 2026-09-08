// Package metrics keeps an in-memory per-URL success/fail tally and periodically
// flushes each interval's deltas to the `report` table in Postgres.
package metrics

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/uptrace/bun"
)

// Report is one interval's success/fail tally for a single URL.
type Report struct {
	bun.BaseModel `bun:"table:report,alias:rpt"`

	ID        int64     `bun:"id,pk,autoincrement" json:"id"`
	URL       string    `bun:"url,notnull" json:"url"`
	Success   int64     `bun:"success,notnull" json:"success"`
	Fail      int64     `bun:"fail,notnull" json:"fail"`
	CreatedAt time.Time `bun:"created_at,notnull" json:"created_at"`
}

// Repo persists Report rows.
type Repo struct {
	db *bun.DB
}

// NewRepo returns a Repo backed by the given bun handle.
func NewRepo(db *bun.DB) *Repo { return &Repo{db: db} }

// EnsureSchema creates the report table if needed.
func (r *Repo) EnsureSchema(ctx context.Context) error {
	_, err := r.db.NewCreateTable().Model((*Report)(nil)).IfNotExists().Exec(ctx)
	return err
}

// InsertBatch inserts the rows in one statement.
func (r *Repo) InsertBatch(ctx context.Context, rows []Report) error {
	if len(rows) == 0 {
		return nil
	}
	_, err := r.db.NewInsert().Model(&rows).Exec(ctx)
	return err
}

// counters holds the cumulative tallies plus the marks taken at the last flush,
// so each flush can emit the delta for the interval.
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

// delta drains the per-interval deltas into Report rows and advances the marks.
func (m *Metrics) delta(now time.Time) []Report {
	m.mu.Lock()
	defer m.mu.Unlock()
	var rows []Report
	for u, c := range m.perURL {
		ds := c.success - c.lastSuccess
		df := c.fail - c.lastFail
		if ds == 0 && df == 0 {
			continue
		}
		rows = append(rows, Report{URL: u, Success: ds, Fail: df, CreatedAt: now})
		c.lastSuccess = c.success
		c.lastFail = c.fail
	}
	return rows
}

func (m *Metrics) flush(ctx context.Context, repo *Repo, out io.Writer) {
	rows := m.delta(time.Now())
	if len(rows) == 0 {
		return
	}
	if err := repo.InsertBatch(ctx, rows); err != nil {
		fmt.Fprintln(out, "report insert error:", err)
	}
}

// Run flushes each interval's per-URL deltas to the report table until ctx is
// cancelled, then does a final flush (with a fresh context so it still writes
// while shutting down).
func (m *Metrics) Run(ctx context.Context, repo *Repo, interval time.Duration, out io.Writer) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
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
		}
	}
}
