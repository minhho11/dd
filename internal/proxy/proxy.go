// Package proxy defines the proxy model persisted with bun and helpers to load
// the proxies that requests should be routed through.
package proxy

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

// Proxy is a single proxy endpoint stored in Postgres. URL is a full proxy URL
// understood by resty/net/http, e.g. http://user:pass@host:port or
// socks5://host:port.
type Proxy struct {
	bun.BaseModel `bun:"table:proxies,alias:p"`

	ID        int64     `bun:"id,pk,autoincrement" json:"id"`
	URL       string    `bun:"url,notnull,unique" json:"url"`
	Active    bool      `bun:"active,notnull,default:true" json:"active"`
	CreatedAt time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
}

// Repo reads and writes proxies.
type Repo struct {
	db *bun.DB
}

// NewRepo returns a Repo backed by the given bun handle.
func NewRepo(db *bun.DB) *Repo {
	return &Repo{db: db}
}

// EnsureSchema creates the proxies table if it does not already exist. It is
// safe to call on every startup.
func (r *Repo) EnsureSchema(ctx context.Context) error {
	_, err := r.db.NewCreateTable().
		Model((*Proxy)(nil)).
		IfNotExists().
		Exec(ctx)
	return err
}

// LoadActive returns every active proxy, ordered by id for stable rotation.
func (r *Repo) LoadActive(ctx context.Context) ([]Proxy, error) {
	var proxies []Proxy
	err := r.db.NewSelect().
		Model(&proxies).
		Where("active = ?", true).
		Order("id ASC").
		Scan(ctx)
	if err != nil {
		return nil, err
	}
	return proxies, nil
}

// Add inserts a proxy URL, ignoring duplicates.
func (r *Repo) Add(ctx context.Context, url string) error {
	_, err := r.db.NewInsert().
		Model(&Proxy{URL: url, Active: true}).
		On("CONFLICT (url) DO NOTHING").
		Exec(ctx)
	return err
}

// URLs projects a proxy slice down to its URL strings.
func URLs(proxies []Proxy) []string {
	out := make([]string, len(proxies))
	for i, p := range proxies {
		out[i] = p.URL
	}
	return out
}
