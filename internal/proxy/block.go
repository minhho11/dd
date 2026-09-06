package proxy

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/uptrace/bun"
)

// ProxyBlock tracks how a proxy is behaving against a single domain.
//
// Lifecycle:
//   - Each blocked response (403/429/503) increments FailCount for (proxy, domain).
//   - When FailCount reaches the threshold (default 5), BlockedUntil is set to
//     now+window (default 30m) and FailCount is reset to 0. While BlockedUntil is
//     in the future the proxy is not used for that domain.
//   - After the window passes the proxy is eligible again ("probation"). The next
//     request decides its fate: a success deletes the row (cleared), another run of
//     failures re-arms the block.
type ProxyBlock struct {
	bun.BaseModel `bun:"table:proxies_blocked,alias:pb"`

	ID           int64     `bun:"id,pk,autoincrement" json:"id"`
	ProxyID      int64     `bun:"proxy_id,notnull,unique:proxy_domain" json:"proxy_id"`
	Domain       string    `bun:"domain,notnull,unique:proxy_domain" json:"domain"`
	FailCount    int       `bun:"fail_count,notnull,default:0" json:"fail_count"`
	BlockedUntil time.Time `bun:"blocked_until,nullzero" json:"blocked_until"`
	UpdatedAt    time.Time `bun:"updated_at,notnull,default:current_timestamp" json:"updated_at"`
	CreatedAt    time.Time `bun:"created_at,notnull,default:current_timestamp" json:"created_at"`
}

// BlockRepo persists and evaluates proxy blocks.
type BlockRepo struct {
	db        *bun.DB
	threshold int           // consecutive blocks before quarantine
	window    time.Duration // how long a quarantine lasts
}

// NewBlockRepo returns a BlockRepo. threshold<1 falls back to 5, window<=0 to 30m.
func NewBlockRepo(db *bun.DB, threshold int, window time.Duration) *BlockRepo {
	if threshold < 1 {
		threshold = 5
	}
	if window <= 0 {
		window = 30 * time.Minute
	}
	return &BlockRepo{db: db, threshold: threshold, window: window}
}

// EnsureSchema creates the proxies_blocked table if needed.
func (r *BlockRepo) EnsureSchema(ctx context.Context) error {
	_, err := r.db.NewCreateTable().
		Model((*ProxyBlock)(nil)).
		IfNotExists().
		Exec(ctx)
	return err
}

// Available reports whether the proxy may be used for the domain right now, i.e.
// there is no active quarantine (BlockedUntil in the future).
func (r *BlockRepo) Available(ctx context.Context, proxyID int64, domain string) (bool, error) {
	blocked, err := r.db.NewSelect().
		Model((*ProxyBlock)(nil)).
		Where("proxy_id = ? AND domain = ?", proxyID, domain).
		Where("blocked_until IS NOT NULL AND blocked_until > ?", time.Now()).
		Exists(ctx)
	if err != nil {
		return false, err
	}
	return !blocked, nil
}

// OnBlocked records a blocked response. It increments the failure counter and,
// once the threshold is reached, quarantines the proxy for the window. It returns
// the time the quarantine lasts until, or the zero time if the proxy is not (yet)
// quarantined.
func (r *BlockRepo) OnBlocked(ctx context.Context, proxyID int64, domain string) (time.Time, error) {
	now := time.Now()
	row := &ProxyBlock{
		ProxyID:   proxyID,
		Domain:    domain,
		FailCount: 1,
		UpdatedAt: now,
		CreatedAt: now,
	}

	// Upsert: insert with fail_count=1, or bump the existing counter. RETURNING
	// scans the resulting fail_count back into row.
	_, err := r.db.NewInsert().
		Model(row).
		On("CONFLICT (proxy_id, domain) DO UPDATE").
		Set("fail_count = pb.fail_count + 1").
		Set("updated_at = ?", now).
		Returning("fail_count").
		Exec(ctx)
	if err != nil {
		return time.Time{}, err
	}

	if row.FailCount < r.threshold {
		return time.Time{}, nil
	}

	until := now.Add(r.window)
	_, err = r.db.NewUpdate().
		Model((*ProxyBlock)(nil)).
		Set("blocked_until = ?", until).
		Set("fail_count = 0").
		Set("updated_at = ?", now).
		Where("proxy_id = ? AND domain = ?", proxyID, domain).
		Exec(ctx)
	if err != nil {
		return time.Time{}, err
	}
	return until, nil
}

// LoadDomain returns the currently-quarantined blocks for a single domain
// (blocked_until in the future). This is the one query the cache runs per domain
// per TTL window — it never loads the whole table.
func (r *BlockRepo) LoadDomain(ctx context.Context, domain string) ([]ProxyBlock, error) {
	var rows []ProxyBlock
	err := r.db.NewSelect().
		Model(&rows).
		Where("domain = ?", domain).
		Where("blocked_until IS NOT NULL AND blocked_until > ?", time.Now()).
		Scan(ctx)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// Prune deletes dead rows so proxies_blocked does not grow without bound:
// expired quarantines (blocked_until in the past) are always removed, and — when
// stalePartial > 0 — partial-failure rows (fail_count 1..threshold-1, never
// quarantined) whose last update is older than stalePartial. Returns the number
// of rows deleted. Intended to run at startup.
func (r *BlockRepo) Prune(ctx context.Context, stalePartial time.Duration) (int64, error) {
	now := time.Now()
	q := r.db.NewDelete().
		Model((*ProxyBlock)(nil)).
		Where("blocked_until IS NOT NULL AND blocked_until < ?", now)
	if stalePartial > 0 {
		q = q.WhereOr("blocked_until IS NULL AND updated_at < ?", now.Add(-stalePartial))
	}
	res, err := q.Exec(ctx)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// OnSuccess clears any block/failure state for the pair. Called after a proxy
// succeeds against a domain (including the first success after quarantine).
func (r *BlockRepo) OnSuccess(ctx context.Context, proxyID int64, domain string) error {
	_, err := r.db.NewDelete().
		Model((*ProxyBlock)(nil)).
		Where("proxy_id = ? AND domain = ?", proxyID, domain).
		Exec(ctx)
	return err
}

// BlockSource is the persistence the cache reads and writes through. Satisfied by
// *BlockRepo; kept as an interface so the cache can be unit-tested without a DB.
type BlockSource interface {
	LoadDomain(ctx context.Context, domain string) ([]ProxyBlock, error)
	OnBlocked(ctx context.Context, proxyID int64, domain string) (time.Time, error)
	OnSuccess(ctx context.Context, proxyID int64, domain string) error
}

// domainBlocks is the cached set of blocked proxies for one domain.
type domainBlocks struct {
	blocked map[int64]time.Time // proxyID -> blocked_until (active only)
	fetched time.Time
	elem    *list.Element // position in the LRU list
}

// BlockCache is a bounded, lazily-loaded cache over a BlockSource, keyed by
// domain. For each domain it caches the set of currently-blocked proxy IDs,
// fetched with a single query (LoadDomain) and trusted for ttl. This keeps proxy
// selection off the DB hot path while bounding memory:
//
//   - It caches only the domains this run actually touches (your -urls), not the
//     whole proxies_blocked table.
//   - At most maxDomains domains are held; the least-recently-used is evicted, so
//     memory is capped regardless of how large the table or proxy pool grows.
//
// It satisfies the pool's BlockStore interface and is safe for concurrent use.
type BlockCache struct {
	src        BlockSource
	ttl        time.Duration
	maxDomains int

	mu    sync.Mutex
	doms  map[string]*domainBlocks
	order *list.List // MRU at front, LRU at back; values are domain strings
}

// NewBlockCache builds a cache. ttl<=0 defaults to 5s, maxDomains<1 to 1024.
func NewBlockCache(src BlockSource, ttl time.Duration, maxDomains int) *BlockCache {
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	if maxDomains < 1 {
		maxDomains = 1024
	}
	return &BlockCache{
		src:        src,
		ttl:        ttl,
		maxDomains: maxDomains,
		doms:       make(map[string]*domainBlocks),
		order:      list.New(),
	}
}

// Available reports whether the proxy may be used for the domain. It serves fresh
// cache entries from memory; on a miss or stale entry it loads that one domain
// from the source. On a load error it fails open (returns available) so a
// transient DB hiccup never stalls the run.
func (c *BlockCache) Available(ctx context.Context, proxyID int64, domain string) (bool, error) {
	now := time.Now()

	c.mu.Lock()
	if de, ok := c.doms[domain]; ok && now.Sub(de.fetched) < c.ttl {
		until, blocked := de.blocked[proxyID]
		c.order.MoveToFront(de.elem)
		c.mu.Unlock()
		return !(blocked && until.After(now)), nil
	}
	c.mu.Unlock()

	rows, err := c.src.LoadDomain(ctx, domain) // no lock held during the query
	if err != nil {
		return true, nil
	}
	set := make(map[int64]time.Time, len(rows))
	for _, r := range rows {
		set[r.ProxyID] = r.BlockedUntil
	}

	c.mu.Lock()
	c.storeLocked(domain, set, now)
	until, blocked := set[proxyID]
	c.mu.Unlock()
	return !(blocked && until.After(now)), nil
}

// storeLocked inserts/updates a domain entry and evicts the LRU past the cap.
func (c *BlockCache) storeLocked(domain string, set map[int64]time.Time, now time.Time) {
	if de, ok := c.doms[domain]; ok {
		de.blocked = set
		de.fetched = now
		c.order.MoveToFront(de.elem)
		return
	}
	de := &domainBlocks{blocked: set, fetched: now}
	de.elem = c.order.PushFront(domain)
	c.doms[domain] = de

	for len(c.doms) > c.maxDomains {
		back := c.order.Back()
		if back == nil {
			break
		}
		c.order.Remove(back)
		delete(c.doms, back.Value.(string))
	}
}

// OnBlocked writes through to the source and, if the proxy just became
// quarantined and the domain is cached, records it immediately.
func (c *BlockCache) OnBlocked(ctx context.Context, proxyID int64, domain string) error {
	until, err := c.src.OnBlocked(ctx, proxyID, domain)
	if err != nil {
		return err
	}
	if until.IsZero() {
		return nil
	}
	c.mu.Lock()
	if de, ok := c.doms[domain]; ok {
		de.blocked[proxyID] = until
		c.order.MoveToFront(de.elem)
	}
	c.mu.Unlock()
	return nil
}

// OnSuccess clears the pair in the source and drops it from the cached set.
func (c *BlockCache) OnSuccess(ctx context.Context, proxyID int64, domain string) error {
	err := c.src.OnSuccess(ctx, proxyID, domain)
	c.mu.Lock()
	if de, ok := c.doms[domain]; ok {
		delete(de.blocked, proxyID)
		c.order.MoveToFront(de.elem)
	}
	c.mu.Unlock()
	return err
}
