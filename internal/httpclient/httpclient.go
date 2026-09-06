// Package httpclient builds resty clients and a pool of them, one per proxy, for
// callers to pick from when routing requests through proxies.
package httpclient

import (
	"time"

	"github.com/go-resty/resty/v2"
)

// Config tunes the resty clients produced by the pool.
type Config struct {
	Timeout         time.Duration // per-request timeout
	RetryCount      int           // resty automatic retries
	InsecureTLS     bool          // skip TLS verification (self-signed proxies/targets)
	FollowRedirects bool
	Human           bool   // send realistic browser headers (per-client identity)
	UserAgent       string // override the User-Agent; disables browser-header rotation
}

// DefaultConfig returns sane defaults for a load tool.
func DefaultConfig() Config {
	return Config{
		Timeout:         30 * time.Second,
		RetryCount:      0,
		InsecureTLS:     false,
		FollowRedirects: true,
		Human:           true,
	}
}

// ProxyEndpoint identifies a proxy: its database ID and full URL.
type ProxyEndpoint struct {
	ID  int64
	URL string
}

// Client is a resty client bound to a specific proxy (ProxyID 0 == direct, no proxy).
type Client struct {
	RC       *resty.Client
	ProxyID  int64
	ProxyURL string
}

// Direct reports whether this client makes requests without a proxy.
func (c *Client) Direct() bool { return c.ProxyID == 0 }

// Pool holds one Client per proxy (resty binds a proxy to a client's transport,
// so a client per proxy is how rotation is done). Callers select among them; the
// worker pool picks randomly. With no proxies it holds a single direct client.
//
// resty proxy support: resty.Client.SetProxy(url) configures the underlying
// http.Transport.Proxy; that is what makes proxying work here.
type Pool struct {
	clients []*Client
}

// New builds a Pool, one Client per endpoint. Empty URLs are skipped. If no
// usable proxy remains, a single direct client is created so the tool still runs.
func New(cfg Config, proxies []ProxyEndpoint) *Pool {
	p := &Pool{}

	for _, pe := range proxies {
		if pe.URL == "" {
			continue
		}
		rc := newClient(cfg)
		rc.SetProxy(pe.URL)
		p.clients = append(p.clients, &Client{RC: rc, ProxyID: pe.ID, ProxyURL: pe.URL})
	}

	if len(p.clients) == 0 {
		p.clients = append(p.clients, &Client{RC: newClient(cfg)})
	}

	return p
}

func newClient(cfg Config) *resty.Client {
	c := resty.New().
		SetTimeout(cfg.Timeout).
		SetRetryCount(cfg.RetryCount)

	// Identity: a fixed override, else a random browser profile (stable for this
	// client, so one proxy consistently looks like one user), else the tool's own UA.
	switch {
	case cfg.UserAgent != "":
		c.SetHeader("User-Agent", cfg.UserAgent)
	case cfg.Human:
		c.SetHeaders(RandomProfile().Headers)
	default:
		c.SetHeader("User-Agent", "dd/1.0")
	}

	if cfg.InsecureTLS {
		c.SetTLSClientConfig(&tlsInsecure)
	}
	if !cfg.FollowRedirects {
		c.SetRedirectPolicy(resty.NoRedirectPolicy())
	}
	return c
}

// Clients returns the underlying client slice (read-only use). Callers choose
// among them; the worker pool picks randomly and skips quarantined proxies.
func (p *Pool) Clients() []*Client { return p.clients }

// Size reports how many clients (proxies, or 1 direct) the pool holds.
func (p *Pool) Size() int { return len(p.clients) }
