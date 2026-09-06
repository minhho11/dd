package pool

import (
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// cbSeq generates cache-buster values. It is seeded from the wall clock so values
// are large and differ between runs (avoiding cache hits on a previous run's
// values), and incremented atomically so every request in a run is unique even
// under heavy concurrency.
var cbSeq atomic.Uint64

func init() { cbSeq.Store(uint64(time.Now().UnixNano())) }

func cacheBustValue() string { return strconv.FormatUint(cbSeq.Add(1), 10) }

// addCacheBuster appends a unique "param=value" to the URL's query so a caching
// layer (nginx proxy_cache, a CDN) treats each request as distinct and forwards
// it to origin. Existing query parameters and any fragment are preserved; the
// buster is appended (order kept, not re-sorted).
func addCacheBuster(rawURL, param string) string {
	if param == "" {
		param = "_"
	}
	kv := param + "=" + cacheBustValue()

	u, err := url.Parse(rawURL)
	if err != nil {
		sep := "?"
		if strings.Contains(rawURL, "?") {
			sep = "&"
		}
		return rawURL + sep + kv
	}
	if u.RawQuery == "" {
		u.RawQuery = kv
	} else {
		u.RawQuery += "&" + kv
	}
	return u.String()
}
