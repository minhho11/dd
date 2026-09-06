package httpclient

import "math/rand/v2"

// BrowserProfile is a coherent set of request headers that mimic a real browser.
// The headers within one profile agree with each other (User-Agent matches the
// Sec-CH-UA client hints and platform) so the request looks like it came from
// that browser rather than an obvious tool.
//
// Note: only header *values* are mimicked here. A determined detector can still
// fingerprint the Go HTTP/TLS stack (header order/casing, JA3, HTTP/2 settings),
// which these headers do not change — useful to know when building defenses.
//
// Accept-Encoding is deliberately omitted: if we set it, Go's transport stops
// transparently decompressing gzip, which would break body-based block detection.
type BrowserProfile struct {
	Name    string
	Headers map[string]string
}

// browserProfiles are a handful of current desktop browsers across platforms.
var browserProfiles = []BrowserProfile{
	{
		Name: "chrome-win",
		Headers: map[string]string{
			"User-Agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
			"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
			"Accept-Language":           "en-US,en;q=0.9",
			"Sec-Ch-Ua":                 `"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"`,
			"Sec-Ch-Ua-Mobile":          "?0",
			"Sec-Ch-Ua-Platform":        `"Windows"`,
			"Sec-Fetch-Dest":            "document",
			"Sec-Fetch-Mode":            "navigate",
			"Sec-Fetch-Site":            "none",
			"Sec-Fetch-User":            "?1",
			"Upgrade-Insecure-Requests": "1",
		},
	},
	{
		Name: "chrome-mac",
		Headers: map[string]string{
			"User-Agent":                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
			"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
			"Accept-Language":           "en-US,en;q=0.9",
			"Sec-Ch-Ua":                 `"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"`,
			"Sec-Ch-Ua-Mobile":          "?0",
			"Sec-Ch-Ua-Platform":        `"macOS"`,
			"Sec-Fetch-Dest":            "document",
			"Sec-Fetch-Mode":            "navigate",
			"Sec-Fetch-Site":            "none",
			"Sec-Fetch-User":            "?1",
			"Upgrade-Insecure-Requests": "1",
		},
	},
	{
		Name: "edge-win",
		Headers: map[string]string{
			"User-Agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
			"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
			"Accept-Language":           "en-US,en;q=0.9",
			"Sec-Ch-Ua":                 `"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"`,
			"Sec-Ch-Ua-Mobile":          "?0",
			"Sec-Ch-Ua-Platform":        `"Windows"`,
			"Sec-Fetch-Dest":            "document",
			"Sec-Fetch-Mode":            "navigate",
			"Sec-Fetch-Site":            "none",
			"Sec-Fetch-User":            "?1",
			"Upgrade-Insecure-Requests": "1",
		},
	},
	{
		Name: "firefox-win",
		Headers: map[string]string{
			"User-Agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
			"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
			"Accept-Language":           "en-US,en;q=0.5",
			"Sec-Fetch-Dest":            "document",
			"Sec-Fetch-Mode":            "navigate",
			"Sec-Fetch-Site":            "none",
			"Sec-Fetch-User":            "?1",
			"Upgrade-Insecure-Requests": "1",
		},
	},
	{
		Name: "safari-mac",
		Headers: map[string]string{
			"User-Agent":                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
			"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
			"Accept-Language":           "en-US,en;q=0.9",
			"Upgrade-Insecure-Requests": "1",
		},
	},
}

// RandomProfile returns a random browser profile. Safe for concurrent use.
func RandomProfile() BrowserProfile {
	return browserProfiles[rand.IntN(len(browserProfiles))]
}
