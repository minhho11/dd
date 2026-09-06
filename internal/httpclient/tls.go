package httpclient

import "crypto/tls"

// tlsInsecure is applied when Config.InsecureTLS is set.
var tlsInsecure = tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in via flag
