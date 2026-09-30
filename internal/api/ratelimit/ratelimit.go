// Package ratelimit is the client address the Python api's slowapi limiter
// keys on (api/middleware/rate_limit.py get_forwarded_ip). The counting
// itself is httpapi.KeyedLimiter, the one keyed limiter.
package ratelimit

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/clientip"
)

// ForwardedIP is the rate-limit client address: get_forwarded_ip's shape (the
// TCP peer, or the forwarded client when the peer is in TRUSTED_PROXIES), but
// the forwarded client is the RIGHTMOST untrusted X-Forwarded-For hop, never the
// leftmost, which a client can write (CHAOS-7204). The rule lives in
// clientip.FromRequest; this only adds the "unknown" key for a request with no
// peer address, as slowapi does.
func ForwardedIP(r *http.Request) string {
	if ip := clientip.FromRequest(r); ip != "" {
		return ip
	}
	return "unknown"
}
