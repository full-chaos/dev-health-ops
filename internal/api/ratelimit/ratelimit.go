// Package ratelimit is the client address the Python api's slowapi limiter
// keys on (api/middleware/rate_limit.py get_forwarded_ip). The counting
// itself is httpapi.KeyedLimiter, the one keyed limiter.
package ratelimit

import (
	"net"
	"net/http"
	"os"
	"strings"
)

// ForwardedIP is get_forwarded_ip: the TCP peer, unless the peer is listed
// in TRUSTED_PROXIES (read on every call, as os.getenv is) and the request
// carries X-Forwarded-For, in which case the header's first hop, stripped.
func ForwardedIP(r *http.Request) string {
	peer := "unknown"
	if r.RemoteAddr != "" {
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			peer = host
		} else {
			peer = r.RemoteAddr
		}
	}
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded == "" {
		return peer
	}
	if !trustedProxies()[peer] {
		return peer
	}
	first := strings.SplitN(forwarded, ",", 2)[0]
	return strings.TrimSpace(first)
}

func trustedProxies() map[string]bool {
	set := make(map[string]bool)
	for _, part := range strings.Split(os.Getenv("TRUSTED_PROXIES"), ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			set[trimmed] = true
		}
	}
	return set
}
