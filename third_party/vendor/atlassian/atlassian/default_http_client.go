package atlassian

import (
	"net/http"
	"time"
)

func refuseRedirects(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// noProxyTransport is the transport of NewDefaultHTTPClient: http.DefaultTransport's settings with no proxy from the
// environment, so the credential reaches only the host the request names. Local modification, patch 0009.
var noProxyTransport = func() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return transport
}()

// NewDefaultHTTPClient is the client every call in this module builds when its caller supplied none: the given timeout and NO redirect
// followed (a 3xx is returned as the response). Local modification, patch 0005 (CHAOS-7921): the upstream defaults were a plain
// `&http.Client{Timeout: ...}`, which follows up to ten redirects and re-sends custom headers to a new host. Production supplies its own
// guarded client; this default only matters when none is supplied, so a caller that needs a cross-host redirect must supply a guarded
// client. Patch 0009: it never dials through a proxy from the environment (HTTP_PROXY, HTTPS_PROXY).
func NewDefaultHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: noProxyTransport, CheckRedirect: refuseRedirects}
}
