package atlassian

import (
	"net/http"
	"time"
)

func refuseRedirects(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// NewDefaultHTTPClient is the client every call in this module builds when its caller supplied none: the given timeout and NO redirect
// followed (a 3xx is returned as the response). Local modification, patch 0005 (CHAOS-7921): the upstream defaults were a plain
// `&http.Client{Timeout: ...}`, which follows up to ten redirects and re-sends custom headers to a new host. Production supplies its own
// guarded client; this default only matters when none is supplied, so a caller that needs a cross-host redirect must supply a guarded
// client.
func NewDefaultHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, CheckRedirect: refuseRedirects}
}
