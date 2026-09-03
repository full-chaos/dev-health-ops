package categorize

import (
	"net/http"
	"time"
)

// hardenedHTTPTimeout is llm/providers/_http.py's make_hardened_*_client
// timeout=60.0.
const hardenedHTTPTimeout = 60 * time.Second

// newHardenedHTTPClient ports llm/providers/_http.py's
// make_hardened_httpx2_client: no redirects (a provider that 30x's a POST
// silently drops or mutates the request body on most clients), no
// environment-derived proxy (trust_env=False -- a provider's request must
// not be captured by an operator's ambient HTTP_PROXY/HTTPS_PROXY), and a
// bounded timeout so a hung connection cannot pin a worker indefinitely.
func newHardenedHTTPClient() *http.Client {
	return &http.Client{
		Timeout: hardenedHTTPTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			Proxy: nil,
		},
	}
}
