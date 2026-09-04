package categorize

import (
	"net/http"
	"strings"
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

// trimBaseURL strips ANY number of trailing slashes from a configured base
// URL, since every call site appends its own leading-slash path segment
// ("/responses", "/chat/completions"). codex round 3 (#2178, bigboy) P2:
// an unnormalized `BaseURL=https://gateway.example/v1/` produced
// `/v1//responses` -- a double slash a strict router can reject -- because
// nothing ever trimmed the operator-configured trailing slash before
// concatenation.
func trimBaseURL(baseURL string) string {
	return strings.TrimRight(baseURL, "/")
}
