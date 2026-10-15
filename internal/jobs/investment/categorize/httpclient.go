package categorize

import (
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/externalurl"
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
// orgHTTPClient picks the client for an org BYO provider. When the org
// supplied a base_url, the client's transport is the SSRF-guarded one: the
// address each connection actually dials is checked against the internal
// ranges (loopback, RFC1918, link-local incl. 169.254.169.254, unique-local,
// multicast, unspecified) and refused before any byte -- the bearer key
// included -- is written. The org's stored base_url is validated once when
// read, but the resolver may answer differently at dial time (DNS
// rebinding); this closes that gap (CHAOS-2558). With no base_url the
// provider's own default endpoint is not customer-controlled and the plain
// hardened client is used (nil here selects it).
//
// transport nil means the guarded transport; a caller (a test against a
// loopback server) may pass its own.
func orgHTTPClient(baseURL string, transport http.RoundTripper) *http.Client {
	if strings.TrimSpace(baseURL) == "" {
		return nil
	}
	client := newHardenedHTTPClient()
	if transport == nil {
		transport = externalurl.GuardedTransport()
	}
	client.Transport = transport
	return client
}

func trimBaseURL(baseURL string) string {
	return strings.TrimRight(baseURL, "/")
}
