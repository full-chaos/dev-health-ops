package llmorgsettings

import (
	"net/url"
	"regexp"
)

// ProviderClef is the provider name of Cloudflare Workers AI clef, a SystemOne
// decision model. It answers typed questions and writes no text, so it is known
// to the categorize purpose only (CheckProviderFor): a default, investigate or
// explain row that names it cannot serve and is refused.
const ProviderClef = "clef"

// clefHost is the one host a clef request may go to, compared byte for byte.
const clefHost = "api.cloudflare.com"

// clefBasePath is the account path a saved base_url must have: 32 lowercase
// hex characters for the account id, then /ai, with at most one trailing slash.
var clefBasePath = regexp.MustCompile(`\A/client/v4/accounts/([0-9a-f]{32})/ai/?\z`)

// clefModelSegments is the closed map from a saved model to the path segment of
// the POST URL. A model that is not a key is refused; the stored text is never
// joined into a path.
var clefModelSegments = map[string]string{"clef": "clef", "clef-flash": "clef-flash"}

// ClefModelSegment returns the URL path segment of a saved clef model.
func ClefModelSegment(model string) (string, bool) {
	segment, ok := clefModelSegments[model]
	return segment, ok
}

// ParseClefBaseURL reads the account id out of a saved clef base_url. It
// accepts https://api.cloudflare.com/client/v4/accounts/<32 hex>/ai and refuses
// everything else: another scheme or host (uppercase, trailing dot, port, IP
// literal, look-alike), user info, a query or fragment (also an empty one), an
// escaped or doubled slash, or any other path.
func ParseClefBaseURL(raw string) (accountID string, ok bool) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Opaque != "" || parsed.User != nil ||
		parsed.Host != clefHost || parsed.Hostname() != clefHost || parsed.Port() != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" {
		return "", false
	}
	if parsed.EscapedPath() != parsed.Path {
		return "", false
	}
	match := clefBasePath.FindStringSubmatch(parsed.Path)
	if match == nil {
		return "", false
	}
	return match[1], true
}

// ClefPostURL is the one URL a clef request is sent to: the account of the
// saved base_url and the segment of the saved model. ok is false when either
// is not acceptable.
func ClefPostURL(baseURL, model string) (string, bool) {
	account, ok := ParseClefBaseURL(baseURL)
	if !ok {
		return "", false
	}
	segment, ok := ClefModelSegment(model)
	if !ok {
		return "", false
	}
	return "https://" + clefHost + "/client/v4/accounts/" + account + "/ai/run/@cf/cloudflare/" + segment, true
}

// Endpoint types an LLM attempt row can record.
const (
	EndpointTypeWorkersAI = "workers_ai"
	EndpointTypeAIGateway = "ai_gateway"
)

// ClefEndpoint derives the endpoint type and the gateway id of a saved clef
// base_url, for the attempt row. It only reads the URL ParseClefBaseURL accepts:
// that is a Workers AI account URL, which holds no gateway id. It never changes
// where a request goes.
func ClefEndpoint(baseURL string) (endpointType, gatewayID string, ok bool) {
	if _, ok := ParseClefBaseURL(baseURL); !ok {
		return "", "", false
	}
	return EndpointTypeWorkersAI, "", true
}
