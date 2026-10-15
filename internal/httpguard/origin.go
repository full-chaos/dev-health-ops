package httpguard

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The one origin rule of every Go client that sends a credential and may be sent somewhere by the server (a redirect
// Location, a Link rel="next" URL, a cursor that is a URL): a request leaves the credential's origin when its scheme,
// host or port differs from the origin the credential was configured for. Scheme counts, so https to http is a
// departure. A subdomain, a sibling host and the same host on another port are other origins (Go's own rule keeps
// Authorization for a subdomain and drops only Authorization and Cookie, never PRIVATE-TOKEN or X-Api-Key).
//
// What a client does when a hop leaves the origin is its own choice: refuse it (a client whose contract needs the same
// origin) or follow it with every credential header removed (StripCredentials). Either way it calls RecordLeftOrigin,
// so a departure is one warn line and one count, never silent.

// Origin is where a request goes: scheme, host and port, lower-cased, with the scheme's default port dropped.
// Userinfo, path, query and fragment are not part of it. The zero value means "no origin" (a nil URL or an empty host);
// it equals no other origin, itself included, so a check that cannot compute an origin fails closed.
type Origin struct{ scheme, host, port string }

// OriginOf is the origin of u (the zero Origin for a nil URL or a URL without a host).
func OriginOf(u *url.URL) Origin {
	if u == nil || u.Hostname() == "" {
		return Origin{}
	}
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	if number, err := strconv.Atoi(port); err == nil {
		port = strconv.Itoa(number)
	}
	if (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
		port = ""
	}
	return Origin{scheme: scheme, host: strings.ToLower(u.Hostname()), port: port}
}

// IsZero reports whether o is "no origin".
func (o Origin) IsZero() bool { return o == Origin{} }

// String is scheme://host[:port], safe to log: it never holds userinfo, path or query.
func (o Origin) String() string {
	if o.IsZero() {
		return ""
	}
	if o.port == "" {
		return o.scheme + "://" + o.host
	}
	return o.scheme + "://" + o.host + ":" + o.port
}

// SameOrigin reports whether a and b are one origin. Two URLs with no origin are not the same.
func SameOrigin(a, b *url.URL) bool {
	oa := OriginOf(a)
	return !oa.IsZero() && oa == OriginOf(b)
}

// CredentialHeaders are the request headers that carry a credential.
var CredentialHeaders = []string{"Authorization", "Proxy-Authorization", "Cookie", "Private-Token", "X-Api-Key", "X-Auth-Token"}

// StripCredentials removes every credential header from req.
func StripCredentials(req *http.Request) {
	for _, name := range CredentialHeaders {
		req.Header.Del(name)
	}
}

// Actions of RecordLeftOrigin.
const (
	ActionRefused  = "refused"
	ActionStripped = "stripped"
)

const leftOriginCounterName = "credential_origin_departures_total"

var leftOriginCounter = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/httpguard").Int64Counter(
		leftOriginCounterName,
		metric.WithDescription("Requests a server sent to another origin than the credential's (redirect, next link), by what the client did and where."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(leftOriginCounterName)
	}
	return counter
}()

// RecordLeftOrigin logs at warn level (origins only, never a path, query or credential) and counts one hop that leaves
// the credential's origin. action is ActionRefused or ActionStripped; site is the caller's own fixed label
// (for example "provider.redirect"), so the label set stays bounded.
func RecordLeftOrigin(ctx context.Context, action, site string, from, to *url.URL) {
	if ctx == nil {
		ctx = context.Background()
	}
	fromOrigin, toOrigin := OriginOf(from), OriginOf(to)
	slog.WarnContext(ctx, "credential origin departure",
		"action", action, "site", site, "from_origin", fromOrigin.String(), "to_origin", toOrigin.String(),
		"scheme_downgrade", fromOrigin.scheme == "https" && toOrigin.scheme == "http")
	leftOriginCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("action", action), attribute.String("site", site)))
}
