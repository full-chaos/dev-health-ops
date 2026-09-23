package httpapi

import (
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// redirectLocationSafe is the safe set Starlette's RedirectResponse passes
// to urllib.parse.quote for its Location header.
const redirectLocationSafe = ":/%#?=@[]!$&'()*+,;"

// slashRedirector is Starlette's Router redirect_slashes: a request no route
// matches, whose path with its trailing slash toggled (all trailing slashes
// removed, or one added) matches a route for any method, is answered 307
// with that URL in Location and no body.
type slashRedirector struct {
	matcher  *http.ServeMux
	patterns map[string]bool
	trust    ForwardedTrust
}

func newSlashRedirector(patterns []string, trust ForwardedTrust) *slashRedirector {
	matcher := http.NewServeMux()
	set := make(map[string]bool, len(patterns))
	for _, pattern := range patterns {
		matcher.Handle(pattern, routeMatch{})
		set[pattern] = true
	}
	return &slashRedirector{matcher: matcher, patterns: set, trust: trust}
}

// routeMatch marks a real match in the matcher mux; any other handler it
// returns is net/http's own redirect or not-found handler.
type routeMatch struct{}

func (routeMatch) ServeHTTP(http.ResponseWriter, *http.Request) {}

// matches reports whether the decoded path matches a route pattern for some
// method, as Starlette matches scope["path"] literally. For its own
// trailing-slash redirect, net/http's mux returns the target's pattern with a
// redirect handler, so only a routeMatch handler counts.
//
// The mux cleans the escaped path before matching, so a "." or ".." segment
// in the decoded path (from "%2e%2e", say) would be resolved away or
// redirected. The probe's escaped path carries such segments percent-encoded:
// the mux leaves them in place and unescapes them for a wildcard, which is
// what Starlette's "[^/]+" does with the decoded segment.
func (s *slashRedirector) matches(r *http.Request, path string) bool {
	probe := r.Clone(r.Context())
	url := *r.URL
	url.Path, url.RawPath = path, ""
	url.RawPath = escapeDotSegments(url.EscapedPath())
	probe.URL = &url
	handler, pattern := s.matcher.Handler(probe)
	_, isMatch := handler.(routeMatch)
	return isMatch && s.patterns[pattern]
}

// wrap answers every request whose path matches no route before net/http's
// mux sees it (so the mux's own trailing-slash redirect never runs): the
// Starlette 307 when the toggled path matches, else notFound.
func (s *slashRedirector) wrap(mux http.Handler, notFound http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.matches(r, r.URL.Path) {
			notFound(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// redirect answers the 307 and reports true when r's toggled path matches
// a route; otherwise it writes nothing.
func (s *slashRedirector) redirect(w http.ResponseWriter, r *http.Request) bool {
	current := r.URL.Path
	if current == "/" || current == "" {
		return false
	}
	target := current + "/"
	if strings.HasSuffix(current, "/") {
		target = strings.TrimRight(current, "/")
	}
	if target == "" || !s.matches(r, target) {
		return false
	}
	// uvicorn builds scope["path"] with urllib.parse.unquote, which decodes
	// the percent-escapes as UTF-8 with errors="replace"; Starlette quotes
	// that str back, so an invalid sequence reaches Location as %EF%BF%BD.
	location := s.trust.scheme(r) + "://" + r.Host + pythonparity.DecodeUTF8Replace(target)
	if r.URL.RawQuery != "" {
		location += "?" + r.URL.RawQuery
	}
	w.Header().Set("Location", pythonparity.Quote(location, redirectLocationSafe))
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusTemporaryRedirect)
	return true
}

// escapeDotSegments percent-encodes every "." and ".." segment of an escaped
// path. Escaping never yields "%2E" itself, so the result still unescapes to
// the same decoded path.
func escapeDotSegments(escaped string) string {
	segments := strings.Split(escaped, "/")
	for index, segment := range segments {
		switch segment {
		case ".":
			segments[index] = "%2E"
		case "..":
			segments[index] = "%2E%2E"
		}
	}
	return strings.Join(segments, "/")
}
