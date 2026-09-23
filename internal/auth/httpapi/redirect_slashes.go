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
}

func newSlashRedirector(patterns []string) *slashRedirector {
	matcher := http.NewServeMux()
	set := make(map[string]bool, len(patterns))
	for _, pattern := range patterns {
		matcher.Handle(pattern, routeMatch{})
		set[pattern] = true
	}
	return &slashRedirector{matcher: matcher, patterns: set}
}

// routeMatch marks a real match in the matcher mux; any other handler it
// returns is net/http's own redirect or not-found handler.
type routeMatch struct{}

func (routeMatch) ServeHTTP(http.ResponseWriter, *http.Request) {}

// matches reports whether path matches a route pattern for some method.
// For its own trailing-slash redirect, net/http's mux returns the target's
// pattern with a redirect handler, so only a routeMatch handler counts.
func (s *slashRedirector) matches(r *http.Request, path string) bool {
	probe := r.Clone(r.Context())
	url := *r.URL
	url.Path, url.RawPath = path, ""
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
	if target == "" || canonicalPath(target) != target {
		return false
	}
	if !s.matches(r, target) {
		return false
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	location := scheme + "://" + r.Host + target
	if r.URL.RawQuery != "" {
		location += "?" + r.URL.RawQuery
	}
	w.Header().Set("Location", pythonparity.Quote(location, redirectLocationSafe))
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusTemporaryRedirect)
	return true
}
