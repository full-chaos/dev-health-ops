// Package buildinfo serves the api's build identity: GET /buildinfo, the read
// the REST prover binds every receipt to, and the response stamp that names
// the plane and build that answered a request.
//
// The body has the same shape as query-api's /buildinfo. It names only the
// build; a route that answers one question stays one question.
package buildinfo

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// Header names the prover reads from every candidate response.
const (
	PlaneHeader = "x-dev-health-plane"
	BuildHeader = "x-dev-health-build"
)

// Routes returns GET /buildinfo behind any authenticated caller. A nil guard
// mounts nothing: with no protected-route runtime the route has no auth to
// stand behind, and an unauthenticated build read is not offered.
func Routes(guard *policy.Guard, info version.Info) []httpapi.Route {
	if guard == nil {
		return nil
	}
	body, err := json.Marshal(info)
	if err != nil {
		return nil // six scalar fields; cannot fail
	}
	body = append(body, '\n')
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	return []httpapi.Route{{
		Method: http.MethodGet, Pattern: "/buildinfo", Allow: http.MethodGet,
		Handler: guard.Wrap(policy.Authenticated, handler),
	}}
}

// Stamp sets x-dev-health-plane: go on every response and, when info names a
// real build, x-dev-health-build. Both are set before the handler runs, so a
// handler that commits its status line first cannot lose them.
func Stamp(info version.Info) func(http.Handler) http.Handler {
	commit := strings.TrimSpace(info.Commit)
	known := commit != "" && commit != "unknown"
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(PlaneHeader, "go")
			if known {
				w.Header().Set(BuildHeader, commit)
			}
			next.ServeHTTP(w, r)
		})
	}
}
