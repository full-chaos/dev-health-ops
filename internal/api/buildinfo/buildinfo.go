// Package buildinfo serves the api's build identity: GET /buildinfo, the read
// the REST prover binds every receipt to, and the response stamp that names
// the plane and build that answered a request.
//
// The body has the same shape as query-api's /buildinfo. It names only the
// build; a route that answers one question stays one question.
package buildinfo

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/buildstamp"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// Routes returns GET /buildinfo behind any authenticated caller. The
// credential is the api's own access or edge token (what mint-edge-token
// prints), not the effective-principal envelope query-api verifies; a prover
// measuring this service must mint the former. A nil guard
// mounts nothing: with no protected-route runtime the route has no auth to
// stand behind, and an unauthenticated build read is not offered.
func Routes(guard *policy.Guard, info version.Info) []httpapi.Route {
	if guard == nil {
		return nil
	}
	body, err := buildstamp.Body(info)
	if err != nil {
		return nil // six scalar fields; cannot fail
	}
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

// Stamp applies buildstamp.SetProvenance to every response, before the
// handler runs, so a handler that commits its status line first cannot lose
// the headers.
func Stamp(info version.Info) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			buildstamp.SetProvenance(w.Header(), info.Commit)
			next.ServeHTTP(w, r)
		})
	}
}
