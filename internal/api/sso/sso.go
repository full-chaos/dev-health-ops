// Package sso serves the enterprise SSO routes of api/auth/sso/router.py
// under /api/v1/auth.
//
// Every route but activate and deactivate is gated by
// @require_feature("sso_saml", required_tier="enterprise"). None of those
// route functions takes a `session` or `org_id` keyword argument, so the
// per-org check (_check_org_feature_async) never runs and only the process
// license decides: has_feature("sso_saml") on the process LicenseManager.
// The Go api runs without a process license (licensing.ProcessTier), and
// sso_saml is not a community feature, so each gated route answers exactly
// what the Python api answers there: its authentication, then its query and
// body validation, then the 402. The SAML, OIDC and OAuth flows behind the
// gate are not reachable on either plane and are not ported.
//
// Activate and deactivate are not gated: they are ported in full.
package sso

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// ssoFeature and ssoRequiredTier are the gate's arguments on every route.
const ssoFeature = "sso_saml"

var ssoRequiredTier = "enterprise"

// Deps are the SSO routes' dependencies.
type Deps struct {
	Pool   *pgxpool.Pool
	Guard  *policy.Guard
	Logger *slog.Logger
	// Now stamps updated_at; nil means time.Now.
	Now func() time.Time
	// Write renders the 404 and 405 the two-segment /oauth dispatcher
	// answers; nil means httpapi.WriteError.
	Write httpapi.ErrorWriter
}

type handlers struct{ Deps }

// Routes mounts the seventeen SSO routes. It returns nothing unless the
// protected-route runtime is configured.
func Routes(deps Deps) []httpapi.Route {
	if deps.Pool == nil || deps.Guard == nil {
		return nil
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Write == nil {
		deps.Write = httpapi.WriteError
	}
	h := handlers{deps}
	g := deps.Guard
	const prefix = "/api/v1/auth"
	route := func(method, path string, handler http.Handler) httpapi.Route {
		return httpapi.Route{Method: method, Pattern: prefix + path, Handler: handler}
	}
	routes := []httpapi.Route{
		route(http.MethodGet, "/sso/providers", g.Wrap(policy.Authenticated, h.gated(listQuery))),
		route(http.MethodPost, "/sso/providers", g.BodyFirst(policy.Authenticated, h.gated(body(ssoProviderCreate)))),
		route(http.MethodGet, "/sso/providers/{provider_id}", g.Wrap(policy.Authenticated, h.gated(noInput))),
		route(http.MethodPatch, "/sso/providers/{provider_id}", g.BodyFirst(policy.Authenticated, h.gated(body(ssoProviderUpdate)))),
		route(http.MethodDelete, "/sso/providers/{provider_id}", g.Wrap(policy.Authenticated, h.gated(noInput))),
		route(http.MethodPost, "/sso/providers/{provider_id}/activate", g.Wrap(policy.Authenticated, h.setStatus("active"))),
		route(http.MethodPost, "/sso/providers/{provider_id}/deactivate", g.Wrap(policy.Authenticated, h.setStatus("inactive"))),
		route(http.MethodGet, "/saml/{provider_id}/metadata", g.Wrap(policy.Public, h.gated(noInput))),
		route(http.MethodPost, "/saml/{provider_id}/initiate", g.BodyFirst(policy.Public, h.gated(body(samlAuthRequest)))),
		route(http.MethodPost, "/saml/{provider_id}/acs", g.BodyFirst(policy.Public, h.gated(body(samlCallbackRequest)))),
		route(http.MethodPost, "/oidc/{provider_id}/authorize", g.BodyFirst(policy.Public, h.gated(body(oidcAuthRequest)))),
		route(http.MethodPost, "/oidc/{provider_id}/callback", g.BodyFirst(policy.Public, h.gated(body(oidcCallbackRequest)))),
		route(http.MethodPost, "/oauth/providers", g.BodyFirst(policy.Authenticated, h.gated(body(oauthProviderCreate)))),
		// PATCH /oauth/providers/{provider_id}, POST /oauth/{provider_id}/authorize,
		// POST /oauth/{provider_id}/callback and GET /oauth/{provider_type}/authorize
		// overlap (/oauth/providers/authorize matches three of them), which
		// net/http refuses to register; one pattern dispatches them as
		// Starlette does (oauthPair).
	}
	pair := h.oauthPair()
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions} {
		// Every route it dispatches declares a response_model.
		routes = append(routes, httpapi.Route{Method: method, Pattern: prefix + "/oauth/{first}/{second}", Handler: pair,
			ResponseModelFor: func(*http.Request) bool { return true }})
	}
	return routes
}

// gated is a gated route: FastAPI validates the query and body (422), then
// the decorator checks the process license (402). Without a process license
// the check always refuses; were a future process tier to hold sso_saml, the
// route would have to be ported first, so it fails loudly instead.
func (h handlers) gated(validate validator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var errs pybody.Errors
		validate(r, &errs)
		if len(errs) > 0 {
			policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
			return
		}
		if !licensing.ProcessHasFeature(ssoFeature) {
			// has_feature logs the denial as a license audit event (a log
			// line only; nothing is stored).
			h.Logger.WarnContext(r.Context(), "License audit: feature_access_denied",
				"feature", ssoFeature, "current_tier", licensing.ProcessTier, "path", r.URL.Path)
			policy.WriteDetail(w, http.StatusPaymentRequired, licensing.FeatureNotLicensedDetail(ssoFeature, &ssoRequiredTier), nil)
			return
		}
		h.Logger.ErrorContext(r.Context(), "api sso: the process tier grants sso_saml, but the licensed SSO flows are not served by the Go api",
			"path", r.URL.Path)
		policy.WriteInternal(w)
	})
}

// pairRoute is one Python route under /oauth/{first}/{second}, in the order
// router.py declares it.
type pairRoute struct {
	method  string
	matches func(first, second string) bool
	handler http.Handler
}

// oauthPair serves every two-segment /oauth path as Starlette's router does:
// the first route whose path and method both match serves it; else the
// first route whose path matches answers 405 with its own method as Allow
// (FastAPI routes do not add HEAD); else 404.
func (h handlers) oauthPair() http.Handler {
	g := h.Guard
	routes := []pairRoute{
		{http.MethodPatch, func(first, _ string) bool { return first == "providers" },
			g.BodyFirst(policy.Authenticated, h.gated(body(oauthProviderUpdate)))},
		{http.MethodPost, func(_, second string) bool { return second == "authorize" },
			g.BodyFirst(policy.Public, h.gated(body(oauthAuthRequest)))},
		{http.MethodPost, func(_, second string) bool { return second == "callback" },
			g.BodyFirst(policy.Public, h.gated(body(oauthCallbackRequest)))},
		{http.MethodGet, func(_, second string) bool { return second == "authorize" },
			g.Wrap(policy.Public, h.gated(byTypeQuery))},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first, second := r.PathValue("first"), r.PathValue("second")
		var partial *pairRoute
		for index := range routes {
			candidate := &routes[index]
			if !candidate.matches(first, second) {
				continue
			}
			if candidate.method == r.Method {
				candidate.handler.ServeHTTP(w, r)
				return
			}
			if partial == nil {
				partial = candidate
			}
		}
		if partial != nil {
			w.Header().Set("Allow", partial.method)
			h.Write(w, r, httpapi.CodeMethodNotAllowed)
			return
		}
		h.Write(w, r, httpapi.CodeNotFound)
	})
}
