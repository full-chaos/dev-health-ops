// Package sso serves the enterprise SSO routes of api/auth/sso/router.py
// under /api/v1/auth.
//
// Every route but activate/deactivate and the two OIDC routes below is
// gated by @require_feature("sso_saml", required_tier="enterprise"). None
// of those route functions takes a `session` or `org_id` keyword argument,
// so the per-org check (_check_org_feature_async) never runs and only the
// process license decides: has_feature("sso_saml") on the process
// LicenseManager (licensing.ProcessHasFeature, verified at start-up by
// processlicense.Install; CHAOS-6663). Without a process license granting
// sso_saml, each gated route answers exactly what the Python api answers
// there: its authentication, then its query and body validation, then the
// 402. With one, the flows behind that gate are not ported here, so gated()
// fails loudly (500) instead of pretending to serve them.
//
// Activate and deactivate are not gated: they are ported in full.
//
// initiateOIDCAuth and oidcCallback (CHAOS-6658) are also ported in full,
// with a REAL entitlement gate (requireEntitlement, gate.go): D2725 ruled
// that Python's dead per-org fallback above is a delta to fix, not a
// parity target, for these two routes specifically. See oidc.go's doc
// comment for the rest of what that PR changed and why.
package sso

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/credentials"
	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
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

	// The following four are used only by initiateOIDCAuth and
	// oidcCallback (CHAOS-6658); the other 15 routes are still the
	// license-shim gated() serves and need none of them.

	// Cipher decrypts sso_providers.encrypted_secrets' client_secret the
	// way core.encryption.decrypt_value does; nil or unconfigured leaves
	// a stored secret as-is, matching _decrypt_secret's legacy-plaintext
	// fallback, which also runs when decryption itself fails. Unrelated
	// to the OIDC state cipher below; a route can run for real without
	// this ever being configured.
	Cipher credentials.Cipher
	// StateSecret derives (via HKDF, state.go) the AES-256-GCM key that
	// AEAD-encrypts the opaque OIDC state value (D2727-amended): the
	// api's own JWT_SECRET_KEY -- the literal value
	// apiservice/service.go passes is deps.GitHubStateSigner.Secret, no
	// new secret material. Routes does not mount the real OIDC handlers
	// without it, since an empty secret can mint no state a callback
	// could ever verify.
	StateSecret string
	// Signer mints the SSOLoginResponse token pair on a successful
	// callback: the same edgetoken.Signer newProtection built from the
	// api's own JWT_SECRET_KEY.
	Signer *edgetoken.Signer
	// HTTPClient reaches the IdP's discovery document, token endpoint,
	// JWKS and userinfo endpoint; nil means a client over
	// externalurl.GuardedTransport() (the SSRF guard credentials.Routes
	// already uses for admin-configured URLs -- an OIDC issuer is exactly
	// that kind of URL).
	HTTPClient *http.Client
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
	if deps.HTTPClient == nil {
		deps.HTTPClient = defaultOIDCClient()
	}
	h := handlers{deps}
	g := deps.Guard
	const prefix = "/api/v1/auth"
	route := func(method, path string, handler http.Handler) httpapi.Route {
		return httpapi.Route{Method: method, Pattern: prefix + path, Handler: handler}
	}
	// initiateOIDCAuth and oidcCallback need StateSecret (the AEAD opaque
	// state token, state.go) and Signer (the login token pair) to run
	// for real; a caller that wires neither -- an existing Deps literal
	// from before this PR, or a deliberately license-shimmed deployment
	// -- gets the same process-tier-only shim the other 15 routes still
	// answer, rather than a route that mints a state no callback could
	// ever decrypt.
	oidcAuthorize := h.gated(body(oidcAuthRequest))
	oidcCallbackHandler := h.gated(body(oidcCallbackRequest))
	if deps.StateSecret != "" && deps.Signer != nil {
		oidcAuthorize = http.HandlerFunc(h.initiateOIDCAuth)
		oidcCallbackHandler = http.HandlerFunc(h.oidcCallback)
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
		route(http.MethodPost, "/oidc/{provider_id}/authorize", g.BodyFirst(policy.Public, oidcAuthorize)),
		route(http.MethodPost, "/oidc/{provider_id}/callback", g.BodyFirst(policy.Public, oidcCallbackHandler)),
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
				"feature", ssoFeature, "current_tier", licensing.ProcessTier(), "path", r.URL.Path)
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
