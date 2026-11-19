// Package sso serves the enterprise SSO routes of api/auth/sso/router.py
// under /api/v1/auth.
//
// Every route but activate/deactivate and the eight OIDC/SAML/OAuth routes
// below is gated by @require_feature("sso_saml", required_tier="enterprise").
// None of those route functions takes a `session` or `org_id` keyword
// argument, so in Python only the process license decided (has_feature on
// the LicenseManager; the per-org _check_org_feature_async never ran). The
// Go api's process license (CHAOS-6663, licensing.ProcessTier) is community
// unless LICENSE_KEY/LICENSE_PUBLIC_KEY are set and valid, and sso_saml is
// not a community feature. While auth-service owns the providers
// (Deps.Providers), the five provider CRUD routes also take the caller
// org's enterprise entitlement (providerGate, gate.go): product rule C1,
// "Enterprise SSO != Enterprise self hosted" (R459). Otherwise they keep
// the process-only gate and the fail-loud stub (gated), as do the sign-in
// shims (authentication, validation, then the 402, as Python answered). The
// OAuth provider create and patch routes are retired: no service writes an
// oauth_* row, so go-api mounts neither (not-found); existing rows sign in.
//
// Activate and deactivate are not gated: they are ported in full.
//
// initiateOIDCAuth/oidcCallback (CHAOS-6658) and initiateOAuthAuth/
// oauthCallback/initiateOAuthByType (CHAOS-6986) are ported in full, each
// with a REAL entitlement gate (requireEntitlement, gate.go): D2725 ruled
// that Python's dead per-org fallback above is a delta to fix, not a
// parity target, for these five routes specifically. See oidc.go's and
// oauth.go's own doc comments for what each PR changed and why. The three
// SAML routes are forward-only: Inner sends them to auth-service, and
// without the forward a configured sign-in answers 503 naming AUTH_PROXY_SAML.
package sso

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/credentials"
	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
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
	// Write renders the 404 and 405 of the two-segment /oauth dispatcher; nil means httpapi.WriteError.
	Write httpapi.ErrorWriter

	// The following four are used only by initiateOIDCAuth and oidcCallback (CHAOS-6658); the other 15
	// routes are still the license-shim gated() serves and need none of them.

	// Cipher decrypts sso_providers.encrypted_secrets' client_secret the
	// way core.encryption.decrypt_value does; nil or unconfigured leaves
	// a stored secret as-is, matching _decrypt_secret's legacy-plaintext
	// fallback, which also runs when decryption itself fails. Unrelated
	// to the OIDC state cipher below; a route can run for real without
	// this ever being configured.
	Cipher credentials.Cipher
	// StateSecret derives (via HKDF, state.go) the AES-256-GCM key that
	// AEAD-encrypts the opaque OIDC state value (D2727-amended):
	// SSO_STATE_SECRET only (apiservice.statePurposeSecrets), never the
	// same key as any other purpose. Without it the eight sign-in routes
	// (OIDC, SAML, OAuth) answer 503 config_missing, since an empty secret
	// can mint no state a callback could ever verify; the process still
	// starts (D6465).
	StateSecret string
	// HTTPClient reaches the IdP's discovery document, token endpoint,
	// JWKS and userinfo endpoint, and the social OAuth providers; nil means
	// defaultOIDCClient (externalurl.IdPTransport) for the OIDC calls and a
	// client over externalurl.GuardedTransport() (the SSRF guard
	// credentials.Routes already uses for admin-configured URLs) for the
	// social OAuth calls.
	HTTPClient *http.Client
	// Inner, when set, replaces the innermost handler of the sign-in routes
	// (OIDC authorize/callback, the OAuth pair, SAML metadata/initiate/acs); the guard
	// and its body checks in front stay. It gets the full route pattern; nil
	// serves local.
	Inner func(pattern string, local http.Handler) http.Handler
	// Issuer, set when auth-service owns sessions (AUTH_PROXY_SESSIONS), issues the SSO login's
	// token pair; go-api mints none and writes no refresh_tokens row. nil: the sign-in routes answer 503
	// naming AUTH_PROXY_SESSIONS.
	Issuer sessionissue.Issuer
	// PlatformRoles is how the sign-in reads take is_superuser (AUTH_PLATFORM_ROLES_OWNED).
	PlatformRoles policy.PlatformRoleRead
	// Providers is the client of auth-service's SSO provider routes while it owns the providers: the five provider CRUD
	// routes and activate/deactivate call it. nil keeps the stub and the direct UPDATE.
	Providers authprovision.SSOProviders
	// OrgEntitled reports whether the organization holds sso_saml; nil is licensing.OrgHasFeature over Pool.
	OrgEntitled func(ctx context.Context, orgID string) (bool, error)

	// oidcClient makes the OIDC calls; newHandlers sets it (HTTPClient when the caller set one).
	oidcClient *http.Client
}

type handlers struct{ Deps }

// Routes mounts the fifteen SSO routes. It returns nothing unless the
// protected-route runtime is configured.
func Routes(deps Deps) []httpapi.Route {
	if deps.Pool == nil || deps.Guard == nil {
		return nil
	}
	h := newHandlers(deps)
	deps = h.Deps
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
	oidcAuthorize := h.orStateMissing(h.gated(body(oidcAuthRequest)))
	oidcCallbackHandler := h.orStateMissing(h.gated(body(oidcCallbackRequest)))
	if h.signIn() {
		oidcAuthorize = h.refuseOIDCNotForwarded(body(oidcAuthRequest))
		oidcCallbackHandler = h.refuseOIDCNotForwarded(body(oidcCallbackRequest))
	}
	// The three SAML routes have no local sign-in in go-api: auth-service
	// serves them, and Inner forwards them while AUTH_PROXY_SAML is on (the
	// forward replaces the handler below). With sign-in configured
	// (StateSecret and Issuer) and no forward, each route still checks its
	// body (422) and then answers 503 config_missing naming AUTH_PROXY_SAML;
	// it reads no provider row. Without StateSecret the routes keep the same
	// process-tier-only shim as before, and with sessions off they answer
	// refuseSessionsOff, as the OIDC routes do.
	samlMetadataHandler := h.orStateMissing(h.gated(noInput))
	samlInitiate := h.orStateMissing(h.gated(body(samlAuthRequest)))
	samlACS := h.orStateMissing(h.gated(body(samlCallbackRequest)))
	if h.signIn() {
		samlMetadataHandler = h.refuseSAMLNotForwarded(noInput)
		samlInitiate = h.refuseSAMLNotForwarded(body(samlAuthRequest))
		samlACS = h.refuseSAMLNotForwarded(body(samlCallbackRequest))
	}
	if h.sessionsOff() {
		samlMetadataHandler, samlInitiate, samlACS = http.HandlerFunc(h.refuseSessionsOff), http.HandlerFunc(h.refuseSessionsOff), http.HandlerFunc(h.refuseSessionsOff)
		oidcAuthorize, oidcCallbackHandler = http.HandlerFunc(h.refuseSessionsOff), http.HandlerFunc(h.refuseSessionsOff)
	}
	inner := func(path string, local http.Handler) http.Handler {
		if deps.Inner == nil {
			return local
		}
		return deps.Inner(prefix+path, local)
	}
	// A provider route is wrapped with its own 422 and org-aware 402 as the guard's late checks when auth-service owns the providers: with
	// the sso namespace enforced, a refused caller gets the 422 or the 402 first, as the legacy route answers it (sso.go gated; the
	// role check is the handler's own, router.py create :262, patch :333, delete :383).
	providerWrap := func(validate validator, serve http.HandlerFunc) http.Handler {
		if h.Providers != nil {
			return g.WrapLate(policy.Authenticated, h.providerGate(validate), serve)
		}
		return g.Wrap(policy.Authenticated, h.gated(validate))
	}
	providerBodyFirst := func(validate validator, serve http.HandlerFunc) http.Handler {
		if h.Providers != nil {
			return g.BodyFirstLate(policy.Authenticated, h.providerGate(validate), serve)
		}
		return g.BodyFirst(policy.Authenticated, h.gated(validate))
	}
	// activate and deactivate call auth-service; without its client they answer the named 503
	// (statusRoute): go-api writes no provider row (CHAOS-14131).
	activate := h.statusRoute("active")
	deactivate := h.statusRoute("inactive")
	routes := []httpapi.Route{
		route(http.MethodGet, "/sso/providers", providerWrap(listQuery, h.listProviders)),
		route(http.MethodPost, "/sso/providers", providerBodyFirst(body(ssoProviderCreate), h.createProvider)),
		route(http.MethodGet, "/sso/providers/{provider_id}", providerWrap(noInput, h.getProvider)),
		route(http.MethodPatch, "/sso/providers/{provider_id}", providerBodyFirst(body(ssoProviderUpdate), h.updateProvider)),
		route(http.MethodDelete, "/sso/providers/{provider_id}", providerWrap(noInput, h.deleteProvider)),
		route(http.MethodPost, "/sso/providers/{provider_id}/activate", g.Wrap(policy.Authenticated, activate)),
		route(http.MethodPost, "/sso/providers/{provider_id}/deactivate", g.Wrap(policy.Authenticated, deactivate)),
		route(http.MethodGet, "/saml/{provider_id}/metadata", g.Wrap(policy.Public, inner("/saml/{provider_id}/metadata", samlMetadataHandler))),
		route(http.MethodPost, "/saml/{provider_id}/initiate", g.BodyFirst(policy.Public, inner("/saml/{provider_id}/initiate", samlInitiate))),
		route(http.MethodPost, "/saml/{provider_id}/acs", g.BodyFirst(policy.Public, inner("/saml/{provider_id}/acs", samlACS))),
		route(http.MethodPost, "/oidc/{provider_id}/authorize", g.BodyFirst(policy.Public, inner("/oidc/{provider_id}/authorize", oidcAuthorize))),
		route(http.MethodPost, "/oidc/{provider_id}/callback", g.BodyFirst(policy.Public, inner("/oidc/{provider_id}/callback", oidcCallbackHandler))),
		// POST /oauth/{provider_id}/authorize, POST /oauth/{provider_id}/callback
		// and GET /oauth/{provider_type}/authorize overlap, which net/http
		// refuses to register; one pattern dispatches them as Starlette does
		// (oauthPair).
	}
	pair := h.oauthPair()
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions} {
		// Every route it dispatches declares a response_model.
		routes = append(routes, httpapi.Route{Method: method, Pattern: prefix + "/oauth/{first}/{second}", Handler: pair,
			ResponseModelFor: func(*http.Request) bool { return true }, MethodFallback: true})
	}
	return routes
}

// orStateMissing is the 503 config_missing handler while SSO_STATE_SECRET is unset, else next; the process still starts.
func (h handlers) orStateMissing(next http.Handler) http.Handler {
	if h.StateSecret != "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { policy.WriteConfigMissing(w, "SSO_STATE_SECRET") })
}

// gated is a gated route: FastAPI validates the query and body (422), then
// the decorator checks the process license (402). D2749: the Go api's own
// process license (CHAOS-6663, licensing.ProcessHasFeature) is real, not
// absent -- it defaults to the community tier, and sso_saml is not a
// community feature, so the check still refuses by default. Were a real,
// higher-tier license (LICENSE_KEY/LICENSE_PUBLIC_KEY set and valid) to
// grant sso_saml, this route would still have to be ported first -- it
// isn't, for every route except the five this package's own doc comment
// names -- so it fails loudly instead of silently letting an unported
// route through on a license upgrade alone.
func (h handlers) gated(validate validator) http.Handler {
	return h.gatedTo(validate, http.HandlerFunc(h.unportedStub))
}

// gatedTo is gated with serve in place of the stub: the 422 and the 402 first, then serve.
func (h handlers) gatedTo(validate validator, serve http.Handler) http.Handler {
	check := h.gate(validate)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if check(w, r) {
			serve.ServeHTTP(w, r)
		}
	})
}

// gate is the 422 and the 402 of a gated route: true when the request may go on, false when the answer is written.
func (h handlers) gate(validate validator) func(http.ResponseWriter, *http.Request) bool {
	return func(w http.ResponseWriter, r *http.Request) bool {
		var errs pybody.Errors
		validate(r, &errs)
		if len(errs) > 0 {
			policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
			return false
		}
		if !licensing.ProcessHasFeature(ssoFeature) {
			// has_feature logs the denial as a license audit event (a log
			// line only; nothing is stored).
			h.Logger.WarnContext(r.Context(), "License audit: feature_access_denied",
				"feature", ssoFeature, "current_tier", licensing.ProcessTier(), "path", r.Pattern)
			policy.WriteDetail(w, http.StatusPaymentRequired, licensing.FeatureNotLicensedDetail(ssoFeature, &ssoRequiredTier), nil)
			return false
		}
		return true
	}
}

// unportedStub is what a gated route answers once the process tier grants sso_saml: it is not served by the Go api.
func (h handlers) unportedStub(w http.ResponseWriter, r *http.Request) {
	h.Logger.ErrorContext(r.Context(), "api sso: the process tier grants sso_saml, but the licensed SSO flows are not served by the Go api",
		"path", r.Pattern)
	policy.WriteInternal(w)
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
	// CHAOS-6986: like OIDC (initiateOIDCAuth/oidcCallback), the three real
	// OAuth handlers need StateSecret (the AEAD opaque state token,
	// oauthstate.go) and Signer (the login token pair, login.go's
	// finishSSOLogin) to run for real; without either, these three stay the
	// process-tier-only shim.
	oauthAuthorize := h.orStateMissing(h.gated(body(oauthAuthRequest)))
	oauthCallbackHandler := h.orStateMissing(h.gated(body(oauthCallbackRequest)))
	oauthByType := h.orStateMissing(h.gated(byTypeQuery))
	if h.signIn() {
		oauthAuthorize = h.refuseOIDCNotForwarded(body(oauthAuthRequest))
		oauthCallbackHandler = h.refuseOIDCNotForwarded(body(oauthCallbackRequest))
		oauthByType = h.refuseOIDCNotForwarded(byTypeQuery)
	}
	if h.sessionsOff() {
		oauthAuthorize, oauthCallbackHandler, oauthByType = http.HandlerFunc(h.refuseSessionsOff), http.HandlerFunc(h.refuseSessionsOff), http.HandlerFunc(h.refuseSessionsOff)
	}
	if h.Inner != nil {
		// auth-service serves the three OAuth routes; the two authorize routes share its pattern.
		oauthAuthorize = h.Inner("/api/v1/auth/oauth/{provider_id}/authorize", oauthAuthorize)
		oauthCallbackHandler = h.Inner("/api/v1/auth/oauth/{provider_id}/callback", oauthCallbackHandler)
		oauthByType = h.Inner("/api/v1/auth/oauth/{provider_id}/authorize", oauthByType)
	}
	routes := []pairRoute{
		{http.MethodPost, func(_, second string) bool { return second == "authorize" },
			g.BodyFirst(policy.Public, oauthAuthorize)},
		{http.MethodPost, func(_, second string) bool { return second == "callback" },
			g.BodyFirst(policy.Public, oauthCallbackHandler)},
		{http.MethodGet, func(_, second string) bool { return second == "authorize" },
			g.Wrap(policy.Public, oauthByType)},
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

// newHandlers is what Routes builds its handlers from: the defaults of Deps, and the client every IdP call uses.
// signIn reports whether the sign-in routes run for real: the state secret is set and auth-service issues the sessions.
func (h handlers) signIn() bool { return h.StateSecret != "" && h.Issuer != nil }

// sessionsOff reports a deployment whose sign-in routes would run but for AUTH_PROXY_SESSIONS being off.
func (h handlers) sessionsOff() bool { return h.StateSecret != "" && h.Issuer == nil }

// refuseSessionsOff answers 503 config_missing naming AUTH_PROXY_SESSIONS: go-api mints no login token pair.
func (h handlers) refuseSessionsOff(w http.ResponseWriter, r *http.Request) {
	h.Logger.ErrorContext(r.Context(), "sso route refused: AUTH_PROXY_SESSIONS is not on", "path", r.Pattern)
	policy.WriteConfigMissing(w, "AUTH_PROXY_SESSIONS")
}

// refuseSAMLNotForwarded answers a SAML route's 422, then 503 config_missing naming AUTH_PROXY_SAML.
func (h handlers) refuseSAMLNotForwarded(validate validator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var errs pybody.Errors
		validate(r, &errs)
		if len(errs) > 0 {
			policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
			return
		}
		h.Logger.ErrorContext(r.Context(), "sso route refused: AUTH_PROXY_SAML is not on", "path", r.Pattern)
		policy.WriteConfigMissing(w, "AUTH_PROXY_SAML")
	})
}

// refuseOIDCNotForwarded is an OIDC or OAuth sign-in route while AUTH_PROXY_OIDC is off: its 422, else 503 config_missing
// naming AUTH_PROXY_OIDC. go-api has no own OIDC or OAuth sign-in; auth-service serves both (CHAOS-14131).
func (h handlers) refuseOIDCNotForwarded(validate validator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var errs pybody.Errors
		validate(r, &errs)
		if len(errs) > 0 {
			policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
			return
		}
		h.Logger.ErrorContext(r.Context(), "sso route refused: AUTH_PROXY_OIDC is not on", "path", r.Pattern)
		policy.WriteConfigMissing(w, "AUTH_PROXY_OIDC")
	})
}

// statusRoute is activate or deactivate: auth-service's status write while it owns the providers. Without its client the
// caller and id checks answer as before, then the named 503: go-api writes no provider row.
func (h handlers) statusRoute(status string) http.Handler {
	if h.Providers != nil {
		return h.setStatusViaProviders(status)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := h.providerScope(w, r, true); !ok {
			return
		}
		if _, ok := h.providerID(w, r); !ok {
			return
		}
		h.providerFault(w, r, "set status", authprovision.ErrUnavailable)
	})
}

func newHandlers(deps Deps) handlers {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Write == nil {
		deps.Write = httpapi.WriteError
	}
	if deps.OrgEntitled == nil {
		pool := deps.Pool
		deps.OrgEntitled = func(ctx context.Context, orgID string) (bool, error) {
			return licensing.OrgHasFeature(ctx, pool, orgID, ssoFeature)
		}
	}
	return handlers{deps}
}
