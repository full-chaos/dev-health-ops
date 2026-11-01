package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"slices"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/oauthgrant"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// Route patterns of the OAuth authorization server (D0 sections 3 and 4). The
// endpoint-profile rows name these exact strings. Until those rows exist nothing
// here mounts.
const (
	PatternOAuthAuthorize = "/oauth/authorize"
	PatternOAuthMetadata  = "/.well-known/oauth-authorization-server/oauth"
	PatternOAuthDiscovery = "/oauth/.well-known/openid-configuration"
	PatternOAuthJWKS      = "/oauth/.well-known/jwks.json"
)

const (
	// oauthConsentPath is web's consent page (W1), under AUTH_OAUTH_WEB_ORIGIN.
	oauthConsentPath = "/oauth/consent"
	// maxOAuthBody is the POST body cap (D0 section 4).
	maxOAuthBody = 16 << 10

	oauthAuthorizeScope = "GET " + PatternOAuthAuthorize
	oauthDecideScope    = "POST " + PatternOAuthAuthorize

	// The operations of the Info line and the counter (D0 section 10).
	oauthOpAuthorize = "authorize"
	oauthOpConsent   = "consent"
	oauthOpMetadata  = "metadata"

	oauthRequestsMetric = "auth.oauth.requests"

	oauthOutcomeAccepted = "accepted"
	oauthOutcomeRefused  = "refused"
	oauthOutcomeError    = "error"
)

// codeChallengePattern is an S256 challenge: 32 bytes, base64url, no padding.
var codeChallengePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

// scopeOfAudience is the one scope of each ACR audience.
var scopeOfAudience = map[string]string{
	contracts.ACRAPIAudience: "acr:api",
	contracts.ACRMCPAudience: "acr:mcp",
}

// OAuthStore is the grant store (*oauthgrant.Store).
type OAuthStore interface {
	CreateRequest(ctx context.Context, in oauthgrant.RequestInput) (uuid.UUID, error)
	Describe(ctx context.Context, in oauthgrant.DescribeInput) (oauthgrant.Described, error)
	Approve(ctx context.Context, in oauthgrant.Decision) (oauthgrant.Approved, error)
	Deny(ctx context.Context, in oauthgrant.Decision) (oauthgrant.Denied, error)
}

// OAuthDeps are the routes' collaborators. Session is the session routes' own: the
// POST route reads its bearer as they do.
type OAuthDeps struct {
	Store    OAuthStore
	Registry oauthgrant.Registry
	Session  Deps
	Limits   *Limits
	Keys     KeySetSource
	// IssuerURL is AUTH_TOKEN_ISSUER_URL. Empty mounts nothing.
	IssuerURL string
	// WebOrigin, ResourceACRAPI and ResourceACRMCP are their settings. An empty one
	// is a refusal at request time.
	WebOrigin, ResourceACRAPI, ResourceACRMCP string
	Meter                                     metric.Meter
}

type oauthHandlers struct {
	OAuthDeps
	session   handlers
	resources map[string]string
	client    *httpapi.KeyedLimiter
	principal *httpapi.KeyedLimiter
	logger    *slog.Logger
	requests  metric.Int64Counter
}

// OAuthRoutes returns the routes when the embedded endpoint-profile document holds
// a row for each. Without them it logs one line and mounts nothing.
func OAuthRoutes(deps OAuthDeps) ([]httpapi.Route, error) {
	return oauthRoutesRows(deps, authv1.AuthServiceEndpointProfiles)
}

func oauthRoutesRows(deps OAuthDeps, rows []byte) ([]httpapi.Route, error) {
	logger := deps.Session.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if deps.IssuerURL == "" {
		return nil, nil
	}
	if missing := oauthRowsMissing(rows); missing != "" {
		logger.Info("auth oauth routes not mounted: no endpoint-profile row", "route", missing)
		return nil, nil
	}
	if deps.Store == nil || deps.Limits == nil || deps.Keys == nil {
		return nil, errors.New("authapi: the OAuth routes need a store, limits and a key set")
	}
	if deps.Session.Sessions == nil || deps.Session.Users == nil || deps.Session.Issuer == nil {
		return nil, errors.New("authapi: the OAuth routes need the session routes' deps")
	}
	meter := deps.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/authapi")
	}
	requests, err := meter.Int64Counter(oauthRequestsMetric,
		metric.WithDescription("OAuth authorization server requests by operation, outcome, audience and client class."))
	if err != nil {
		return nil, err
	}
	rec, err := newRecorder(logger, meter)
	if err != nil {
		return nil, err
	}
	h := oauthHandlers{
		OAuthDeps: deps, session: handlers{Deps: deps.Session, rec: rec},
		resources: map[string]string{contracts.ACRAPIAudience: deps.ResourceACRAPI, contracts.ACRMCPAudience: deps.ResourceACRMCP},
		client:    deps.Limits.limiter(oauthClientLimit), principal: deps.Limits.limiter(oauthPrincipalLimit),
		logger: logger, requests: requests,
	}
	known := WellKnownDeps{Keys: deps.Keys, IssuerURL: deps.IssuerURL, Logger: logger}
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: PatternOAuthAuthorize, Handler: http.HandlerFunc(h.authorize), EmptyBody: true},
		{Method: http.MethodPost, Pattern: PatternOAuthAuthorize, Handler: http.HandlerFunc(h.decide), MaxBodyBytes: maxOAuthBody},
		{Method: http.MethodGet, Pattern: PatternOAuthMetadata, Handler: http.HandlerFunc(h.metadata), EmptyBody: true, PublicClass: true},
		{Method: http.MethodGet, Pattern: PatternOAuthDiscovery, Handler: http.HandlerFunc(known.serveDiscovery), EmptyBody: true, PublicClass: true},
		{Method: http.MethodGet, Pattern: PatternOAuthJWKS, Handler: http.HandlerFunc(known.serveJWKS), EmptyBody: true, PublicClass: true},
	}, nil
}

// oauthRowsMissing names the first route with no row in raw, "" when all have one.
func oauthRowsMissing(raw []byte) string {
	for _, route := range []struct{ method, pattern string }{
		{http.MethodGet, PatternOAuthAuthorize},
		{http.MethodPost, PatternOAuthAuthorize},
		{http.MethodGet, PatternOAuthMetadata},
		{http.MethodGet, PatternOAuthDiscovery},
		{http.MethodGet, PatternOAuthJWKS},
	} {
		if !endpointRowExists(raw, route.method, route.pattern) {
			return route.method + " " + route.pattern
		}
	}
	return ""
}

// noRedirect is a refusal of GET /oauth/authorize with no redirect: the redirect is
// not proven, so nothing may be sent to it. client is the id to log, host only.
func (h oauthHandlers) noRedirect(w http.ResponseWriter, r *http.Request, reason, client string) {
	h.record(r.Context(), oauthEvent{operation: oauthOpAuthorize, outcome: oauthOutcomeRefused, reason: reason, client: client})
	writeRefusal(w, r, refusal{http.StatusBadRequest, reasonMalformed})
}

// authorize is GET /oauth/authorize. An unknown client or an inexact redirect gets a
// 400 and no redirect. After the redirect is proven, an error goes to it. It writes
// one request row and sends the browser to web's consent page with the request id
// only: no user interaction happens here.
func (h oauthHandlers) authorize(w http.ResponseWriter, r *http.Request) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		h.noRedirect(w, r, "invalid_request", "")
		return
	}
	clientID, clientOK := single(query, "client_id")
	redirectURI, redirectOK := single(query, "redirect_uri")
	if !clientOK || !redirectOK {
		h.noRedirect(w, r, "invalid_request", clientHost(clientID))
		return
	}
	client, known := h.Registry.Client(clientID)
	if !known {
		h.noRedirect(w, r, "unknown_client", clientHost(clientID))
		return
	}
	if !admit(w, r, httpapi.WriteError, oauthAuthorizeScope,
		charge{h.client, h.Limits.digest("oauth-client", client.ID)}) {
		h.record(r.Context(), oauthEvent{operation: oauthOpAuthorize, outcome: oauthOutcomeRefused, reason: "rate_limited", class: client.Class, client: client.ID})
		return
	}
	if h.WebOrigin == "" {
		h.noRedirect(w, r, "web_origin_unset", client.ID)
		return
	}
	if !slices.Contains(h.redirectsOf(client), redirectURI) {
		h.noRedirect(w, r, "redirect_mismatch", client.ID)
		return
	}
	audience := client.Audiences[0]
	resource := h.resources[audience]
	if resource == "" {
		h.noRedirect(w, r, "resource_unset", client.ID)
		return
	}
	state, stateOK := single(query, "state")
	fail := func(code string) {
		h.record(r.Context(), oauthEvent{operation: oauthOpAuthorize, outcome: oauthOutcomeRefused, reason: code, audience: audience, class: client.Class, client: client.ID})
		h.redirect(w, r, redirectURI, url.Values{"error": {code}}, state, stateOK)
	}
	if !stateOK || state == "" || len(state) > 1024 {
		fail("invalid_request")
		return
	}
	responseType, ok := single(query, "response_type")
	if !ok || responseType != "code" {
		fail("unsupported_response_type")
		return
	}
	method, methodOK := single(query, "code_challenge_method")
	challenge, _ := single(query, "code_challenge")
	if !methodOK || method != "S256" || !codeChallengePattern.MatchString(challenge) {
		fail("invalid_request")
		return
	}
	if got, ok := single(query, "resource"); !ok || got != resource {
		fail("invalid_target")
		return
	}
	scope := scopeOfAudience[audience]
	if values, present := query["scope"]; present && (len(values) != 1 || values[0] != scope) {
		fail("invalid_scope")
		return
	}
	id, err := h.Store.CreateRequest(r.Context(), oauthgrant.RequestInput{
		ClientID: client.ID, ClientClass: client.Class, RedirectURI: redirectURI, Resource: resource,
		Audience: audience, Scope: scope, State: state, CodeChallenge: challenge,
	})
	if err != nil {
		h.fail(r.Context(), oauthEvent{operation: oauthOpAuthorize, audience: audience, class: client.Class, client: client.ID}, err)
		h.redirect(w, r, redirectURI, url.Values{"error": {"temporarily_unavailable"}}, state, true)
		return
	}
	h.record(r.Context(), oauthEvent{operation: oauthOpAuthorize, outcome: oauthOutcomeAccepted, reason: reasonAccepted, audience: audience, class: client.Class, client: client.ID})
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, h.WebOrigin+oauthConsentPath+"?request_id="+id.String(), http.StatusFound)
}

// redirectsOf is the client's exact redirects: a first-party row's path against the
// web origin, or a fixed-URI row's URIs.
func (h oauthHandlers) redirectsOf(client oauthgrant.Client) []string {
	if client.RedirectPath != "" {
		return []string{h.WebOrigin + client.RedirectPath}
	}
	return client.RedirectURIs
}

// redirect sends an RFC 6749 answer to a proven redirect, with the issuer (RFC 9207)
// and the client's state.
func (h oauthHandlers) redirect(w http.ResponseWriter, r *http.Request, to string, values url.Values, state string, withState bool) {
	values.Set("iss", h.IssuerURL)
	if withState {
		values.Set("state", state)
	}
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, to+"?"+values.Encode(), http.StatusFound)
}

// single is a parameter present exactly once.
func single(values url.Values, name string) (string, bool) {
	got := values[name]
	if len(got) != 1 {
		return "", false
	}
	return got[0], true
}

// decide is POST /oauth/authorize, from web's server side, under the web session's
// access token. action is describe, approve or deny.
func (h oauthHandlers) decide(w http.ResponseWriter, r *http.Request) {
	bearer, record, ok := h.session.authenticateWith(w, r, oauthOpConsent, h.refuseBearer)
	if !ok {
		return
	}
	if !admit(w, r, httpapi.WriteError, oauthDecideScope,
		charge{h.principal, h.Limits.digest("principal", record.PrincipalID.String())}) {
		h.record(r.Context(), oauthEvent{operation: oauthOpConsent, outcome: oauthOutcomeRefused, reason: "rate_limited"})
		return
	}
	body, ok := readOAuthForm(r)
	if !ok {
		h.refuse(w, r, refusal{http.StatusBadRequest, reasonMalformed}, "invalid_request")
		return
	}
	request, err := uuid.Parse(body["request_id"])
	if err != nil || request.String() != body["request_id"] {
		h.refuse(w, r, refusal{http.StatusBadRequest, reasonMalformed}, "invalid_request")
		return
	}
	switch body["action"] {
	case "describe":
		if len(body) != 2 {
			h.refuse(w, r, refusal{http.StatusBadRequest, reasonMalformed}, "invalid_request")
			return
		}
		h.describe(w, r, request, record)
	case "approve", "deny":
		token, present := body["csrf_token"]
		if !present {
			h.refuse(w, r, refusal{http.StatusBadRequest, reasonMalformed}, "invalid_request")
			return
		}
		decision := oauthgrant.Decision{RequestID: request, WebSessionID: bearer.SessionID, PrincipalID: record.PrincipalID,
			CSRF: contracts.NewSecretString(token)}
		if body["action"] == "approve" {
			h.approve(w, r, decision)
		} else {
			h.deny(w, r, decision)
		}
	default:
		h.refuse(w, r, refusal{http.StatusBadRequest, reasonMalformed}, "invalid_request")
	}
}

type oauthDescription struct {
	ClientID    string `json:"client_id"`
	ClientName  string `json:"client_name"`
	ClientClass string `json:"client_class"`
	// FirstParty comes from the registry: such a client has no consent click.
	FirstParty bool   `json:"first_party"`
	Resource   string `json:"resource"`
	Audience   string `json:"audience"`
	Scope      string `json:"scope"`
	CSRFToken  string `json:"csrf_token"`
}

func (h oauthHandlers) describe(w http.ResponseWriter, r *http.Request, request uuid.UUID, record session.Record) {
	described, err := h.Store.Describe(r.Context(), oauthgrant.DescribeInput{
		RequestID: request, WebSessionID: record.ID, PrincipalID: record.PrincipalID, OrganizationID: record.OrganizationID,
	})
	if err != nil {
		h.refuseDecision(w, r, err)
		return
	}
	client, known := h.Registry.Client(described.ClientID)
	if !known {
		h.fail(r.Context(), oauthEvent{operation: oauthOpConsent, audience: described.Audience, class: described.ClientClass},
			errors.New("a bound request names a client outside the registry"))
		writeRefusal(w, r, unavailable(reasonDependency))
		return
	}
	h.record(r.Context(), oauthEvent{operation: oauthOpConsent, outcome: oauthOutcomeAccepted, reason: reasonAccepted, audience: described.Audience, class: described.ClientClass, client: client.ID})
	writeJSON(w, http.StatusOK, oauthDescription{
		ClientID: client.ID, ClientName: client.Name, ClientClass: client.Class, FirstParty: client.FirstParty,
		Resource: described.Resource, Audience: described.Audience, Scope: described.Scope, CSRFToken: described.CSRF.Reveal(),
	})
}

type oauthRedirectAnswer struct {
	RedirectTo string `json:"redirect_to"`
}

func (h oauthHandlers) approve(w http.ResponseWriter, r *http.Request, decision oauthgrant.Decision) {
	approved, err := h.Store.Approve(r.Context(), decision)
	if err != nil {
		h.refuseDecision(w, r, err)
		return
	}
	h.record(r.Context(), oauthEvent{operation: oauthOpConsent, outcome: oauthOutcomeAccepted, reason: reasonAccepted,
		audience: approved.Audience, class: approved.ClientClass, client: approved.ClientID, grant: approved.GrantID.String()})
	h.answerRedirect(w, approved.RedirectURI, url.Values{"code": {approved.Code.Reveal()}, "state": {approved.State}})
}

func (h oauthHandlers) deny(w http.ResponseWriter, r *http.Request, decision oauthgrant.Decision) {
	denied, err := h.Store.Deny(r.Context(), decision)
	if err != nil {
		h.refuseDecision(w, r, err)
		return
	}
	h.record(r.Context(), oauthEvent{operation: oauthOpConsent, outcome: oauthOutcomeRefused, reason: "access_denied",
		audience: denied.Audience, class: denied.ClientClass, client: denied.ClientID})
	h.answerRedirect(w, denied.RedirectURI, url.Values{"error": {"access_denied"}, "state": {denied.State}})
}

// answerRedirect is the answer of a decision: web's server side sends the browser on.
func (h oauthHandlers) answerRedirect(w http.ResponseWriter, to string, values url.Values) {
	values.Set("iss", h.IssuerURL)
	writeJSON(w, http.StatusOK, oauthRedirectAnswer{RedirectTo: to + "?" + values.Encode()})
}

// refuseDecision maps a store refusal. Everything a caller could use to learn about
// a request it does not hold has one answer.
func (h oauthHandlers) refuseDecision(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, oauthgrant.ErrRequestNotFound), errors.Is(err, oauthgrant.ErrOtherSession), errors.Is(err, oauthgrant.ErrRequestNotBound),
		errors.Is(err, oauthgrant.ErrBadCSRF), errors.Is(err, oauthgrant.ErrRequestExpired), errors.Is(err, oauthgrant.ErrRequestBound),
		errors.Is(err, oauthgrant.ErrAlreadyDecided):
		h.refuse(w, r, refusal{http.StatusBadRequest, reasonMalformed}, "invalid_request")
	case errors.Is(err, oauthgrant.ErrActorRefused), errors.Is(err, session.ErrWebSessionDelegated):
		h.refuse(w, r, refusal{http.StatusForbidden, reasonMalformed}, "impersonating")
	case errors.Is(err, oauthgrant.ErrNoOrganization), errors.Is(err, session.ErrWebSessionNoOrganization):
		h.refuse(w, r, refusal{http.StatusForbidden, reasonMalformed}, "no_organization")
	case errors.Is(err, session.ErrWebSessionNotLive):
		h.refuse(w, r, unauthorized(reasonSessionRevoked), "session_not_live")
	default:
		h.fail(r.Context(), oauthEvent{operation: oauthOpConsent}, err)
		writeRefusal(w, r, unavailable(reasonStoreUnavailable))
	}
}

func (h oauthHandlers) refuse(w http.ResponseWriter, r *http.Request, answer refusal, reason string) {
	h.record(r.Context(), oauthEvent{operation: oauthOpConsent, outcome: oauthOutcomeRefused, reason: reason})
	writeRefusal(w, r, answer)
}

// refuseBearer is the refuser of the POST route's bearer read.
func (h oauthHandlers) refuseBearer(w http.ResponseWriter, r *http.Request, _ string, answer refusal, err error) {
	if err != nil && answer.status >= http.StatusInternalServerError {
		h.fail(r.Context(), oauthEvent{operation: oauthOpConsent}, err)
	} else {
		h.record(r.Context(), oauthEvent{operation: oauthOpConsent, outcome: oauthOutcomeRefused, reason: answer.reason})
	}
	writeRefusal(w, r, answer)
}

// readOAuthForm reads a form body with each of the closed names at most once. Any
// other name, a repeated one or another content type is no form.
func readOAuthForm(r *http.Request) (map[string]string, bool) {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/x-www-form-urlencoded" {
		return nil, false
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, false
	}
	values, err := url.ParseQuery(string(raw))
	if err != nil {
		return nil, false
	}
	out := make(map[string]string, len(values))
	for name, got := range values {
		if len(got) != 1 || !slices.Contains([]string{"action", "request_id", "csrf_token"}, name) {
			return nil, false
		}
		out[name] = got[0]
	}
	return out, true
}

type oauthMetadataDocument struct {
	Issuer                string   `json:"issuer"`
	AuthorizationEndpoint string   `json:"authorization_endpoint"`
	TokenEndpoint         string   `json:"token_endpoint"`
	RevocationEndpoint    string   `json:"revocation_endpoint"`
	JWKSURI               string   `json:"jwks_uri"`
	ResponseTypes         []string `json:"response_types_supported"`
	GrantTypes            []string `json:"grant_types_supported"`
	CodeChallengeMethods  []string `json:"code_challenge_methods_supported"`
	TokenAuthMethods      []string `json:"token_endpoint_auth_methods_supported"`
	Scopes                []string `json:"scopes_supported"`
	IssParameter          bool     `json:"authorization_response_iss_parameter_supported"`
}

// metadata is the RFC 8414 document. It is built from the pinned issuer, never from
// the request.
func (h oauthHandlers) metadata(w http.ResponseWriter, r *http.Request) {
	h.record(r.Context(), oauthEvent{operation: oauthOpMetadata, outcome: oauthOutcomeAccepted, reason: reasonAccepted})
	writeCacheable(w, r, oauthMetadataDocument{
		Issuer: h.IssuerURL, AuthorizationEndpoint: h.IssuerURL + "/authorize", TokenEndpoint: h.IssuerURL + "/token",
		RevocationEndpoint: h.IssuerURL + "/revoke", JWKSURI: h.IssuerURL + contracts.DiscoveryJWKSPath,
		ResponseTypes: []string{"code"}, GrantTypes: []string{"authorization_code", "refresh_token"},
		CodeChallengeMethods: []string{"S256"}, TokenAuthMethods: []string{"none"},
		Scopes: []string{"acr:api", "acr:mcp"}, IssParameter: true,
	})
}

// oauthEvent is one operation's Info line and counter (D0 section 10). It holds no
// token, code, verifier, state or client-supplied text: client is a registry id, or
// a metadata client's host.
type oauthEvent struct {
	operation, outcome, reason, audience, class, client, grant string
}

func (h oauthHandlers) record(ctx context.Context, e oauthEvent) {
	h.requests.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", e.operation), attribute.String("outcome", e.outcome),
		attribute.String("audience", e.audience), attribute.String("client_class", e.class)))
	h.logger.InfoContext(ctx, "auth oauth request", slog.String("operation", e.operation), slog.String("outcome", e.outcome),
		slog.String("reason", e.reason), slog.String("audience", e.audience), slog.String("client_class", e.class),
		slog.String("binding", bindingOf(e.class)), slog.String("client", e.client), slog.String("grant", e.grant),
		slog.String("request_id", httpapi.LoggableRequestID(ctx)), slog.String("audit_request_id", requestid.Wire(ctx)))
}

func (h oauthHandlers) fail(ctx context.Context, e oauthEvent, err error) {
	h.requests.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", e.operation), attribute.String("outcome", oauthOutcomeError),
		attribute.String("audience", e.audience), attribute.String("client_class", e.class)))
	h.logger.ErrorContext(ctx, "auth oauth request failed", slog.String("operation", e.operation), slog.String("client", e.client),
		slog.String("request_id", httpapi.LoggableRequestID(ctx)), logging.ErrorAttr(err))
}

// clientHost is what may be logged of a client_id the registry does not hold: the
// host of an https URL, and nothing of any other text.
func clientHost(id string) string {
	parsed, err := url.Parse(id)
	if err != nil || parsed.Scheme != "https" {
		return ""
	}
	return parsed.Hostname()
}

// endpointRowExists reports whether the document holds an auth-service row for the
// method and pattern.
func endpointRowExists(raw []byte, method, pattern string) bool {
	var document struct {
		Rows []struct {
			Service string `json:"service"`
			Method  string `json:"method"`
			Route   string `json:"route"`
		} `json:"rows"`
	}
	if json.Unmarshal(raw, &document) != nil {
		return false
	}
	return slices.ContainsFunc(document.Rows, func(row struct {
		Service string `json:"service"`
		Method  string `json:"method"`
		Route   string `json:"route"`
	}) bool {
		return row.Service == AccessService && row.Method == method && row.Route == pattern
	})
}

func bindingOf(class string) string {
	switch class {
	case oauthgrant.ClassBrowser:
		return "web_session"
	case oauthgrant.ClassMCP:
		return "client_session"
	}
	return ""
}
