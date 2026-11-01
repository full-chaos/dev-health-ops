package authapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"unicode/utf16"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// PatternCompatValidate is go-api's token check, POST /api/v1/auth/validate
// (internal/api/session validate): the same strict body, limit and answer. It
// is mounted only when the embedded endpoint-profile document holds its row;
// without the row it answers 404 and the service starts.
const PatternCompatValidate = "/api/v1/auth/validate"

const opCompatValidate = "compat_validate"

// validateProfile is the route template the token is judged against.
const validateProfile = http.MethodPost + " " + PatternCompatValidate

// validateLimit is go-api's auth_validate budget: the count go-api uses
// (httpapi.ValidateLimitCount) per its window, under this service's prefix.
var validateLimit = httpapi.Limit{ID: "auth_svc_validate", Count: httpapi.ValidateLimitCount, Window: httpapi.ValidateLimitWindow}

// ValidateConfig mounts POST /api/v1/auth/validate (CompatDeps.Validate).
type ValidateConfig struct {
	// Sessions is go-api's own session read (policy.PGStore over the auth
	// schema): the row go-api's sid check reads.
	Sessions policy.SessionStore
	// IssuerURL and Keys add the EdDSA user access token of audience
	// dev-health-api, verified as go-api's routed validator verifies it, with
	// the key set this service signs with (never a fetch of its own JWKS).
	// Without both the route judges the HS256 token only.
	IssuerURL string
	Keys      KeySetSource
}

func (c *ValidateConfig) eddsa() bool { return c.IssuerURL != "" && c.Keys != nil }

// errValidateUnread is a policy.Store read go-api's token decision never makes.
var errValidateUnread = errors.New("authapi: the validate route does not make this read")

// errValidateKey is go-api's UnicodeEncodeError of the limit key: a token
// holding a lone surrogate cannot be encoded to digest it.
var errValidateKey = errors.New("authapi: the validate token holds a lone surrogate")

// validateStore is the store go-api's token decision reads on this route, and
// no more: the users row and the membership through Users (public.users and
// public.memberships, columns of authschema.PublicReadColumns, so no new
// grant), the session row through go-api's own session read. The two other
// policy.Store reads are never made by Authenticate and refuse. The answer
// carries no superuser value, so the store passes none: no platform decision
// is made here.
type validateStore struct {
	users    Users
	sessions policy.SessionStore
}

func (s validateStore) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	state, found, err := s.users.State(ctx, id)
	if err != nil || !found {
		return policy.UserState{}, false, err
	}
	return policy.UserState{IsActive: state.IsActive, TokenVersion: state.TokenVersion}, true, nil
}

func (s validateStore) AccessUser(ctx context.Context, id uuid.UUID) (policy.AccessUserRow, bool, error) {
	state, found, err := s.users.State(ctx, id)
	if err != nil || !found {
		return policy.AccessUserRow{}, false, err
	}
	return policy.AccessUserRow{
		State: policy.UserState{IsActive: state.IsActive, TokenVersion: state.TokenVersion},
		Email: state.Email, Username: state.Username, FullName: state.FullName,
	}, true, nil
}

func (s validateStore) Membership(ctx context.Context, userID, orgID uuid.UUID) (string, bool, error) {
	role, found, err := s.users.Role(ctx, userID, orgID)
	if err != nil || !found || role == nil {
		return "", found && err == nil, err
	}
	return *role, true, nil
}

func (s validateStore) SessionState(ctx context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	return s.sessions.SessionState(ctx, id)
}

func (validateStore) OrgMembership(context.Context, uuid.UUID, uuid.UUID) (string, bool, bool, error) {
	return "", false, false, errValidateUnread
}

func (validateStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, errValidateUnread
}

func (validateStore) ReadsPlatformRoles() bool { return false }

// newValidateAuth is go-api's token decision over the store: the HS256 token
// verified by the go-api verifier this Issuer holds, then CheckClaims
// (Authenticate). With an EdDSA configuration it is go-api's routed decision
// (internal/apiservice accessverify.go and accessrows.go) over this route's
// one row: the token's alg picks the HS256 path or the EdDSA user token of
// audience dev-health-api, whose users row, session and membership go-api's
// accessTokenUser checks.
func newValidateAuth(deps CompatDeps) (*policy.Authenticator, error) {
	auth, err := policy.NewAuthenticator(deps.Issuer.verifier, validateStore{users: deps.Users, sessions: deps.Validate.Sessions}, deps.Logger)
	if err != nil || !deps.Validate.eddsa() {
		return auth, err
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{
		Issuer: deps.Validate.IssuerURL, Audience: contracts.PlatformAPIAudience, Keys: publishedKeys{source: deps.Validate.Keys},
	})
	if err != nil {
		return nil, err
	}
	meter := deps.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/accessverify")
	}
	validator, err := accessverify.New(accessverify.Config{
		Rows:   []accessverify.Row{{Service: AccessService, Profile: validateProfile, AcceptsUser: true, LegacyHS256: true}},
		EdDSA:  verifier,
		Legacy: validateLegacy(auth.AuthenticateLegacy),
		Meter:  meter, Logger: deps.Logger,
	})
	if err != nil {
		return nil, err
	}
	if err := auth.RouteAccess(validateRouted{validator}, AccessService); err != nil {
		return nil, err
	}
	return auth, nil
}

// validateLegacy is go-api's legacyVerifier: a refusal and a claim of the
// wrong type stay refusals, any other failure is the validator's outage.
func validateLegacy(authenticate func(context.Context, string) (*policy.User, error)) accessverify.LegacyVerifier {
	return func(ctx context.Context, token string) (any, error) {
		user, err := authenticate(ctx, token)
		switch {
		case err == nil:
			return user, nil
		case policy.IsRefusal(err), policy.IsClaimTypeError(err):
			return nil, err
		default:
			return nil, fmt.Errorf("%w: %w", accessverify.ErrUnavailable, err)
		}
	}
}

// validateRouted is go-api's routedAccessVerify.
type validateRouted struct{ validator *accessverify.Validator }

var errValidateResult = errors.New("authapi: access verify result of an unexpected class")

func (v validateRouted) Validate(ctx context.Context, service, profile, bearer string) (*policy.RoutedResult, error) {
	result, err := v.validator.Validate(ctx, service, profile, bearer)
	switch {
	case errors.Is(err, authverify.ErrKeySourceUnavailable):
		return nil, fmt.Errorf("%w: %w", policy.ErrKeySourceUnavailable, err)
	case errors.Is(err, accessverify.ErrUnavailable):
		return nil, fmt.Errorf("%w: %w", policy.ErrUnavailable, err)
	case err != nil:
		return nil, err
	}
	switch {
	case result.Class == accessverify.ClassLegacy:
		if user, ok := result.Legacy.(*policy.User); ok && user != nil {
			return &policy.RoutedResult{Legacy: user}, nil
		}
	case result.Class == accessverify.ClassUser && result.Claims != nil:
		claims := result.Claims
		return &policy.RoutedResult{Claims: &policy.AccessTokenClaims{Subject: claims.Subject, SessionID: claims.SessionID, OrgID: claims.OrgID}}, nil
	}
	return nil, errValidateResult
}

// withValidateRoute mounts the validate route when rows holds its row: an
// auth-service row that accepts the HS256 token, and the user access token
// too when the EdDSA configuration is set. Without it it logs one line naming
// the route and mounts nothing.
func (h compatHandlers) withValidateRoute(routes []httpapi.Route, rows []byte) ([]httpapi.Route, error) {
	if h.Validate == nil {
		return routes, nil
	}
	if h.Validate.Sessions == nil {
		return nil, errors.New("authapi: the validate route needs go-api's session read")
	}
	classes := []string{accessverify.ClassLegacy}
	if h.Validate.eddsa() {
		classes = append(classes, accessverify.ClassUser)
	}
	if rowsAcceptFor(rows, []string{PatternCompatValidate}, classes) != nil {
		h.rec.logger.Info("auth validate route not mounted: no endpoint-profile row", "route", PatternCompatValidate)
		return routes, nil
	}
	auth, err := newValidateAuth(h.CompatDeps)
	if err != nil {
		return nil, err
	}
	v := validateHandler{compatHandlers: h, auth: auth, limiter: h.Limits.limiter(validateLimit)}
	return append(routes, httpapi.Route{Method: http.MethodPost, Pattern: PatternCompatValidate, Handler: http.HandlerFunc(v.compatValidate)}), nil
}

type validateHandler struct {
	compatHandlers
	auth    *policy.Authenticator
	limiter *httpapi.KeyedLimiter
}

// validateKey is go-api's get_validate_key: the token's SHA-256 cut to 32 hex
// digits, or the client address when the token is empty. A token holding a
// lone surrogate cannot be keyed (go-api's 500, before the request counts).
func validateKey(r *http.Request, token string) (string, error) {
	if token == "" {
		return "validate-ip:" + clientKey(r), nil
	}
	for _, c := range pyjson.Runes(token) {
		if utf16.IsSurrogate(c) {
			return "", errValidateKey
		}
	}
	digest := sha256.Sum256([]byte(token))
	return "validate-token:" + hex.EncodeToString(digest[:])[:32], nil
}

// compatValidate is POST /api/v1/auth/validate with go-api's wire: the body
// read and refused as go-api's BodyFirst and TokenValidateRequest do, counted
// by go-api's key, then the token judged by go-api's own decision. A token
// that names a session then needs that session alive in auth-service's own
// state (its effective revoke and expiry, as the session state route reads
// them): stricter than go-api, which reads only the row's own revoke (acp
// answers-12395-2 (b)). A refused token is 200 valid:false; a store fault is
// go-api's 503, never a pass. The members are go-api's: the user its decision
// returns, no read of its own.
func (h validateHandler) compatValidate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body, ok := h.readCompatBody(w, r, opCompatValidate, nil)
	if !ok {
		return
	}
	var errs pybody.Errors
	var token string
	if object, ok := errs.Object(body); ok {
		token, _ = errs.RequiredString(object, "token", 0, 0)
	}
	if len(errs) > 0 {
		h.rec.record(ctx, opCompatValidate, "denied", reasonMalformed, nil)
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	key, err := validateKey(r, token)
	if err != nil {
		h.fail(w, r, opCompatValidate, err)
		return
	}
	if !admit(w, r, goAPIRefusal, PatternCompatValidate, charge{h.limiter, key}) {
		return
	}
	user, err := h.auth.Authenticate(policy.WithRouteProfile(ctx, validateProfile), token)
	switch {
	case policy.IsRefusal(err):
		h.refuseToken(w, r, reasonInvalidCredentials)
		return
	case err != nil:
		h.validateFault(w, r, err)
		return
	}
	if user.SessionID != nil {
		record, err := h.Sessions.Current(ctx, *user.SessionID)
		switch {
		case errors.Is(err, session.ErrSessionNotFound):
			h.refuseToken(w, r, reasonSessionNotFound)
			return
		case err != nil:
			h.unavailableCompat(w, r, opCompatValidate, err)
			return
		case record.RevokedAt != nil:
			h.refuseToken(w, r, reasonSessionRevoked)
			return
		case !record.ExpiresAt.After(h.Issuer.now()):
			h.refuseToken(w, r, reasonSessionExpired)
			return
		}
	}
	h.rec.record(ctx, opCompatValidate, "success", reasonAccepted, nil)
	out := pyjson.NewObject()
	out.Set("valid", true)
	out.Set("user_id", user.UserID)
	out.Set("email", user.Email)
	out.Set("org_id", user.OrgID)
	out.Set("role", user.Role)
	out.Set("expires_at", nil)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// refuseToken is go-api's answer to a refused token: 200, valid false and
// every other member null.
func (h validateHandler) refuseToken(w http.ResponseWriter, r *http.Request, reason string) {
	h.rec.record(r.Context(), opCompatValidate, "denied", reason, nil)
	out := pyjson.NewObject()
	out.Set("valid", false)
	out.Set("user_id", nil)
	out.Set("email", nil)
	out.Set("org_id", nil)
	out.Set("role", nil)
	out.Set("expires_at", nil)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// validateFault answers a decision that could not be made: a store or key
// set that is down is go-api's 503 (unavailableCompat), anything else the
// generic 500. go-api's own handler answers both with its 500
// (internal/api/session fail checks sessionissue.ErrUnavailable only).
func (h validateHandler) validateFault(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, policy.ErrKeySourceUnavailable) {
		h.unavailableCompat(w, r, opCompatValidate, err)
		return
	}
	h.fail(w, r, opCompatValidate, err)
}
