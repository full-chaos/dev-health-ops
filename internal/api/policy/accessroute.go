package policy

import (
	"context"
	"errors"
	"net/http"
)

// RoutedValidator validates a bearer token against the request's
// endpoint-profile row (service, route template). An error wrapping
// ErrUnavailable means it could not decide; any other error is a refusal.
// go-api adapts *accessverify.Validator to it; this package does not import
// the verifier, so the auth-service binaries that import policy do not link it.
type RoutedValidator interface {
	Validate(ctx context.Context, service, profile, bearer string) (*RoutedResult, error)
}

// RoutedResult is an accepted token: Legacy for an HS256 ops access token
// (AuthenticateLegacy's user), Claims for a user access token. Exactly one is
// set.
type RoutedResult struct {
	Legacy *User
	Claims *AccessTokenClaims
}

// AccessTokenClaims are the verified access-token.v1 claims the per-request
// checks read.
type AccessTokenClaims struct {
	Subject   string
	SessionID string
	OrgID     *string
}

type routedAccess struct {
	validator RoutedValidator
	service   string
}

// RouteAccess makes Authenticate pick the access-token validator from the
// request's endpoint-profile row (service, route template). Call it once,
// before the Authenticator serves. Its validator's legacy verifier must be
// AuthenticateLegacy, never Authenticate.
func (a *Authenticator) RouteAccess(validator RoutedValidator, service string) error {
	if validator == nil || service == "" {
		return errors.New("policy: routed access needs a validator and a service")
	}
	a.routed = &routedAccess{validator: validator, service: service}
	return nil
}

// Routed reports whether RouteAccess was applied.
func (a *Authenticator) Routed() bool { return a.routed != nil }

// routeProfileKey is its own type, so no contextKey value can collide with it.
type routeProfileKey struct{}

// WithRouteProfile returns ctx carrying the request's route template
// ("METHOD /pattern").
func WithRouteProfile(ctx context.Context, profile string) context.Context {
	return context.WithValue(ctx, routeProfileKey{}, profile)
}

// RouteProfileFrom is the route template the request carries, or "".
func RouteProfileFrom(ctx context.Context) string {
	profile, _ := ctx.Value(routeProfileKey{}).(string)
	return profile
}

// RouteProfiles records, before routing, the route template match finds for
// the request, so the scope middlewares and in-handler callers authenticate
// against the route's row.
func RouteProfiles(match func(*http.Request) (string, bool)) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if profile, ok := match(r); ok {
				r = r.WithContext(WithRouteProfile(r.Context(), profile))
			}
			next.ServeHTTP(w, r)
		})
	}
}

// routeCtx is the context a guard authenticates with: inside a route the
// mux's own pattern is the profile.
func (a *Authenticator) routeCtx(r *http.Request) context.Context {
	if a.routed == nil || r.Pattern == "" {
		return r.Context()
	}
	return WithRouteProfile(r.Context(), r.Pattern)
}

func (a *Authenticator) authenticateRouted(ctx context.Context, token string) (*User, error) {
	result, err := a.routed.validator.Validate(ctx, a.routed.service, RouteProfileFrom(ctx), token)
	switch {
	case errors.Is(err, ErrUnavailable):
		return nil, err
	case err != nil, result == nil:
		return nil, errRejected
	case result.Legacy != nil && result.Claims == nil:
		return result.Legacy, nil
	case result.Claims != nil && result.Legacy == nil:
		return a.accessTokenUser(ctx, result.Claims)
	default:
		return nil, errRejected
	}
}

// IsClaimTypeError reports a legacy token claim whose JSON type the Python
// api raised on (a 500 there).
func IsClaimTypeError(err error) bool { return errors.Is(err, errClaimType) }

// mayImpersonate decides whether Impersonation runs its full check, from the
// Principal decision alone: a user access token (it carries no is_superuser
// claim), or a legacy token whose verified is_superuser claim is truthy. With
// no decision, or a failed lookup, the check runs and refuses (fail closed).
func (s *Scope) mayImpersonate(r *http.Request) bool {
	decision, ok := decisionFrom(r.Context())
	if !ok || decision.err != nil {
		return true
	}
	return decision.user != nil && (decision.user.accessToken || decision.user.superuserClaim)
}
