package policy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

// RoutedValidator is satisfied by *accessverify.Validator.
type RoutedValidator interface {
	Validate(ctx context.Context, service, profile, bearer string) (*accessverify.Result, error)
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

const routeProfileKey contextKey = 101

// WithRouteProfile returns ctx carrying the request's route template
// ("METHOD /pattern").
func WithRouteProfile(ctx context.Context, profile string) context.Context {
	return context.WithValue(ctx, routeProfileKey, profile)
}

// RouteProfileFrom is the route template the request carries, or "".
func RouteProfileFrom(ctx context.Context) string {
	profile, _ := ctx.Value(routeProfileKey).(string)
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
	case errors.Is(err, accessverify.ErrUnavailable):
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	case err != nil:
		return nil, errRejected
	case result == nil:
		return nil, errRejected
	}
	switch result.Class {
	case accessverify.ClassLegacy:
		user, ok := result.Legacy.(*User)
		if !ok || user == nil {
			return nil, errRejected
		}
		return user, nil
	case accessverify.ClassUser:
		if result.Claims == nil {
			return nil, errRejected
		}
		return a.accessTokenUser(ctx, result.Claims)
	default:
		return nil, errRejected
	}
}

// IsClaimTypeError reports a legacy token claim whose JSON type the Python
// api raised on (a 500 there).
func IsClaimTypeError(err error) bool { return errors.Is(err, errClaimType) }

// mayImpersonate decides whether Impersonation runs its full check. A user
// access token carries no is_superuser claim, so on a routed Authenticator an
// EdDSA bearer always runs it.
func (s *Scope) mayImpersonate(r *http.Request) bool {
	if maybeSuperuser(r) {
		return true
	}
	return s.auth != nil && s.auth.routed != nil && bearerAlg(r) == "EdDSA"
}

// bearerAlg is the alg of the first Authorization header's bearer token,
// unverified; it only decides whether a check runs.
func bearerAlg(r *http.Request) string {
	values := r.Header.Values("Authorization")
	if len(values) == 0 {
		return ""
	}
	token, ok := BearerToken(values[0])
	if !ok {
		return ""
	}
	head, _, _ := strings.Cut(token, ".")
	raw, err := base64.RawURLEncoding.DecodeString(head)
	if err != nil {
		return ""
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if json.Unmarshal(raw, &header) != nil {
		return ""
	}
	return header.Alg
}
