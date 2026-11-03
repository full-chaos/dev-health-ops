package principal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// AccessClass is the credential class a bearer token's JOSE header names,
// read before any verifier runs.
type AccessClass int

const (
	// AccessClassOther is every token that is neither class below: the
	// effective-principal envelope (EdDSA, typ jwt) and anything unreadable.
	AccessClassOther AccessClass = iota
	// AccessClassEdge is the HS256 edge access token (ops_access_token_hs256).
	AccessClassEdge
	// AccessClassUser is the EdDSA user access token (user_access_token,
	// access-token.v1): alg EdDSA and typ exactly "at+jwt".
	AccessClassUser
)

// UserAccessTokenTyp is the JWS typ of access-token.v1. It is the one header
// member that tells a user access token from the envelope, which shares its
// alg.
const UserAccessTokenTyp = "at+jwt"

// AccessClassOf reads alg and typ from the token's header segment without
// verifying anything. The segment rules are those of the HS256 dispatch the
// REST and /query paths already used (three segments, base64url, a JSON
// object with a non-empty alg), so an HS256 token is classed as before.
func AccessClassOf(token string) AccessClass {
	segments := strings.Split(token, ".")
	if len(segments) != 3 || segments[0] == "" {
		return AccessClassOther
	}
	raw, err := base64.RawURLEncoding.DecodeString(segments[0])
	if err != nil {
		return AccessClassOther
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(raw, &header); err != nil || header.Alg == "" {
		return AccessClassOther
	}
	switch {
	case header.Alg == EdgeAlgorithm:
		return AccessClassEdge
	case header.Alg == "EdDSA" && header.Typ == UserAccessTokenTyp:
		return AccessClassUser
	}
	return AccessClassOther
}

// RoutedAccess adapts the profile-routed validator to policy's routed
// validator: an unavailable validator is policy.ErrUnavailable (503), any
// other error a refusal (401). It is go-api's adapter (apiservice
// routedAccessVerify), which query-api cannot import.
func RoutedAccess(validator *accessverify.Validator) policy.RoutedValidator {
	return routedAccess{validator: validator}
}

type routedAccess struct{ validator *accessverify.Validator }

var errUnexpectedAccessResult = errors.New("principal: access verify: result of an unexpected class")

func (r routedAccess) Validate(ctx context.Context, service, profile, bearer string) (*policy.RoutedResult, error) {
	result, err := r.validator.Validate(ctx, service, profile, bearer)
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
	return nil, errUnexpectedAccessResult
}

// LegacyVerifier maps an HS256 authenticate function's errors the way go-api
// does (acp D3): a refusal and a claim of the wrong type are refusals (401);
// any other store failure is unavailable (503), never a pass.
func LegacyVerifier(authenticate func(context.Context, string) (*policy.User, error)) accessverify.LegacyVerifier {
	return func(ctx context.Context, token string) (any, error) {
		user, err := authenticate(ctx, token)
		return legacyResult(user, err)
	}
}

func legacyResult(user *policy.User, err error) (any, error) {
	switch {
	case err == nil:
		return user, nil
	case policy.IsRefusal(err), policy.IsClaimTypeError(err):
		return nil, err
	default:
		return nil, fmt.Errorf("%w: %w", accessverify.ErrUnavailable, err)
	}
}

// RouteAccess makes VerifyRouted pick the validator from the request's
// endpoint-profile row. Call it once, before the verifier serves.
func (v *EdgeVerifier) RouteAccess(validator *accessverify.Validator, service string) error {
	if validator == nil {
		return errors.New("principal: edge: routed access needs a validator")
	}
	return v.auth.RouteAccess(RoutedAccess(validator), service)
}

// Routed reports whether RouteAccess was applied; false for a nil verifier.
func (v *EdgeVerifier) Routed() bool { return v != nil && v.auth.Routed() }

// LegacyAccess is the validator's HS256 verifier for the REST routes: the
// JWT and users-row half of Verify, with Verify's per-reason telemetry, and
// without the organization rule, which VerifyRouted applies once the
// validator has accepted the token.
func (v *EdgeVerifier) LegacyAccess() accessverify.LegacyVerifier {
	return func(ctx context.Context, token string) (any, error) {
		meta := requestMetaFrom(ctx)
		claims, err := v.verifier.Verify(token)
		if err != nil {
			v.reject(ctx, edgetoken.ReasonOf(err), claims, meta)
			return nil, fmt.Errorf("principal: edge: %w", err)
		}
		user, err := v.auth.CheckClaims(ctx, claims)
		if err != nil {
			reason := ReasonUserRefused
			if !policy.IsRefusal(err) {
				reason = ReasonUserLookupFailed
			}
			v.reject(ctx, reason, claims, meta)
		}
		return legacyResult(user, err)
	}
}

// VerifyRouted authenticates token against the row of profile ("METHOD
// /route") through the profile-routed validator, then applies Verify's
// organization rule to either class: a caller with no organization is
// refused with OrgRequiredError unless the live users row is a superuser's.
// An error wrapping policy.ErrUnavailable decided nothing; a refusal is
// policy.IsRefusal. The edge outcome telemetry counts the HS256 class only.
func (v *EdgeVerifier) VerifyRouted(ctx context.Context, profile, token string) (*EdgeClaims, error) {
	edge := AccessClassOf(token) == AccessClassEdge
	user, err := v.auth.Authenticate(policy.WithRouteProfile(ctx, profile), token)
	if err != nil {
		return nil, fmt.Errorf("principal: edge: %w", err)
	}
	if user.OrgID == "" && !user.IsSuperuser {
		if edge {
			v.reject(ctx, ReasonOrgMissing, nil, requestMetaFrom(ctx))
		}
		return nil, &OrgRequiredError{}
	}
	if edge {
		recordEdgeVerifyOutcome("verified")
	}
	return &EdgeClaims{OrgID: user.OrgID}, nil
}
