package principal

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

type stubEdDSA struct {
	claims *authverify.AccessClaims
	err    error
}

func (s stubEdDSA) Verify(string, time.Time) (*authverify.AccessClaims, error) {
	return s.claims, s.err
}

func stubValidator(t *testing.T, eddsa accessverify.EdDSAVerifier, legacy accessverify.LegacyVerifier) *accessverify.Validator {
	t.Helper()
	validator, err := accessverify.New(accessverify.Config{
		Rows:   []accessverify.Row{{Service: "svc", Profile: "GET /r", AcceptsUser: true, LegacyHS256: true}},
		EdDSA:  eddsa,
		Legacy: legacy,
		Meter:  sdkmetric.NewMeterProvider().Meter("test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return validator
}

func headerToken(header string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(header)) + ".e30.c2ln"
}

// RoutedAccess maps the validator's answers for policy: a JWKS outage is
// policy.ErrKeySourceUnavailable, another unavailable validator
// policy.ErrUnavailable, a refusal stays a refusal, and a result of neither
// class is an error, never a pass.
func TestRoutedAccessMapsTheValidatorForPolicy(t *testing.T) {
	user := headerToken(`{"alg":"EdDSA","typ":"at+jwt","kid":"k"}`)
	legacy := headerToken(`{"alg":"HS256"}`)
	org := "o"
	cases := map[string]struct {
		eddsa  stubEdDSA
		legacy accessverify.LegacyVerifier
		token  string
		check  func(*policy.RoutedResult, error) error
	}{
		"jwks outage": {stubEdDSA{err: authverify.ErrKeySourceUnavailable}, nil, user, func(_ *policy.RoutedResult, err error) error {
			if !errors.Is(err, policy.ErrKeySourceUnavailable) || !errors.Is(err, policy.ErrUnavailable) {
				return fmt.Errorf("err %v", err)
			}
			return nil
		}},
		"legacy store outage": {stubEdDSA{}, func(context.Context, string) (any, error) {
			return nil, fmt.Errorf("%w: down", accessverify.ErrUnavailable)
		}, legacy, func(_ *policy.RoutedResult, err error) error {
			if !errors.Is(err, policy.ErrUnavailable) || errors.Is(err, policy.ErrKeySourceUnavailable) {
				return fmt.Errorf("err %v", err)
			}
			return nil
		}},
		"refusal": {stubEdDSA{err: errors.New("bad signature")}, nil, user, func(_ *policy.RoutedResult, err error) error {
			if !errors.Is(err, accessverify.ErrRefused) || errors.Is(err, policy.ErrUnavailable) {
				return fmt.Errorf("err %v", err)
			}
			return nil
		}},
		"user claims": {stubEdDSA{claims: &authverify.AccessClaims{Subject: "prn_x", SessionID: "ses_y", OrgID: &org}}, nil, user, func(r *policy.RoutedResult, err error) error {
			if err != nil || r.Legacy != nil || r.Claims == nil || r.Claims.Subject != "prn_x" || r.Claims.SessionID != "ses_y" || r.Claims.OrgID != &org {
				return fmt.Errorf("result %+v err %v", r, err)
			}
			return nil
		}},
		"legacy user": {stubEdDSA{}, func(context.Context, string) (any, error) { return &policy.User{UserID: "u"}, nil }, legacy, func(r *policy.RoutedResult, err error) error {
			if err != nil || r.Claims != nil || r.Legacy == nil || r.Legacy.UserID != "u" {
				return fmt.Errorf("result %+v err %v", r, err)
			}
			return nil
		}},
		"legacy result of another type": {stubEdDSA{}, func(context.Context, string) (any, error) { return "user", nil }, legacy, func(r *policy.RoutedResult, err error) error {
			if r != nil || !errors.Is(err, errUnexpectedAccessResult) {
				return fmt.Errorf("result %+v err %v", r, err)
			}
			return nil
		}},
		"legacy nil user": {stubEdDSA{}, func(context.Context, string) (any, error) { return (*policy.User)(nil), nil }, legacy, func(r *policy.RoutedResult, err error) error {
			if r != nil || !errors.Is(err, errUnexpectedAccessResult) {
				return fmt.Errorf("result %+v err %v", r, err)
			}
			return nil
		}},
	}
	for name, tc := range cases {
		result, err := RoutedAccess(stubValidator(t, tc.eddsa, tc.legacy)).Validate(context.Background(), "svc", "GET /r", tc.token)
		if problem := tc.check(result, err); problem != nil {
			t.Errorf("%s: %v", name, problem)
		}
	}
}

// LegacyVerifier (acp D3): a refusal and a claim of the wrong type are
// refusals; any other failure is unavailable, never a pass.
func TestLegacyVerifierMapsTheHS256Errors(t *testing.T) {
	user := &policy.User{UserID: "u"}
	if got, err := LegacyVerifier(func(context.Context, string) (*policy.User, error) { return user, nil })(context.Background(), "t"); err != nil || got != user {
		t.Fatalf("accept: %v %v", got, err)
	}
	for name, tc := range map[string]struct {
		err         error
		unavailable bool
	}{
		"store outage": {fmt.Errorf("x: %w", policy.ErrUnavailable), true},
		"store error":  {errors.New("syntax error"), true},
	} {
		_, err := LegacyVerifier(func(context.Context, string) (*policy.User, error) { return nil, tc.err })(context.Background(), "t")
		if errors.Is(err, accessverify.ErrUnavailable) != tc.unavailable {
			t.Errorf("%s: err %v, unavailable want %v", name, err, tc.unavailable)
		}
	}
}

func TestVerifyRoutedNeedsRouteAccess(t *testing.T) {
	var nilVerifier *EdgeVerifier
	if nilVerifier.Routed() {
		t.Fatal("a nil verifier reports routed")
	}
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)
	if v.Routed() {
		t.Fatal("NewEdgeVerifier alone is routed")
	}
	if err := v.RouteAccess(nil, "svc"); err == nil {
		t.Fatal("RouteAccess accepted a nil validator")
	}
	if err := v.RouteAccess(stubValidator(t, stubEdDSA{}, nil), "svc"); err != nil || !v.Routed() {
		t.Fatalf("RouteAccess: err %v routed %v", err, v.Routed())
	}
}
