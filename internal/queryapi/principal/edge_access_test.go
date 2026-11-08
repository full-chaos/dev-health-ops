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

func stubValidator(t *testing.T, eddsa accessverify.EdDSAVerifier) *accessverify.Validator {
	t.Helper()
	validator, err := accessverify.New(accessverify.Config{
		Rows:  []accessverify.Row{{Service: "svc", Profile: "GET /r", AcceptsUser: true}},
		EdDSA: eddsa,
		Meter: sdkmetric.NewMeterProvider().Meter("test"),
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
	hs256 := headerToken(`{"alg":"HS256"}`)
	org := "o"
	cases := map[string]struct {
		eddsa stubEdDSA
		token string
		check func(*policy.RoutedResult, error) error
	}{
		"jwks outage": {stubEdDSA{err: authverify.ErrKeySourceUnavailable}, user, func(_ *policy.RoutedResult, err error) error {
			if !errors.Is(err, policy.ErrKeySourceUnavailable) || !errors.Is(err, policy.ErrUnavailable) {
				return fmt.Errorf("err %v", err)
			}
			return nil
		}},
		"refusal": {stubEdDSA{err: errors.New("bad signature")}, user, func(_ *policy.RoutedResult, err error) error {
			if !errors.Is(err, accessverify.ErrRefused) || errors.Is(err, policy.ErrUnavailable) {
				return fmt.Errorf("err %v", err)
			}
			return nil
		}},
		"hs256 refused": {stubEdDSA{claims: &authverify.AccessClaims{Subject: "prn_x"}}, hs256, func(r *policy.RoutedResult, err error) error {
			if r != nil || !errors.Is(err, accessverify.ErrRefused) || errors.Is(err, policy.ErrUnavailable) {
				return fmt.Errorf("result %+v err %v", r, err)
			}
			return nil
		}},
		"user claims": {stubEdDSA{claims: &authverify.AccessClaims{Subject: "prn_x", SessionID: "ses_y", OrgID: &org}}, user, func(r *policy.RoutedResult, err error) error {
			if err != nil || r.Claims == nil || r.Claims.Subject != "prn_x" || r.Claims.SessionID != "ses_y" || r.Claims.OrgID != &org {
				return fmt.Errorf("result %+v err %v", r, err)
			}
			return nil
		}},
	}
	for name, tc := range cases {
		result, err := RoutedAccess(stubValidator(t, tc.eddsa)).Validate(context.Background(), "svc", "GET /r", tc.token)
		if problem := tc.check(result, err); problem != nil {
			t.Errorf("%s: %v", name, problem)
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
	if err := v.RouteAccess(stubValidator(t, stubEdDSA{}), "svc"); err != nil || !v.Routed() {
		t.Fatalf("RouteAccess: err %v routed %v", err, v.Routed())
	}
}

// TestRoutedAccessCarriesTheActor: query-api's adapter hands the act claim of
// a delegated session's access token to policy, as go-api's does.
func TestRoutedAccessCarriesTheActor(t *testing.T) {
	user := headerToken(`{"alg":"EdDSA","typ":"at+jwt","kid":"k"}`)
	actor := "prn_actor"
	result, err := RoutedAccess(stubValidator(t, stubEdDSA{claims: &authverify.AccessClaims{Subject: "prn_x", SessionID: "ses_y", ActorSubject: &actor}})).Validate(context.Background(), "svc", "GET /r", user)
	if err != nil || result.Claims == nil || result.Claims.Actor == nil || *result.Claims.Actor != actor {
		t.Fatalf("result %+v err %v, want Actor %s", result, err, actor)
	}
	result, err = RoutedAccess(stubValidator(t, stubEdDSA{claims: &authverify.AccessClaims{Subject: "prn_x", SessionID: "ses_y"}})).Validate(context.Background(), "svc", "GET /r", user)
	if err != nil || result.Claims == nil || result.Claims.Actor != nil {
		t.Fatalf("result %+v err %v, want no Actor", result, err)
	}
}
