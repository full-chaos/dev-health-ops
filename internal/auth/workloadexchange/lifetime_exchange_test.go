package workloadexchange

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

type stubValidator struct{ identity authverify.SubjectIdentity }

func (s stubValidator) Validate(context.Context, string) (authverify.SubjectIdentity, error) {
	return s.identity, nil
}

type stubResolver struct{}

func (stubResolver) Resolve(context.Context, authverify.SubjectIdentity) (authverify.WorkloadBinding, error) {
	return authverify.WorkloadBinding{BindingID: "binding", OrgID: "org", GrantedScopes: []string{testAction}}, nil
}

type stubIssuer struct{ issued authverify.IssuedToken }

func (s stubIssuer) Issue(context.Context, authverify.WorkloadBinding, []string, time.Time) (authverify.IssuedToken, error) {
	return s.issued, nil
}

func TestOpsCeilingEqualsAuthverifyCeiling(t *testing.T) {
	if contracts.WorkloadTokenLifetimeCeiling != authverify.WorkloadTokenLifetimeCeiling {
		t.Fatalf("ops ceiling %v, dev-health-go ceiling %v", contracts.WorkloadTokenLifetimeCeiling, authverify.WorkloadTokenLifetimeCeiling)
	}
}

func TestMintedTokenPassesExchangeLifetimeCheck(t *testing.T) {
	cases := []struct {
		name        string
		subjectLife time.Duration
		lifetime    time.Duration
	}{
		{"full ceiling", time.Hour, 300 * time.Second},
		{"cut by subject expiry", 100 * time.Second, 100 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			answer := decodeAnswer(t, h.post(form(subjectToken(h.now.Add(tc.subjectLife)), "auth-service", "").Encode()))
			now := h.now
			exp := now.Add(tc.lifetime)
			exchange := func(issued authverify.IssuedToken) error {
				svc, err := authverify.NewWorkloadTokenExchangeService(stubValidator{}, stubResolver{}, stubIssuer{issued: issued})
				if err != nil {
					t.Fatal(err)
				}
				_, err = svc.Exchange(t.Context(), "subject", nil)
				return err
			}
			if err := exchange(authverify.IssuedToken{Token: answer.AccessToken, IssuedAt: &now, ExpiresAt: &exp}); err != nil {
				t.Fatalf("the ops-minted token fails the dev-health-go Exchange lifetime rule: %v", err)
			}
			if err := exchange(authverify.IssuedToken{Token: answer.AccessToken, ExpiresAt: &exp}); !errors.Is(err, authverify.ErrIssuedLifetimeUnknown) {
				t.Fatalf("nil IssuedAt: got %v, want ErrIssuedLifetimeUnknown", err)
			}
			late := now.Add(time.Second)
			if err := exchange(authverify.IssuedToken{Token: answer.AccessToken, IssuedAt: &late, ExpiresAt: &exp}); !errors.Is(err, authverify.ErrIssuedLifetimeMismatch) {
				t.Fatalf("IssuedAt +1s: got %v, want ErrIssuedLifetimeMismatch", err)
			}
		})
	}
}
