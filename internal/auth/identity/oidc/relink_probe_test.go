package oidc

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// r1 review probe: an OIDC subject the id-token check refuses is never waived by readiness.
func TestWhitespaceOIDCSubjectIsNotRelinkable(t *testing.T) {
	s := func(v string) *string { return &v }
	row := uuid.New()
	for _, sub := range []string{" sub", "sub ", "sub ", "　sub"} {
		if validSubject(sub) {
			t.Errorf("%q: validSubject = true, want false", sub)
		}
		if identity.IsSSORelinkable(s("oidc"), s(sub), &row, s("oidc")) {
			t.Errorf("%q: IsSSORelinkable = true, want false", sub)
		}
	}
}
