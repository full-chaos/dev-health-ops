package oidc

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// An OIDC subject the id-token check refuses is never waived by readiness, and the id-token check and the relink
// waiver apply one key rule.
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
	// The id-token check and the relink waiver use one key rule.
	for _, sub := range []string{"sub", "su b", " sub", "sub\t", "s\ufffdub", strings.Repeat("s", maxSubjectBytes+1)} {
		if validSubject(sub) != identity.ValidSubject(sub) {
			t.Errorf("%q: validSubject = %v, identity.ValidSubject = %v", sub, validSubject(sub), identity.ValidSubject(sub))
		}
	}
}
