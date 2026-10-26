package saml

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// r1 review probe: go-api stores only NameID.Value, so readiness cannot tell a transient value (which Subject
// refuses) from a persistent one. A bound SAML account is never waived.
func TestTransientNameIDIsNotRelinkable(t *testing.T) {
	s := func(v string) *string { return &v }
	row := uuid.New()
	value := "_8f2c1e0a9b7d"
	if _, err := Subject(NameID{Format: NameIDFormatTransient, Value: value}); !errors.Is(err, ErrSubject) {
		t.Fatalf("Subject(transient) = %v, want ErrSubject", err)
	}
	if identity.IsSSORelinkable(s("saml"), s(value), &row, s("saml")) {
		t.Fatal("IsSSORelinkable(saml, stored NameID value) = true: readiness waives an account whose NameID may be transient")
	}
}
