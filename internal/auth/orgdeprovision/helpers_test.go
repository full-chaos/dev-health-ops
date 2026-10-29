package orgdeprovision

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func mustSchema(t *testing.T) authschema.ValidatedIdentifier {
	t.Helper()
	s, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return s
}
