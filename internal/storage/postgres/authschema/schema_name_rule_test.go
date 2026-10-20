package authschema

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// The ops migrator exempts exactly one schema from its foreign-database check:
// the one the auth migrator is configured to own. It must therefore accept a
// name only if the auth migrator accepts it too (and never "public", which the
// ops migrator owns). Both call internal/platform/pgident; this table pins the
// behaviour of both callers so neither can drift from the other.
func TestBothMigratorsAgreeOnWhichSchemaNamesTheAuthMigratorCanOwn(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		authOK   bool
		dhoUsage bool
	}{
		{"auth", true, true},
		{"identity_plane", true, true},
		{"with", false, false},
		{"user", false, false},
		{"select", false, false},
		{"Auth", false, false},
		{"1auth", false, false},
		{"a-b", false, false},
		{`a"b`, false, false},
		{"", false, false},
		{strings.Repeat("a", 64), false, false},
		{"public", true, false},
	} {
		if got := ValidateIdentifier(testCase.name) == nil; got != testCase.authOK {
			t.Errorf("ValidateIdentifier(%q) ok = %v, want %v", testCase.name, got, testCase.authOK)
		}
		if got := pgmigrate.AuthSchemaUsable(testCase.name); got != testCase.dhoUsage {
			t.Errorf("pgmigrate.AuthSchemaUsable(%q) = %v, want %v", testCase.name, got, testCase.dhoUsage)
		}
	}
}
