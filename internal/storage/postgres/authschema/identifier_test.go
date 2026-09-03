package authschema

import (
	"strings"
	"testing"
)

// TestNewValidatedIdentifierRefusesEveryInjectionShape moves the injection
// corpus onto the CONSTRUCTOR, which is now the security boundary.
//
// The 18 shapes were previously asserted against ValidateIdentifier, which was
// correct but incomplete: nothing forced an interpolation site to CALL it. Now
// quoteIdentifier accepts only ValidatedIdentifier and the only way to obtain
// one is this constructor, so refusing here is refusing everywhere the type is
// required.
func TestNewValidatedIdentifierRefusesEveryInjectionShape(t *testing.T) {
	for _, testCase := range []struct{ name, identifier string }{
		{"double quote closes the quoted identifier", `auth"`},
		{"embedded quote mid-token", `au"th`},
		{"statement separator", "auth;DROP SCHEMA public"},
		{"leading whitespace", " auth"},
		{"trailing whitespace", "auth "},
		{"internal whitespace", "auth schema"},
		{"SQL line comment", "auth--comment"},
		{"block comment open", "auth/*"},
		{"leading digit", "1auth"},
		{"uppercase", "Auth"},
		{"reserved word", "select"},
		{"64 characters, one past the limit", strings.Repeat("a", 64)},
		{"empty", ""},
		{"non-ASCII letter that looks ASCII", "authа"}, // Cyrillic a
		{"newline", "auth\nschema"},
		{"null byte", "auth\x00"},
		{"backslash", `auth\`},
		{"dollar quote", "auth$$"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := NewValidatedIdentifier(testCase.identifier)
			if err == nil {
				t.Errorf("NewValidatedIdentifier(%q) succeeded; it could then be "+
					"interpolated into DDL", testCase.identifier)
			}
			if got != (ValidatedIdentifier{}) {
				t.Errorf("a rejected identifier returned a non-zero value %q; a failed "+
					"construction must not yield something quoteIdentifier would render",
					got.String())
			}
		})
	}

	// The accepting row. Without it a constructor that refused everything would
	// pass every case above and the package would migrate nothing at all.
	for _, valid := range []string{"auth", "a", "auth_schema", "auth2", strings.Repeat("a", 63)} {
		id, err := NewValidatedIdentifier(valid)
		if err != nil {
			t.Errorf("NewValidatedIdentifier(%q) = %v, want acceptance", valid, err)
			continue
		}
		if id.String() != valid {
			t.Errorf("round-trip lost the name: %q became %q", valid, id.String())
		}
		if quoted := quoteIdentifier(id); quoted != `"`+valid+`"` {
			t.Errorf("quoteIdentifier(%q) = %q", valid, quoted)
		}
	}
}

// TestZeroValueIdentifierCannotReachSQL closes the one hole the type leaves
// open to its own package: ValidatedIdentifier{} satisfies the type without
// having passed the constructor. Rendering it would emit `""` and produce a
// syntax error at the server, far from the mistake and after a statement has
// already been built. It panics instead, which is correct for a state no input
// can produce -- valid or hostile.
func TestZeroValueIdentifierCannotReachSQL(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("quoteIdentifier rendered a zero-value identifier instead of refusing it")
		}
		if !strings.Contains(recovered.(string), "NewValidatedIdentifier") {
			t.Errorf("panic message does not name the remedy: %v", recovered)
		}
	}()
	_ = quoteIdentifier(ValidatedIdentifier{})
}
