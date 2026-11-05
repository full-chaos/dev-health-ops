package llmorgsettings

import "testing"

// The model list is closed: only the listed versioned ids, never a moving alias
// or another version.
func TestTypeSafeModelsAreAClosedListOfVersionedIDs(t *testing.T) {
	for _, model := range []string{"jev-1.13.0"} {
		if !TypeSafeModelAllowed(model) {
			t.Errorf("%q refused", model)
		}
	}
	for _, model := range []string{"", "jev-latest", "jev-preview", "jev-1.14.0", "jev-1.13", "JEV-1.13.0", " jev-1.13.0", "jev-1.13.0 ", "jev-1.13.01", "clef"} {
		if TypeSafeModelAllowed(model) {
			t.Errorf("%q accepted", model)
		}
	}
	if got := TypeSafeModels(); len(got) != 1 || got[0] != "jev-1.13.0" {
		t.Fatalf("TypeSafeModels = %v", got)
	}
}

// A typesafe override needs a key of its own: a write with none, and none to
// inherit, is refused (purposeNeedsKey reads this rule).
func TestATypeSafeOverrideNeedsAKey(t *testing.T) {
	if CredentialsComplete(ProviderTypeSafe, "") {
		t.Fatal("typesafe is complete with no key")
	}
	if !CredentialsComplete(ProviderTypeSafe, testTypeSafeKey) {
		t.Fatal("typesafe is incomplete with a key")
	}
}
