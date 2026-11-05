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

// A categorize override that switches provider must bring its own key: the
// stored key belongs to the other vendor.
func TestAProviderChangeOfTheCategorizeOverrideNeedsANewKey(t *testing.T) {
	clef, typesafe, blank := "clef", "typesafe", ""
	for name, tc := range map[string]struct {
		purpose        Purpose
		keyStored      bool
		storedProvider *string
		newProvider    string
		want           bool
	}{
		"clef to typesafe":         {PurposeCategorize, true, &clef, "typesafe", true},
		"typesafe to clef":         {PurposeCategorize, true, &typesafe, "clef", true},
		"typesafe to openai":       {PurposeCategorize, true, &typesafe, "openai", true},
		"same provider":            {PurposeCategorize, true, &typesafe, "typesafe", false},
		"same provider, spelled":   {PurposeCategorize, true, &typesafe, " TypeSafe ", false},
		"stored provider, spelled": {PurposeCategorize, true, func() *string { s := " CLEF "; return &s }(), "clef", false},
		"no provider row":          {PurposeCategorize, true, nil, "typesafe", true},
		"blank provider row":       {PurposeCategorize, true, &blank, "typesafe", true},
		"no key stored":            {PurposeCategorize, false, &clef, "typesafe", false},
		"explain is not this rule": {PurposeExplain, true, &clef, "typesafe", false},
	} {
		if got := ProviderChangeNeedsKey(tc.purpose, tc.keyStored, tc.storedProvider, tc.newProvider); got != tc.want {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
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
