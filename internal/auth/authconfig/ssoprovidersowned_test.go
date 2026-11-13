package authconfig

import "testing"

func TestSSOProvidersOwnedFromLookup(t *testing.T) {
	for value, want := range map[string]bool{"": false, "false": false, " FALSE ": false, "true": true, " True ": true} {
		got, err := SSOProvidersOwnedFromLookup(func(string) (string, bool) { return value, value != "" })
		if err != nil || got != want {
			t.Errorf("%q: %v, %v; want %v", value, got, err, want)
		}
	}
	for _, bad := range []string{"1", "yes", "on", "maybe"} {
		if _, err := SSOProvidersOwnedFromLookup(func(string) (string, bool) { return bad, true }); err == nil {
			t.Errorf("%q is accepted", bad)
		}
	}
}
