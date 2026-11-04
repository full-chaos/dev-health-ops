package authconfig

import "testing"

func TestUsersOwnedLookupParse(t *testing.T) {
	for value, want := range map[string]bool{"": false, "false": false, "true": true, "TRUE": true, " true ": true, "False": false} {
		got, err := UsersOwnedFromLookup(lookupFrom(map[string]string{EnvUsersOwned: value}))
		if err != nil || got != want {
			t.Fatalf("value %q: got %v, %v; want %v", value, got, err, want)
		}
	}
	for _, value := range []string{"yes", "1", "on", "enabled", "maybe"} {
		if _, err := UsersOwnedFromLookup(lookupFrom(map[string]string{EnvUsersOwned: value})); err == nil || err.Error() != "AUTH_USERS_OWNED must be true or false" {
			t.Fatalf("value %q: err = %v", value, err)
		}
	}
}
