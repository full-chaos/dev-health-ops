package authconfig

import "testing"

func TestOwnedLookupsShareOneParse(t *testing.T) {
	for _, value := range []string{"", "false", "true", "TRUE", " true ", "False", "yes", "1", "on", "enabled"} {
		users, uerr := UsersOwnedFromLookup(lookupFrom(map[string]string{EnvUsersOwned: value}))
		orgs, oerr := OrgsOwnedFromLookup(lookupFrom(map[string]string{EnvOrgsOwned: value}))
		if (uerr == nil) != (oerr == nil) || users != orgs {
			t.Fatalf("value %q: users=%v/%v orgs=%v/%v", value, users, uerr, orgs, oerr)
		}
	}
	if _, err := OrgsOwnedFromLookup(lookupFrom(map[string]string{EnvOrgsOwned: "maybe"})); err == nil || err.Error() != "AUTH_ORGS_OWNED must be true or false" {
		t.Fatalf("err = %v", err)
	}
}
