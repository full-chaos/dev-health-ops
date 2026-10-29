package authconfig

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
)

var switchValues = []string{"", "false", "true", "TRUE", " true ", "False", "yes", "0", "on", "1"}

// auth-service reads AUTH_PLATFORM_ROLES_OWNED through the one parse.
func TestPlatformRolesOwnedIsParsedByOneFunction(t *testing.T) {
	for _, value := range switchValues {
		env := baseEnv()
		env[platformrole.OwnedEnv] = value
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		want, werr := platformrole.OwnedFromLookup(lookupFrom(env))
		if (err != nil) != (werr != nil) || (err == nil && cfg.PlatformRolesOwned != want) {
			t.Errorf("%q: owned %v err %v, want %v err %v", value, cfg.PlatformRolesOwned, err, want, werr)
		}
	}
}

// The switch parses exactly like AUTH_USERS_OWNED.
func TestPlatformRolesOwnedParsesLikeUsersOwned(t *testing.T) {
	for _, value := range switchValues {
		on, err := platformrole.OwnedFromLookup(func(name string) (string, bool) { return value, name == platformrole.OwnedEnv })
		users, uerr := UsersOwnedFromLookup(func(name string) (string, bool) { return value, name == EnvUsersOwned })
		if on != users || (err == nil) != (uerr == nil) {
			t.Errorf("%q: platform roles on=%v err=%v, users on=%v err=%v", value, on, err, users, uerr)
		}
	}
}
