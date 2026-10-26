package usersbackfill

import (
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

// A github user with no provider id is skipped by the users copy (the users_owned guard then refuses the switch); with the
// owned leg's provider id it is carried. The guard itself refusing such a user in a live PostgreSQL is
// TestUsersOwnedGuardCountsEachGapInLivePostgres (internal/auth/authruntime, container).
func TestOwnedLegSeedMakesEveryGithubUserCarryableAndTheOtherLegsKeepOlgaUncarried(t *testing.T) {
	skipped := func(owned bool) []string {
		var emails []string
		for _, user := range sessionscenario.SeedUsers(owned) {
			row := Row{Email: user.Email}
			if user.Provider != "" {
				row.AuthProvider = &user.Provider
			}
			if user.ProviderID != "" {
				row.AuthProviderID = &user.ProviderID
			}
			if _, class, _ := Map(row); class == SkipInvalidIdentity {
				emails = append(emails, user.Email)
			}
		}
		return emails
	}
	if got := skipped(false); !slices.Equal(got, []string{"olga@example.com"}) {
		t.Errorf("the other legs' users the copy skips: %q, want only olga", got)
	}
	if got := skipped(true); len(got) != 0 {
		t.Errorf("the owned leg's users the copy skips: %q, want none", got)
	}
}
