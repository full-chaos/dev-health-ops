package authflowvenue_test

import (
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/usersbackfill"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

// seedAround is the order of the corpus seed around the users-owned switch: every run seeds all its users first, the run
// that hands off to the script with the seed of the leg that owns users (olga with a provider id, so the users copy
// carries her), and the switch follows. Nobody seeds after it.
func seedAround(handOff bool, seedAll func(ownedLeg bool), flip func()) {
	seedAll(handOff)
	flip()
}

func recordSeedOrder(handOff bool) []string {
	var events []string
	seedAround(handOff,
		func(ownedLeg bool) {
			if ownedLeg {
				events = append(events, "seed of the owned leg")
				return
			}
			events = append(events, "seed")
		},
		func() { events = append(events, "flip") })
	return events
}

func TestSeedAroundSeedsBeforeTheFlipWhenUsersAreNotOwned(t *testing.T) {
	if got, want := recordSeedOrder(false), []string{"seed", "flip"}; !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSeedAroundSeedsTheOwnedLegsSeedBeforeTheFlipAndNothingAfter(t *testing.T) {
	if got, want := recordSeedOrder(true), []string{"seed of the owned leg", "flip"}; !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// A github user with no provider id is skipped by the users copy (the guard then refuses the switch); with the owned leg's
// provider id it is carried. The users_owned guard itself refusing such a user in a live PostgreSQL is
// TestUsersOwnedGuardCountsEachGapInLivePostgres (internal/auth/authruntime, container).
func TestOwnedLegSeedMakesEveryGithubUserCarryableAndTheOtherLegsKeepOlgaUncarried(t *testing.T) {
	skipped := func(owned bool) []string {
		var emails []string
		for _, user := range sessionscenario.SeedUsers(owned) {
			row := usersbackfill.Row{Email: user.Email}
			if user.Provider != "" {
				row.AuthProvider = &user.Provider
			}
			if user.ProviderID != "" {
				row.AuthProviderID = &user.ProviderID
			}
			if _, class, _ := usersbackfill.Map(row); class == usersbackfill.SkipInvalidIdentity {
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
