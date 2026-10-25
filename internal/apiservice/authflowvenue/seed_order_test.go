package authflowvenue_test

import (
	"slices"
	"testing"
)

// seedAround is the order of the corpus seed around the users-owned switch: every run seeds all its users first; the
// run that hands off to the script leaves olga (a github user with no provider id, which the users copy cannot carry)
// out and seeds her once the switch is done.
func seedAround(handOff bool, seedAll func(deferOlga bool), flip, seedOlga func()) {
	seedAll(handOff)
	flip()
	if handOff {
		seedOlga()
	}
}

func recordSeedOrder(handOff bool) []string {
	var events []string
	seedAround(handOff,
		func(deferOlga bool) {
			if deferOlga {
				events = append(events, "seed without olga")
				return
			}
			events = append(events, "seed with olga")
		},
		func() { events = append(events, "flip") },
		func() { events = append(events, "seed olga") })
	return events
}

func TestSeedAroundSeedsOlgaBeforeTheFlipWhenUsersAreNotOwned(t *testing.T) {
	if got, want := recordSeedOrder(false), []string{"seed with olga", "flip"}; !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSeedAroundSeedsOlgaOnlyAfterTheFlipWhenUsersAreOwned(t *testing.T) {
	if got, want := recordSeedOrder(true), []string{"seed without olga", "flip", "seed olga"}; !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}
