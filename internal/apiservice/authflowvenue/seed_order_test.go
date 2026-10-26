package authflowvenue_test

import (
	"slices"
	"testing"
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
