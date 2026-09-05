package icfinalize

import (
	"testing"

	"github.com/google/uuid"
)

// CHAOS-4290. SynthesizedRepoID is what makes a redrive safe, so these pin the
// three properties the redrive policy actually depends on -- not merely that
// the function returns something.

// The property the whole no-fail-open policy rests on: a redrive must land on
// the SAME dedup key, so the later computed_at supersedes instead of appending.
func TestSynthesizedRepoIDIsStableAcrossRuns(t *testing.T) {
	first := SynthesizedRepoID("org-1", "wi-only@example.com")
	second := SynthesizedRepoID("org-1", "wi-only@example.com")
	if first != second {
		t.Fatalf("%s != %s -- a redrive would write a NEW dedup key and the rows "+
			"would accumulate rather than supersede, which is the defect this "+
			"deterministic id exists to remove", first, second)
	}
}

// Distinctness, in both fields independently. A "deterministic" id that
// collapsed two identities onto one repo_id would ALSO be stable, and would
// silently merge two people's rows -- stability alone is not the property.
func TestSynthesizedRepoIDSeparatesOrgsAndIdentities(t *testing.T) {
	base := SynthesizedRepoID("org-1", "a@example.com")
	if other := SynthesizedRepoID("org-2", "a@example.com"); other == base {
		t.Fatal("the same identity in two orgs got one repo_id -- cross-org rows " +
			"would collide on the dedup key")
	}
	if other := SynthesizedRepoID("org-1", "b@example.com"); other == base {
		t.Fatal("two identities in one org got one repo_id -- their rows would " +
			"collapse into a single dedup key")
	}
}

// The separator is load-bearing. Without it ("org1" + "ab") and ("org1a" + "b")
// hash identically, so a boundary-forging pair must be shown NOT to collide --
// this is the case a naive concatenation gets wrong.
func TestSynthesizedRepoIDCannotHaveItsFieldBoundaryForged(t *testing.T) {
	if SynthesizedRepoID("org1", "ab") == SynthesizedRepoID("org1a", "b") {
		t.Fatal("the org/identity boundary can be forged by concatenation -- two " +
			"different (org, identity) pairs share a repo_id")
	}
}

// It must NOT vary with anything else -- the day above all. Day is already a
// column of the dedup key, so seeding the id with it would gain no uniqueness
// while fragmenting one identity's synthetic repo across days.
//
// This is a COMPILE-TIME pin, deliberately not a test function. A test body
// asserting `fn != nil` on an assigned function can never fail, so it would
// report success whatever the signature became; the assignment below is what
// actually breaks if a day parameter is ever added.
var _ func(string, string) uuid.UUID = SynthesizedRepoID
