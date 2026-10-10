package providersync

import (
	"testing"
	"time"
)

// planRenames (CHAOS-9144) decides which open rows a run replaces because the
// provider returned the same person (the same provider user id) under a new member
// id. Every clause of the rule has its own case.
func TestPlanRenamesOneClauseAtATime(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	first := at.AddDate(0, 0, -30)
	later := at.AddDate(0, 0, -10)
	id := func(value string) *string { return &value }
	open := func(team, member string, providerID *string, from, updated time.Time) openMembership {
		return openMembership{TeamID: team, MemberID: member, ProviderUserID: providerID, ValidFrom: from, UpdatedAt: updated}
	}
	row := func(team, member string, providerID *string) githubMembershipRow {
		return githubMembershipRow{TeamID: team, MemberID: member, ProviderUserID: providerID, ValidFrom: at}
	}
	key := func(team, member string) string { return team + "\x00" + member }
	// run plans the renames of one case; written defaults to every observed row.
	run := func(openRows []openMembership, observed []githubMembershipRow, written []githubMembershipRow, held ...string) (map[string]bool, map[string]time.Time) {
		t.Helper()
		heldFacts := map[string]bool{}
		for _, fact := range held {
			heldFacts[fact] = true
		}
		for _, o := range observed {
			heldFacts[key(o.TeamID, o.MemberID)] = true
		}
		stamp := map[string]time.Time{}
		for _, o := range observed {
			stamp[key(o.TeamID, o.MemberID)] = at
		}
		if written == nil {
			written = observed
		}
		return githubMembershipWriter.planRenames(openRows, observed, written, heldFacts, stamp, at), stamp
	}

	t.Run("the same id under a new member id in the same team is a rename and keeps the old valid_from", func(t *testing.T) {
		replaced, stamp := run([]openMembership{open("t", "gh:old", id("7"), first, first)}, []githubMembershipRow{row("t", "gh:new", id("7"))}, nil)
		if !replaced[key("t", "gh:old")] || len(replaced) != 1 {
			t.Fatalf("replaced = %v, want gh:old", replaced)
		}
		if !stamp[key("t", "gh:new")].Equal(first) {
			t.Errorf("valid_from of the new member = %s, want the old open row's %s", stamp[key("t", "gh:new")], first)
		}
	})
	t.Run("two open rows of the id: the earliest valid_from is kept and both are replaced", func(t *testing.T) {
		replaced, stamp := run([]openMembership{
			open("t", "gh:a", id("7"), later, later), open("t", "gh:b", id("7"), first, first),
		}, []githubMembershipRow{row("t", "gh:new", id("7"))}, nil)
		if len(replaced) != 2 || !stamp[key("t", "gh:new")].Equal(first) {
			t.Fatalf("replaced = %v, valid_from %s, want both rows and %s", replaced, stamp[key("t", "gh:new")], first)
		}
	})
	t.Run("one open row continues as ONE new member id: the provider id under two new member ids", func(t *testing.T) {
		replaced, stamp := run([]openMembership{open("t", "gh:old", id("7"), first, first)},
			[]githubMembershipRow{row("t", "gh:zeta", id("7")), row("t", "gh:alpha", id("7"))}, nil)
		if len(replaced) != 1 || !replaced[key("t", "gh:old")] {
			t.Fatalf("replaced = %v, want gh:old once", replaced)
		}
		if !stamp[key("t", "gh:alpha")].Equal(first) || !stamp[key("t", "gh:zeta")].Equal(at) {
			t.Errorf("valid_from alpha = %s, zeta = %s, want the old row's %s for the first member id only and the run time %s for the other",
				stamp[key("t", "gh:alpha")], stamp[key("t", "gh:zeta")], first, at)
		}
	})
	t.Run("another id is another person", func(t *testing.T) {
		replaced, stamp := run([]openMembership{open("t", "gh:old", id("7"), first, first)}, []githubMembershipRow{row("t", "gh:new", id("8"))}, nil)
		if len(replaced) != 0 || !stamp[key("t", "gh:new")].Equal(at) {
			t.Fatalf("replaced = %v, valid_from %s, want nothing and the run time", replaced, stamp[key("t", "gh:new")])
		}
	})
	t.Run("the same id in ANOTHER team is not a rename", func(t *testing.T) {
		replaced, _ := run([]openMembership{open("other", "gh:old", id("7"), first, first)}, []githubMembershipRow{row("t", "gh:new", id("7"))}, nil)
		if len(replaced) != 0 {
			t.Fatalf("replaced = %v, want nothing: the open row is of another team", replaced)
		}
	})
	t.Run("an open row with no stored id, and a blank id, match nothing", func(t *testing.T) {
		blank := id("  ")
		replaced, _ := run([]openMembership{open("t", "gh:old", nil, first, first), open("t", "gh:blank", blank, first, first)},
			[]githubMembershipRow{row("t", "gh:new", id("7")), row("t", "gh:other", blank), row("t", "gh:none", nil)}, nil)
		if len(replaced) != 0 {
			t.Fatalf("replaced = %v, want nothing", replaced)
		}
	})
	t.Run("a member already open under its member id is not renamed", func(t *testing.T) {
		replaced, _ := run([]openMembership{open("t", "gh:same", id("7"), first, first), open("t", "gh:old", id("7"), first, first)},
			[]githubMembershipRow{row("t", "gh:same", id("7"))}, nil)
		if len(replaced) != 0 {
			t.Fatalf("replaced = %v, want nothing: the member is open already", replaced)
		}
	})
	t.Run("an open row that is still observed is never replaced", func(t *testing.T) {
		replaced, _ := run([]openMembership{open("t", "gh:old", id("7"), first, first)},
			[]githubMembershipRow{row("t", "gh:new", id("7")), row("t", "gh:old", id("9"))}, nil)
		if len(replaced) != 0 {
			t.Fatalf("replaced = %v, want nothing: the old member is still returned", replaced)
		}
	})
	t.Run("a row newer than the read is never replaced", func(t *testing.T) {
		replaced, _ := run([]openMembership{open("t", "gh:old", id("7"), first, at.Add(time.Minute))}, []githubMembershipRow{row("t", "gh:new", id("7"))}, nil)
		if len(replaced) != 0 {
			t.Fatalf("replaced = %v, want nothing: a run that read the provider after this one wrote the row", replaced)
		}
	})
	t.Run("a new member the guard keeps out is not written, so it replaces nothing", func(t *testing.T) {
		replaced, _ := run([]openMembership{open("t", "gh:old", id("7"), first, first)}, []githubMembershipRow{row("t", "gh:new", id("7"))}, []githubMembershipRow{})
		if len(replaced) != 0 {
			t.Fatalf("replaced = %v, want nothing", replaced)
		}
	})
	t.Run("a writer with no id accessor detects no rename", func(t *testing.T) {
		writer := githubMembershipWriter
		writer.ProviderUserID = nil
		observed := []githubMembershipRow{row("t", "gh:new", id("7"))}
		replaced := writer.planRenames([]openMembership{open("t", "gh:old", id("7"), first, first)}, observed, observed,
			map[string]bool{key("t", "gh:new"): true}, map[string]time.Time{}, at)
		if len(replaced) != 0 {
			t.Fatalf("replaced = %v, want nothing", replaced)
		}
	})
}

// planIDReuse (round 122 r4): the same member id back with another provider user id is a new person.
func TestPlanIDReuseOneClauseAtATime(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	first := at.AddDate(0, 0, -30)
	id := func(value string) *string { return &value }
	open := func(member string, providerID *string, updated time.Time) openMembership {
		return openMembership{TeamID: "t", MemberID: member, ProviderUserID: providerID, ValidFrom: first, UpdatedAt: updated}
	}
	row := func(member string, providerID *string) githubMembershipRow {
		return githubMembershipRow{TeamID: "t", MemberID: member, ProviderUserID: providerID, ValidFrom: at}
	}
	run := func(openRows []openMembership, observed []githubMembershipRow, written []githubMembershipRow) ([]openMembership, map[string]time.Time) {
		stamp := map[string]time.Time{}
		for _, o := range observed {
			stamp["t\x00"+o.MemberID] = first
		}
		if written == nil {
			written = observed
		}
		return githubMembershipWriter.planIDReuse(openRows, observed, written, stamp, at), stamp
	}
	t.Run("another id on the same member id closes the open row and starts at the run time", func(t *testing.T) {
		closing, stamp := run([]openMembership{open("gh:a", id("2"), first)}, []githubMembershipRow{row("gh:a", id("3"))}, nil)
		if len(closing) != 1 || !stamp["t\x00gh:a"].Equal(at) {
			t.Fatalf("closing = %v, valid_from %s, want the open row and %s", closing, stamp["t\x00gh:a"], at)
		}
	})
	t.Run("the same id is the same person", func(t *testing.T) {
		if closing, _ := run([]openMembership{open("gh:a", id(" 3 "), first)}, []githubMembershipRow{row("gh:a", id("3"))}, nil); len(closing) != 0 {
			t.Fatalf("closing = %v, want nothing", closing)
		}
	})
	t.Run("a stored row with no id, or a blank id, is never judged another person", func(t *testing.T) {
		if closing, _ := run([]openMembership{open("gh:a", nil, first), open("gh:b", id(" "), first)}, []githubMembershipRow{row("gh:a", id("3")), row("gh:b", id("3"))}, nil); len(closing) != 0 {
			t.Fatalf("closing = %v, want nothing", closing)
		}
	})
	t.Run("an observed row with no id changes nothing", func(t *testing.T) {
		if closing, _ := run([]openMembership{open("gh:a", id("2"), first), open("gh:b", id("2"), first)}, []githubMembershipRow{row("gh:a", nil), row("gh:b", id(" "))}, nil); len(closing) != 0 {
			t.Fatalf("closing = %v, want nothing", closing)
		}
	})
	t.Run("one open row of the fact with no id or the same id keeps the fact one person", func(t *testing.T) {
		if closing, _ := run([]openMembership{open("gh:a", id("2"), first), open("gh:a", nil, first)}, []githubMembershipRow{row("gh:a", id("3"))}, nil); len(closing) != 0 {
			t.Fatalf("closing = %v, want nothing", closing)
		}
	})
	t.Run("a member the guard keeps out is not written, so nothing changes", func(t *testing.T) {
		if closing, _ := run([]openMembership{open("gh:a", id("2"), first)}, []githubMembershipRow{row("gh:a", id("3"))}, []githubMembershipRow{}); len(closing) != 0 {
			t.Fatalf("closing = %v, want nothing", closing)
		}
	})
	t.Run("an open row newer than the read is left alone", func(t *testing.T) {
		if closing, _ := run([]openMembership{open("gh:a", id("2"), at.Add(time.Hour))}, []githubMembershipRow{row("gh:a", id("3"))}, nil); len(closing) != 0 {
			t.Fatalf("closing = %v, want nothing", closing)
		}
	})
}
