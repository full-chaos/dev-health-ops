package providersync

import (
	"testing"
	"time"
)

// Clauses of decideMembershipContinuity that the table's axes do not vary. One clause per case.
func TestMembershipContinuityClauses(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	first, later := at.AddDate(0, 0, -30), at.AddDate(0, 0, -10)
	text := func(value string) *string { return &value }
	const provider = "github"
	openRow := func(team, member string, id *string, from, updated time.Time) openMembership {
		return openMembership{Provider: provider, TeamID: team, MemberID: member, ProviderUserID: id, ValidFrom: from, UpdatedAt: updated}
	}
	ret := func(team, member string, id *string, written bool) membershipReturned {
		return membershipReturned{TeamID: team, MemberID: member, ProviderUserID: id, Written: written}
	}
	decide := func(open []openMembership, returned ...membershipReturned) map[string]membershipFactDecision {
		return decideMembershipContinuity(provider, open, returned, at)
	}
	key := membershipFactKey

	t.Run("one open row continues as ONE new member id: the first in sorted order", func(t *testing.T) {
		got := decide([]openMembership{openRow("t", "gh:old", text("7"), first, first)}, ret("t", "gh:zeta", text("7"), true), ret("t", "gh:alpha", text("7"), true))
		if got[key("t", "gh:alpha")].Action != membershipRenameContinue || got[key("t", "gh:zeta")].Action != membershipNewRow {
			t.Fatalf("alpha = %s, zeta = %s, want rename_continue and new_row", got[key("t", "gh:alpha")].Action, got[key("t", "gh:zeta")].Action)
		}
	})
	t.Run("the same id in ANOTHER team is not a rename", func(t *testing.T) {
		if got := decide([]openMembership{openRow("other", "gh:old", text("7"), first, first)}, ret("t", "gh:new", text("7"), true)); got[key("t", "gh:new")].Action != membershipNewRow {
			t.Fatalf("action = %s, want new_row", got[key("t", "gh:new")].Action)
		}
	})
	t.Run("an open row still returned is never replaced", func(t *testing.T) {
		got := decide([]openMembership{openRow("t", "gh:old", text("7"), first, first)}, ret("t", "gh:new", text("7"), true), ret("t", "gh:old", text("9"), true))
		if got[key("t", "gh:new")].Action != membershipNewRow {
			t.Fatalf("action = %s, want new_row", got[key("t", "gh:new")].Action)
		}
	})
	t.Run("a row newer than the read is never replaced and never judged another person", func(t *testing.T) {
		newer := at.Add(time.Minute)
		got := decide([]openMembership{openRow("t", "gh:old", text("7"), first, newer), openRow("t", "gh:m", text("9"), first, newer)}, ret("t", "gh:new", text("7"), true), ret("t", "gh:m", text("3"), true))
		if got[key("t", "gh:new")].Action != membershipNewRow || got[key("t", "gh:m")].Action != membershipContinueRow {
			t.Fatalf("new = %s, m = %s, want new_row and continue_row", got[key("t", "gh:new")].Action, got[key("t", "gh:m")].Action)
		}
	})
	t.Run("a member the guard keeps out (not written) decides nothing", func(t *testing.T) {
		got := decide([]openMembership{openRow("t", "gh:old", text("7"), first, first)}, ret("t", "gh:new", text("7"), false))
		if got[key("t", "gh:new")].Action != membershipNoChange {
			t.Fatalf("action = %s, want no_change", got[key("t", "gh:new")].Action)
		}
	})
	t.Run("a blank id matches nothing, stored or incoming", func(t *testing.T) {
		got := decide([]openMembership{openRow("t", "gh:old", text(" "), first, first)}, ret("t", "gh:new", text(" "), true))
		if got[key("t", "gh:new")].Action != membershipNoChange {
			t.Fatalf("action = %s, want no_change", got[key("t", "gh:new")].Action)
		}
	})
	t.Run("two open rows of the id: the earliest valid_from", func(t *testing.T) {
		got := decide([]openMembership{openRow("t", "gh:a", text("7"), later, later), openRow("t", "gh:b", text("7"), first, first)}, ret("t", "gh:new", text("7"), true))
		decision := got[key("t", "gh:new")]
		if len(decision.Replaced) != 2 || decision.ValidFrom == nil || !decision.ValidFrom.Equal(first) {
			t.Fatalf("replaced = %d, valid_from = %v, want 2 and %s", len(decision.Replaced), decision.ValidFrom, first)
		}
	})
	t.Run("an open row of another provider is never touched", func(t *testing.T) {
		row := openRow("t", "gh:old", text("7"), first, first)
		row.Provider = "gitlab"
		if got := decide([]openMembership{row}, ret("t", "gh:new", text("7"), true)); got[key("t", "gh:new")].Action != membershipNewRow {
			t.Fatalf("action = %s, want new_row", got[key("t", "gh:new")].Action)
		}
	})

	t.Run("a member returned twice is decided once", func(t *testing.T) {
		got := decide([]openMembership{openRow("t", "gh:old", text("7"), first, first)}, ret("t", "gh:new", text("7"), true), ret("t", "gh:new", text("7"), true))
		if got[key("t", "gh:new")].Action != membershipRenameContinue {
			t.Fatalf("action = %s, want rename_continue (the second copy must not decide again)", got[key("t", "gh:new")].Action)
		}
	})
	t.Run("a rename never moves a valid_from later than the rule's", func(t *testing.T) {
		older := at.AddDate(0, 0, -90)
		observed := []githubMembershipRow{{OrgID: "o", Provider: provider, TeamID: "t", MemberID: "gh:new", ProviderUserID: text("7"), ValidFrom: older, UpdatedAt: at}}
		open := []openMembership{openRow("t", "gh:old", text("7"), first, first)}
		stamp := map[string]time.Time{key("t", "gh:new"): older}
		githubMembershipWriter.applyContinuity(open, observed, observed, stamp, at)
		if !stamp[key("t", "gh:new")].Equal(older) {
			t.Fatalf("valid_from = %s, want the rule's earlier %s", stamp[key("t", "gh:new")], older)
		}
	})
}
