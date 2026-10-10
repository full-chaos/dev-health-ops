package providersync

import (
	"sort"
	"strings"
	"time"
)

// CHAOS-9144: ONE decision says what a member the provider returns does to the
// open team_memberships rows of its team: which open row it continues, which it
// closes as history, and from when the written row counts. Every provider writer
// reaches it through MembershipSnapshotWriter.Snapshot; no other code compares a
// provider user id. The decision table is in
// docs/contribute/architecture/team-attribution.md ("Membership continuity") and
// is enumerated cell by cell in membership_continuity_table_test.go.

// membershipAction is the result of the decision for one returned member.
type membershipAction int

const (
	// membershipNoChange: the provider's id is no statement (the row is not
	// written, or carries no id): the snapshot rule alone decides.
	membershipNoChange membershipAction = iota
	// membershipContinueRow: an open row of the same member id is the same person
	// (its stored id is the same, NULL, or not all different): the row continues.
	membershipContinueRow
	// membershipNewRow: no open row holds the member or its id: a new fact.
	membershipNewRow
	// membershipRenameContinue: the member is new under this member id but an open
	// row of the team under another member id holds its id: that row continues here
	// (it is retired as a rename), the written row keeps its earliest valid_from.
	membershipRenameContinue
	// membershipCloseHistoryNewRow: the member id came back with another id than
	// every open row of it holds: those rows are closed as history, the written
	// row starts at the run time.
	membershipCloseHistoryNewRow
	// membershipCloseHistoryRenameContinue: both: the rows of the member id are
	// closed as history and the written row continues the open row(s) that hold
	// its id under another member id.
	membershipCloseHistoryRenameContinue
)

func (action membershipAction) String() string {
	switch action {
	case membershipNoChange:
		return "no_change"
	case membershipContinueRow:
		return "continue_row"
	case membershipNewRow:
		return "new_row"
	case membershipRenameContinue:
		return "rename_continue"
	case membershipCloseHistoryNewRow:
		return "close_history_new_row"
	case membershipCloseHistoryRenameContinue:
		return "close_history_rename_continue"
	}
	return "unknown"
}

// membershipReturned is one member the provider returned, as the decision reads it.
type membershipReturned struct {
	TeamID, MemberID string
	// ProviderUserID is the provider's own id of the member; nil or blank = none.
	ProviderUserID *string
	// Written: the conflict guard keeps the row (it is written this run).
	Written bool
}

// membershipFactDecision is the result for one returned member (a fact).
type membershipFactDecision struct {
	Action membershipAction
	// ValidFrom is the valid_from of the written row when the decision sets it
	// (nil: the snapshot rule's own value stands).
	ValidFrom *time.Time
	// Replaced: open rows of OTHER member ids this row continues (closed as a rename).
	Replaced []openMembership
	// History: open rows of THIS member id closed as history (another person).
	History []openMembership
}

func membershipFactKey(teamID, memberID string) string {
	return MembershipSnapshotKey(MembershipSnapshotRow{TeamID: teamID, MemberID: memberID})
}

// decideMembershipContinuity decides every returned member against the open rows
// of one provider. It is pure: no clock, no store, no provider call. Open rows of
// another provider, and rows newer than the read (at), are never touched. It
// returns one decision per returned fact, by membershipFactKey.
//
// Rules, in order, for a returned member R with a non-blank id I that is written:
//  1. SAME member id: when R has open rows and EVERY one of them stores an id
//     other than I (none is NULL, blank, equal to I, or newer than the read), R is
//     another person under a reused member id: the rows are closed as history and
//     R starts at the run time. Otherwise R's open rows continue.
//  2. RENAME: when R's open rows continue, nothing else happens. Otherwise the open
//     rows of the team under ANOTHER member id that store I (and are not returned,
//     not already taken by an earlier returned member in sorted order, and not newer
//     than the read) are replaced: R takes the earliest valid_from among them.
func decideMembershipContinuity(provider string, open []openMembership, returned []membershipReturned, at time.Time) map[string]membershipFactDecision {
	decisions := map[string]membershipFactDecision{}
	held := map[string]bool{}
	for _, row := range returned {
		held[membershipFactKey(row.TeamID, row.MemberID)] = true
	}
	openByFact := map[string][]openMembership{}
	// A fact with ANY open row newer than the read is never replaced: a run that read the
	// provider after this one wrote it (CHAOS-9204).
	factNewer := map[string]bool{}
	type idKey struct{ team, id string }
	byID := map[idKey][]openMembership{}
	for _, row := range open {
		if row.Provider != provider {
			continue
		}
		key := membershipFactKey(row.TeamID, row.MemberID)
		openByFact[key] = append(openByFact[key], row)
		if row.UpdatedAt.After(at) {
			factNewer[key] = true
		}
		if row.ProviderUserID != nil {
			if id := strings.TrimSpace(*row.ProviderUserID); id != "" {
				byID[idKey{row.TeamID, id}] = append(byID[idKey{row.TeamID, id}], row)
			}
		}
	}
	ordered := append([]membershipReturned(nil), returned...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].TeamID+"\x00"+ordered[i].MemberID < ordered[j].TeamID+"\x00"+ordered[j].MemberID
	})
	taken := map[string]bool{}
	for _, row := range ordered {
		key := membershipFactKey(row.TeamID, row.MemberID)
		if _, seen := decisions[key]; seen {
			continue
		}
		decision := membershipFactDecision{Action: membershipNoChange}
		id := ""
		if row.ProviderUserID != nil {
			id = strings.TrimSpace(*row.ProviderUserID)
		}
		if !row.Written || id == "" {
			decisions[key] = decision
			continue
		}
		same := openByFact[key]
		reused := len(same) > 0
		for _, openRow := range same {
			stored := ""
			if openRow.ProviderUserID != nil {
				stored = strings.TrimSpace(*openRow.ProviderUserID)
			}
			if stored == "" || stored == id || openRow.UpdatedAt.After(at) {
				reused = false
				break
			}
		}
		continues := len(same) > 0 && !reused
		if reused {
			decision.History = same
		}
		if !continues {
			var earliest *time.Time
			for _, candidate := range byID[idKey{row.TeamID, id}] {
				candidateKey := membershipFactKey(candidate.TeamID, candidate.MemberID)
				if held[candidateKey] || taken[candidateKey] || factNewer[candidateKey] {
					continue
				}
				decision.Replaced = append(decision.Replaced, candidate)
				if earliest == nil || candidate.ValidFrom.Before(*earliest) {
					from := candidate.ValidFrom
					earliest = &from
				}
			}
			for _, candidate := range decision.Replaced {
				taken[membershipFactKey(candidate.TeamID, candidate.MemberID)] = true
			}
			if earliest != nil {
				from := *earliest
				decision.ValidFrom = &from
			}
		}
		switch {
		case reused && len(decision.Replaced) > 0:
			decision.Action = membershipCloseHistoryRenameContinue
		case reused:
			decision.Action = membershipCloseHistoryNewRow
			from := at
			decision.ValidFrom = &from
		case len(decision.Replaced) > 0:
			decision.Action = membershipRenameContinue
		case continues:
			decision.Action = membershipContinueRow
		default:
			decision.Action = membershipNewRow
		}
		decisions[key] = decision
	}
	return decisions
}
