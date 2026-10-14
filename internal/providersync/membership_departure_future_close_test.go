package providersync

import (
	"context"
	"testing"
	"time"
)

// The two closes the departure makes outside the snapshot rule (a rename
// replaced row, a reused member id's history rows) keep the close after the
// open, also for a row that opens after the run.
func TestMembershipDepartureDirectClosesOfAFutureRowAreNotEmpty(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	future := at.Add(time.Hour)
	text := func(value string) *string { return &value }
	// An unproven read: the snapshot rule retracts nothing, so only the direct closes act.
	unproven := GitHubTeamMembershipKind(nil).Snapshot(ScopeProof{stated: true},
		ProveSnapshot(SnapshotTerm{Holds: true, Reason: "read_returned"}), AbsenceByCloseWriter[MembershipSnapshotRow]())
	provider := githubMembershipWriter.Provider
	incoming := githubMembershipRow{OrgID: "o", Provider: provider, TeamID: "t", MemberID: "gh:m", ProviderUserID: text("7"), ValidFrom: at, UpdatedAt: at}

	cases := map[string][]openMembership{
		"rename: the open row of another member id holding the incoming id": {
			{OrgID: "o", Provider: provider, TeamID: "t", MemberID: "gh:old", ProviderUserID: text("7"), ValidFrom: future, UpdatedAt: at},
		},
		"history: the member id came back with another provider user id": {
			{OrgID: "o", Provider: provider, TeamID: "t", MemberID: "gh:m", ProviderUserID: text("9"), ValidFrom: future, UpdatedAt: at},
		},
	}
	for name, open := range cases {
		t.Run(name, func(t *testing.T) {
			rows, _, err := githubMembershipWriter.planSnapshot(context.Background(), "o", open, []githubMembershipRow{incoming}, []githubMembershipRow{incoming}, at, nil, unproven)
			if err != nil {
				t.Fatal(err)
			}
			closed := 0
			for _, row := range rows {
				if row.ValidTo == nil {
					continue
				}
				closed++
				if !row.ValidTo.After(row.ValidFrom) {
					t.Errorf("closed row valid_from=%v valid_to=%v: the close is not after the open", row.ValidFrom, row.ValidTo)
				}
			}
			if closed != 1 {
				t.Fatalf("%d closed rows, want the one direct close", closed)
			}
		})
	}
}
