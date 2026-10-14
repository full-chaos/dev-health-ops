package providersync

import (
	"context"
	"testing"
	"time"
)

// A row that opens after the closing run closes 1 ms past its open, in each
// interval table the shared snapshot planner writes.
func TestSnapshotRetractionOfAFutureRowIsNotEmptyPerTable(t *testing.T) {
	at := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	future := at.Add(time.Hour)

	t.Run("team_repo_ownership", func(t *testing.T) {
		open := []githubTeamRepoOwnershipRow{{TeamID: "gh:ops", RepoFullName: "acme/api", Source: githubTeamCatalogSource, ValidFrom: future}}
		decision := decideOwnershipClose(context.Background(), staticScopeCensus{}, ownershipCloseRequest{
			ref: TeamCatalogReference{OrgID: "org", IntegrationID: "integration-a"}, provider: githubTeamCatalogProvider,
			dropped: []string{"gh:ops"}, teamListing: wholeListing(1), teamLookups: NewTeamAbsenceLookups(context.Background(), nil),
		})
		rows, plan := githubRepoOwnershipSnapshot(nil, open, at, decision.snapshot(GitHubTeamRepoGrantKind, "the team's repositories", nil))
		if len(plan.Retract) != 1 || len(rows) != 1 || rows[0].ValidTo == nil || !rows[0].ValidTo.After(rows[0].ValidFrom) {
			t.Fatalf("retract=%+v rows=%+v, want one close after its open", plan.Retract, rows)
		}
	})

	t.Run("team_memberships", func(t *testing.T) {
		open := []openMembership{{TeamID: "a", MemberID: "m1", ValidFrom: future}}
		snapshot := GitHubTeamMembershipKind([]string{"a"}).Snapshot(ScopeProof{stated: true},
			ProveSnapshot(SnapshotTerm{Holds: true, Reason: "read_returned"}), AbsenceByCloseWriter[MembershipSnapshotRow]())
		_, retractions, plan := planMembershipSnapshot(open, nil, at, snapshot)
		if len(plan.Retract) != 1 || len(retractions) != 1 || !retractions[0].closedAt.After(future) {
			t.Fatalf("retractions=%+v, want one close after its open %v", retractions, future)
		}
	})
}
