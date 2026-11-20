//go:build integration

package providersync

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// CHAOS-9349 condition (b): a missing input row must not void the closed
// history of a co-owner it pushes below the share floor.

func (org *retractionGuardOrg) link(provider, projectID, teamID, repo string) string {
	org.t.Helper()
	org.issue++
	workItemID := fmt.Sprintf("%s:F-%d", provider, org.issue)
	seedWorkItem(org.t, org.ctx, org.conn, org.orgID, workItemID, provider, uuid.Nil, projectID, org.at)
	seedWorkGraphIssuePR(org.t, org.ctx, org.conn, org.orgID, org.repos[repo], workItemID, uint32(org.issue), org.at)
	return workItemID
}

type floorVoidRow struct {
	isPrimary uint8
	validFrom time.Time
	validTo   *time.Time
}

func (org *retractionGuardOrg) teamRows(repo, team string) []floorVoidRow {
	org.t.Helper()
	rows, err := org.conn.Query(org.ctx, `
SELECT is_primary, valid_from, valid_to
FROM team_repo_ownership FINAL
WHERE org_id = ? AND repo_full_name = ? AND team_id = ? AND source = 'inferred'
ORDER BY valid_from, is_primary`, org.orgID, repo, team)
	if err != nil {
		org.t.Fatalf("read rows of %s: %v", team, err)
	}
	defer rows.Close()
	var out []floorVoidRow
	for rows.Next() {
		var row floorVoidRow
		if err := rows.Scan(&row.isPrimary, &row.validFrom, &row.validTo); err != nil {
			org.t.Fatalf("scan: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		org.t.Fatalf("rows.Err: %v", err)
	}
	return out
}

func TestDerivedRepoOwnershipMissingRowDoesNotVoidACoOwnersClosedHistory(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, provider := range retractionGuardIntegrationProviders {
		for _, table := range missingInputTables {
			t.Run(provider+"_"+table, func(t *testing.T) {
				org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-floor-"+provider+"-"+table, "acme/repo-x")
				seedTeamProjectOwnership(t, ctx, conn, org.orgID, provider, provider+"-proj-a", "team-a", true, org.at)
				seedTeamProjectOwnership(t, ctx, conn, org.orgID, provider, provider+"-proj-b", "team-b", true, org.at)
				org.link(provider, provider+"-proj-b", "team-b", "acme/repo-x")
				missing := org.link(provider, provider+"-proj-b", "team-b", "acme/repo-x")
				if written, _, _, _ := org.derive(); written != 1 {
					t.Fatalf("run 1 (team-b alone): written=%d, want 1", written)
				}

				// team-a takes the repo with 150 links; team-b keeps 2 of 152 (1.3%):
				// team-b's main row closes and a co-owner row opens.
				for i := 0; i < 150; i++ {
					org.link(provider, provider+"-proj-a", "team-a", "acme/repo-x")
				}
				_, _, _, stats := org.derive()
				if stats.RoleChanges != 1 {
					t.Fatalf("run 2: role changes=%d, want 1 (team-b main -> co-owner)", stats.RoleChanges)
				}
				before := org.teamRows("acme/repo-x", "team-b")
				closed := 0
				for _, row := range before {
					if row.validTo != nil && row.validTo.After(row.validFrom) {
						closed++
					}
				}
				if closed != 1 {
					t.Fatalf("run 2: team-b rows %+v, want one closed main row", before)
				}

				// One of team-b's two rows is missing from run 3: 1 of 151 (0.66%) is below the floor.
				org.deleteRows(table, missing)
				written, retracted, inputsReady, stats := org.derive()
				t.Logf("missing %s row %s: inputsReady=%v written=%d retracted=%d held=%d voided=%d below_floor=%d",
					table, missing, inputsReady, written, retracted, stats.RetractionsHeld, stats.CoOwnersVoided, stats.CoOwnersBelowShareFloor)
				for _, row := range org.teamRows("acme/repo-x", "team-b") {
					if row.validTo != nil && row.validTo.Equal(row.validFrom) {
						t.Errorf("team-b row is_primary=%d valid_from=%s voided (valid_to = valid_from) on a missing input row",
							row.isPrimary, row.validFrom.Format(time.RFC3339Nano))
					}
				}
			})
		}
	}
}
