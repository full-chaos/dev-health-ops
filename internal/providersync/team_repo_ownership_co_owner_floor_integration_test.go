//go:build integration

package providersync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// TestTeamRepoOwnershipDerivationRetractsACoOwnerThatFallsBelowTheShareFloor:
// a Jira team with one text link beside a Linear main owner holds 10% of the
// repository, so the first run writes its co-owner row. More native links of
// the main owner put the Jira team at 0.5%; the second run gives it no row
// and retracts its open row (valid_to set, never a delete). The main owner's
// row stays open.
func TestTeamRepoOwnershipDerivationRetractsACoOwnerThatFallsBelowTheShareFloor(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := "co-owner-share-floor-org"
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

	repoID := uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{repoID: "full-chaos/dev-health-acr"})
	seedTeamProjectOwnership(t, ctx, conn, orgID, "linear", "proj-chaos", "team-fullchaos", true, t0)
	seedTeamProjectOwnership(t, ctx, conn, orgID, "jira", "proj-mobile", "team-mobile", true, t0)
	seedWorkItem(t, ctx, conn, orgID, "jira:API-1", "jira", uuid.Nil, "proj-mobile", t0)
	seedWorkGraphIssuePRWithProvenance(t, ctx, conn, orgID, repoID, "jira:API-1", 704, "explicit_text", t0)
	for n := 1; n <= 9; n++ {
		id := fmt.Sprintf("linear:CHAOS-%d", n)
		seedWorkItem(t, ctx, conn, orgID, id, "linear", uuid.Nil, "proj-chaos", t0)
		seedWorkGraphIssuePR(t, ctx, conn, orgID, repoID, id, uint32(n), t0)
	}

	service := TeamRepoOwnershipDerivationService{Conn: conn, Now: func() time.Time { return t0.Add(time.Hour) }}
	if _, _, _, _, err := service.Derive(ctx, orgID); err != nil {
		t.Fatalf("first Derive: %v", err)
	}
	if got := openOwnershipTeams(t, ctx, conn, orgID); fmt.Sprint(got) != "[team-fullchaos team-mobile]" {
		t.Fatalf("open teams after the first run = %v, want both", got)
	}

	for n := 10; n <= 199; n++ {
		id := fmt.Sprintf("linear:CHAOS-%d", n)
		seedWorkItem(t, ctx, conn, orgID, id, "linear", uuid.Nil, "proj-chaos", t1)
		seedWorkGraphIssuePR(t, ctx, conn, orgID, repoID, id, uint32(n), t1)
	}
	service.Now = func() time.Time { return t1 }
	_, retracted, _, _, stats, err := service.DeriveWithStats(ctx, orgID)
	if err != nil {
		t.Fatalf("second Derive: %v", err)
	}
	if retracted != 1 || stats.CoOwnersBelowShareFloor != 1 {
		t.Fatalf("second run retracted=%d below_floor=%d, want 1 and 1", retracted, stats.CoOwnersBelowShareFloor)
	}
	if got := openOwnershipTeams(t, ctx, conn, orgID); fmt.Sprint(got) != "[team-fullchaos]" {
		t.Fatalf("open teams after the second run = %v, want only the main owner", got)
	}
}

func openOwnershipTeams(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT DISTINCT team_id FROM team_repo_ownership FINAL
WHERE org_id = ? AND source = 'inferred' AND valid_to IS NULL
ORDER BY team_id`, orgID)
	if err != nil {
		t.Fatalf("read open rows: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var team string
		if err := rows.Scan(&team); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, team)
	}
	return out
}
