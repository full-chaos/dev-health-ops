//go:build integration

package providersync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// TestADroppedCoOwnerHoldsNoShareOnAnyDay: a co-owner at 10% gets a row; more
// native links of the main owner put it at 0.5%, and the share floor drops
// it. The share is a measure over the whole history, so as of EVERY day the
// row covered the co-owner holds no share: its rows are voided, not closed at
// the run (a close at the run kept the share on every earlier day, and the
// daily team writers recomputing those days wrote it back). The main owner
// keeps its share on every day. The co-owner's project is of each provider.
func TestADroppedCoOwnerHoldsNoShareOnAnyDay(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, provider := range []string{"jira", "linear", "github", "gitlab"} {
		t.Run(provider, func(t *testing.T) {
			orgID := "void-dropped-co-owner-" + provider
			t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
			t1 := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

			repoID := uuid.New()
			seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{repoID: "full-chaos/void-" + provider})
			seedTeamProjectOwnership(t, ctx, conn, orgID, "linear", "proj-main", "team-main", true, t0)
			seedTeamProjectOwnership(t, ctx, conn, orgID, provider, "proj-co", "team-co", true, t0)
			coItem := fmt.Sprintf("%s:CO-1", provider)
			seedWorkItem(t, ctx, conn, orgID, coItem, provider, uuid.Nil, "proj-co", t0)
			seedWorkGraphIssuePRWithProvenance(t, ctx, conn, orgID, repoID, coItem, 704, "explicit_text", t0)
			for n := 1; n <= 9; n++ {
				id := fmt.Sprintf("linear:MAIN-%d", n)
				seedWorkItem(t, ctx, conn, orgID, id, "linear", uuid.Nil, "proj-main", t0)
				seedWorkGraphIssuePR(t, ctx, conn, orgID, repoID, id, uint32(n), t0)
			}

			service := TeamRepoOwnershipDerivationService{Conn: conn, Now: func() time.Time { return t0.Add(time.Hour) }}
			if _, _, _, _, err := service.Derive(ctx, orgID); err != nil {
				t.Fatalf("first Derive: %v", err)
			}
			if share := ownerShareOn(t, ctx, conn, orgID, repoID, "team-co", t0.Add(48*time.Hour)); share == 0 {
				t.Fatalf("after the first run the co-owner holds no share; the seed did not make a co-owner")
			}

			for n := 10; n <= 199; n++ {
				id := fmt.Sprintf("linear:MAIN-%d", n)
				seedWorkItem(t, ctx, conn, orgID, id, "linear", uuid.Nil, "proj-main", t1)
				seedWorkGraphIssuePR(t, ctx, conn, orgID, repoID, id, uint32(n), t1)
			}
			service.Now = func() time.Time { return t1 }
			_, _, _, _, stats, err := service.DeriveWithStats(ctx, orgID)
			if err != nil {
				t.Fatalf("second Derive: %v", err)
			}
			if stats.CoOwnersBelowShareFloor != 1 || stats.CoOwnersVoided != 1 {
				t.Fatalf("second run below_floor=%d voided=%d, want 1 and 1", stats.CoOwnersBelowShareFloor, stats.CoOwnersVoided)
			}

			for day := t0.Add(2 * time.Hour); !day.After(t1.Add(24 * time.Hour)); day = day.Add(24 * time.Hour) {
				if share := ownerShareOn(t, ctx, conn, orgID, repoID, "team-co", day); share != 0 {
					t.Fatalf("as of %s the dropped co-owner holds share %v, want none on any day", day.Format(time.RFC3339), share)
				}
				if share := ownerShareOn(t, ctx, conn, orgID, repoID, "team-main", day); share == 0 {
					t.Fatalf("as of %s the main owner holds no share", day.Format(time.RFC3339))
				}
			}
		})
	}
}

func ownerShareOn(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID, teamID string, asOf time.Time) float64 {
	t.Helper()
	owners, err := teamownership.OwnerSharesByRepo(ctx, conn, orgID, asOf)
	if err != nil {
		t.Fatalf("OwnerSharesByRepo as of %s: %v", asOf.Format(time.RFC3339), err)
	}
	for _, owner := range owners[repoID.String()] {
		if owner.TeamID == teamID {
			return owner.Share
		}
	}
	return 0
}
