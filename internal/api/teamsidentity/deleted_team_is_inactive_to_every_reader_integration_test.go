//go:build integration

package teamsidentity

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamactive"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The rows a metric family stored under a team are retracted at the next
// compute when the team is INACTIVE: each family drops an inactive team from
// its resolution (teamactive.LoadInactive, the one read of the daily job, and
// the Inactive flag of the attribution's team facts), and the stale-key rule
// then writes a row of zeros over every key the compute no longer produces.
// Two censuses hold those two halves for every family and every team-keyed
// table (TestEveryReadOfTeamsInTheDailyJobAppliesTheActiveTeamRuleCensus,
// TestEveryTeamKeyedTableCensusHasADecision).
//
// This is the link between the admin delete and that chain: after the real
// delete the team IS in the set those readers use, and it was not before. A
// team that comes back (an admin create, a later write of the row) is out of
// the set again. Without this link a deleted team's stored rows stay live in
// every team-keyed table. One case for a team of each provider and for an
// admin's custom team.
func TestADeletedTeamIsInactiveToEveryReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	store := Store{Conn: conn}
	h := newTestHandlers(store)

	for index, origin := range []string{"github", "gitlab", "jira", "linear", teamid.Custom} {
		t.Run(origin, func(t *testing.T) {
			org := fmt.Sprintf("00000000-0000-4000-8000-00000091193%d", index)
			gone, stays := teamid.Of(origin, "platform"), teamid.Of(origin, "apps")
			for id, name := range map[string]string{gone: "Platform", stays: "Apps"} {
				if _, err := store.CreateOrUpdateTeam(ctx, org, TeamWrite{Origin: origin, TeamID: id, Name: name}); err != nil {
					t.Fatal(err)
				}
			}
			// inactive is what the two readers say of a team: the set of the
			// daily job, and the flag of the attribution's team facts.
			inactive := func(teamID string) (inSet, flagged, known bool) {
				t.Helper()
				set, err := teamactive.LoadInactive(ctx, conn, org)
				if err != nil {
					t.Fatal(err)
				}
				facts, err := teamattribution.ClickHouseFactSource{Conn: conn}.LoadTeams(ctx, org)
				if err != nil {
					t.Fatal(err)
				}
				for _, fact := range facts {
					if fact.TeamID == teamID {
						known, flagged = true, fact.Inactive
					}
				}
				return set.Has(teamID), flagged, known
			}
			check := func(when, teamID string, want bool) {
				t.Helper()
				inSet, flagged, known := inactive(teamID)
				if inSet != want || flagged != want || !known {
					t.Errorf("%s, team %s: in the inactive set = %v, flagged inactive by the attribution = %v, known to it = %v; want %v, %v, true",
						when, teamID, inSet, flagged, known, want, want)
				}
			}
			check("before the delete", gone, false)
			recorder := callWithBody(t, h, func(w http.ResponseWriter, r *http.Request) {
				r.SetPathValue("team_id", gone)
				h.deleteTeam(w, r)
			}, http.MethodDelete, "/api/v1/admin/teams/"+gone, org, nil)
			if recorder.Code != http.StatusOK {
				t.Fatalf("delete: status %d body %s", recorder.Code, recorder.Body.String())
			}
			check("after the delete", gone, true)
			check("after the delete of the other team", stays, false)

			if _, err := store.CreateOrUpdateTeam(ctx, org, TeamWrite{Origin: origin, TeamID: gone, Name: "Platform again"}); err != nil {
				t.Fatal(err)
			}
			check("after an admin create under the same id", gone, false)
		})
	}
}
