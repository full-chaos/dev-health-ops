//go:build integration

package analytics

import (
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// A one-team filter narrows the ORG series of PIPELINE_QUEUE_TIME to that team's pooled value: the summed queue seconds
// over the runs that have a queue sample (the measure's own pooled mean, orgPooledTerms), not the other team's, not the
// unfiltered pool, not a mean of team means. Teams own repositories (teamscope), no team_id column.
// team-a owns A: 2 runs, 100 s and 200 s -> 150. team-b owns B: 4 runs of 600 s -> 600.
// Unfiltered ORG 2700/6 = 450; mean of the team means 375.
func TestTestopsOrgQueueTimeOneTeamFilterNarrowsTheOrgSeries(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13894-queue-team"
	const a, b = "13894000-0000-4000-8000-0000000000a1", "13894000-0000-4000-8000-0000000000b1"
	e.repo(org, a, "acme/qta")
	e.repo(org, b, "acme/qtb")
	e.owner(org, "team-a", a, "acme/qta", 1, openFrom(), nil)
	e.owner(org, "team-b", b, "acme/qtb", 1, openFrom(), nil)
	e.runs(org, a,
		poolRun{id: "qta-0", started: atHour(day1, 1), duration: pf(10), queue: pf(100)},
		poolRun{id: "qta-1", started: atHour(day1, 2), duration: pf(10), queue: pf(200)})
	for i := 0; i < 4; i++ {
		e.runs(org, b, poolRun{id: fmt.Sprintf("qtb-%d", i), started: atHour(day1, 3+i), duration: pf(10), queue: pf(600)})
	}
	e.write(org, day1, a, b)

	m := model.MeasureInputPipelineQueueTime
	_, orgAll := e.dimSeries(org, model.DimensionInputTeam, m, model.BucketIntervalInputDay, nil)
	wantOne(t, "queue ORG no filter (pooled 2700/6, not the team mean 375)", orgAll, 450)
	teams, orgA := e.dimSeries(org, model.DimensionInputTeam, m, model.BucketIntervalInputDay, teamScope("team-a"))
	wantOne(t, "queue ORG [team-a] (team-a pooled, not team-b 600, not 450)", orgA, 150)
	wantOne(t, "queue team-a series under [team-a]", teams["team-a"], 150)
	_, orgB := e.dimSeries(org, model.DimensionInputTeam, m, model.BucketIntervalInputDay, teamScope("team-b"))
	wantOne(t, "queue ORG [team-b]", orgB, 600)
}
