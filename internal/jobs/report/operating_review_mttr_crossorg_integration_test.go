//go:build integration

package report

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pooledmttr"
)

// Two organizations hold rows under the SAME repository id, service-mapped alike and on the same days: the pooled MTTR
// scorecard of one organization is made of its own rows only.
func TestPooledMTTRScorecardIsScopedToTheRequestOrganization(t *testing.T) {
	e := startPooledEnv(t)
	client := mttrTileClient(e)
	const repo = "14026000-0000-4000-8000-0000000000c1"
	at := func(i int) time.Time { return deliveryDay1.Add(time.Duration(i+1) * 30 * time.Minute) }
	seed := func(org, service string, bugHours, incidentHours int) {
		e.insertRepo(org, repo)
		e.mapIncidentService(org, service, repo)
		e.insertBug(org, repo, "b-1", "bug", at(0), at(0).Add(time.Duration(bugHours)*time.Hour))
		e.insertIncident(org, service, "i-1", at(0), at(0).Add(time.Duration(incidentHours)*time.Hour), 0)
		e.insertPullRequest(org, repo, 1, at(0), 5, 5)
		e.computeDelivery(org, []string{repo}, deliveryDay1.Format(time.DateOnly))
	}
	orgA, orgB := e.newOrg(), e.newOrg()
	seed(orgA, "svc-x", 2, 4)
	seed(orgB, "svc-x", 100, 50)
	start, end := deliveryDay1, deliveryDay1.AddDate(0, 0, 7)
	for _, tc := range []struct {
		org       string
		p50, mean float64
		name      string
	}{{orgA, 4, 2, "A"}, {orgB, 50, 100, "B"}} {
		p50, mean, err := pooledmttr.MTTR(e.ctx, client, tc.org, start, end, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if p50 == nil || mean == nil || !near(*p50, tc.p50) || !near(*mean, tc.mean) {
			t.Errorf("org %s: scorecard p50 %v mean %v, want %v %v (its own rows only)", tc.name, p50, mean, tc.p50, tc.mean)
		}
		if value, has := mttrTile(e, client, tc.org); !has || !near(value, tc.p50) {
			t.Errorf("org %s: tile %v (hasData %v), want %v", tc.name, value, has, tc.p50)
		}
		if chart := e.deliveryChart(tc.org, "mttr_p50_hours", "", "scorecard", "2026-02-02", "2026-02-08")["total"]; !near(chart, tc.p50) {
			t.Errorf("org %s: report chart p50 %v, want %v", tc.name, chart, tc.p50)
		}
	}
}
