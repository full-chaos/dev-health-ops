package aianalytics

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

func ownershipFailsClient(c oracleBCase) *fixtureClientB {
	return &fixtureClientB{
		fixtureClient: &fixtureClient{c: oracleCase{SlugRows: c.SlugRows, TeamRows: c.TeamRows, RepoRows: c.RepoRows}, ds: dataset{Daily: c.Daily}},
		c:             c,
	}
}

// A team scope whose ownership read fails answers no data, as every other team-scoped AI read does (scope.go, teamRepos):
// it never widens to the organisation. The recorded Python answer of the oracle case "overview/teamCatalogueFails" is the
// org-wide mix under the team scope; that case holds the widen this cell refuses.
func TestTeamScopeWithUnreadableOwnershipDoesNotWidenToTheOrg(t *testing.T) {
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}

	overviewCase := namesCase(t, "overview/teamCatalogueFails")
	control, err := AttributionOverview(context.Background(), ownershipFailsClient(overviewCase), "org-synthetic-1", dr, nil, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(control.Mix) == 0 || control.TotalAttributed == 0 || !control.DataAvailable {
		t.Fatalf("control: the org scope answers mix %v, totalAttributed %d, dataAvailable %v; want the org mix", control.Mix, control.TotalAttributed, control.DataAvailable)
	}
	t.Logf("control: the org scope answers totalAttributed %d over %d kinds", control.TotalAttributed, len(control.Mix))

	team := "team-b"
	overview, err := AttributionOverview(context.Background(), ownershipFailsClient(overviewCase), "org-synthetic-1", dr,
		&model.AIAttributionScopeInput{TeamID: &team}, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(overview.Mix) != 0 || overview.TotalAttributed != 0 || overview.DataAvailable {
		t.Errorf("aiAttributionOverview teamId %s, ownership read failed: mix %v, totalAttributed %d, dataAvailable %v; want mix [], totalAttributed 0, dataAvailable false: the team scope widened to the org",
			team, overview.Mix, overview.TotalAttributed, overview.DataAvailable)
	}

	prs, err := AttributedPrs(context.Background(), ownershipFailsClient(namesCase(t, "prs/teamCatalogueFails")), "org-synthetic-1", dr,
		&model.AIScopeInput{TeamID: &team}, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(prs.Rows) != 0 || prs.Total != 0 || prs.HasMore || prs.DataAvailable {
		t.Errorf("aiAttributedPrs teamId %s, ownership read failed: %d rows, total %d, hasMore %v, dataAvailable %v; want 0 rows, total 0, hasMore false, dataAvailable false: the page was read org-wide",
			team, len(prs.Rows), prs.Total, prs.HasMore, prs.DataAvailable)
	}
}
