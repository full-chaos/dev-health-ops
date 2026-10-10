//go:build integration

package aianalytics

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework/prreworktest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// The "high rework" flow opportunity reads the pull request rework ratio of a
// repository's window as the ratio of the summed counts over REVIEWED pull
// requests, not as the mean of the stored one-day ratios over all merged pull
// requests. Rows computed and written by the daily job's own compute and
// writer, five days each (the detector needs five days of data):
//
//	repo H  each day 4 merged: 1 reviewed with changes requested, 3 with no review data
//	        -> 5 of 5 reviewed: 100 % (the stored ratios average 25 %)
//	repo M  2 days: 1 merged, reviewed, changes requested; 3 days: 10 merged, reviewed, none
//	        -> 2 of 32 reviewed: 6 %, under the threshold (the stored ratios average 40 %)
//	repo U  each day 6 merged, no review data -> no value, no opportunity
func TestRealClickHouse_HighReworkReadsReviewedPullRequestsOnly(t *testing.T) {
	ctx, conn, client := startStore(t)
	const org = "org-rework-flow"
	repoH, repoM, repoU := uuid.New(), uuid.New(), uuid.New()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	computedAt := time.Now().UTC()
	unreviewed := prreworktest.PullRequest{}
	reviewed := prreworktest.PullRequest{Reviews: 2}
	rework := prreworktest.PullRequest{Reviews: 3, ChangesRequested: 1}
	repeat := func(pullRequest prreworktest.PullRequest, count int) []prreworktest.PullRequest {
		out := make([]prreworktest.PullRequest, count)
		for i := range out {
			out[i] = pullRequest
		}
		return out
	}
	for back := 1; back <= 5; back++ {
		day := today.AddDate(0, 0, -back)
		prreworktest.WriteDay(ctx, t, conn, org, repoH, "github", day, computedAt, append(repeat(rework, 1), repeat(unreviewed, 3)...))
		if back <= 2 {
			prreworktest.WriteDay(ctx, t, conn, org, repoM, "github", day, computedAt, repeat(rework, 1))
		} else {
			prreworktest.WriteDay(ctx, t, conn, org, repoM, "github", day, computedAt, repeat(reviewed, 10))
		}
		prreworktest.WriteDay(ctx, t, conn, org, repoU, "github", day, computedAt, repeat(unreviewed, 6))
	}

	got, err := FlowOpportunities(ctx, client, org, nil, 100, 30)
	if err != nil {
		t.Fatal(err)
	}
	fired := map[string]model.ImproveOpportunity{}
	for _, opportunity := range got.Opportunities {
		if opportunity.Kind == model.ImproveOpportunityKindHighRework {
			fired[opportunity.EntityID] = opportunity
		}
	}
	high, isHigh := fired[repoH.String()]
	if !isHigh || !strings.Contains(high.Rationale, "100%") {
		t.Errorf("repo H: fired %v with rationale %q, want the ratio over its reviewed pull requests, 100%%", isHigh, high.Rationale)
	}
	if opportunity, fired := fired[repoM.String()]; fired {
		t.Errorf("repo M has 2 of 32 reviewed pull requests with changes requested and fired: %q", opportunity.Rationale)
	}
	if opportunity, fired := fired[repoU.String()]; fired {
		t.Errorf("repo U has no review data and fired: %q", opportunity.Rationale)
	}
}
