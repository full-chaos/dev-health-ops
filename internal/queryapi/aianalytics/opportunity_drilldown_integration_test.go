//go:build integration

package aianalytics

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiworkflow"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-9225, the cross-reader proof: the pull request edges are written by the REAL writer
// (aiworkflow.Compute -> WriteAIWorkflowArtifactEdges, into a real ClickHouse), the opportunity is
// projected from the evidence ref the opportunity query emits, and the chip's own root id is walked by
// the REAL drilldown. A chip whose root id is not the stored one returns 0 edges (prod rev 271: 5 of 5).
func TestOpportunityChipWalksTheEdgesTheWriterStored(t *testing.T) {
	ctx, conn, client := startStore(t)
	org := uuid.NewString()
	repo := uuid.New()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	result := aiworkflow.Compute([]aiworkflow.PullRequestRow{{
		RepoID: repo, Number: 434, Title: "Improve error messages", Body: "This was ai assisted work.",
		AuthorName: "dev-c", CreatedAt: now.Add(-48 * time.Hour), MergedAt: &now, LastSynced: now,
	}}, uuid.MustParse(org), "github", nil, now)
	if len(result.ArtifactEdges) != 1 {
		t.Fatalf("the writer made %d pull request edges, want 1", len(result.ArtifactEdges))
	}
	if _, err := daily.WriteAIWorkflowArtifactEdges(ctx, conn, result.ArtifactEdges, now); err != nil {
		t.Fatal(err)
	}

	// Each defect alone, so each is its own red: the separator (a real pull request ref) and the
	// metric ref (never a chip).
	for name, refs := range map[string][]string{
		"the root id of a pull request chip is the stored one": {"git_pull_requests:" + repo.String() + ":434"},
		"a metric ref makes no chip":                           {"git_pull_requests:migration_prs:" + repo.String()},
	} {
		t.Run(name, func(t *testing.T) {
			drills := opportunity{repoID: repo.String(), refs: refs}.project(nil, nil).WorkGraphDrilldowns
			if name == "a metric ref makes no chip" {
				if len(drills) != 0 {
					t.Fatalf("a metric ref was served as a chip: %+v", drills)
				}
				return
			}
			if len(drills) != 1 {
				t.Fatalf("drilldowns = %+v, want exactly the pull request chip", drills)
			}
			got, err := WorkflowDrilldown(ctx, client, org, model.AIWorkflowRootTypeInputPr, drills[0].RootID, 2, 100)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Edges) == 0 {
				t.Fatalf("the chip %q returned %d edges and %d nodes; the writer stored %q", drills[0].RootID, len(got.Edges), len(got.Nodes), result.ArtifactEdges[0].ArtifactID)
			}
		})
	}
}
