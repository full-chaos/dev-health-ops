//go:build integration

package workerservice

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type fallbackInvariantWiringObserver struct{ jobruntime.Observer }

func (fallbackInvariantWiringObserver) ObserveWorkGraphIssueEdges(map[jobruntime.WorkGraphIssueEdgeOutcome]int, int, time.Duration) error {
	return nil
}

// TestConstructedFallbackInvariantRunsAfterEveryIssuePRWriter builds the real
// pre-step list through workgraphBuildPreSteps against a real ClickHouse and
// runs the constructed invariant step: it must sit after every step that
// writes work_graph_issue_pr, report FAILED with the organization and the
// counts on a planted breach, and pass with 0 for a clean organization.
func TestConstructedFallbackInvariantRunsAfterEveryIssuePRWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)

	steps, postSteps, err := workgraphBuildPreSteps(
		config.Config{ClickHouseURI: secrets.NewValue(instance.URI)},
		[]jobruntime.HandlerSpec{{Kind: jobcontract.KindWorkGraphBuild}},
		fallbackInvariantWiringObserver{}, slog.Default(),
	)
	if err != nil {
		t.Fatalf("workgraphBuildPreSteps: %v", err)
	}
	if len(postSteps) != 0 {
		t.Fatalf("post-steps = %d, want 0", len(postSteps))
	}
	position := map[string]int{}
	for index, step := range steps {
		position[step.Name()] = index
	}
	check, constructed := position["issue_pr_fallback_invariant"]
	if !constructed {
		t.Fatalf("constructed pre-steps have no issue_pr_fallback_invariant step: a native-first breach is silent")
	}
	for _, writer := range []string{"issue_pr_links", "issue_pr_fallback_retraction", "issue_pr_edges_text_parse", "issue_pr_edges_heuristic"} {
		at, ok := position[writer]
		if !ok {
			t.Fatalf("constructed pre-steps have no %q step", writer)
		}
		if at >= check {
			t.Fatalf("issue_pr_fallback_invariant (%d) is constructed before %q (%d), which writes work_graph_issue_pr", check, writer, at)
		}
	}
	invariant := steps[check]

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writer, err := issueprlinks.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	breached, clean := uuid.NewString(), uuid.NewString()
	repo := uuid.New()
	stamp := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	link := func(org string, pr uint32, workItemID, provenance string) issueprlinks.Link {
		return issueprlinks.Link{OrgID: org, RepoID: repo, PRNumber: pr, WorkItemID: workItemID,
			Provenance: provenance, Confidence: 0.9, Evidence: "fixture", LastSynced: stamp}
	}
	if err := writer.Write(ctx, []issueprlinks.Link{
		link(breached, 704, "linear:CHAOS-1", "native"),
		link(breached, 704, "jira:API-1", "explicit_text"),
		link(breached, 705, "linear:CHAOS-1", "heuristic"),
		link(clean, 704, "linear:CHAOS-1", "native"),
		link(clean, 705, "jira:API-1", "explicit_text"),
	}); err != nil {
		t.Fatal(err)
	}

	_, err = invariant.Run(ctx, workgraph.Claim{Request: workgraph.Request{OrganizationID: breached}})
	var failure *workgraph.StepFailure
	if !errors.As(err, &failure) {
		t.Fatalf("Run(breached) err = %v, want a workgraph.StepFailure", err)
	}
	if failure.Evidence["organization_id"] != breached || failure.Evidence["fallback_beside_native"] != 2 ||
		failure.Evidence["beside_native_pull_request"] != 1 || failure.Evidence["beside_native_issue"] != 1 {
		t.Fatalf("breach evidence = %v", failure.Evidence)
	}

	fragment, err := invariant.Run(ctx, workgraph.Claim{Request: workgraph.Request{OrganizationID: clean}})
	if err != nil || fragment["organization_id"] != clean || fragment["fallback_beside_native"] != 0 {
		t.Fatalf("Run(clean) = %v, %v; want 0 for the clean organization", fragment, err)
	}
}
