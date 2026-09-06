//go:build integration

package edges

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestDeleteStalePRDependencyIssueEdgesAgainstLiveClickHouse is CHAOS-4924's
// red/green proof for the Go port of Python's
// `_delete_stale_pr_dependency_issue_edges` (deleted from builder.py by this
// same cutover): it seeds the exact stale-row shape Python's WHERE clause
// targeted (a PR-sourced edge mislabelled source_type='issue', linked to a
// genuine Linear issue via a 'linear_attachment' evidence tag) plus three
// negative-control rows that must each survive for a distinct reason, then
// asserts DeleteStalePRDependencyIssueEdges removes ONLY the stale row.
func TestDeleteStalePRDependencyIssueEdgesAgainstLiveClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	chschema.Apply(ctx, t, instance)

	const org = "70d529e0-3c06-4597-8480-794fd0230000"
	const otherOrg = "70d529e0-3c06-4597-8480-794fd0239999"
	seededAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	type edgeRow struct {
		id, sourceType, sourceID, targetType, targetID, evidence, orgID string
	}
	insert := func(row edgeRow) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO work_graph_edges
(edge_id, source_type, source_id, target_type, target_id, edge_type, provenance,
 confidence, evidence, discovered_at, last_synced, event_ts, day, org_id)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			row.id, row.sourceType, row.sourceID, row.targetType, row.targetID, "relates", "native",
			float32(0.9), row.evidence, seededAt, seededAt, seededAt, seededAt, row.orgID,
		); err != nil {
			t.Fatal(err)
		}
	}

	// The stale row Python's cleanup targeted: mislabelled source_type='issue'
	// but source_id is really a PR (ghpr: prefix), linked to a genuine Linear
	// issue via the linear_attachment evidence tag.
	const staleID = "stale-ghpr-source"
	insert(edgeRow{
		id: staleID, sourceType: "issue", sourceID: "ghpr:acme/app#1",
		targetType: "issue", targetID: "linear:ACME-1", evidence: "linear_attachment", orgID: org,
	})

	// Negative control 1: same shape but source_id is gitlab:-prefixed, not
	// ghpr: -- also stale by Python's WHERE clause (the OR branch), must ALSO
	// be deleted. Proves the OR condition, not just the ghpr: half.
	const staleGitlabID = "stale-gitlab-source"
	insert(edgeRow{
		id: staleGitlabID, sourceType: "issue", sourceID: "gitlab:acme/app#2",
		targetType: "issue", targetID: "linear:ACME-2", evidence: "linear_attachment", orgID: org,
	})

	// Negative control 2: correctly-typed edge (source_type='pull_request',
	// not 'issue') -- must survive. Proves the delete doesn't just match on
	// the ghpr:/gitlab: prefix alone.
	const correctlyTypedID = "correctly-typed-survives"
	insert(edgeRow{
		id: correctlyTypedID, sourceType: "pull_request", sourceID: "ghpr:acme/app#3",
		targetType: "issue", targetID: "linear:ACME-3", evidence: "linear_attachment", orgID: org,
	})

	// Negative control 3: right shape, wrong evidence tag -- must survive.
	// Proves the delete is scoped to linear_attachment specifically, not any
	// issue<->issue edge with a ghpr:/gitlab: source.
	const wrongEvidenceID = "wrong-evidence-survives"
	insert(edgeRow{
		id: wrongEvidenceID, sourceType: "issue", sourceID: "ghpr:acme/app#4",
		targetType: "issue", targetID: "linear:ACME-4", evidence: "text_reference", orgID: org,
	})

	// Negative control 4: the exact stale shape, but a DIFFERENT org -- must
	// survive. Proves the delete is org-scoped, not a blanket sweep.
	const otherOrgID = "other-org-survives"
	insert(edgeRow{
		id: otherOrgID, sourceType: "issue", sourceID: "ghpr:acme/app#5",
		targetType: "issue", targetID: "linear:ACME-5", evidence: "linear_attachment", orgID: otherOrg,
	})

	if err := DeleteStalePRDependencyIssueEdges(ctx, conn, org); err != nil {
		t.Fatalf("DeleteStalePRDependencyIssueEdges: %v", err)
	}

	count := func(id string) uint64 {
		t.Helper()
		var got uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM work_graph_edges FINAL WHERE edge_id = ?`, id,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	for _, id := range []string{staleID, staleGitlabID} {
		if got := count(id); got != 0 {
			t.Errorf("stale edge %s still present after cleanup (count=%d)", id, got)
		}
	}
	for _, id := range []string{correctlyTypedID, wrongEvidenceID, otherOrgID} {
		if got := count(id); got != 1 {
			t.Errorf("control edge %s was deleted (count=%d), want 1 -- it must survive", id, got)
		}
	}

	// Scope refusal: an empty org must refuse rather than silently sweeping
	// every tenant's stale rows.
	if err := DeleteStalePRDependencyIssueEdges(ctx, conn, ""); err == nil {
		t.Fatal("DeleteStalePRDependencyIssueEdges(\"\") succeeded, want ErrScopeRequired")
	}
}
