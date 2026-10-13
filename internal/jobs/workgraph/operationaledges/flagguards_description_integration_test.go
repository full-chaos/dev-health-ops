//go:build integration

package operationaledges

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

// Synced work-item descriptions reach the flag-guard builder: a flag key named
// only in a description yields a GUARDS edge and a feature_flag_link row once
// the description is stored, and none while it is NULL.
func TestFlagGuardsSeeAFlagNamedOnlyInASyncedDescription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn := newSchemaConn(t, ctx)

	const orgID = "70d529e0-3c06-4597-8480-794fd02328b6"
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx,
		`INSERT INTO feature_flag (org_id, provider, flag_key, project_key, repo_id, environment, flag_type, created_at, last_synced)
		 VALUES (?, 'launchdarkly', 'checkout_v2', 'web', '', 'prod', 'release', ?, ?)`, orgID, now, now); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO work_items (org_id, work_item_id, title, description) VALUES (?, 'gh:acme/web#7', 'Fix cart totals', NULL)`, orgID); err != nil {
		t.Fatal(err)
	}

	before, beforeLinks, err := BuildFlagGuardsEdges(ctx, conn, orgID, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 0 || len(beforeLinks) != 0 {
		t.Fatalf("title-only work item produced %d edges, %d links; want none", len(before), len(beforeLinks))
	}

	if err := conn.Exec(ctx, `INSERT INTO work_items (org_id, work_item_id, title, description) VALUES (?, 'gh:acme/web#7', 'Fix cart totals', 'Only reproduces behind checkout_v2 in prod.')`, orgID); err != nil {
		t.Fatal(err)
	}
	after, afterLinks, err := BuildFlagGuardsEdges(ctx, conn, orgID, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 || len(afterLinks) != 1 {
		t.Fatalf("described work item produced %d edges, %d links; want 1 and 1", len(after), len(afterLinks))
	}
	if got := after[0]; got.EdgeType != edges.EdgeTypeGuards || got.TargetID != "gh:acme/web#7" || got.Provenance != edges.ProvenanceExplicitText {
		t.Fatalf("edge = %s %s %s, want a GUARDS edge to the issue with explicit_text provenance", got.EdgeType, got.TargetID, got.Provenance)
	}
}
