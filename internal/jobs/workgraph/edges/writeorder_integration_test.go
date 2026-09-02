//go:build integration

package edges

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestVariantCSurvivesOnlyWhenThisWriterGoesLast is the proof that this
// producer must run AFTER the Python bridge rather than before it.
//
// Python's `_build_issue_issue_edges` writes `confidence=1.0` (builder.py:905);
// this port writes variant-C's 0.9 for the associative family. Those are not
// two rows. `work_graph_edges` is `ReplacingMergeTree(last_synced)` with
//
//	ORDER BY (source_type, source_id, edge_type, target_type, target_id)
//
// which does NOT include `confidence`, so both writes address the SAME row and
// collapse by `last_synced`. The divergence is erased precisely because the
// dedup key excludes the column that diverges.
//
// The test asserts BOTH orders on a real engine. Asserting only the correct one
// would pass against a pre-step regression too, since a test that never
// exercises the losing order cannot tell a surviving 0.9 from a 0.9 that was
// simply never contested.
func TestVariantCSurvivesOnlyWhenThisWriterGoesLast(t *testing.T) {
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

	// Production DDL, transcribed rather than paraphrased so the ORDER BY under
	// test is the real one: 014_work_graph.sql:6-22, then the two ALTERs that
	// followed it -- 016_work_graph_event_ts.sql:2-3 (event_ts, day) and
	// 024_add_org_id.sql:58 (org_id).
	//
	// The first draft of this test transcribed only 014 and the write failed
	// with "No such column event_ts". That is the schema drift this lane's
	// standing note warns about, caught here by the engine rather than by
	// reading: a hand-copied DDL is a second source of truth and goes stale.
	if err := conn.Exec(ctx, `CREATE TABLE work_graph_edges (
    edge_id String, source_type String, source_id String, target_type String, target_id String,
    edge_type String, repo_id Nullable(UUID), provider Nullable(String), provenance String,
    confidence Float32, evidence String,
    discovered_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'),
    event_ts DateTime64(3, 'UTC') DEFAULT now(), day Date DEFAULT toDate(event_ts),
    org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (source_type, source_id, edge_type, target_type, target_id)`); err != nil {
		t.Fatal(err)
	}

	const org = "70d529e0-3c06-4597-8480-794fd02328b6"
	// A `relates` row: the associative family, which is where variant-C differs.
	rows := []DependencyRow{{
		SourceWorkItemID: "gh:acme/app#1", TargetWorkItemID: "gh:acme/app#2",
		RelationshipType: "relates", RelationshipRaw: "relates",
		LastSynced: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
	}}

	earlier := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	later := time.Date(2026, 9, 1, 10, 0, 1, 0, time.UTC)

	derived, err := DeriveIssueIssueEdges(rows, earlier)
	if err != nil {
		t.Fatal(err)
	}
	if len(derived.Edges) != 1 {
		t.Fatalf("expected one edge, got %d", len(derived.Edges))
	}
	if got := derived.Edges[0].Confidence; got != AssociativeConfidence {
		t.Fatalf("this port derived %v for a `relates` edge, want variant-C %v",
			got, AssociativeConfidence)
	}
	edgeID := derived.Edges[0].EdgeID

	// Python's write, at the value and shape builder.py produces.
	writePython := func(when time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO work_graph_edges
(edge_id, source_type, source_id, target_type, target_id, edge_type, provenance,
 confidence, evidence, discovered_at, last_synced, event_ts, day, org_id)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			edgeID, "issue", "gh:acme/app#1", "issue", "gh:acme/app#2", "relates", "native",
			float32(1.0), "relates", when, when, when, when, org); err != nil {
			t.Fatal(err)
		}
	}
	writeThisPort := func(when time.Time) {
		t.Helper()
		stamped := make([]Row, len(derived.Edges))
		copy(stamped, derived.Edges)
		for index := range stamped {
			stamped[index].LastSynced = when
			stamped[index].DiscoveredAt = when
		}
		if _, err := WriteEdges(ctx, conn, org, stamped); err != nil {
			t.Fatal(err)
		}
	}
	finalConfidence := func() float32 {
		t.Helper()
		var confidence float32
		row := conn.QueryRow(ctx,
			`SELECT confidence FROM work_graph_edges FINAL WHERE edge_id = ?`, edgeID)
		if err := row.Scan(&confidence); err != nil {
			t.Fatal(err)
		}
		return confidence
	}
	truncate := func() {
		t.Helper()
		if err := conn.Exec(ctx, `TRUNCATE TABLE work_graph_edges`); err != nil {
			t.Fatal(err)
		}
	}

	// POST-STEP ORDER — the shipped arrangement. Python writes, then this port.
	writePython(earlier)
	writeThisPort(later)
	if got := finalConfidence(); got != AssociativeConfidence {
		t.Errorf("post-step order returned %v, want variant-C %v — this port ran last and its "+
			"value must be the one that survives", got, AssociativeConfidence)
	}

	// PRE-STEP ORDER — the regression this arrangement exists to prevent.
	truncate()
	writeThisPort(earlier)
	writePython(later)
	if got := finalConfidence(); got != 1.0 {
		t.Errorf("pre-step order returned %v, want 1.0 — if this does not come back as Python's "+
			"value then the test cannot detect a pre-step regression, and the post-step "+
			"assertion above proves nothing", got)
	}
}
