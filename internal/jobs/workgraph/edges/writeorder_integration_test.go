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

	// The REAL migration chain, not a transcription. `chschema.Apply` shells out
	// to the repo's Python and runs `migrations/clickhouse/` in order, so the
	// table under test is the production table -- including every ALTER that
	// followed 014_work_graph.sql.
	//
	// The first version of this test hand-copied the DDL from 014 alone and
	// failed on "No such column event_ts", which 016 adds. That was the visible
	// half of the problem. The invisible half is what this replaces: a copied
	// schema is a SECOND SOURCE OF TRUTH, so a production change to the engine's
	// version column or sorting key would leave this test green while the real
	// table no longer collapses Python's 1.0 and this port's 0.9 at all -- and
	// the collapse is the entire thing being proven.
	chschema.Apply(ctx, t, instance)

	// Merges are stopped for this test, BEFORE anything is seeded, and this line
	// is what makes the assertion below capable of failing.
	//
	// Both writes target the SAME dedup key -- the ORDER BY excludes confidence --
	// so without this the engine collapses them before the read, and a read that
	// has lost its FINAL still returns the version winner. The assertion then
	// passes on the ENGINE's dedup rather than the QUERY's, and deleting FINAL is
	// undetectable. Measured on this fixture before this line existed: the
	// FINAL-removed mutant survived 3 of 3 runs (CHAOS-4952).
	//
	// Bare form, no table argument: it is server-wide and covers everything this
	// test seeds in one statement. The per-table form is where the
	// storage-versus-name hazard bites after EXCHANGE TABLES, and is not needed
	// here.
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES`); err != nil {
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
	rawRowCount := func() uint64 {
		t.Helper()
		var count uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM work_graph_edges WHERE edge_id = ?`, edgeID,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	// finalConfidence asserts the contested state EXISTS, then that the read
	// collapses it to exactly one row, then that the row is the right one.
	//
	// THE COUNT AND THE VALUE CATCH DIFFERENT FAILURES, which is why both are
	// here rather than the value alone:
	//
	//   count == 1   catches a DELETED FINAL. With merges stopped there are two
	//                rows on this key, so a read without FINAL returns 2 and
	//                fails DETERMINISTICALLY -- independent of which row the
	//                engine would have handed back first.
	//   confidence   catches a DEFEATED FINAL: argMax over the wrong version
	//                column, or a filter applied before the merge, which still
	//                returns exactly one row while carrying the wrong value.
	//
	// The pre-read count assertion is not decoration either: it proves the two
	// rows are actually present at read time rather than inferring it from "two
	// inserts happened". A single INSERT ... SELECT collapses at part-creation
	// rather than at merge, so that inference does not hold in general.
	//
	// Deliberately NO ORDER BY. Adding one would make the leak deterministic by
	// changing what this reader does, and the fixture would then pass for a
	// reason the production read does not have.
	finalConfidence := func() float32 {
		t.Helper()
		if got := rawRowCount(); got != 2 {
			t.Fatalf("expected the two contested rows to be present before the read, got %d; "+
				"without both, a read that lost its FINAL cannot be detected", got)
		}
		rows, err := conn.Query(ctx,
			`SELECT confidence FROM work_graph_edges FINAL WHERE edge_id = ?`, edgeID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var collected []float32
		for rows.Next() {
			var confidence float32
			if err := rows.Scan(&confidence); err != nil {
				t.Fatal(err)
			}
			collected = append(collected, confidence)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(collected) != 1 {
			t.Fatalf("the deduplicating read returned %d rows (%v), want exactly 1; "+
				"more than one means the read is not deduplicating", len(collected), collected)
		}
		return collected[0]
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
