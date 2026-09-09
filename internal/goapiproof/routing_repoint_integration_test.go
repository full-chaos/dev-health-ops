//go:build integration

package goapiproof

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const repointRunningBuild = "ffd9e5d5dc8ee21de5befa1bae47ba9195be135e"

// seedRow puts one row at the shape the live compose stack had on
// 2026-09-09: a registered candidate build plus a routing row naming it.
func seedRow(t *testing.T, ctx context.Context, operation, documentDigest, mode, build string, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, registerCandidateBuildSQL, testSchemaDigest, documentDigest, operation, build); err != nil {
		t.Fatalf("register %s: %v", operation, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build,
			 owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', $5, 100, 'seeded', 'test')`,
		testSchemaDigest, documentDigest, operation, build, mode); err != nil {
		t.Fatalf("seed %s: %v", operation, err)
	}
}

// THE DEFECT THIS VERB EXISTS FOR. A shadow row could not be re-pointed
// by any supported verb: enable --mode takes only canary|primary, and
// disable never writes current_candidate_build. Measured on compose
// 2026-09-09 with three shadow rows stuck at an old build while the proof
// runner refused on all fifteen. Here the shadow row moves to the running
// build AND STAYS SHADOW.
func TestRepointMovesAShadowRowWithoutMakingItReachable(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	seedRow(t, ctx, "hotspots", testDocumentDigest, "shadow", testCandidateBuild, pool)

	outcomes, err := Repoint(ctx, pool, RepointRequest{
		SchemaDigest:   testSchemaDigest,
		RunningBuild:   repointRunningBuild,
		RecordedBy:     "lane-stack-owner",
		ReviewEvidence: "CHAOS-5486 re-point to the running build",
	})
	if err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	if len(outcomes) != 1 {
		t.Fatalf("outcomes = %d, want 1", len(outcomes))
	}
	if got := outcomes[0]; got.ModeBefore != "shadow" || got.ModeAfter != "shadow" || !got.Changed {
		t.Fatalf("outcome = %+v, want shadow->shadow and Changed", got)
	}

	var mode, build, recordedBy, evidence string
	if err := pool.QueryRow(ctx, `
		SELECT mode, current_candidate_build, recorded_by, review_evidence
		  FROM go_api_routing_state WHERE selected_operation = 'hotspots'`).
		Scan(&mode, &build, &recordedBy, &evidence); err != nil {
		t.Fatal(err)
	}
	if mode != "shadow" {
		t.Fatalf("mode = %q after a re-point, want shadow -- a provenance write must never change reachability", mode)
	}
	if build != repointRunningBuild {
		t.Fatalf("current_candidate_build = %q, want the running build", build)
	}
	if recordedBy != "lane-stack-owner" || evidence == "seeded" {
		t.Fatalf("provenance not recorded: recorded_by=%q review_evidence=%q", recordedBy, evidence)
	}
}

// Every reachability column survives, not just mode.
func TestRepointLeavesOwnerAndRolloutUntouched(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	seedRow(t, ctx, "flowMatrix", testDocumentDigest, "canary", testCandidateBuild, pool)
	if _, err := pool.Exec(ctx, `UPDATE go_api_routing_state SET rollout_percentage = 42 WHERE selected_operation = 'flowMatrix'`); err != nil {
		t.Fatal(err)
	}

	if _, err := Repoint(ctx, pool, RepointRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e",
	}); err != nil {
		t.Fatalf("Repoint: %v", err)
	}

	var owner, mode string
	var rollout int
	if err := pool.QueryRow(ctx, `SELECT owner, mode, rollout_percentage FROM go_api_routing_state WHERE selected_operation = 'flowMatrix'`).
		Scan(&owner, &mode, &rollout); err != nil {
		t.Fatal(err)
	}
	if owner != "go" || mode != "canary" || rollout != 42 {
		t.Fatalf("owner=%q mode=%q rollout=%d -- want go/canary/42 untouched", owner, mode, rollout)
	}
}

// The FK is the reason ordering matters: register first, or the update is
// rejected. Re-pointing to a build nothing has registered must still work,
// because registration happens inside the same transaction.
func TestRepointRegistersTheBuildItPointsAt(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	seedRow(t, ctx, "cognitiveLoad", testDocumentDigest, "canary", testCandidateBuild, pool)

	var before int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_candidate_build WHERE candidate_build = $1`, repointRunningBuild).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before != 0 {
		t.Fatalf("precondition: running build already registered (%d rows)", before)
	}
	if _, err := Repoint(ctx, pool, RepointRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e",
	}); err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	var after int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_candidate_build WHERE candidate_build = $1`, repointRunningBuild).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != 1 {
		t.Fatalf("candidate build rows for the running build = %d, want 1", after)
	}
}

// Re-running is a no-op in EFFECT and says so, rather than reporting a
// write that changed nothing.
func TestRepointIsIdempotentAndReportsUnchanged(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	seedRow(t, ctx, "reviewEdges", testDocumentDigest, "canary", testCandidateBuild, pool)

	request := RepointRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e",
	}
	first, err := Repoint(ctx, pool, request)
	if err != nil {
		t.Fatalf("first Repoint: %v", err)
	}
	if !first[0].Changed {
		t.Fatal("first run must report Changed")
	}
	second, err := Repoint(ctx, pool, request)
	if err != nil {
		t.Fatalf("second Repoint: %v", err)
	}
	if second[0].Changed {
		t.Fatal("second run must report Unchanged: the row already names the running build")
	}
	if summary := Summarize(second); summary.Unchanged != 1 || summary.Changed != 0 {
		t.Fatalf("summary = %+v, want Unchanged 1 / Changed 0", summary)
	}
}

// A dry run reports exactly what a real run would do and writes nothing.
func TestRepointDryRunWritesNothing(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	seedRow(t, ctx, "workGraphEdges", testDocumentDigest, "shadow", testCandidateBuild, pool)

	outcomes, err := Repoint(ctx, pool, RepointRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e", DryRun: true,
	})
	if err != nil {
		t.Fatalf("Repoint dry-run: %v", err)
	}
	if !outcomes[0].Changed {
		t.Fatal("dry run must still report that the row WOULD change")
	}
	var build string
	if err := pool.QueryRow(ctx, `SELECT current_candidate_build FROM go_api_routing_state WHERE selected_operation = 'workGraphEdges'`).Scan(&build); err != nil {
		t.Fatal(err)
	}
	if build != testCandidateBuild {
		t.Fatalf("dry run wrote: build = %q, want the original %q", build, testCandidateBuild)
	}
}

// Mixed modes in one call: the live shape is 12 canary + 3 shadow, and
// every one must keep its own mode.
func TestRepointPreservesEachRowsOwnMode(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	seedRow(t, ctx, "canaryOp", "aa"+testDocumentDigest[2:], "canary", testCandidateBuild, pool)
	seedRow(t, ctx, "shadowOp", "bb"+testDocumentDigest[2:], "shadow", testCandidateBuild, pool)
	seedRow(t, ctx, "pythonOp", "cc"+testDocumentDigest[2:], "python", testCandidateBuild, pool)

	outcomes, err := Repoint(ctx, pool, RepointRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e",
	})
	if err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	if len(outcomes) != 3 {
		t.Fatalf("outcomes = %d, want 3", len(outcomes))
	}
	want := map[string]string{"canaryOp": "canary", "shadowOp": "shadow", "pythonOp": "python"}
	rows, err := pool.Query(ctx, `SELECT selected_operation, mode, current_candidate_build FROM go_api_routing_state`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var operation, mode, build string
		if err := rows.Scan(&operation, &mode, &build); err != nil {
			t.Fatal(err)
		}
		if mode != want[operation] {
			t.Fatalf("%s mode = %q, want %q", operation, mode, want[operation])
		}
		if build != repointRunningBuild {
			t.Fatalf("%s build = %q, want the running build", operation, build)
		}
		seen++
	}
	if seen != 3 {
		t.Fatalf("saw %d rows, want 3", seen)
	}
}

// Selecting a subset must leave every other row alone.
func TestRepointHonoursTheOperationFilter(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	seedRow(t, ctx, "chosen", "aa"+testDocumentDigest[2:], "canary", testCandidateBuild, pool)
	seedRow(t, ctx, "untouched", "bb"+testDocumentDigest[2:], "shadow", testCandidateBuild, pool)

	if _, err := Repoint(ctx, pool, RepointRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		Operations: []string{"chosen"}, RecordedBy: "t", ReviewEvidence: "e",
	}); err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	var build string
	if err := pool.QueryRow(ctx, `SELECT current_candidate_build FROM go_api_routing_state WHERE selected_operation = 'untouched'`).Scan(&build); err != nil {
		t.Fatal(err)
	}
	if build != testCandidateBuild {
		t.Fatalf("unselected row moved to %q", build)
	}
}

// "No rows" is an error, not an empty success: an unreachable or empty
// registry and a fully-correct one must never read alike.
func TestRepointRefusesWhenNothingMatches(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	if _, err := Repoint(ctx, pool, RepointRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: repointRunningBuild,
		RecordedBy: "t", ReviewEvidence: "e",
	}); !errors.Is(err, ErrRepointNoRows) {
		t.Fatalf("Repoint on an empty registry = %v, want ErrRepointNoRows", err)
	}
}
