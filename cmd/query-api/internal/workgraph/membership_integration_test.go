//go:build integration

package workgraph

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestBatchResolveMembership_TupledMatchExcludesCrossProductRows is
// CHAOS-4655's red-first proof against a REAL ClickHouse engine (not a
// fake client, and not merely a changed SQL string): seeds
// work_unit_membership with the two endpoints an edge actually requests
// PLUS two "phantom" rows that satisfy an INDEPENDENT node_type-IN /
// node_id-IN filter (each phantom's type is in the request's type-set and
// its id is in the request's id-set) while NOT themselves being a
// requested (node_type, node_id) pair -- the exact cross-product shape
// codex's engine-level repro on CHAOS-4647's PR demonstrated (100k edges /
// 200k ids / 3 node types -> ClickHouse Code 396, max_result_rows
// exceeded).
//
// A tupled `(m.node_type, m.node_id) IN {node_pairs:...}` match must
// exclude both phantoms; the independent-IN shape this query had before
// CHAOS-4655 matches them.
//
// RED ON ORIGIN/MAIN (512c4e77b): with membership.go and membership_test.go
// reverted to that commit (this file left in place -- membershipKey,
// edgeEndpoint, newFilterScope and batchResolveMembership's signature are
// all unchanged by the fix, so this file compiles against either shape),
// this test FAILS: the result carries 4 entries (both phantoms included)
// instead of 2. Restoring the fix turns it green. See the PR's
// TEST-EVIDENCE for the exact commands run to produce that red, and
// membershipPairsLiteral's and TestBatchResolveMembership_QueriesATupledMatch's
// (membership_test.go) fake-client test for the corresponding SQL-shape
// regression guard that runs in the ordinary unit gate.
func TestBatchResolveMembership_TupledMatchExcludesCrossProductRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const orgID = "org-4655"
	const runID = "run-4655"
	now := time.Now().UTC()

	// Marker row FIRST, matching this table's write protocol
	// (047_work_unit_membership_run_id.sql: a run is visible to readers
	// only once its completion marker exists) -- not load-bearing for
	// this single-run fixture, but writing it in protocol order keeps the
	// seed honest about what a real materializer write looks like.
	if err := admin.Exec(ctx, `
        INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at)
        VALUES (?, ?, ?)
    `, orgID, runID, now); err != nil {
		t.Fatalf("seed work_unit_membership_runs: %v", err)
	}

	type seedRow struct {
		nodeType, nodeID, category string
	}
	// requested-1/requested-2 are the edge's real endpoints. phantom-1
	// (issue, ep-2) and phantom-2 (pr, ep-1) are CROSS-PRODUCT rows: their
	// node_type is in {issue, pr} and their node_id is in {ep-1, ep-2} --
	// exactly what an independent IN/IN filter matches -- but neither
	// pair was ever requested by the edge.
	seeds := []seedRow{
		{"issue", "ep-1", "requested-1"},
		{"pr", "ep-2", "requested-2"},
		{"issue", "ep-2", "phantom-1"},
		{"pr", "ep-1", "phantom-2"},
	}
	batch, err := admin.PrepareBatch(ctx, `
        INSERT INTO work_unit_membership (
            org_id, node_type, node_id, work_unit_id, category_kind, category,
            weight, is_dominant, categorization_status, computed_at, run_id
        )
    `)
	if err != nil {
		t.Fatalf("prepare membership batch: %v", err)
	}
	for _, seed := range seeds {
		if err := batch.Append(
			orgID, seed.nodeType, seed.nodeID, "wu-"+seed.category, "theme", seed.category,
			1.0, uint8(1), "ok", now, runID,
		); err != nil {
			t.Fatalf("append membership row %+v: %v", seed, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send membership batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	edgeRows := []edgeEndpoint{{sourceType: "issue", sourceID: "ep-1", targetType: "pr", targetID: "ep-2"}}
	result, err := batchResolveMembership(ctx, client, orgID, edgeRows, newFilterScope(nil, nil))
	if err != nil {
		t.Fatalf("batchResolveMembership: %v", err)
	}

	if _, ok := result[membershipKey{nodeType: "issue", nodeID: "ep-2"}]; ok {
		t.Fatalf("phantom cross-product row (issue, ep-2) leaked into the result -- "+
			"independent-IN shape, not a tupled match: %+v", result)
	}
	if _, ok := result[membershipKey{nodeType: "pr", nodeID: "ep-1"}]; ok {
		t.Fatalf("phantom cross-product row (pr, ep-1) leaked into the result -- "+
			"independent-IN shape, not a tupled match: %+v", result)
	}
	if len(result) != 2 {
		t.Fatalf("got %d membership entries, want exactly the 2 requested endpoints: %+v", len(result), result)
	}
	if got := result[membershipKey{nodeType: "issue", nodeID: "ep-1"}].dominantTheme; got != "requested-1" {
		t.Fatalf("issue/ep-1 dominantTheme = %q, want %q", got, "requested-1")
	}
	if got := result[membershipKey{nodeType: "pr", nodeID: "ep-2"}].dominantTheme; got != "requested-2" {
		t.Fatalf("pr/ep-2 dominantTheme = %q, want %q", got, "requested-2")
	}
}
