//go:build integration

package workgraph

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-5449: duplicate ReplacingMergeTree versions must not consume the
// LIMIT budget.
//
// WHAT THE TICKET SAID, AND WHY IT WAS WRONG. CHAOS-5449 was filed as a
// tie-ORDERING divergence -- "the values are a valid set on each side but
// the ORDER differs, apply CHAOS-4381's tie-ordering rules". That
// diagnosis does not survive contact with the two implementations: both
// sides sort identically, `ORDER BY confidence DESC, edge_id ASC`
// (Python at resolvers/work_graph.py:1163, Go at edges.go's
// fetchDedupedEdgeRows). There is no ordering difference to reconcile,
// and relaxing the comparator's ordering rules here would have HIDDEN
// the real defect rather than fixed it.
//
// WHAT THE MEASUREMENT ACTUALLY SHOWED. Re-reading the 2026-09-07
// enablement run's stored payloads for a 1000-row request:
//
//	Python: 1000 rows,  738 DISTINCT edgeIds  (262 duplicate rows)
//	Go:     1000 rows, 1000 DISTINCT edgeIds  (0 duplicates)
//	set(python) - set(go) = 0     set(go) - set(python) = 262
//
// Go's result is a strict SUPERSET: every edge Python returned, plus 262
// further genuine edges Python could not reach because its duplicates
// had eaten the budget. The reported index-5 "different edgeId" is just
// Python repeating its own index 4, shifting everything after it.
//
// WHY. work_graph_edges is ReplacingMergeTree(last_synced), and edge_id
// is a deterministic hash of the identity columns (014_work_graph.sql:7),
// so two pre-merge versions of one logical edge tie on the ENTIRE sort
// key -- no tie-break drawn from the row's own columns can separate
// them. Go collapses them with argMax(tuple(...), last_synced) GROUP BY
// the identity BEFORE ORDER BY/LIMIT; Python reads raw. LIMIT therefore
// counts distinct EDGES on the Go side and physical ROWS on the Python
// side.
//
// WHAT THIS FILE ADDS. edges_integration_test.go already covers the
// OTHER property of that same argMax (CHAOS-4985: never assembling a
// hybrid row from two candidates on a last_synced tie). Nothing covered
// the ordering of dedup relative to LIMIT, which is the property
// CHAOS-5449 actually exposed and the one a future "make the comparator
// match Python" edit would break first. edges.go's own doc comment
// already forbids that edit -- "Never revert this to a raw (un-deduped)
// read to make the dual-run comparator match" -- and this file is the
// executable form of that instruction.

const (
	// All three edges share one confidence so `ORDER BY confidence DESC,
	// edge_id ASC` reduces to edge_id ASC, making the expected LIMIT
	// window exact rather than approximate.
	dedupBudgetConfidence = float32(1.0)

	// Lexicographically ordered ids: alpha < beta < gamma. `alpha` is the
	// one seeded twice.
	dedupBudgetEdgeIDAlpha = "aaaa0000000000000000000000000000000000000000000000000000000000a1"
	dedupBudgetEdgeIDBeta  = "bbbb0000000000000000000000000000000000000000000000000000000000b2"
	dedupBudgetEdgeIDGamma = "cccc0000000000000000000000000000000000000000000000000000000000c3"
)

type dedupBudgetEdge struct {
	edgeID     string
	sourceID   string
	targetID   string
	evidence   string
	lastSynced time.Time
}

// TestFetchDedupedEdgeRowsDoesNotSpendTheLimitBudgetOnDuplicateVersions
// pins the property CHAOS-5449 exposed: with LIMIT 2 over a table whose
// first logical edge has two unmerged versions, the deduped read returns
// TWO DISTINCT edges (alpha, beta), where a raw read returns alpha twice
// and never reaches beta.
//
// The un-deduped control is run in the same test, against the same
// engine and the same rows, so "Python loses beta here" is a measured
// result rather than a claim about code that lives in another language
// in another directory.
func TestFetchDedupedEdgeRowsDoesNotSpendTheLimitBudgetOnDuplicateVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const orgID = "org-5449-dedup-budget"

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

	assertEdgesDedupContract(t, ctx, admin)

	// alpha twice (two versions of ONE logical edge -- same identity
	// columns, so the same edge_id), beta and gamma once each. Separate
	// Exec calls put each row in its own part: a single multi-row INSERT
	// could be collapsed before the read and the fixture would prove
	// nothing.
	base := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	seedDedupBudgetEdges(t, ctx, admin, orgID, []dedupBudgetEdge{
		{dedupBudgetEdgeIDAlpha, "issue:OPS-1", "pr:owner/repo#1", "alpha-v1", base},
		{dedupBudgetEdgeIDAlpha, "issue:OPS-1", "pr:owner/repo#1", "alpha-v2", base.Add(time.Hour)},
		{dedupBudgetEdgeIDBeta, "issue:OPS-2", "pr:owner/repo#2", "beta-v1", base},
		{dedupBudgetEdgeIDGamma, "issue:OPS-3", "pr:owner/repo#3", "gamma-v1", base},
	})
	assertUnmergedEdgeParts(t, ctx, admin)

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const limit = 2
	rows, err := fetchDedupedEdgeRows(ctx, client, orgID, newFilterScope(nil, nil), limit)
	if err != nil {
		t.Fatalf("fetchDedupedEdgeRows: %v", err)
	}

	if len(rows) != limit {
		t.Fatalf("got %d row(s) for LIMIT %d, want exactly %d: %+v", len(rows), limit, limit, rows)
	}

	got := make([]string, 0, len(rows))
	for _, r := range rows {
		got = append(got, r.edgeID)
	}
	sort.Strings(got)
	want := []string{dedupBudgetEdgeIDAlpha, dedupBudgetEdgeIDBeta}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("LIMIT %d returned %v, want %v -- beta is the edge a raw, un-deduped read never reaches, "+
			"because alpha's two versions spend the whole budget; getting alpha twice here means the "+
			"argMax dedup no longer runs BEFORE ORDER BY/LIMIT", limit, got, want)
	}

	distinct := map[string]int{}
	for _, r := range rows {
		distinct[r.edgeID]++
	}
	if len(distinct) != limit {
		t.Errorf("LIMIT %d returned only %d distinct edge id(s) (%v) -- LIMIT must count distinct EDGES, "+
			"never physical rows", limit, len(distinct), distinct)
	}

	// The control: Python's shape, same engine, same rows, same sort,
	// same limit. If this ever stops losing beta, the fixture has stopped
	// exercising the duplicate-version condition and the assertions above
	// are passing for the wrong reason.
	rawIDs := scanRawEdgeIDsPythonShape(t, ctx, admin, orgID, limit)
	if len(rawIDs) != limit {
		t.Fatalf("un-deduped control returned %d row(s), want %d: %v", len(rawIDs), limit, rawIDs)
	}
	rawDistinct := map[string]struct{}{}
	for _, id := range rawIDs {
		rawDistinct[id] = struct{}{}
	}
	if len(rawDistinct) != 1 {
		t.Fatalf("un-deduped control returned %d distinct edge id(s) (%v), want exactly 1; the seeded "+
			"duplicate is not reaching the raw read, so this test proves nothing", len(rawDistinct), rawIDs)
	}
	if _, reached := rawDistinct[dedupBudgetEdgeIDBeta]; reached {
		t.Fatalf("un-deduped control reached beta, which contradicts the premise that duplicates "+
			"consume the LIMIT budget: %v", rawIDs)
	}
}

func seedDedupBudgetEdges(t *testing.T, ctx context.Context, admin stdclickhouse.Conn, orgID string, edges []dedupBudgetEdge) {
	t.Helper()
	for _, e := range edges {
		if err := admin.Exec(ctx, `
            INSERT INTO work_graph_edges (
                edge_id, source_type, source_id, target_type, target_id, edge_type,
                repo_id, provider, provenance, confidence, evidence,
                discovered_at, last_synced, event_ts, org_id
            ) VALUES (?, 'issue', ?, 'pr', ?, 'implements', NULL, 'github', 'native', ?, ?, ?, ?, ?, ?)
        `,
			e.edgeID, e.sourceID, e.targetID,
			dedupBudgetConfidence, e.evidence,
			e.lastSynced, e.lastSynced, e.lastSynced, orgID,
		); err != nil {
			t.Fatalf("insert edge %s (%s): %v", e.edgeID, e.evidence, err)
		}
	}
}

// assertEdgesDedupContract reads the two schema properties this test
// depends on back from the MIGRATED table rather than assuming them: the
// engine must collapse duplicate versions by last_synced, and the sorting
// key must be the edge identity. If a migration ever adds a
// version-distinguishing column to that key, two versions of one edge
// stop being duplicates and every assertion here would pass vacuously.
func assertEdgesDedupContract(t *testing.T, ctx context.Context, admin stdclickhouse.Conn) {
	t.Helper()
	var engineFull, sortingKey string
	row := admin.QueryRow(ctx,
		"SELECT engine_full, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'work_graph_edges'")
	if err := row.Scan(&engineFull, &sortingKey); err != nil {
		t.Fatalf("read work_graph_edges schema from the migrated database: %v", err)
	}
	if !strings.Contains(engineFull, "ReplacingMergeTree(last_synced)") {
		t.Fatalf("work_graph_edges engine is %q; this test only means something while the table collapses "+
			"duplicate versions by last_synced", engineFull)
	}
	for _, column := range []string{"source_type", "source_id", "edge_type", "target_type", "target_id"} {
		if !strings.Contains(sortingKey, column) {
			t.Fatalf("work_graph_edges sorting key is %q, missing identity column %q -- the dedup identity "+
				"this test seeds duplicates against no longer holds", sortingKey, column)
		}
	}
}

func assertUnmergedEdgeParts(t *testing.T, ctx context.Context, admin stdclickhouse.Conn) {
	t.Helper()
	var parts uint64
	row := admin.QueryRow(ctx,
		"SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'work_graph_edges' AND active")
	if err := row.Scan(&parts); err != nil {
		t.Fatalf("count active parts: %v", err)
	}
	if parts < 2 {
		t.Fatalf("fixture is vacuous: work_graph_edges has %d active part(s), so alpha's two versions were "+
			"already merged and no duplicate can reach either read", parts)
	}
}

// scanRawEdgeIDsPythonShape runs the un-deduped read Python performs
// (resolvers/work_graph.py:1164-1181 -- a plain SELECT over
// work_graph_edges with no FINAL and no argMax collapse), with the same
// `ORDER BY confidence DESC, edge_id ASC` both implementations share and
// the same LIMIT.
func scanRawEdgeIDsPythonShape(t *testing.T, ctx context.Context, admin stdclickhouse.Conn, orgID string, limit int) []string {
	t.Helper()
	query := fmt.Sprintf(`
        SELECT edge_id
        FROM work_graph_edges
        WHERE org_id = '%s'
        ORDER BY confidence DESC, edge_id ASC
        LIMIT %d
    `, orgID, limit)
	rows, err := admin.Query(ctx, query)
	if err != nil {
		t.Fatalf("un-deduped control query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("un-deduped control scan: %v", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("un-deduped control rows: %v", err)
	}
	return out
}
