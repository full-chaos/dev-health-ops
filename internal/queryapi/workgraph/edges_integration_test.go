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

// TestFetchDedupedEdgeRowsNeverInventsAHybridRowOnATie is CHAOS-4985: the
// same defect class codex round 1 on #2183/CHAOS-4924 found in the sibling
// work_graph_issue_pr reader, closed here for work_graph_edges.
// fetchDedupedEdgeRows used to collapse repo_id/provider/provenance/
// confidence/evidence via FIVE INDEPENDENT argMax(..., last_synced) calls.
// Two unmerged rows can share the exact same last_synced to the
// millisecond -- e.g. a batch write stamping one captured timestamp across
// multiple edges -- and ClickHouse documents argMax's tie-break as
// implementation-defined, so five independent calls could each break that
// tie differently and assemble a row that never existed in any single
// physical insert.
//
// This asserts the result's full (repo_id, provider, provenance,
// confidence, evidence) set equals one of the two candidates' full sets
// EXACTLY, checked as one unit -- never a mix of two candidates. Candidates
// differ in ALL FIVE fields deliberately, not just one or two, so a fix
// that only tuples a subset of the columns would still be caught diverging
// on whichever field it left out.
func TestFetchDedupedEdgeRowsNeverInventsAHybridRowOnATie(t *testing.T) {
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

	const (
		orgID    = "org-4985-tie"
		sourceID = "issue:OPS-4"
		targetID = "pr:owner/repo#42"
	)
	tiedLastSynced := time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC)

	// candidateFields is the FULL set for one physical row, checked as ONE
	// unit -- see the test's doc comment for why a chained per-field check
	// would be unsound (the exact false-green codex round 2 on #2183 found
	// and this test's design deliberately avoids repeating).
	type candidateFields struct {
		repoID     string
		provider   string
		provenance string
		confidence float32
		evidence   string
	}
	candidates := map[candidateFields]struct{}{
		{"11111111-4985-0000-0000-000000000001", "github", "native", 0.70, "edge-token-a"}:        {},
		{"22222222-4985-0000-0000-000000000002", "gitlab", "explicit_text", 0.95, "edge-token-b"}: {},
	}
	for candidate := range candidates {
		if err := admin.Exec(ctx, `
            INSERT INTO work_graph_edges (
                edge_id, source_type, source_id, target_type, target_id, edge_type,
                repo_id, provider, provenance, confidence, evidence,
                discovered_at, last_synced, event_ts, org_id
            ) VALUES (?, 'issue', ?, 'pr', ?, 'implements', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
			"edge-4985-tie", sourceID, targetID,
			candidate.repoID, candidate.provider, candidate.provenance, candidate.confidence, candidate.evidence,
			tiedLastSynced, tiedLastSynced, tiedLastSynced, orgID,
		); err != nil {
			t.Fatalf("insert tied row %+v: %v", candidate, err)
		}
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	scope := newFilterScope(nil, nil)
	rows, err := fetchDedupedEdgeRows(ctx, client, orgID, scope, 1000)
	if err != nil {
		t.Fatalf("fetchDedupedEdgeRows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d row(s), want exactly 1: %+v", len(rows), rows)
	}
	got := rows[0]
	gotFields := candidateFields{got.repoID, got.provider, got.provenance, float32(got.confidence), got.evidence}
	if _, ok := candidates[gotFields]; !ok {
		t.Fatalf("HYBRID row: %+v matches neither candidate's full set whole -- candidates: %v", gotFields, candidates)
	}
}
