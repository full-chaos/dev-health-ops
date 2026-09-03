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

// TestFetchLinkedIssueRowsFastPathMatchesFinal is CHAOS-4924's golden
// proof, against a REAL ClickHouse engine: fetchLinkedIssueRowsFastPath
// (argMax(col, version_rank), no FINAL) must return exactly what
// fetchLinkedIssueRowsFinal (the Python-mirroring oracle, FINAL +
// argMax(col, last_synced)) returns, on a fixture seeded with UNMERGED
// duplicate physical rows -- i.e. before any background merge has
// collapsed them, the exact state a live read hits between merges.
//
// The seed's first identity is deliberately the CHAOS-4769 trap migration
// 084 exists to fix: a native row with an OLDER last_synced than a
// competing explicit_text row. A reader keyed on last_synced alone (naive,
// pre-084 behavior) would pick explicit_text; both readers under test here
// must pick native instead, because version_rank ranks provenance ahead of
// timestamp. If either query regressed to plain last_synced ordering, this
// assertion (not just the differential-equality one) would catch it.
func TestFetchLinkedIssueRowsFastPathMatchesFinal(t *testing.T) {
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
		orgID  = "org-4924"
		repoID = "00000000-4924-0000-0000-000000000001"
	)

	type seedRow struct {
		workItemID           string
		prNumber             uint32
		confidence           float32
		provenance, evidence string
		lastSynced           time.Time
	}
	// Identity 1 (pr_number 99, issue:OPS-1): TWO unmerged physical rows for
	// the SAME key -- native's last_synced is EARLIER than explicit_text's,
	// the exact CHAOS-4769 shape. version_rank must still rank native
	// first.
	// Identity 2 (pr_number 100, issue:OPS-2): a single row, no duplicates
	// -- the ordinary case, proving both readers agree there too.
	seeds := []seedRow{
		{"issue:OPS-1", 99, 0.60, "explicit_text", "explicit-token", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"issue:OPS-1", 99, 0.90, "native", "native-token", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"issue:OPS-2", 100, 0.50, "heuristic", "heuristic-token", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	batch, err := admin.PrepareBatch(ctx, `
        INSERT INTO work_graph_issue_pr (
            org_id, repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced
        )
    `)
	if err != nil {
		t.Fatalf("prepare work_graph_issue_pr batch: %v", err)
	}
	for _, seed := range seeds {
		if err := batch.Append(
			orgID, repoID, seed.workItemID, seed.prNumber, seed.confidence,
			seed.provenance, seed.evidence, seed.lastSynced,
		); err != nil {
			t.Fatalf("append work_graph_issue_pr row %+v: %v", seed, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_graph_issue_pr batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	for _, testCase := range []struct {
		name       string
		prNumber   int
		wantOnlyID string
		wantProv   string
		wantEvid   string
	}{
		{"unmerged duplicates, provenance decides over a newer competing last_synced", 99, "issue:OPS-1", "native", "native-token"},
		{"single row, no duplicates", 100, "issue:OPS-2", "heuristic", "heuristic-token"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			final, err := fetchLinkedIssueRowsFinal(ctx, client, orgID, repoID, testCase.prNumber)
			if err != nil {
				t.Fatalf("fetchLinkedIssueRowsFinal: %v", err)
			}
			fastPath, err := fetchLinkedIssueRowsFastPath(ctx, client, orgID, repoID, testCase.prNumber)
			if err != nil {
				t.Fatalf("fetchLinkedIssueRowsFastPath: %v", err)
			}

			if len(final) != 1 || len(fastPath) != 1 {
				t.Fatalf("got %d final row(s), %d fast-path row(s), want exactly 1 each: final=%+v fastPath=%+v",
					len(final), len(fastPath), final, fastPath)
			}
			if final[0] != fastPath[0] {
				t.Fatalf("fast path diverged from the FINAL oracle: final=%+v fastPath=%+v", final[0], fastPath[0])
			}
			if got := final[0]; got.workItemID != testCase.wantOnlyID || got.provenance != testCase.wantProv || got.evidence != testCase.wantEvid {
				t.Fatalf("got %+v, want work_item_id=%q provenance=%q evidence=%q (both readers agreed with EACH OTHER but not with the expected provenance-ranked winner -- see the test's own doc comment)",
					got, testCase.wantOnlyID, testCase.wantProv, testCase.wantEvid)
			}
		})
	}
}

// TestFetchLinkedIssueRowsFastPathNeverInventsAHybridRowOnATie is codex round
// 1 on #2183's P2 finding, closed: two unmerged rows for one identity can
// share the EXACT SAME version_rank -- same provenance and the same
// last_synced to the millisecond, e.g. two writes in one batch stamped with a
// single captured timestamp. ClickHouse documents argMax's tie-break as
// implementation-defined; THREE separate argMax(col, version_rank) calls (the
// pre-fix shape) could each independently break that tie a different way and
// return a row that never existed -- confidence from one physical insert,
// evidence from another.
//
// This does NOT assert the fast path agrees with fetchLinkedIssueRowsFinal on
// this exact scenario -- it deliberately cannot, per fetchLinkedIssueRowsFastPath's
// own doc comment: matching FINAL's physical insertion-order tie-break under
// an exact value tie would require paying for FINAL, defeating the point of
// this reader. What IS asserted, and IS achievable: the result must be one of
// the two candidate rows' full (confidence, evidence) pairs, in full,
// never a mix of the two.
func TestFetchLinkedIssueRowsFastPathNeverInventsAHybridRowOnATie(t *testing.T) {
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
		orgID      = "org-4924-tie"
		repoID     = "00000000-4924-0000-0000-000000000002"
		workItemID = "issue:OPS-3"
		prNumber   = 200
	)
	tiedLastSynced := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)

	candidates := map[[2]string]struct{}{
		{"native", "batch-token-a"}: {},
		{"native", "batch-token-b"}: {},
	}
	batch, err := admin.PrepareBatch(ctx, `
        INSERT INTO work_graph_issue_pr (
            org_id, repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced
        )
    `)
	if err != nil {
		t.Fatalf("prepare work_graph_issue_pr batch: %v", err)
	}
	confidences := map[string]float32{"batch-token-a": 0.70, "batch-token-b": 0.95}
	for pair := range candidates {
		provenance, evidence := pair[0], pair[1]
		if err := batch.Append(
			orgID, repoID, workItemID, prNumber, confidences[evidence], provenance, evidence, tiedLastSynced,
		); err != nil {
			t.Fatalf("append tied row (provenance=%q evidence=%q): %v", provenance, evidence, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_graph_issue_pr batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	fastPath, err := fetchLinkedIssueRowsFastPath(ctx, client, orgID, repoID, prNumber)
	if err != nil {
		t.Fatalf("fetchLinkedIssueRowsFastPath: %v", err)
	}
	if len(fastPath) != 1 {
		t.Fatalf("got %d fast-path row(s), want exactly 1: %+v", len(fastPath), fastPath)
	}
	got := fastPath[0]
	if got.workItemID != workItemID {
		t.Fatalf("work_item_id = %q, want %q", got.workItemID, workItemID)
	}
	// Anchor on evidence -- the one field that unambiguously names which of
	// the two candidate rows this result claims to be -- then verify
	// provenance AND confidence match THAT SAME row's real values. A hybrid
	// (evidence from one physical row, confidence or provenance from the
	// other) fails here even though each field individually looks valid.
	wantConfidence, isKnownEvidence := confidences[got.evidence]
	if !isKnownEvidence {
		t.Fatalf("evidence %q matches neither candidate row: %+v", got.evidence, got)
	}
	if _, ok := candidates[[2]string{got.provenance, got.evidence}]; !ok {
		t.Fatalf("HYBRID row: provenance %q does not belong with evidence %q in either candidate: %+v",
			got.provenance, got.evidence, got)
	}
	if got.confidence != float64(wantConfidence) {
		t.Fatalf("HYBRID row: evidence %q implies confidence %v, got %v: %+v",
			got.evidence, wantConfidence, got.confidence, got)
	}
}
