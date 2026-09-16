//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// githubTestsCommitReportWindowPass drives one full incremental window through
// the production (chunked) route, which emits its batch across more than one
// page. Every destination's rows are gathered across every emission and
// committed exactly once for the window -- the ledger's generation identity
// is per claim, not per page, so committing each raw emission on its own
// (a partial, changing view of the same generation) is rejected as a
// conflict. It returns the total row count actually written by the sink,
// across every destination.
func githubTestsCommitReportWindowPass(
	t *testing.T, ctx context.Context, sink GitHubTestsClickHouseEffects,
	doer providerfoundation.HTTPDoer, orgID string, since, before, normalizedAt time.Time,
) int {
	t.Helper()
	claim := nativeTestClaim("github", "tests")
	claim.OrgID = orgID
	claim.ID = uuid.NewString()
	claim.SyncRunID = uuid.NewString()
	claim.SinceAt, claim.BeforeAt = &since, &before

	order := make([]string, 0, 6)
	rowsByDestination := map[string][]json.RawMessage{}
	recoveryByDestination := map[string]EffectRecoveryPolicy{}
	err := GitHubTestsRouteHandler{}.CollectChunks(
		ctx, claim, providerfoundation.Credential{}, githubTestsClient(t, doer), normalizedAt, "",
		func(emission ChunkRouteEmission) error {
			for _, effect := range emission.Batch.Effects {
				if _, seen := rowsByDestination[effect.Destination]; !seen {
					order = append(order, effect.Destination)
				}
				rowsByDestination[effect.Destination] = append(rowsByDestination[effect.Destination], effect.Rows...)
				recoveryByDestination[effect.Destination] = effect.Recovery
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}

	batches := make([]EffectBatch, 0, len(order))
	for _, destination := range order {
		if len(rowsByDestination[destination]) == 0 {
			continue
		}
		batch, buildErr := BuildEffectBatch(destination, recoveryByDestination[destination], rowsByDestination[destination])
		if buildErr != nil {
			t.Fatalf("BuildEffectBatch %s: %v", destination, buildErr)
		}
		batches = append(batches, batch)
	}
	if len(batches) == 0 {
		return 0
	}

	ledger := &memoryEffectLedger{}
	committer := EffectCommitter{
		Ledger: ledger, Sink: sink, Readback: sink,
		Now: func() time.Time { return normalizedAt },
	}
	result, commitErr := committer.Commit(ctx, claim, batches, normalizedAt)
	if commitErr != nil {
		t.Fatalf("Commit: %v", commitErr)
	}
	written := result.Written
	return written
}

func githubTestsRawCount(t *testing.T, ctx context.Context, sink GitHubTestsClickHouseEffects, table, orgID string) uint64 {
	t.Helper()
	var count uint64
	if err := sink.Conn.QueryRow(
		ctx, "SELECT count() FROM "+table+" WHERE org_id = ?", orgID,
	).Scan(&count); err != nil {
		t.Fatalf("raw count %s: %v", table, err)
	}
	return count
}

func githubTestsDistinctKeyCount(t *testing.T, ctx context.Context, sink GitHubTestsClickHouseEffects, table, keys, orgID string) uint64 {
	t.Helper()
	var count uint64
	if err := sink.Conn.QueryRow(
		ctx, "SELECT uniqExact(("+keys+")) FROM "+table+" WHERE org_id = ?", orgID,
	).Scan(&count); err != nil {
		t.Fatalf("distinct count %s: %v", table, err)
	}
	return count
}

// A run that stopped changing well before an incremental window opens is
// genuinely stale. Two windows, days apart, over the same unchanged run: the
// second must never re-request that run's artifacts at all -- this is the
// observable that separates "we skipped this run" from "we re-downloaded it
// and happened to write the same rows again," which still costs a redundant
// write.
func TestGitHubTestsReportPhaseIntegrationSkipsAStaleRunAcrossPasses(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	orgID := "org-stale-" + uuid.NewString()

	first := githubTestsReportWindowDoerFor(t, "2026-07-22T10:05:00Z")
	written := githubTestsCommitReportWindowPass(t, ctx, sink, first, orgID,
		time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 11, 5, 0, 0, time.UTC))
	if !first.fetchedArtifacts() || written == 0 {
		t.Fatalf("window 1 did not collect the run: fetched=%v written=%d", first.fetchedArtifacts(), written)
	}

	// Same run, unchanged, two days later -- far outside any grace tied to a
	// one-hour window.
	second := githubTestsReportWindowDoerFor(t, "2026-07-22T10:05:00Z")
	githubTestsCommitReportWindowPass(t, ctx, sink, second, orgID,
		time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 24, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 24, 11, 5, 0, 0, time.UTC))
	if second.fetchedArtifacts() {
		t.Fatalf("window 2 re-requested a stale run's artifacts: %v", second.requests)
	}
}

// The dedicated no-FINAL uniqueness check the fix is actually for: after two
// passes over the same unchanged run, test_suite_results and
// test_case_results carry exactly one physical row per logical key -- read
// WITHOUT FINAL, which is where the duplication was invisible before. A raw
// count read through FINAL always reports 1.0 regardless of how many times a
// row was rewritten, which is why this defect went unnoticed until a raw
// reader tripped over it.
func TestGitHubTestsReportPhaseIntegrationRawRowsStaySingleAcrossPasses(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	orgID := "org-unique-" + uuid.NewString()

	pass := func(updatedAt string, since, before, normalizedAt time.Time) {
		doer := githubTestsReportWindowDoerFor(t, updatedAt)
		githubTestsCommitReportWindowPass(t, ctx, sink, doer, orgID, since, before, normalizedAt)
	}
	pass("2026-07-22T10:05:00Z",
		time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 11, 5, 0, 0, time.UTC))
	pass("2026-07-22T10:05:00Z",
		time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 24, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 24, 11, 5, 0, 0, time.UTC))

	for _, table := range []struct{ name, keys string }{
		{"test_suite_results", "org_id, repo_id, run_id, suite_id"},
		{"test_case_results", "org_id, repo_id, run_id, suite_id, case_id"},
	} {
		raw := githubTestsRawCount(t, ctx, sink, table.name, orgID)
		distinct := githubTestsDistinctKeyCount(t, ctx, sink, table.name, table.keys, orgID)
		if raw == 0 {
			t.Fatalf("%s: no rows were written by either pass", table.name)
		}
		if raw != distinct {
			t.Fatalf("%s: raw=%d distinct=%d -- the second pass re-wrote the unchanged run", table.name, raw, distinct)
		}
	}
}

// A run that genuinely changes between windows -- a re-run, a retried job, a
// late-arriving suite -- must still be written on the later pass. This is the
// non-regression guard for the fix above: bounding the fetch on the run's
// last-updated time must not turn into bounding it on the run's identity.
func TestGitHubTestsReportPhaseIntegrationStillWritesAGenuinelyUpdatedRunOnASecondPass(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	orgID := "org-rerun-" + uuid.NewString()

	first := githubTestsReportWindowDoerFor(t, "2026-07-22T10:05:00Z")
	githubTestsCommitReportWindowPass(t, ctx, sink, first, orgID,
		time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 11, 5, 0, 0, time.UTC))

	// The run was re-triggered: updated_at moves into window 2, two days
	// later. It must be fetched and written again despite the two-day gap.
	second := githubTestsReportWindowDoerFor(t, "2026-07-24T10:30:00Z")
	writtenSecond := githubTestsCommitReportWindowPass(t, ctx, sink, second, orgID,
		time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 24, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 24, 11, 5, 0, 0, time.UTC))
	if !second.fetchedArtifacts() || writtenSecond == 0 {
		t.Fatalf("a genuinely updated run was skipped: fetched=%v written=%d", second.fetchedArtifacts(), writtenSecond)
	}

	// The fixture's suite carries 2 cases, so 2 legitimate passes write 4
	// physical rows -- one full set per pass, none suppressed.
	raw := githubTestsRawCount(t, ctx, sink, "test_case_results", orgID)
	if raw != 4 {
		t.Fatalf("expected exactly 4 physical rows (2 cases x 2 passes) for a run that legitimately changed, got %d", raw)
	}
}

// The end-to-end analogue of the in-memory late-artifact guard: a report that
// becomes listable only after the run's own updated_at has settled must still
// reach real ClickHouse, not just an in-memory decoded effect.
func TestGitHubTestsReportPhaseIntegrationLatePublishedArtifactStillReachesClickHouse(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	orgID := "org-late-" + uuid.NewString()
	archive := githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture})
	const settled = "2026-07-22T10:44:59Z" // inside window 1, and inside window 2's grace

	first := &githubTestsLateArtifactDoer{t: t, archive: archive, updatedAt: settled, artifactsLive: false}
	written := githubTestsCommitReportWindowPass(t, ctx, sink, first, orgID,
		time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC))
	if !first.listedArtifacts() {
		t.Fatal("window 1 never listed artifacts at all")
	}
	if raw := githubTestsRawCount(t, ctx, sink, "test_case_results", orgID); raw != 0 {
		t.Fatalf("window 1 should have written no cases yet, got %d", raw)
	}
	_ = written

	// The artifact is now listable; the run itself did not change.
	second := &githubTestsLateArtifactDoer{t: t, archive: archive, updatedAt: settled, artifactsLive: true}
	githubTestsCommitReportWindowPass(t, ctx, sink, second, orgID,
		time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC))
	if !second.listedArtifacts() {
		t.Fatalf("window 2 never listed the run's artifacts, so the late-published report can never be collected: requests=%v", second.requests)
	}
	if raw := githubTestsRawCount(t, ctx, sink, "test_case_results", orgID); raw == 0 {
		t.Fatal("window 2 listed artifacts but ClickHouse still has no case rows: the late report was lost")
	}
}
