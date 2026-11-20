//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/google/uuid"
)

func githubTestsFinalCount(t *testing.T, ctx context.Context, sink GitHubTestsClickHouseEffects, table, orgID string) uint64 {
	t.Helper()
	var count uint64
	if err := sink.Conn.QueryRow(
		ctx, "SELECT count() FROM "+table+" FINAL WHERE org_id = ?", orgID,
	).Scan(&count); err != nil {
		t.Fatalf("final count %s: %v", table, err)
	}
	return count
}

// A second read of one run writes the same keys again: no new key and no new
// FINAL row. Shape: a 1-day chunk before a 7-day chunk with no tiling on the
// claims (BackfillNext nil), which reads the run twice
// (TestGitHubTestsBackfillUnknownTilingKeepsTheSameWidthRule). Was
// TestGitHubTestsBackfillIntegrationRESIDUALSecondReadWritesNoNewRow.
func TestGitHubTestsBackfillIntegrationSecondReadWritesNoNewRow(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	orgID := "org-backfill-residual-" + uuid.NewString()
	a := time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)
	created, updated := a.Add(-12*time.Hour), a.AddDate(0, 0, 3)
	tables := []struct{ name, keys string }{
		{"test_suite_results", "org_id, repo_id, run_id, suite_id"},
		{"test_case_results", "org_id, repo_id, run_id, suite_id, case_id"},
	}
	type counts struct{ raw, distinct, final uint64 }
	snapshot := func() map[string]counts {
		out := map[string]counts{}
		for _, table := range tables {
			out[table.name] = counts{
				raw:      githubTestsRawCount(t, ctx, sink, table.name, orgID),
				distinct: githubTestsDistinctKeyCount(t, ctx, sink, table.name, table.keys, orgID),
				final:    githubTestsFinalCount(t, ctx, sink, table.name, orgID),
			}
		}
		return out
	}
	pass := func(since, before time.Time) {
		doer := githubTestsReportWindowDoerFor(t, updated.Format(time.RFC3339))
		doer.createdAt = created.Format(time.RFC3339)
		doer.startedAt = created.Add(time.Minute).Format(time.RFC3339)
		doer.filterCreated = true
		githubTestsCommitReportWindowPassMode(t, ctx, sink, fakehttp.Client(doer), orgID, "backfill", since, before, before.Add(5*time.Minute))
		if !doer.fetchedArtifacts() {
			t.Fatalf("chunk [%s, %s) did not read the run: %v", since.Format(time.RFC3339), before.Format(time.RFC3339), doer.requests)
		}
	}
	pass(a, a.AddDate(0, 0, 1))
	first := snapshot()
	pass(a.AddDate(0, 0, 1), a.AddDate(0, 0, 8))
	second := snapshot()
	for _, table := range tables {
		before, after := first[table.name], second[table.name]
		t.Logf("CELL %s: after read 1 raw=%d distinct=%d final=%d; after read 2 raw=%d distinct=%d final=%d",
			table.name, before.raw, before.distinct, before.final, after.raw, after.distinct, after.final)
		if before.distinct == 0 {
			t.Fatalf("%s: the first read wrote no row", table.name)
		}
		if after.distinct != before.distinct || after.final != before.final {
			t.Errorf("%s: the second read added a row: distinct %d -> %d, final %d -> %d",
				table.name, before.distinct, after.distinct, before.final, after.final)
		}
	}
}

// CHAOS-14222 (acp condition C). The last backfill chunk now reads a run it
// lists that is updated after its before; the incremental window that holds
// that updated_at reads the same run again. Both reads write the same
// ReplacingMergeTree keys: no new key and no new FINAL row after the second.
func TestGitHubTestsBackfillIntegrationLastChunkAndIncrementalWriteOneRow(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	orgID := "org-backfill-last-" + uuid.NewString()
	d := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	created, updated := d.Add(12*time.Hour), d.AddDate(0, 0, 1).Add(2*time.Hour)
	tables := []struct{ name, keys string }{
		{"test_suite_results", "org_id, repo_id, run_id, suite_id"},
		{"test_case_results", "org_id, repo_id, run_id, suite_id, case_id"},
	}
	type counts struct{ raw, distinct, final uint64 }
	snapshot := func() map[string]counts {
		out := map[string]counts{}
		for _, table := range tables {
			out[table.name] = counts{
				raw:      githubTestsRawCount(t, ctx, sink, table.name, orgID),
				distinct: githubTestsDistinctKeyCount(t, ctx, sink, table.name, table.keys, orgID),
				final:    githubTestsFinalCount(t, ctx, sink, table.name, orgID),
			}
		}
		return out
	}
	pass := func(name, mode string, since, before time.Time, next *BackfillNextWindow) {
		doer := githubTestsReportWindowDoerFor(t, updated.Format(time.RFC3339))
		doer.createdAt = created.Format(time.RFC3339)
		doer.startedAt = created.Add(time.Minute).Format(time.RFC3339)
		doer.filterCreated = true
		claim := nativeTestClaim("github", "tests")
		claim.Mode, claim.OrgID, claim.ID, claim.SyncRunID = mode, orgID, uuid.NewString(), uuid.NewString()
		claim.SinceAt, claim.BeforeAt, claim.BackfillNext = &since, &before, next
		githubTestsCommitReportWindowPassClaim(t, ctx, sink, fakehttp.Client(doer), claim, updated.Add(5*time.Minute))
		if !doer.fetchedArtifacts() {
			t.Fatalf("%s [%s, %s) did not read the run: %v", name, since.Format(time.RFC3339), before.Format(time.RFC3339), doer.requests)
		}
	}
	pass("last backfill chunk", "backfill", d, d.AddDate(0, 0, 1), &BackfillNextWindow{})
	first := snapshot()
	pass("incremental window", "incremental", updated.Add(-time.Hour), updated.Add(time.Hour), nil)
	second := snapshot()
	for _, table := range tables {
		before, after := first[table.name], second[table.name]
		t.Logf("CELL %s: after the last chunk raw=%d distinct=%d final=%d; after the incremental raw=%d distinct=%d final=%d",
			table.name, before.raw, before.distinct, before.final, after.raw, after.distinct, after.final)
		if before.distinct == 0 {
			t.Fatalf("%s: the last chunk wrote no row", table.name)
		}
		if after.distinct != before.distinct || after.final != before.final {
			t.Errorf("%s: the incremental read added a row: distinct %d -> %d, final %d -> %d",
				table.name, before.distinct, after.distinct, before.final, after.final)
		}
	}
}
