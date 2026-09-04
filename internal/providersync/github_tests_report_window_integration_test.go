//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTestsJUnitRetriedFixture is githubTestsJUnitFixture with the SAME suite
// and case names -- so the ReplacingMergeTree keys are identical -- but the
// failing case now passes. It stands in for the update this fix must never
// drop: a retried job, or a suite that arrived after the first collection.
const githubTestsJUnitRetriedFixture = `<testsuites><testsuite name="api" time="3.5">
<testcase name="passes" classname="tests/test_api.py::TestAPI" file="services/api/test_api.py" time="1.25"/>
<testcase name="fails" classname="tests/test_api.py::TestAPI" file="services/api/test_api.py" time="2.25"/>
</testsuite></testsuites>`

// githubTestsReportWindowCounts reads the RAW row count alongside the distinct
// natural-key count. The raw count is deliberately read WITHOUT FINAL: FINAL
// collapses a ReplacingMergeTree key to one logical row, so a readback through
// it would report a clean 1:1 no matter how many times the same run had been
// re-ingested -- which is exactly why CHAOS-5045 went unnoticed until the
// still-Python testops loader, which reads raw rows, started tripping its
// row cap on the duplicates.
func githubTestsReportWindowCounts(
	ctx context.Context, t *testing.T, sink GitHubTestsClickHouseEffects, orgID string,
) (raw, distinct uint64) {
	t.Helper()
	if err := sink.Conn.QueryRow(ctx,
		`SELECT count(), uniqExact((run_id, suite_id, case_id)) FROM test_case_results WHERE org_id = ?`,
		orgID,
	).Scan(&raw, &distinct); err != nil {
		t.Fatalf("count test_case_results: %v", err)
	}
	return raw, distinct
}

// CHAOS-5045, proof (a). Two consecutive hourly windows over one unchanged run
// must leave ONE raw row per (run, suite, case). Before the updated_at lower
// bound in the report phase, the second window re-downloaded the artifact and
// re-inserted every case with a fresh last_synced, and the duplicate was
// invisible to every reader that went through FINAL.
func TestGitHubTestsReportWindowWritesOneRawRowPerKeyAcrossWindows(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	base := nativeTestClaim("github", "tests")

	collect := func(doer *githubTestsReportWindowDoer, since, before, normalizedAt time.Time) {
		t.Helper()
		claim := base
		claim.SinceAt, claim.BeforeAt = &since, &before
		if err := (GitHubTestsRouteHandler{}).CollectChunks(
			ctx, claim, providerfoundation.Credential{},
			githubTestsClient(t, doer), normalizedAt, "",
			func(emission ChunkRouteEmission) error {
				for _, effect := range emission.Batch.Effects {
					if err := sink.WriteEffect(ctx, claim, effect); err != nil {
						return err
					}
				}
				return nil
			},
		); err != nil {
			t.Fatalf("CollectChunks [%s, %s): %v", since, before, err)
		}
	}

	// Window 1 collects the run for the first time.
	first := githubTestsReportWindowDoerFor(t, "2026-07-22T10:05:00Z")
	collect(first,
		time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 11, 5, 0, 0, time.UTC))
	raw, distinct := githubTestsReportWindowCounts(ctx, t, sink, base.OrgID)
	if raw == 0 || raw != distinct {
		t.Fatalf("window 1 wrote raw=%d distinct=%d, want equal and non-zero", raw, distinct)
	}
	afterFirst := raw

	// Windows 2 and 3 see the SAME unchanged run -- GitHub keeps returning it,
	// because the server-side report filter is date-granular by design.
	for _, window := range []struct{ since, before, at time.Time }{
		{time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC), time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC), time.Date(2026, 7, 22, 13, 5, 0, 0, time.UTC)},
		{time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC), time.Date(2026, 7, 22, 14, 0, 0, 0, time.UTC), time.Date(2026, 7, 22, 14, 5, 0, 0, time.UTC)},
	} {
		collect(githubTestsReportWindowDoerFor(t, "2026-07-22T10:05:00Z"),
			window.since, window.before, window.at)
	}
	raw, distinct = githubTestsReportWindowCounts(ctx, t, sink, base.OrgID)
	if raw != afterFirst || raw != distinct {
		t.Fatalf("two further windows over an unchanged run wrote raw=%d distinct=%d, "+
			"want raw unchanged at %d -- the run was re-projected", raw, distinct, afterFirst)
	}

	// Parity: the run is retried, its updated_at moves into window 4, and its
	// report changed. It MUST be re-collected, so a second raw row per key is
	// the correct outcome here, and FINAL must resolve to the new status.
	retried := githubTestsReportWindowDoerFor(t, "2026-07-22T14:30:00Z")
	retried.archive = githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitRetriedFixture})
	collect(retried,
		time.Date(2026, 7, 22, 14, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 15, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 15, 5, 0, 0, time.UTC))
	raw, distinct = githubTestsReportWindowCounts(ctx, t, sink, base.OrgID)
	if distinct != afterFirst || raw != afterFirst*2 {
		t.Fatalf("retried run wrote raw=%d distinct=%d, want raw=%d distinct=%d -- "+
			"a legitimately updated run must still be re-ingested",
			raw, distinct, afterFirst*2, afterFirst)
	}

	var failed uint64
	if err := sink.Conn.QueryRow(ctx,
		`SELECT count() FROM test_case_results FINAL WHERE org_id = ? AND status = 'failed'`,
		base.OrgID,
	).Scan(&failed); err != nil {
		t.Fatalf("count failed cases: %v", err)
	}
	if failed != 0 {
		t.Fatalf("FINAL still reports %d failed case(s) after the retry passed -- "+
			"the update did not win the ReplacingMergeTree collapse", failed)
	}
}
