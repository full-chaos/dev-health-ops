//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"
)

// CHAOS-14259 sink cells for the retraction row of a dropped report, on a ClickHouse migrated to the chain head (171 adds the
// retracted column).

func retiredSinkRow(claim Claim, lastSynced time.Time, retracted bool) testSuiteResultRow {
	started := time.Date(2026, 7, 22, 10, 1, 0, 0, time.UTC)
	row := testSuiteResultRow{OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "43600014259", SuiteID: "suite-total-b-left",
		SuiteName: "left", StartedAt: &started, LastSynced: lastSynced, ReportSlot: "total-b", RunAttempt: 1, LegacySuiteIDs: []string{"legacy-left"}}
	if retracted {
		row.Retracted = true
		return row
	}
	duration := 3.0
	row.TotalCount, row.PassedCount, row.DurationSeconds = 1, 1, &duration
	return row
}

func retiredSinkEffect(t *testing.T, rows ...testSuiteResultRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("test_suite_results", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func retiredSinkStored(t *testing.T, ctx context.Context, sink TestOpsClickHouseEffects) (rows uint64, retracted uint8, total uint32, synced time.Time) {
	t.Helper()
	if err := sink.Conn.QueryRow(ctx, `SELECT count(), any(retracted), any(total_count), any(last_synced) FROM test_suite_results FINAL
WHERE org_id = 'org-acme' AND run_id = '43600014259'`).Scan(&rows, &retracted, &total, &synced); err != nil {
		t.Fatal(err)
	}
	return rows, retracted, total, synced.UTC()
}

func TestTestOpsSinkReadsBackTheRetractionRowOfADroppedReport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	sink := newTestOpsMigratedSink(t, ctx)
	claim := nativeTestClaim("github", "tests")
	older, newer := time.Date(2026, 7, 22, 11, 5, 0, 0, time.UTC), time.Date(2026, 7, 22, 12, 5, 0, 0, time.UTC)

	// K11: the retraction row over the stale live row of its key. It is written with retracted = 1, read back exactly (the flag
	// is part of the compared row), and a replay of the same emission is exact again and leaves one current row.
	if err := sink.WriteEffect(ctx, claim, retiredSinkEffect(t, retiredSinkRow(claim, older, false))); err != nil {
		t.Fatal(err)
	}
	retraction := retiredSinkEffect(t, retiredSinkRow(claim, newer, true))
	if got, err := sink.InspectEffect(ctx, claim, retraction); err != nil || got != EffectAbsent {
		t.Fatalf("before the write the retraction reads %v (%v), want absent: the stored row is older", got, err)
	}
	for pass := 1; pass <= 2; pass++ {
		if err := sink.WriteEffect(ctx, claim, retraction); err != nil {
			t.Fatal(err)
		}
		if got, err := sink.InspectEffect(ctx, claim, retraction); err != nil || got != EffectExact {
			t.Fatalf("write %d: the retraction reads back %v (%v), want exact", pass, got, err)
		}
	}
	if rows, retracted, total, synced := retiredSinkStored(t, ctx, sink); rows != 1 || retracted != 1 || total != 0 || !synced.Equal(newer) {
		t.Fatalf("current row: %d rows, retracted %d, total %d, last_synced %s; want 1 zero-measure retracted row at %s", rows, retracted, total, synced, newer)
	}
	// The same version with the flag cleared is another row: a conflict, not a settle.
	if got, err := sink.InspectEffect(ctx, claim, retiredSinkEffect(t, func() testSuiteResultRow {
		row := retiredSinkRow(claim, newer, true)
		row.Retracted = false
		return row
	}())); err != nil || got != EffectConflict {
		t.Fatalf("a live row at the retraction's version reads %v (%v), want conflict", got, err)
	}

	// K10: a newer walk revived the slot (a live row, newer last_synced); an older unit's retraction replayed later is settled by
	// the stored newer row and never wins under FINAL.
	revived := newer.Add(time.Hour)
	if err := sink.WriteEffect(ctx, claim, retiredSinkEffect(t, retiredSinkRow(claim, revived, false))); err != nil {
		t.Fatal(err)
	}
	if got, err := sink.InspectEffect(ctx, claim, retraction); err != nil || got != EffectExact {
		t.Fatalf("the older retraction under a newer live row reads %v (%v), want exact (settled)", got, err)
	}
	if rows, retracted, total, synced := retiredSinkStored(t, ctx, sink); rows != 1 || retracted != 0 || total != 1 || !synced.Equal(revived) {
		t.Fatalf("current row: %d rows, retracted %d, total %d, last_synced %s; want the live row at %s", rows, retracted, total, synced, revived)
	}
}

// A case retraction row (no name, no status, retracted) replaces the stale case row at its key and reads back exactly.
func TestTestOpsSinkReadsBackTheCaseRetractionRowOfADroppedReport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	sink := newTestOpsMigratedSink(t, ctx)
	claim := nativeTestClaim("github", "tests")
	older, newer := time.Date(2026, 7, 22, 11, 5, 0, 0, time.UTC), time.Date(2026, 7, 22, 12, 5, 0, 0, time.UTC)
	message := "boom"
	stale := testCaseResultRow{OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "43600014259", SuiteID: "suite-total-b-left",
		CaseID: "case-a", CaseName: "a", Status: "failed", FailureMessage: &message, LastSynced: older}
	retraction := testCaseResultRow{OrgID: stale.OrgID, RepoID: stale.RepoID, RunID: stale.RunID, SuiteID: stale.SuiteID, CaseID: stale.CaseID,
		LastSynced: newer, Retracted: true}
	build := func(row testCaseResultRow) EffectBatch {
		effect, err := effectBatchFromValues("test_case_results", EffectReadbackRequired, []testCaseResultRow{row})
		if err != nil {
			t.Fatal(err)
		}
		return effect
	}
	if err := sink.WriteEffect(ctx, claim, build(stale)); err != nil {
		t.Fatal(err)
	}
	for pass := 1; pass <= 2; pass++ {
		if err := sink.WriteEffect(ctx, claim, build(retraction)); err != nil {
			t.Fatal(err)
		}
		if got, err := sink.InspectEffect(ctx, claim, build(retraction)); err != nil || got != EffectExact {
			t.Fatalf("write %d: the case retraction reads back %v (%v), want exact", pass, got, err)
		}
	}
	var rows uint64
	var retracted uint8
	var name, status string
	if err := sink.Conn.QueryRow(ctx, `SELECT count(), any(retracted), any(case_name), any(status) FROM test_case_results FINAL
WHERE org_id = 'org-acme' AND run_id = '43600014259'`).Scan(&rows, &retracted, &name, &status); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || retracted != 1 || name != "" || status != "" {
		t.Fatalf("current case row: %d rows, retracted %d, name %q, status %q; want 1 retracted row with no name or status", rows, retracted, name, status)
	}
}
