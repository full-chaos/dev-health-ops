package daily

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// benchmarkingNoLiveInputConn answers the live input read with probe (or
// probeErr) and each live-key read of a benchmarking table with live[table].
// Every read of the compute fails, which the compute swallows.
func benchmarkingNoLiveInputConn(probe [][]any, probeErr error, live map[string][][]any) *retireTestConn {
	routes := []retireTestRoute{{needles: []string{"AS input_table", "live_keys"}, err: probeErr, rows: func() [][]any { return probe }}}
	for _, table := range benchmarkingEmptyRunTables {
		rows := live[table]
		routes = append(routes, retireTestRoute{needles: []string{"FROM " + table + " WHERE org_id = ?"}, rows: func() [][]any { return rows }})
	}
	return &retireTestConn{routes: routes}
}

func benchmarkingNoLiveInputRun(t *testing.T, fullOrg bool, conn *retireTestConn, now time.Time) (int, error) {
	t.Helper()
	store := finalizeStoreWithClaim()
	store.run.FullOrg = fullOrg
	store.run.TargetDay = benchmarkingEmptyRunDay
	executor, err := NewBenchmarkingFinalizeExecutor(store, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	executor.nowUTC = func() time.Time { return now }
	return executor.ComputeFinalizeFamily(context.Background(), Run{ID: testRunID, OrganizationID: testOrgID, TargetDay: benchmarkingEmptyRunDay})
}

func retractionBatches(conn *retireTestConn) int {
	count := 0
	for _, batch := range conn.batches {
		if strings.Contains(batch.query, RetractedColumn) {
			count++
		}
	}
	return count
}

func outputLiveKeyReads(conn *retireTestConn) int {
	count := 0
	for _, query := range conn.queries {
		for _, table := range benchmarkingEmptyRunTables {
			if strings.Contains(query, "FROM "+table+" WHERE org_id = ?") {
				count++
			}
		}
	}
	return count
}

func storedOutputKeys(stored time.Time) map[string][][]any {
	return map[string][][]any{
		"testops_maturity_bands":     {{"pass_rate", "repo", benchmarkingEmptyRunRepo, stored}},
		"testops_benchmark_insights": {{"insight-1", stored}},
	}
}

// A run with partitions retires the day when no input key of the window is
// live, on a run of the whole organization only. Live input computes as
// before. A failed live input read fails the run and retires nothing.
func TestBenchmarkingRetiresADayWithNoLiveInputOnlyOnAWholeOrganizationRun(t *testing.T) {
	now := time.Date(2026, 9, 16, 8, 0, 0, 0, time.UTC)
	stored := now.Add(-time.Hour)
	zero := [][]any{}

	t.Run("no live input, whole organization: the day retires", func(t *testing.T) {
		conn := benchmarkingNoLiveInputConn(zero, nil, storedOutputKeys(stored))
		written, err := benchmarkingNoLiveInputRun(t, true, conn, now)
		t.Logf("written=%d err=%v retraction_batches=%d", written, err, retractionBatches(conn))
		if err != nil || written != 2 {
			t.Errorf("finalize = (%d, %v), want (2, nil)", written, err)
		}
		if got := retractionBatches(conn); got != 2 {
			t.Errorf("%d retraction batch(es), want 2", got)
		}
		if bands := conn.appended("testops_maturity_bands"); len(bands) != 1 || !rowHolds(bands[0], uint8(1), now) {
			t.Errorf("maturity band retraction rows = %v, want one row retracted at %s", bands, now)
		}
	})

	t.Run("no live input, not the whole organization: rows stay", func(t *testing.T) {
		conn := benchmarkingNoLiveInputConn(zero, nil, storedOutputKeys(stored))
		written, err := benchmarkingNoLiveInputRun(t, false, conn, now)
		t.Logf("written=%d err=%v retraction_batches=%d output_reads=%d", written, err, retractionBatches(conn), outputLiveKeyReads(conn))
		if err != nil {
			t.Errorf("finalize = %v, want nil", err)
		}
		if got := retractionBatches(conn); got != 0 || outputLiveKeyReads(conn) != 0 {
			t.Errorf("%d retraction batch(es), %d live-key read(s) of the outputs, want none", got, outputLiveKeyReads(conn))
		}
	})

	t.Run("live input in one table: rows stay", func(t *testing.T) {
		conn := benchmarkingNoLiveInputConn([][]any{{"testops_coverage_metrics_daily", uint64(1)}}, nil, storedOutputKeys(stored))
		written, err := benchmarkingNoLiveInputRun(t, true, conn, now)
		t.Logf("written=%d err=%v retraction_batches=%d", written, err, retractionBatches(conn))
		if err != nil {
			t.Errorf("finalize = %v, want nil", err)
		}
		if got := retractionBatches(conn); got != 0 || outputLiveKeyReads(conn) != 0 {
			t.Errorf("%d retraction batch(es), %d live-key read(s) of the outputs, want none", got, outputLiveKeyReads(conn))
		}
	})

	t.Run("live input read fails: run fails, nothing retired", func(t *testing.T) {
		readErr := errors.New("clickhouse: live input read refused")
		conn := benchmarkingNoLiveInputConn(nil, readErr, storedOutputKeys(stored))
		written, err := benchmarkingNoLiveInputRun(t, true, conn, now)
		t.Logf("written=%d err=%v retraction_batches=%d batches=%d", written, err, retractionBatches(conn), len(conn.batches))
		if !errors.Is(err, readErr) || written != 0 {
			t.Errorf("finalize = (%d, %v), want (0, the read error)", written, err)
		}
		if len(conn.batches) != 0 || outputLiveKeyReads(conn) != 0 {
			t.Errorf("%d batch(es), %d live-key read(s) of the outputs after a failed live input read, want none",
				len(conn.batches), outputLiveKeyReads(conn))
		}
	})
}
