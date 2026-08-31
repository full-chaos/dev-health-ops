package syncdispatchruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
)

// decodeClaimDeferralLine reads the single JSON log record
// emitClaimSnapshotDeferral wrote, failing the test if there is not exactly
// one.
func decodeClaimDeferralLine(t *testing.T, logged *bytes.Buffer) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(logged.Bytes()))
	var records []map[string]any
	for decoder.More() {
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			t.Fatalf("decode log record: %v", err)
		}
		records = append(records, record)
	}
	if len(records) != 1 {
		t.Fatalf("got %d log records, want exactly 1: %s", len(records), logged.String())
	}
	return records[0]
}

// TestEmitClaimSnapshotDeferralIsSilentWhenNothingWasDeferred pins the
// no-news-is-no-log half of CHAOS-4605's telemetry: the common case (the
// guard's snapshot and the claim agreed) must not add a WARN line to every
// dispatch pass.
func TestEmitClaimSnapshotDeferralIsSilentWhenNothingWasDeferred(t *testing.T) {
	var logged bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))
	emitClaimSnapshotDeferral(context.Background(), logger, "run-1", nil)
	emitClaimSnapshotDeferral(context.Background(), logger, "run-1", []string{})
	if logged.Len() != 0 {
		t.Fatalf("emitted %q for an empty deferral set; want nothing", logged.String())
	}
}

// TestEmitClaimSnapshotDeferralReportsTheFullCountAndABoundedSample pins the
// shape an operator reads: the COUNT is exact (it is the metric that says
// how often the snapshot-to-claim window was observed), while the unit ids
// are a bounded SAMPLE -- a run may hold up to SYNC_RUN_MAX_UNITS units and
// a log line must not carry a thousand uuids.
func TestEmitClaimSnapshotDeferralReportsTheFullCountAndABoundedSample(t *testing.T) {
	var logged bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))

	deferred := make([]string, 0, claimDeferralSampleSize+5)
	for index := 0; index < claimDeferralSampleSize+5; index++ {
		deferred = append(deferred, fmt.Sprintf("unit-%02d", index))
	}
	emitClaimSnapshotDeferral(context.Background(), logger, "run-1", deferred)

	record := decodeClaimDeferralLine(t, &logged)
	if record["msg"] != "dispatch_sync_run.claim_deferred_outside_guard_snapshot" {
		t.Fatalf("msg=%v want dispatch_sync_run.claim_deferred_outside_guard_snapshot", record["msg"])
	}
	if record["level"] != "WARN" {
		t.Fatalf("level=%v want WARN", record["level"])
	}
	if record["sync_run_id"] != "run-1" {
		t.Fatalf("sync_run_id=%v want run-1", record["sync_run_id"])
	}
	count, ok := record["claim.deferred_outside_snapshot"].(float64)
	if !ok || int(count) != len(deferred) {
		t.Fatalf("claim.deferred_outside_snapshot=%v want %d (the exact total, never the sample size)", record["claim.deferred_outside_snapshot"], len(deferred))
	}
	sample, ok := record["claim.deferred_unit_id_sample"].([]any)
	if !ok {
		t.Fatalf("claim.deferred_unit_id_sample=%v want a list", record["claim.deferred_unit_id_sample"])
	}
	if len(sample) != claimDeferralSampleSize {
		t.Fatalf("sample length=%d want %d", len(sample), claimDeferralSampleSize)
	}
	if sample[0] != deferred[0] {
		t.Fatalf("sample[0]=%v want %s (the sample is the head of the id-ordered set)", sample[0], deferred[0])
	}
}

// TestEmitClaimSnapshotDeferralUnderTheSampleSizeLogsEveryID pins the
// boundary: below the cap, nothing is trimmed.
func TestEmitClaimSnapshotDeferralUnderTheSampleSizeLogsEveryID(t *testing.T) {
	var logged bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))
	emitClaimSnapshotDeferral(context.Background(), logger, "run-1", []string{"unit-a", "unit-b"})

	record := decodeClaimDeferralLine(t, &logged)
	sample, ok := record["claim.deferred_unit_id_sample"].([]any)
	if !ok || len(sample) != 2 {
		t.Fatalf("claim.deferred_unit_id_sample=%v want both ids", record["claim.deferred_unit_id_sample"])
	}
}
