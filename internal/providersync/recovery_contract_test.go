package providersync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

// TestLinearExpiredLeaseRecoveryContractMatchesPythonAST compares this
// package's Linear recovery constants against a frozen snapshot instead of
// shelling out to a live Python AST parse: the Python producer these values
// mirrored (workers/sync_units.py's run_sync_unit and its
// _LINEAR_BACKFILL_WORK_ITEM_DATASETS / _LINEAR_BACKFILL_WORK_ITEM_IN_BAND_
// WRITE_SURFACES / _CLICKHOUSE_RETRY_PROVEN_SAFE_SURFACES constants) is
// deleted -- the native Go recovery decision (recovery_contract.go) is the
// only production path now. See testdata/oracle_frozen/README.md for the
// freezing convention this reuses.
func TestLinearExpiredLeaseRecoveryContractMatchesPythonAST(t *testing.T) {
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	frozen, err := os.ReadFile(filepath.Join(
		packageDir, "testdata", "oracle_frozen", "linear_expired_lease_recovery.json",
	))
	if err != nil {
		t.Fatalf("read frozen Linear recovery oracle snapshot: %v", err)
	}
	var want struct {
		Datasets           []string `json:"datasets"`
		RetrySurfaces      []string `json:"retry_surfaces"`
		ProvenSafeSurfaces []string `json:"proven_safe_surfaces"`
	}
	if err := json.Unmarshal(frozen, &want); err != nil {
		t.Fatalf("decode frozen Linear recovery oracle snapshot: %v: %s", err, frozen)
	}
	if got := workitemcontract.LinearBackfillWorkItemDatasets(); !reflect.DeepEqual(got, want.Datasets) {
		t.Fatalf("datasets=%v want=%v", got, want.Datasets)
	}
	if got := workitemcontract.LinearExpiredLeaseRetryDestinations(); !reflect.DeepEqual(got, want.RetrySurfaces) {
		t.Fatalf("retry surfaces=%v want=%v", got, want.RetrySurfaces)
	}
	if !reflect.DeepEqual(clickHouseRetryProvenSafeSurfaces, want.ProvenSafeSurfaces) {
		t.Fatalf("proven-safe surfaces=%v want=%v", clickHouseRetryProvenSafeSurfaces, want.ProvenSafeSurfaces)
	}
}

func TestLinearExpiredLeaseRecoveryEligibilityIsFailClosed(t *testing.T) {
	t.Parallel()
	base := Unit{Provider: "linear", Dataset: "work-items", Mode: "backfill"}
	decision := LinearExpiredLeaseRetryDecision(base, 0, 1)
	if !decision.ShouldRetry || decision.RetryExhausted || decision.NextRetryCount != 1 ||
		!reflect.DeepEqual(decision.RetrySurfaces, workitemcontract.LinearExpiredLeaseRetryDestinations()) {
		t.Fatalf("eligible decision=%+v", decision)
	}
	exhausted := LinearExpiredLeaseRetryDecision(base, 1, 1)
	if exhausted.ShouldRetry || !exhausted.RetryExhausted {
		t.Fatalf("exhausted decision=%+v", exhausted)
	}
	for _, mutation := range []func(*Unit){
		func(unit *Unit) { unit.Provider = "jira" },
		func(unit *Unit) { unit.Mode = "incremental" },
		func(unit *Unit) { unit.Dataset = "incidents" },
	} {
		unit := base
		mutation(&unit)
		got := LinearExpiredLeaseRetryDecision(unit, 0, 1)
		if got.ShouldRetry || got.RetryExhausted || len(got.RetrySurfaces) != 0 {
			t.Fatalf("mutated unit=%+v decision=%+v", unit, got)
		}
	}
	zeroRetries := LinearExpiredLeaseRetryDecision(base, 0, 0)
	if zeroRetries.ShouldRetry || !zeroRetries.RetryExhausted {
		t.Fatalf("zero-retry decision=%+v", zeroRetries)
	}
}
