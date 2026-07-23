package providersync

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestLinearExpiredLeaseRecoveryContractMatchesPythonAST(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	command := exec.Command(
		python,
		filepath.Join(packageDir, "testdata", "python_linear_recovery_oracle.py"),
		filepath.Join(packageDir, "..", "..", "src", "dev_health_ops", "workers", "sync_units.py"),
		filepath.Join(packageDir, "..", "..", "src", "dev_health_ops", "sync", "datasets.py"),
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Python Linear recovery oracle: %v: %s", err, output)
	}
	var want struct {
		Datasets           []string `json:"datasets"`
		RetrySurfaces      []string `json:"retry_surfaces"`
		ProvenSafeSurfaces []string `json:"proven_safe_surfaces"`
	}
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode Python Linear recovery oracle: %v: %s", err, output)
	}
	if !reflect.DeepEqual(linearBackfillWorkItemDatasets, want.Datasets) {
		t.Fatalf("datasets=%v want=%v", linearBackfillWorkItemDatasets, want.Datasets)
	}
	if !reflect.DeepEqual(linearBackfillWorkItemRetrySurfaces, want.RetrySurfaces) {
		t.Fatalf("retry surfaces=%v want=%v", linearBackfillWorkItemRetrySurfaces, want.RetrySurfaces)
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
		!reflect.DeepEqual(decision.RetrySurfaces, linearBackfillWorkItemRetrySurfaces) {
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
