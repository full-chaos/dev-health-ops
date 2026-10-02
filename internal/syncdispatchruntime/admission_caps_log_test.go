package syncdispatchruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5/pgxpool"
)

type noopBudgetEstimator struct{}

func (noopBudgetEstimator) DispatchBudgetEstimate(context.Context, string, string, []string) (map[string][]budgetEstimate, error) {
	return nil, nil
}

type noopPolicyRegistry struct{}

func (noopPolicyRegistry) Descriptor(string) (jobruntime.Descriptor, bool) {
	return jobruntime.Descriptor{}, false
}

// TestDispatchServiceConstructionLogsTheAdmissionCaps pins CHAOS-7881 on the
// side that APPLIES the cap: constructing the service the dispatch guard runs
// in writes one Info line with the cap per cost class, the budget limit of the
// same class and the clamp THIS process read, and no tenant data.
func TestDispatchServiceConstructionLogsTheAdmissionCaps(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "3")
	var logs bytes.Buffer
	service, err := NewNativeDispatchSyncRunService(
		&pgxpool.Pool{}, slog.New(slog.NewJSONHandler(&logs, nil)),
		noopBudgetEstimator{}, &joboutbox.Producer{}, noopPolicyRegistry{},
	)
	if err != nil || service == nil {
		t.Fatalf("NewNativeDispatchSyncRunService = %v, %v", service, err)
	}
	var found map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "sync_dispatch_admission_caps" {
			if found != nil {
				t.Fatalf("more than one sync_dispatch_admission_caps line:\n%s", logs.String())
			}
			found = record
		}
	}
	if found == nil {
		t.Fatalf("no sync_dispatch_admission_caps line in:\n%s", logs.String())
	}
	want := map[string]float64{
		"admission_cap_light": 3, "admission_cap_medium": 2, "admission_cap_heavy": 1,
		"budget_limit_light": 4, "budget_limit_medium": 2, "budget_limit_heavy": 1,
		"admission_clamp": 3,
	}
	for key, value := range want {
		if found[key] != value {
			t.Fatalf("%s = %v; want %v in %v", key, found[key], value, found)
		}
	}
	for key := range found {
		if strings.Contains(key, "org") || strings.Contains(key, "tenant") {
			t.Fatalf("tenant-shaped attribute %q: %v", key, found)
		}
	}
}

// TestDispatchServiceConstructionLogsTheClampWhereItDiffersFromEveryCap pins
// the unclamped case: with SYNC_UNIT_CONCURRENCY_PER_BUCKET=8 the caps are the
// table's 4/2/1 and the clamp attribute is 8, so a line that prints the clamp
// in place of a cap (or a cap in place of the clamp) cannot pass (CHAOS-7881).
func TestDispatchServiceConstructionLogsTheClampWhereItDiffersFromEveryCap(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
	var logs bytes.Buffer
	if _, err := NewNativeDispatchSyncRunService(
		&pgxpool.Pool{}, slog.New(slog.NewJSONHandler(&logs, nil)),
		noopBudgetEstimator{}, &joboutbox.Producer{}, noopPolicyRegistry{},
	); err != nil {
		t.Fatal(err)
	}
	var found map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "sync_dispatch_admission_caps" {
			found = record
		}
	}
	if found == nil {
		t.Fatalf("no sync_dispatch_admission_caps line in:\n%s", logs.String())
	}
	want := map[string]float64{
		"admission_cap_light": 4, "admission_cap_medium": 2, "admission_cap_heavy": 1,
		"budget_limit_light": 4, "budget_limit_medium": 2, "budget_limit_heavy": 1,
		"admission_clamp": 8,
	}
	for key, value := range want {
		if found[key] != value {
			t.Fatalf("%s = %v; want %v in %v", key, found[key], value, found)
		}
	}
}

// TestDispatchServiceConstructionLogsEveryCapBelowItsBudgetLimit pins the
// setting where each cap leaves its budget limit: with the clamp at 1 the caps
// are 1/1/1 while the budget limits stay 4/2/1, so a cap printed from the
// budget table (or the reverse) cannot pass (CHAOS-7881).
func TestDispatchServiceConstructionLogsEveryCapBelowItsBudgetLimit(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
	var logs bytes.Buffer
	if _, err := NewNativeDispatchSyncRunService(
		&pgxpool.Pool{}, slog.New(slog.NewJSONHandler(&logs, nil)),
		noopBudgetEstimator{}, &joboutbox.Producer{}, noopPolicyRegistry{},
	); err != nil {
		t.Fatal(err)
	}
	var found map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "sync_dispatch_admission_caps" {
			found = record
		}
	}
	if found == nil {
		t.Fatalf("no sync_dispatch_admission_caps line in:\n%s", logs.String())
	}
	want := map[string]float64{
		"admission_cap_light": 1, "admission_cap_medium": 1, "admission_cap_heavy": 1,
		"budget_limit_light": 4, "budget_limit_medium": 2, "budget_limit_heavy": 1,
		"admission_clamp": 1,
	}
	for key, value := range want {
		if found[key] != value {
			t.Fatalf("%s = %v; want %v in %v", key, found[key], value, found)
		}
	}
}
