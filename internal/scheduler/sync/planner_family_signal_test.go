package sync

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

// CHAOS-8773: a planner-managed parent with no enabled work-item family row
// plans no work-items unit. These tests pin the signal that replaced the
// silence: a sync_plan_gate_total count for every such plan, and a WARN only
// when the integration finished work items before. They change the default
// slog logger and the package-level counter, so none of them runs in parallel.

type capturedLog struct {
	Msg    string
	Fields map[string]any
}

func captureDefaultLogs(t *testing.T) func() []capturedLog {
	t.Helper()
	var buf bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(original) })
	return func() []capturedLog {
		var logs []capturedLog
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if line == "" {
				continue
			}
			fields := map[string]any{}
			if err := json.Unmarshal([]byte(line), &fields); err != nil {
				t.Fatalf("log line is not JSON: %v", err)
			}
			msg, _ := fields["msg"].(string)
			logs = append(logs, capturedLog{Msg: msg, Fields: fields})
		}
		return logs
	}
}

func stoppedFamilyLogs(logs []capturedLog) []capturedLog {
	var stopped []capturedLog
	for _, entry := range logs {
		if entry.Msg == "sync.plan.work_item_family_stopped" {
			stopped = append(stopped, entry)
		}
	}
	return stopped
}

// nonFamilyDatasets returns one dataset the provider plans that is not in the
// work-item family, so the plan has an enabled row but no family row. A
// provider that plans only the family (linear) gets an empty list: every row
// disabled.
func nonFamilyDatasets(provider string) []PlanDataset {
	for _, capability := range providersync.Capabilities(provider) {
		if !workitemcontract.IsFamilyDataset(capability.Dataset) {
			return []PlanDataset{{Key: capability.Dataset}}
		}
	}
	return nil
}

func familyPlanInput(provider string, datasets []PlanDataset) PlannerInput {
	return PlannerInput{
		OrgID: "org-8773", IntegrationID: "integration-8773", Mode: SyncModeIncremental,
		Now:                  time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC),
		PlannerManagedParent: true,
		Sources: []PlanSource{
			{ID: "s1", ExternalID: "one", Provider: provider, FullName: "one"},
			{ID: "s2", ExternalID: "two", Provider: provider, FullName: "two"},
		},
		Datasets: datasets,
	}
}

func TestWorkItemFamilyStoppedSignalPerProvider(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		other := nonFamilyDatasets(provider)
		t.Run(provider+"/ran before: count and WARN", func(t *testing.T) {
			globalPlanGateTelemetry.resetForTest()
			logs := captureDefaultLogs(t)
			input := familyPlanInput(provider, other)
			input.WorkItemUnitsRanBefore = true
			if _, err := BuildScheduledPlan(input); err != nil {
				t.Fatal(err)
			}
			// Two sources, one plan: one count and one WARN, not one per source.
			if got := globalPlanGateTelemetry.snapshotForTest(provider, "work-items", planGateOutcomeFamilyNotEnabled); got != 1 {
				t.Fatalf("family_not_enabled count = %d, want 1", got)
			}
			stopped := stoppedFamilyLogs(logs())
			if len(stopped) != 1 {
				t.Fatalf("stopped WARN count = %d, want 1: %+v", len(stopped), logs())
			}
			fields := stopped[0].Fields
			want := map[string]any{
				"level": "WARN", "provider": provider, "org_id": "org-8773",
				"integration_id": "integration-8773", "family": "work-items",
			}
			for key, value := range want {
				if fields[key] != value {
					t.Fatalf("log field %s = %v, want %v (%v)", key, fields[key], value, fields)
				}
			}
			// The log call carries no error operand: only the fixed field set.
			allowed := []string{"time", "level", "msg", "provider", "org_id", "integration_id", "family"}
			for key := range fields {
				if !slices.Contains(allowed, key) {
					t.Fatalf("unexpected log field %q in %v", key, fields)
				}
			}
		})
		t.Run(provider+"/never ran: count, no WARN", func(t *testing.T) {
			globalPlanGateTelemetry.resetForTest()
			logs := captureDefaultLogs(t)
			if _, err := BuildScheduledPlan(familyPlanInput(provider, other)); err != nil {
				t.Fatal(err)
			}
			if got := globalPlanGateTelemetry.snapshotForTest(provider, "work-items", planGateOutcomeFamilyNotEnabled); got != 1 {
				t.Fatalf("family_not_enabled count = %d, want 1", got)
			}
			if stopped := stoppedFamilyLogs(logs()); len(stopped) != 0 {
				t.Fatalf("opted-out-never-ran must not warn: %+v", stopped)
			}
		})
		t.Run(provider+"/enabled family row: neither", func(t *testing.T) {
			globalPlanGateTelemetry.resetForTest()
			logs := captureDefaultLogs(t)
			input := familyPlanInput(provider, append([]PlanDataset{{Key: "work-items"}}, other...))
			input.WorkItemUnitsRanBefore = true
			if _, err := BuildScheduledPlan(input); err != nil {
				t.Fatal(err)
			}
			if got := globalPlanGateTelemetry.snapshotForTest(provider, "work-items", planGateOutcomeFamilyNotEnabled); got != 0 {
				t.Fatalf("family_not_enabled count = %d, want 0", got)
			}
			if stopped := stoppedFamilyLogs(logs()); len(stopped) != 0 {
				t.Fatalf("enabled family must not warn: %+v", stopped)
			}
		})
		t.Run(provider+"/not a planner-managed parent: neither", func(t *testing.T) {
			globalPlanGateTelemetry.resetForTest()
			logs := captureDefaultLogs(t)
			input := familyPlanInput(provider, other)
			input.PlannerManagedParent = false
			input.WorkItemUnitsRanBefore = true
			if _, err := BuildScheduledPlan(input); err != nil {
				t.Fatal(err)
			}
			if got := globalPlanGateTelemetry.snapshotForTest(provider, "work-items", planGateOutcomeFamilyNotEnabled); got != 0 {
				t.Fatalf("family_not_enabled count = %d, want 0", got)
			}
			if stopped := stoppedFamilyLogs(logs()); len(stopped) != 0 {
				t.Fatalf("a non-parent plan must not warn: %+v", stopped)
			}
		})
	}
}

func TestWorkItemFamilyStoppedSignalIgnoresOtherProviders(t *testing.T) {
	globalPlanGateTelemetry.resetForTest()
	logs := captureDefaultLogs(t)
	input := familyPlanInput("pagerduty", []PlanDataset{{Key: "incidents"}})
	input.WorkItemUnitsRanBefore = true
	if _, err := BuildScheduledPlan(input); err != nil {
		t.Fatal(err)
	}
	if got := globalPlanGateTelemetry.snapshotForTest("pagerduty", "work-items", planGateOutcomeFamilyNotEnabled); got != 0 {
		t.Fatalf("pagerduty has no work-item family; count = %d", got)
	}
	if stopped := stoppedFamilyLogs(logs()); len(stopped) != 0 {
		t.Fatalf("unexpected WARN: %+v", stopped)
	}
}

func TestWorkItemFamilyStoppedSignalRendersOnTheGateMetric(t *testing.T) {
	globalPlanGateTelemetry.resetForTest()
	if _, err := BuildScheduledPlan(familyPlanInput("jira", nil)); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := globalPlanGateTelemetry.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	want := `sync_plan_gate_total{provider="jira",dataset="work-items",outcome="family_not_enabled"} 1`
	if !strings.Contains(buf.String(), want) {
		t.Fatalf("scrape lacks %q:\n%s", want, buf.String())
	}
}

func TestPlanNeedsWorkItemRanBeforeOnlyForAnEmptyFamily(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		other := nonFamilyDatasets(provider)
		withFamily := append([]PlanDataset{{Key: "work-item-labels"}}, other...)
		if !PlanNeedsWorkItemRanBefore(provider, true, other) {
			t.Errorf("%s: an empty family on a planner-managed parent must load the fact", provider)
		}
		if !PlanNeedsWorkItemRanBefore(provider, true, nil) {
			t.Errorf("%s: no datasets at all must load the fact", provider)
		}
		if PlanNeedsWorkItemRanBefore(provider, true, withFamily) {
			t.Errorf("%s: a family row means no query", provider)
		}
		if PlanNeedsWorkItemRanBefore(provider, false, other) {
			t.Errorf("%s: a non-parent config means no query", provider)
		}
	}
	if PlanNeedsWorkItemRanBefore("pagerduty", true, nil) {
		t.Error("a provider with no work-item family means no query")
	}
}

// The ran-before probe runs on the coordinator pool; the posture manifest
// must keep SELECT on sync_run_units for that role (SELECT is implied for
// every RequiredTables entry).
func TestCoordinatorPostureGrantsSelectOnSyncRunUnits(t *testing.T) {
	for _, table := range postgres.CoordinatorPosture().RequiredTables {
		if table.TableName == "sync_run_units" {
			return
		}
	}
	t.Fatal("coordinator posture does not require sync_run_units: the ran-before probe would fail in production")
}
