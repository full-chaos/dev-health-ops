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

var familyPlanNow = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

func familyPlanInput(provider string, datasets []PlanDataset) PlannerInput {
	return PlannerInput{
		OrgID: "org-8773", IntegrationID: "integration-8773", Mode: SyncModeIncremental,
		Now:                  familyPlanNow,
		WorkItemClockAt:      familyPlanNow,
		PlannerManagedParent: true,
		Sources: []PlanSource{
			{ID: "s1", ExternalID: "one", Provider: provider, FullName: "one"},
			{ID: "s2", ExternalID: "two", Provider: provider, FullName: "two"},
		},
		Datasets: datasets,
	}
}

// agoPtr is the newest successful work-items unit, d before the plan's Now.
func agoPtr(d time.Duration) *time.Time {
	at := familyPlanNow.Add(-d)
	return &at
}

func TestWorkItemFamilyStoppedSignalPerProvider(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		other := nonFamilyDatasets(provider)
		t.Run(provider+"/recent history: count and WARN", func(t *testing.T) {
			globalPlanGateTelemetry.resetForTest()
			logs := captureDefaultLogs(t)
			input := familyPlanInput(provider, other)
			input.LastWorkItemSuccessAt = agoPtr(3 * 24 * time.Hour)
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
				"last_success_age_days": float64(3), "warn_window_days": float64(14),
			}
			for key, value := range want {
				if fields[key] != value {
					t.Fatalf("log field %s = %v, want %v (%v)", key, fields[key], value, fields)
				}
			}
			// The log call carries no error operand: only the fixed field set.
			allowed := []string{"time", "level", "msg", "provider", "org_id", "integration_id", "family", "last_success_age_days", "warn_window_days"}
			for key := range fields {
				if !slices.Contains(allowed, key) {
					t.Fatalf("unexpected log field %q in %v", key, fields)
				}
			}
		})
		t.Run(provider+"/history older than the window: count only", func(t *testing.T) {
			globalPlanGateTelemetry.resetForTest()
			logs := captureDefaultLogs(t)
			input := familyPlanInput(provider, other)
			input.LastWorkItemSuccessAt = agoPtr(15 * 24 * time.Hour)
			if _, err := BuildScheduledPlan(input); err != nil {
				t.Fatal(err)
			}
			if got := globalPlanGateTelemetry.snapshotForTest(provider, "work-items", planGateOutcomeFamilyNotEnabled); got != 1 {
				t.Fatalf("family_not_enabled count = %d, want 1", got)
			}
			if stopped := stoppedFamilyLogs(logs()); len(stopped) != 0 {
				t.Fatalf("old history must not warn: %+v", stopped)
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
			input.LastWorkItemSuccessAt = agoPtr(3 * 24 * time.Hour)
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
			input.LastWorkItemSuccessAt = agoPtr(3 * 24 * time.Hour)
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
	input.LastWorkItemSuccessAt = agoPtr(3 * 24 * time.Hour)
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

func TestPlanNeedsWorkItemLastSuccessOnlyForAnEmptyFamily(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		other := nonFamilyDatasets(provider)
		withFamily := append([]PlanDataset{{Key: "work-item-labels"}}, other...)
		if !PlanNeedsWorkItemLastSuccess(provider, true, other) {
			t.Errorf("%s: an empty family on a planner-managed parent must load the fact", provider)
		}
		if !PlanNeedsWorkItemLastSuccess(provider, true, nil) {
			t.Errorf("%s: no datasets at all must load the fact", provider)
		}
		if PlanNeedsWorkItemLastSuccess(provider, true, withFamily) {
			t.Errorf("%s: a family row means no query", provider)
		}
		if PlanNeedsWorkItemLastSuccess(provider, false, other) {
			t.Errorf("%s: a non-parent config means no query", provider)
		}
	}
	if PlanNeedsWorkItemLastSuccess("pagerduty", true, nil) {
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

// The age is measured on the database clock, not the occurrence's scheduled
// time: a replayed old occurrence (Now 20 days before the clock) must not
// turn a 16-day-old success into a negative age that warns.
func TestWorkItemFamilyStoppedWarnWindowUsesTheDatabaseClockNotNow(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		other := nonFamilyDatasets(provider)
		for _, tc := range []struct {
			name     string
			age      time.Duration
			wantWarn bool
		}{
			{"16 days old by the clock", 16 * 24 * time.Hour, false},
			{"3 days old by the clock", 3 * 24 * time.Hour, true},
		} {
			t.Run(provider+"/"+tc.name, func(t *testing.T) {
				globalPlanGateTelemetry.resetForTest()
				logs := captureDefaultLogs(t)
				input := familyPlanInput(provider, other)
				input.Now = familyPlanNow.Add(-20 * 24 * time.Hour) // replayed oldest occurrence
				input.LastWorkItemSuccessAt = agoPtr(tc.age)
				if _, err := BuildScheduledPlan(input); err != nil {
					t.Fatal(err)
				}
				if got := globalPlanGateTelemetry.snapshotForTest(provider, "work-items", planGateOutcomeFamilyNotEnabled); got != 1 {
					t.Fatalf("family_not_enabled count = %d, want 1", got)
				}
				stopped := stoppedFamilyLogs(logs())
				if tc.wantWarn != (len(stopped) == 1) {
					t.Fatalf("wantWarn=%v, WARN entries=%+v", tc.wantWarn, stopped)
				}
				if tc.wantWarn && stopped[0].Fields["last_success_age_days"] != float64(3) {
					t.Fatalf("age = %v, want 3", stopped[0].Fields["last_success_age_days"])
				}
			})
		}
	}
}

// The window boundary with literal times: a unit exactly 14 days old still
// warns (age 14 whole days); one second older does not; one second younger
// warns with 13 whole days; a unit after Now (clock skew) warns at age 0.
func TestWorkItemFamilyStoppedWarnWindowBoundary(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	at := func(s string) *time.Time {
		parsed, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return &parsed
	}
	cases := []struct {
		name     string
		last     *time.Time
		wantDays int
		wantWarn bool
	}{
		{"never ran", nil, 0, false},
		{"just now", at("2026-10-06T12:00:00Z"), 0, true},
		{"after now", at("2026-10-06T12:00:01Z"), 0, true},
		{"2 days after now", at("2026-10-08T12:00:00Z"), 0, true},
		{"13 days 23:59:59", at("2026-09-22T12:00:01Z"), 13, true},
		{"exactly 14 days", at("2026-09-22T12:00:00Z"), 14, true},
		{"14 days and 1 second", at("2026-09-22T11:59:59Z"), 0, false},
		{"15 days", at("2026-09-21T12:00:00Z"), 0, false},
	}
	for _, tc := range cases {
		days, warn := workItemFamilyStoppedAgeDays(now, tc.last)
		if warn != tc.wantWarn || days != tc.wantDays {
			t.Errorf("%s: got (%d, %v), want (%d, %v)", tc.name, days, warn, tc.wantDays, tc.wantWarn)
		}
	}
	if workItemFamilyStoppedWarnWindow != 14*24*time.Hour {
		t.Errorf("window = %v, the documented rule is 14 days", workItemFamilyStoppedWarnWindow)
	}
}
