package synccoverage

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMergeAndSubtractIntervalsMatchCoverageContract(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	merged := mergeIntervals([]coverageInterval{
		{Since: base, Before: base.Add(time.Hour)},
		{Since: base.Add(time.Hour).Add(time.Microsecond), Before: base.Add(3 * time.Hour)},
	})
	if len(merged) != 1 || !merged[0].Since.Equal(base) || !merged[0].Before.Equal(base.Add(3*time.Hour)) {
		t.Fatalf("merged intervals = %#v", merged)
	}

	gaps := subtractIntervals(
		[]coverageInterval{{Since: base, Before: base.Add(4 * time.Hour)}},
		[]coverageInterval{{Since: base.Add(time.Hour), Before: base.Add(3 * time.Hour)}},
	)
	if len(gaps) != 2 || !gaps[0].Before.Equal(base.Add(time.Hour)) ||
		!gaps[1].Since.Equal(base.Add(3*time.Hour)) {
		t.Fatalf("gaps = %#v", gaps)
	}
}

func TestEffectiveWorkItemFamilyExpansion(t *testing.T) {
	t.Parallel()
	flags := json.RawMessage(`{
        "family_dataset_work_items": true,
        "family_dataset_work_item_comments": true,
        "family_dataset_work_item_history": false
    }`)
	got := effectiveDatasetKeys("work-items", flags)
	want := []string{"work-items", "work-item-comments"}
	if len(got) != len(want) {
		t.Fatalf("effective dataset keys = %v", got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("effective dataset keys = %v, want %v", got, want)
		}
	}
	if got := effectiveDatasetKeys("commits", flags); len(got) != 1 || got[0] != "commits" {
		t.Fatalf("non-family effective keys = %v", got)
	}
}

func TestPayloadBackfillScheduleAndStatusSemantics(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	configID := uuid.New()
	integrationID := uuid.New()
	sourceID := uuid.New()
	payload, err := buildPayload(payloadInput{
		Config: syncConfig{ID: configID, OrgID: "org-acme", Provider: "linear", Active: true, IntegrationID: &integrationID},
		Scope:  effectiveScope{IntegrationID: &integrationID, Sources: []source{{ID: sourceID, Name: "team"}}, DatasetKeys: []string{"work-items"}},
		Backfills: []coverageInterval{{
			Since: now.AddDate(0, 0, -2), Before: day(now.AddDate(0, 0, -1)),
			SourceIDs: []string{sourceID.String()}, DatasetKeys: []string{"work-items"},
		}},
		ActivePairs: map[string]struct{}{}, HasSchedule: false, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	overall := payload["overall"].(map[string]any)
	if overall["health"] != "gaps" || overall["gap_count"] != 1 {
		t.Fatalf("overall = %#v", overall)
	}
	datasets := payload["datasets"].([]any)
	workItems := datasets[0].(map[string]any)
	if workItems["status"] != "gaps" || len(workItems["requested_ranges"].([]any)) != 1 ||
		len(workItems["gaps"].([]any)) != 1 {
		t.Fatalf("work-items = %#v", workItems)
	}
	backfillWindows := payload["backfill_windows"].([]any)
	if len(backfillWindows) != 1 {
		t.Fatalf("backfill windows = %#v", backfillWindows)
	}
	window := backfillWindows[0].(map[string]any)
	if window["since"] != "2026-08-10" || window["before"] != "2026-08-10" {
		t.Fatalf("canonical backfill window = %#v", window)
	}
}

func TestScheduleAwareStaleness(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	covered := now.Add(-7 * time.Hour)
	hourly := time.Hour
	if got := classifyStaleness(&covered, now, &hourly, false, true); got != "stale" {
		t.Fatalf("hourly stale classification = %q", got)
	}
	if got := classifyStaleness(&covered, now, &hourly, true, true); got != "paused" {
		t.Fatalf("paused classification = %q", got)
	}
	if got := classifyStaleness(&covered, now, &hourly, false, false); got != "not_scheduled" {
		t.Fatalf("unscheduled classification = %q", got)
	}
}

func TestStatusPrecedenceMatchesPythonProjection(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   statusParts
		want string
	}{
		{"failure wins", statusParts{FailedCount: 1, GapCount: 1, StaleStatus: "stale", HasData: true, Running: true}, "failed"},
		{"gap wins", statusParts{GapCount: 1, StaleStatus: "stale", HasData: true, Running: true}, "gaps"},
		{"no data", statusParts{StaleStatus: "paused"}, "insufficient_data"},
		{"paused", statusParts{StaleStatus: "paused", HasData: true}, "paused"},
		{"running before stale", statusParts{StaleStatus: "stale", HasData: true, Running: true}, "running"},
		{"stale", statusParts{StaleStatus: "stale", HasData: true}, "stale"},
		{"healthy", statusParts{StaleStatus: "healthy", HasData: true}, "healthy"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := statusFromParts(test.in); got != test.want {
				t.Fatalf("status = %q, want %q", got, test.want)
			}
		})
	}
}
