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
	// The gap starts at 12:00 and is advertised at exactly that instant. Two
	// earlier revisions got this wrong in opposite directions: the first
	// emitted {since: "2026-08-10", before: "2026-08-10"} -- a date-only,
	// half-open range covering nothing, manufactured by truncating a partial
	// day onto day boundaries -- and the second suppressed the window
	// entirely, which made the dialog unable to offer any suggestion at all,
	// since real gaps are almost never midnight-aligned (CHAOS-3915).
	//
	// Verbatim boundaries are submittable: the planner chunks on whole days
	// but keeps the requested instants at the outer edges.
	backfillWindows := payload["backfill_windows"].([]any)
	if len(backfillWindows) != 1 {
		t.Fatalf("partial-day range must offer one backfill window, got %#v", backfillWindows)
	}
	window := backfillWindows[0].(map[string]any)
	if window["since"] != "2026-08-10T12:00:00+00:00" {
		t.Fatalf("window since = %#v, want the gap's exact start", window["since"])
	}
	if window["before"] != "2026-08-11T00:00:00+00:00" {
		t.Fatalf("window before = %#v, want the gap's exact end", window["before"])
	}
}

func TestCanonicalBackfillWindowsAreExactAndScoped(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	integrationID := uuid.New()
	first, second := uuid.New(), uuid.New()
	payload, err := buildPayload(payloadInput{
		Config: syncConfig{
			ID: uuid.New(), OrgID: "org-acme", Provider: "github",
			Active: true, IntegrationID: &integrationID,
		},
		Scope: effectiveScope{
			IntegrationID: &integrationID,
			Sources:       []source{{ID: first, Name: "acme/api"}, {ID: second, Name: "acme/web"}},
			DatasetKeys:   []string{"commits", "prs"},
		},
		Backfills: []coverageInterval{
			{
				Since: day(now.AddDate(0, 0, -3)), Before: day(now.AddDate(0, 0, -2)),
				SourceIDs: []string{first.String()}, DatasetKeys: []string{"prs"},
			},
			{
				// Same days, different source and dataset: Python keys the scope
				// on (since, before, source_id, dataset_key) and never merges, so
				// this must stay its own window rather than widening the first.
				Since: day(now.AddDate(0, 0, -3)), Before: day(now.AddDate(0, 0, -2)),
				SourceIDs: []string{second.String()}, DatasetKeys: []string{"commits"},
			},
			{
				// Partial day on the `before` end -- advertised verbatim, and
				// neither truncated onto a day boundary nor suppressed.
				Since: day(now.AddDate(0, 0, -5)), Before: now.AddDate(0, 0, -4),
				SourceIDs: []string{first.String()}, DatasetKeys: []string{"prs"},
			},
		},
		ActivePairs: map[string]struct{}{}, HasSchedule: false, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	windows := payload["backfill_windows"].([]any)
	if len(windows) != 3 {
		t.Fatalf("backfill windows = %#v", windows)
	}
	// Sorted by (since, before, dataset_keys, source_ids): the partial-day
	// range sorts first on its earlier `since`, and "commits" precedes "prs"
	// on an identical day range.
	for index, want := range []map[string]any{
		{
			"since": "2026-08-07T00:00:00+00:00", "before": "2026-08-08T12:00:00+00:00",
			"dataset_keys": "prs", "source_ids": first.String(),
		},
		{
			"since": "2026-08-09T00:00:00+00:00", "before": "2026-08-10T00:00:00+00:00",
			"dataset_keys": "commits", "source_ids": second.String(),
		},
		{
			"since": "2026-08-09T00:00:00+00:00", "before": "2026-08-10T00:00:00+00:00",
			"dataset_keys": "prs", "source_ids": first.String(),
		},
	} {
		window := windows[index].(map[string]any)
		if window["since"] != want["since"] || window["before"] != want["before"] {
			t.Fatalf("window %d bounds = %#v, want %v..%v", index, window, want["since"], want["before"])
		}
		datasets := window["dataset_keys"].([]string)
		sources := window["source_ids"].([]string)
		if len(datasets) != 1 || datasets[0] != want["dataset_keys"] {
			t.Fatalf("window %d dataset_keys = %#v, want %v", index, datasets, want["dataset_keys"])
		}
		if len(sources) != 1 || sources[0] != want["source_ids"] {
			t.Fatalf("window %d source_ids = %#v, want %v", index, sources, want["source_ids"])
		}
		if reasons := window["reasons"].([]string); len(reasons) != 1 || reasons[0] != "gap" {
			t.Fatalf("window %d reasons = %#v", index, reasons)
		}
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
