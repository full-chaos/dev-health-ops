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

// CHAOS-4393: mergeIntervals unions SourceIDs across ANY time-adjacent
// interval, so fusing two different sources' gaps with it blends a real
// gap on one source onto a fully-covered sibling source. Mirrors the exact
// reported repro (org f0f0f0f0..., dataset "blame"): source A has an
// open gap (unit still RUNNING), source B is 100% covered for the same
// window with zero gap of its own.
func TestMergeIntervalsBySourceScopeKeepsSourceScopedGapsSeparate(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, time.August, 28, 10, 0, 0, 0, time.UTC)
	gapA := coverageInterval{Since: base, Before: base.Add(90 * time.Minute), SourceIDs: []string{"source-a"}}
	gapB := coverageInterval{Since: base.Add(89 * time.Minute), Before: base.Add(3 * time.Hour), SourceIDs: []string{"source-b"}}

	fused := mergeIntervals([]coverageInterval{gapA, gapB})
	if len(fused) != 1 {
		t.Fatalf("sanity check: plain mergeIntervals should fuse these two adjacent intervals, got %#v", fused)
	}

	scoped := mergeIntervalsBySourceScope([]coverageInterval{gapA, gapB})
	if len(scoped) != 2 {
		t.Fatalf("mergeIntervalsBySourceScope(gapA, gapB) = %#v, want 2 source-scoped intervals, not fused", scoped)
	}
	for _, interval := range scoped {
		if len(interval.SourceIDs) != 1 {
			t.Fatalf("interval %#v blends more than one source", interval)
		}
	}
}

// The inverse: two intervals with the IDENTICAL source scope still merge,
// exactly like plain mergeIntervals would.
func TestMergeIntervalsBySourceScopeStillMergesSameSourceScope(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, time.August, 28, 10, 0, 0, 0, time.UTC)
	got := mergeIntervalsBySourceScope([]coverageInterval{
		{Since: base, Before: base.Add(time.Hour), SourceIDs: []string{"source-a"}},
		{Since: base.Add(time.Hour).Add(time.Microsecond), Before: base.Add(3 * time.Hour), SourceIDs: []string{"source-a"}},
	})
	if len(got) != 1 || !got[0].Since.Equal(base) || !got[0].Before.Equal(base.Add(3*time.Hour)) {
		t.Fatalf("mergeIntervalsBySourceScope(same-source pair) = %#v", got)
	}
}

// buildDatasetCoverage-level repro of the live prod shape: two pairs on the
// SAME dataset key but different sources -- one with a gap, one fully
// covered -- must not fuse into one blended gap interval naming both
// sources.
func TestBuildDatasetCoverageKeepsPerSourceGapsSeparate(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, time.August, 28, 10, 0, 0, 0, time.UTC)
	pairs := []pairCoverage{
		{
			SourceID:   "source-a",
			DatasetKey: "blame",
			Requested:  []coverageInterval{{Since: base, Before: base.Add(90 * time.Minute), SourceIDs: []string{"source-a"}}},
			Gaps:       []coverageInterval{{Since: base, Before: base.Add(90 * time.Minute), SourceIDs: []string{"source-a"}}},
			Status:     "gaps",
		},
		// source-b's own gap is unrelated (a different, independently-closed
		// slice) but time-overlaps source-a's still-open gap. Plain
		// mergeIntervals fuses the two into one interval naming BOTH
		// sources -- the exact reported shape (org f0f0f0f0..., dataset
		// "blame": source aaaaaaaa has a real open gap, source bbbbbbbb is
		// 100% covered elsewhere but its own gap interval still overlaps).
		{
			SourceID:   "source-b",
			DatasetKey: "blame",
			Requested:  []coverageInterval{{Since: base, Before: base.Add(3 * time.Hour), SourceIDs: []string{"source-b"}}},
			Gaps:       []coverageInterval{{Since: base.Add(89 * time.Minute), Before: base.Add(3 * time.Hour), SourceIDs: []string{"source-b"}}},
			Status:     "gaps",
		},
	}

	result := buildDatasetCoverage([]string{"blame"}, pairs)
	if len(result) != 1 {
		t.Fatalf("buildDatasetCoverage returned %d datasets, want 1", len(result))
	}
	blame := result[0]
	if len(blame.Gaps) != 2 {
		t.Fatalf("blame.Gaps = %#v, want 2 source-scoped gaps, not fused into one", blame.Gaps)
	}
	for _, gap := range blame.Gaps {
		if len(gap.SourceIDs) != 1 {
			t.Fatalf("blame.Gaps entry %#v blends more than one source", gap)
		}
	}
}

func TestEffectiveWorkItemFamilyExpansion(t *testing.T) {
	t.Parallel()
	flags := json.RawMessage(`{
        "family_dataset_work_items": true,
        "family_dataset_work_item_comments": true,
        "family_dataset_work_item_history": false
    }`)
	got, _ := effectiveDatasetKeys("work-items", flags)
	want := []string{"work-items", "work-item-comments"}
	if len(got) != len(want) {
		t.Fatalf("effective dataset keys = %v", got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("effective dataset keys = %v, want %v", got, want)
		}
	}
	if got, _ := effectiveDatasetKeys("commits", flags); len(got) != 1 || got[0] != "commits" {
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

// TestClosedWithReasonDecodesOnlyClosedPairs pins the CHAOS-13988 decoder: only
// a non-empty incomplete list whose every observation is report_member
// artifact_expired, malformed or unsafe_doctype closes a window; everything
// else keeps the gap.
func TestClosedWithReasonDecodesOnlyClosedPairs(t *testing.T) {
	t.Parallel()
	text := func(value string) *string { return &value }
	cases := []struct {
		name       string
		incomplete *string
		want       []string
	}{
		{"missing key", nil, nil},
		{"json null", text(`null`), nil},
		{"empty list", text(`[]`), nil},
		{"not a list", text(`{"component":"report_member","cause":"malformed"}`), nil},
		{"undecodable element", text(`[{"component":1,"cause":"malformed"}]`), nil},
		{"null element", text(`[null]`), nil},
		{"expired only", text(`[{"component":"report_member","cause":"artifact_expired","count":4}]`), []string{"artifact_expired"}},
		{"malformed twice", text(`[{"component":"report_member","cause":"malformed","count":1},{"component":"report_member","cause":"malformed","count":2}]`), []string{"malformed"}},
		{"both sorted", text(`[{"component":"report_member","cause":"malformed"},{"component":"report_member","cause":"artifact_expired"}]`), []string{"artifact_expired", "malformed"}},
		{"closed cause on another component", text(`[{"component":"run_reports","cause":"malformed"}]`), nil},
		{"expired beside per_run_cap", text(`[{"component":"report_member","cause":"artifact_expired"},{"component":"run_reports","cause":"per_run_cap"}]`), nil},
		{"unknown cause", text(`[{"component":"report_member","cause":"something_new"}]`), nil},
		{"unreadable", text(`[{"component":"report_member","cause":"unreadable"}]`), nil},
		{"unsafe_doctype only", text(`[{"component":"report_member","cause":"unsafe_doctype"}]`), []string{"unsafe_doctype"}},
		{"all three sorted", text(`[{"component":"report_member","cause":"unsafe_doctype"},{"component":"report_member","cause":"malformed"},{"component":"report_member","cause":"artifact_expired"}]`), []string{"artifact_expired", "malformed", "unsafe_doctype"}},
		{"unsafe_doctype beside per_run_cap", text(`[{"component":"report_member","cause":"unsafe_doctype"},{"component":"run_reports","cause":"per_run_cap"}]`), nil},
	}
	for _, tc := range cases {
		got := closedWithReason(tc.incomplete)
		if len(got) != len(tc.want) {
			t.Fatalf("%s: closedWithReason = %#v, want %#v", tc.name, got, tc.want)
		}
		for index := range got {
			if got[index] != tc.want[index] {
				t.Fatalf("%s: closedWithReason = %#v, want %#v", tc.name, got, tc.want)
			}
		}
	}
}

// TestClosedRangesCompleteWinsAndKeepScope pins the closed range algebra: a
// complete window is subtracted, and ranges merge only within one source scope
// and one reason set.
func TestClosedRangesCompleteWinsAndKeepScope(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	expired := []string{"artifact_expired"}
	remaining := subtractClosedRanges([]closedRange{
		{Interval: coverageInterval{Since: base, Before: base.Add(2 * time.Hour)}, Reasons: expired},
		{Interval: coverageInterval{Since: base.Add(2 * time.Hour), Before: base.Add(4 * time.Hour)}, Reasons: expired},
	}, []coverageInterval{{Since: base.Add(3 * time.Hour), Before: base.Add(5 * time.Hour)}})
	if len(remaining) != 1 || !remaining[0].Interval.Since.Equal(base) || !remaining[0].Interval.Before.Equal(base.Add(3*time.Hour)) ||
		len(remaining[0].Reasons) != 1 || remaining[0].Reasons[0] != "artifact_expired" {
		t.Fatalf("remaining = %#v", remaining)
	}
	merged := mergeClosedRanges([]closedRange{
		{Interval: coverageInterval{Since: base, Before: base.Add(time.Hour), SourceIDs: []string{"a"}}, Reasons: expired},
		{Interval: coverageInterval{Since: base.Add(time.Hour), Before: base.Add(2 * time.Hour), SourceIDs: []string{"a"}}, Reasons: expired},
		{Interval: coverageInterval{Since: base.Add(time.Hour), Before: base.Add(2 * time.Hour), SourceIDs: []string{"b"}}, Reasons: expired},
		{Interval: coverageInterval{Since: base.Add(2 * time.Hour), Before: base.Add(3 * time.Hour), SourceIDs: []string{"a"}}, Reasons: []string{"malformed"}},
	})
	if len(merged) != 3 {
		t.Fatalf("merged = %#v, want 3 ranges (a expired, b expired, a malformed)", merged)
	}
	if got := merged[0]; !got.Interval.Before.Equal(base.Add(2*time.Hour)) || got.Interval.SourceIDs[0] != "a" || got.Reasons[0] != "artifact_expired" {
		t.Fatalf("first = %#v", got)
	}
	payload := closedRangePayloads(merged[:1])
	item := payload[0].(map[string]any)
	if item["since"] != "2026-08-01T00:00:00+00:00" || item["before"] != "2026-08-01T02:00:00+00:00" {
		t.Fatalf("payload = %#v", item)
	}
	if _, has := item["run_ids"]; has {
		t.Fatalf("payload = %#v, want no run_ids", item)
	}
}
