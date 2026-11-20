package synccoverage

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

const testOccurrenceID = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func derivedTestRunID(t *testing.T, occurrence string) uuid.UUID {
	t.Helper()
	derived, err := schedulersync.DeterministicSyncRunID(occurrence)
	if err != nil {
		t.Fatal(err)
	}
	return uuid.MustParse(derived)
}

func TestBackfillRunIDResolvesOccurrenceMarker(t *testing.T) {
	task := "occurrence:" + testOccurrenceID
	runID, marker := backfillRunID(&task)
	if marker != backfillMarkerOccurrence || runID != derivedTestRunID(t, testOccurrenceID) {
		t.Fatalf("backfillRunID(%q) = (%s, %q), want the occurrence's deterministic run", task, runID, marker)
	}
}

func TestBackfillRunIDResolvesIdempotentOccurrenceMarker(t *testing.T) {
	task := "idem:" + strings.Repeat("a", 32) + ";occurrence:" + testOccurrenceID
	runID, marker := backfillRunID(&task)
	if marker != backfillMarkerOccurrence || runID != derivedTestRunID(t, testOccurrenceID) {
		t.Fatalf("backfillRunID(%q) = (%s, %q), want the occurrence's deterministic run", task, runID, marker)
	}
}

func TestBackfillRunIDKeepsSyncRunMarkersAndRefusesOtherText(t *testing.T) {
	runID := uuid.MustParse("6f1d2c3b-4a59-4e6f-8a7b-9c0d1e2f3a4b")
	for _, test := range []struct {
		name, task string
		want       uuid.UUID
		marker     string
	}{
		{"sync_run marker", "sync_run:" + runID.String(), runID, backfillMarkerSyncRun},
		{"sync_run after an occurrence", "occurrence:" + testOccurrenceID + ";sync_run:" + runID.String(), runID, backfillMarkerSyncRun},
		{"malformed sync_run", "sync_run:not-a-uuid", uuid.Nil, ""},
		{"empty", "", uuid.Nil, ""},
		{"celery task id", "4f6c1a2e-celery", uuid.Nil, ""},
		{"empty occurrence", "occurrence:", uuid.Nil, ""},
		{"empty idempotent occurrence", "idem:" + strings.Repeat("a", 32) + ";occurrence:;", uuid.Nil, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := test.task
			got, marker := backfillRunID(&task)
			if got != test.want || marker != test.marker {
				t.Fatalf("backfillRunID(%q) = (%s, %q), want (%s, %q)", task, got, marker, test.want, test.marker)
			}
		})
	}
	if got, marker := backfillRunID(nil); got != uuid.Nil || marker != "" {
		t.Fatalf("backfillRunID(nil) = (%s, %q), want none", got, marker)
	}
}

func TestBackfillResolutionMetricsWriteEveryOutcome(t *testing.T) {
	metrics := NewBackfillResolutionMetrics()
	metrics.observe(map[string]int{backfillOutcomeOccurrence: 2, backfillOutcomeOccurrenceUnplanned: 1, "bogus": 5})
	var out strings.Builder
	if err := metrics.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"# TYPE dev_health_sync_coverage_backfill_resolutions_total counter",
		`dev_health_sync_coverage_backfill_resolutions_total{outcome="occurrence"} 2`,
		`dev_health_sync_coverage_backfill_resolutions_total{outcome="occurrence_unplanned"} 1`,
		`dev_health_sync_coverage_backfill_resolutions_total{outcome="sync_run"} 0`,
		`dev_health_sync_coverage_backfill_resolutions_total{outcome="unresolved"} 0`,
	} {
		if !strings.Contains(out.String(), line+"\n") {
			t.Fatalf("metrics output lacks %q:\n%s", line, out.String())
		}
	}
	if strings.Contains(out.String(), "bogus") {
		t.Fatalf("an outcome outside the closed set reached the output:\n%s", out.String())
	}
}
