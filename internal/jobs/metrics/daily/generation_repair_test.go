package daily

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

const repairTestOrg = "00000000-0000-4000-8000-000000010072"

func TestProvenFanoutGenerationRecoversTheGenerationFromTheRunID(t *testing.T) {
	day := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	for name, occurrence := range map[string]time.Time{
		"on schedule":         time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC),
		"late by a minute":    time.Date(2026, 10, 12, 1, 1, 0, 0, time.UTC),
		"a day before":        time.Date(2026, 10, 11, 1, 0, 0, 0, time.UTC),
		"odd second":          time.Date(2026, 10, 12, 1, 0, 37, 0, time.UTC),
		"ahead of created_at": time.Date(2026, 10, 12, 1, 0, 3, 0, time.UTC),
	} {
		t.Run(name, func(t *testing.T) {
			want := ScheduledFanoutGenerationPrefix + occurrence.Format(time.RFC3339)
			run := newRun(repairTestOrg, day, want)
			createdAt := occurrence.Add(2 * time.Second)
			if name == "late by a minute" {
				createdAt = occurrence.Add(40 * time.Second)
			}
			got, ok := provenFanoutGeneration(repairTestOrg, "2026-10-12", createdAt, uuid.MustParse(run.ID))
			if !ok || got != want {
				t.Fatalf("provenFanoutGeneration = %q, %v, want %q", got, ok, want)
			}
		})
	}
}

func TestProvenFanoutGenerationRefusesARunCreatedUnderAnotherGeneration(t *testing.T) {
	day := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	createdAt := time.Date(2026, 10, 12, 1, 0, 2, 0, time.UTC)
	for _, generation := range []string{
		"post-sync:5a1f3d52-7a2f-4a53-9b0e-5d3c3a8b6b11",
		"manual-daily:abc",
		"fixed-schedule:daily_metrics_fanout:2026-10-12T01:00:00+00:00",
		ScheduledFanoutGenerationPrefix + "2026-10-09T01:00:00Z",
	} {
		run := newRun(repairTestOrg, day, generation)
		if got, ok := provenFanoutGeneration(repairTestOrg, "2026-10-12", createdAt, uuid.MustParse(run.ID)); ok {
			t.Errorf("generation %q was reported as proven fan-out %q", generation, got)
		}
	}
	// The same fan-out generation for another day is a different run id.
	other := newRun(repairTestOrg, day.AddDate(0, 0, 1), ScheduledFanoutGenerationPrefix+"2026-10-12T01:00:00Z")
	if _, ok := provenFanoutGeneration(repairTestOrg, "2026-10-12", createdAt, uuid.MustParse(other.ID)); ok {
		t.Error("a run id of another target day was proven")
	}
}
