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

func TestProveGenerationTriesEveryFamily(t *testing.T) {
	day := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	createdAt := time.Date(2026, 10, 12, 13, 5, 0, 0, time.UTC)
	syncID := "5a1f3d52-7a2f-4a53-9b0e-5d3c3a8b6b11"
	otherID := "6b2f4e63-8b3f-4b64-8c1f-6e4d4b9c7c22"
	sources := generationRepairSources{
		syncRuns: []repairSyncRun{{id: otherID, createdAt: createdAt.Add(-time.Hour)}, {id: syncID, createdAt: createdAt.Add(-time.Minute)}},
		dailyIDs: []string{otherID, syncID},
	}
	for _, tc := range []struct{ generation, family string }{
		{"post-sync:" + syncID, "post-sync"},
		{TouchedDrainGenerationPrefix + "n:" + syncID, "touched-drain"},
		{TouchedDrainGenerationPrefix + "e:" + otherID, "touched-drain"},
		{ManualDailyRunGeneration(repairTestOrg, "2026-10-12", nil), "manual-daily"},
		{ScheduledFanoutGenerationPrefix + "2026-10-12T01:00:00Z", "fixed-schedule"},
	} {
		run := newRun(repairTestOrg, day, tc.generation)
		got, family, ok := proveGeneration(repairTestOrg, "2026-10-12", createdAt.Add(0), uuid.MustParse(run.ID), sources)
		if tc.family == "fixed-schedule" {
			got, family, ok = proveGeneration(repairTestOrg, "2026-10-12", time.Date(2026, 10, 12, 1, 0, 2, 0, time.UTC), uuid.MustParse(run.ID), sources)
		}
		if !ok || got != tc.generation || family != tc.family {
			t.Errorf("%q: got %q %q %v, want family %q", tc.generation, got, family, ok, tc.family)
		}
	}
	for name, generation := range map[string]string{
		"manual with a repository list": ManualDailyRunGeneration(repairTestOrg, "2026-10-12", []RepositoryID{"r1"}),
		"post-sync of an unknown run":   "post-sync:7c3f5f74-9c4f-4c75-9d2f-7f5e5cad8d33",
	} {
		run := newRun(repairTestOrg, day, generation)
		if got, family, ok := proveGeneration(repairTestOrg, "2026-10-12", createdAt, uuid.MustParse(run.ID), sources); ok {
			t.Errorf("%s was proven as %q (%s)", name, got, family)
		}
	}
	// A sync run of any age before the daily run can be its source.
	old := generationRepairSources{syncRuns: []repairSyncRun{{id: syncID, createdAt: createdAt.Add(-400 * 24 * time.Hour)}}}
	oldRun := newRun(repairTestOrg, day, "post-sync:"+syncID)
	if got, family, ok := proveGeneration(repairTestOrg, "2026-10-12", createdAt, uuid.MustParse(oldRun.ID), old); !ok || family != "post-sync" || got != "post-sync:"+syncID {
		t.Errorf("a sync run 400 days older than the daily run was not proven: %q %q %v", got, family, ok)
	}
	// A sync run created after the daily run cannot be its source.
	late := generationRepairSources{syncRuns: []repairSyncRun{{id: syncID, createdAt: createdAt.Add(time.Hour)}}}
	run := newRun(repairTestOrg, day, "post-sync:"+syncID)
	if _, _, ok := proveGeneration(repairTestOrg, "2026-10-12", createdAt, uuid.MustParse(run.ID), late); ok {
		t.Error("a sync run created after the daily run was accepted")
	}
}
