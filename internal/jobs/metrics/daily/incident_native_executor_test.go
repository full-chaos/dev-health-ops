package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewIncidentExecutorRejectsNilConn(t *testing.T) {
	if _, err := NewIncidentExecutor(nil); !errors.Is(err, errIncidentUnavailable) {
		t.Fatalf("err=%v, want errIncidentUnavailable", err)
	}
}

func TestIncidentComputeFamilyRejectsMissingOrganizationOrDay(t *testing.T) {
	executor, err := NewIncidentExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ComputeFamily(context.Background(), Run{}, Partition{ID: testPartitionID}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

func TestIncidentComputeFamilyRejectsUnparseablePartitionRepoIDs(t *testing.T) {
	executor, err := NewIncidentExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: testOrgID, TargetDay: mustParseDay(t, "2026-08-24")}
	partition := Partition{ID: testPartitionID, RepoIDs: []RepositoryID{"not-a-uuid"}}
	if _, err := executor.ComputeFamily(context.Background(), run, partition); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

func TestIncidentComputeFamilyNoOpsOnEmptyPartition(t *testing.T) {
	executor, err := NewIncidentExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: testOrgID, TargetDay: mustParseDay(t, "2026-08-24")}
	// No RepoIDs: must return (0, nil) without ever touching the stub
	// connection (which panics on any real call) -- MaterializeScheduledFanout
	// already terminalizes the no_repositories case upstream, same
	// precondition RepoUserCommitExecutor.ComputeFamily documents.
	written, err := executor.ComputeFamily(context.Background(), run, Partition{ID: testPartitionID})
	if err != nil {
		t.Fatal(err)
	}
	if written != 0 {
		t.Fatalf("written=%d, want 0", written)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }

// TestComputeIncidentMetricsDailyFiltersByResolvedDayNotStartedDay mirrors
// compute_incident_metrics_daily's own day filter (compute_incidents.py:54-59):
// only incidents RESOLVED within [day, day+1) count, regardless of when
// they started -- an incident that started on `day` but resolved the next
// day must be excluded, and one that started before `day` but resolved on
// it must be included.
func TestComputeIncidentMetricsDailyFiltersByResolvedDayNotStartedDay(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	repoID := uuid.MustParse("00000000-0000-0000-0000-0000000000a1")

	incidents := []IncidentRow{
		{ // started AND resolved on day: counted.
			RepoID: repoID, IncidentID: "in-day",
			StartedAt:  time.Date(2026, 8, 24, 8, 0, 0, 0, time.UTC),
			ResolvedAt: ptrTime(time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)),
		},
		{ // started on day, resolved the NEXT day: excluded.
			RepoID: repoID, IncidentID: "resolved-next-day",
			StartedAt:  time.Date(2026, 8, 24, 20, 0, 0, 0, time.UTC),
			ResolvedAt: ptrTime(time.Date(2026, 8, 25, 2, 0, 0, 0, time.UTC)),
		},
		{ // started the day BEFORE, resolved on day: counted.
			RepoID: repoID, IncidentID: "started-prior-day",
			StartedAt:  time.Date(2026, 8, 23, 22, 0, 0, 0, time.UTC),
			ResolvedAt: ptrTime(time.Date(2026, 8, 24, 4, 0, 0, 0, time.UTC)),
		},
		{ // never resolved: excluded (compute_incidents.py:55: `if not
			// isinstance(resolved_at, datetime): continue`).
			RepoID: repoID, IncidentID: "still-open",
			StartedAt:  time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC),
			ResolvedAt: nil,
		},
	}

	records := computeIncidentMetricsDaily(day, incidents)
	if len(records) != 1 {
		t.Fatalf("expected 1 repo record, got %d: %#v", len(records), records)
	}
	if records[0].IncidentsCount != 2 {
		t.Fatalf("incidents_count = %d, want 2 (in-day + started-prior-day)", records[0].IncidentsCount)
	}
}

// TestComputeIncidentMetricsDailyExcludesNegativeMTTR mirrors
// compute_incidents.py:71 (`if mttr >= 0: bucket["mttr_hours"].append(...)`):
// a malformed row where resolved_at precedes started_at still counts toward
// incidents_count but must never contribute a negative MTTR sample.
func TestComputeIncidentMetricsDailyExcludesNegativeMTTR(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	repoID := uuid.MustParse("00000000-0000-0000-0000-0000000000a1")
	incidents := []IncidentRow{
		{
			RepoID: repoID, IncidentID: "backwards",
			StartedAt:  time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
			ResolvedAt: ptrTime(time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)), // resolved BEFORE started
		},
	}
	records := computeIncidentMetricsDaily(day, incidents)
	if len(records) != 1 || records[0].IncidentsCount != 1 {
		t.Fatalf("expected 1 record with incidents_count=1, got %#v", records)
	}
	if records[0].MTTRP50Hours != nil || records[0].MTTRP90Hours != nil {
		t.Fatalf("mttr percentiles must be nil for an all-negative-MTTR bucket, got p50=%v p90=%v", records[0].MTTRP50Hours, records[0].MTTRP90Hours)
	}
}

// TestComputeIncidentMetricsDailyBucketsByRepoAndSortsOutput mirrors
// compute_incidents.py:75 (`sorted(by_repo.items(), key=lambda kv: kv[0])`).
func TestComputeIncidentMetricsDailyBucketsByRepoAndSortsOutput(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	repoB := uuid.MustParse("00000000-0000-0000-0000-0000000000b2")
	repoA := uuid.MustParse("00000000-0000-0000-0000-0000000000a1")
	resolved := ptrTime(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC))
	started := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	incidents := []IncidentRow{
		{RepoID: repoB, IncidentID: "b1", StartedAt: started, ResolvedAt: resolved},
		{RepoID: repoA, IncidentID: "a1", StartedAt: started, ResolvedAt: resolved},
		{RepoID: repoA, IncidentID: "a2", StartedAt: started, ResolvedAt: resolved},
	}
	records := computeIncidentMetricsDaily(day, incidents)
	if len(records) != 2 {
		t.Fatalf("expected 2 repo records, got %d", len(records))
	}
	if records[0].RepoID != repoA || records[0].IncidentsCount != 2 {
		t.Fatalf("record[0] = %#v, want repoA with 2 incidents (sorted first)", records[0])
	}
	if records[1].RepoID != repoB || records[1].IncidentsCount != 1 {
		t.Fatalf("record[1] = %#v, want repoB with 1 incident", records[1])
	}
}

// TestIncidentPercentileMatchesPythonLinearInterpolation pins the exact
// values compute_incidents.py's module-level _percentile produces for a
// known input, the same style of pinned-values test repouser's percentile
// tests use for their own ported kernel.
func TestIncidentPercentileMatchesPythonLinearInterpolation(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5}
	cases := []struct {
		pct  float64
		want float64
	}{
		{0, 1},
		{50, 3},
		{90, 4.6},
		{100, 5},
	}
	for _, tc := range cases {
		got := incidentPercentile(values, tc.pct)
		if got != tc.want {
			t.Fatalf("incidentPercentile(%v, %v) = %v, want %v", values, tc.pct, got, tc.want)
		}
	}
}

func TestIncidentPercentileSingleValue(t *testing.T) {
	if got := incidentPercentile([]float64{7.5}, 50); got != 7.5 {
		t.Fatalf("incidentPercentile single value = %v, want 7.5", got)
	}
}

func TestIncidentPercentileEmpty(t *testing.T) {
	if got := incidentPercentile(nil, 50); got != 0 {
		t.Fatalf("incidentPercentile(nil) = %v, want 0", got)
	}
}
