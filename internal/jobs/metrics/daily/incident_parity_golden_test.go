package daily

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestComputeIncidentMetricsDailyMatchesFrozenPythonGolden is a
// FROZEN-golden test, not a live-dual-execution one: there is no Python
// compute for compute_incident_metrics_daily left in this repository, so
// nothing can execute a live oracle at test time.
//
// The golden fixture (testdata/incident_parity_golden.json) was captured by
// running the Python authority's own two-repo, four-incident test fixture
// against that revision's own Python source, offline, ONCE, via a
// throwaway script never committed -- the same capture discipline
// testops/parity_golden_test.go and testops_risk_parity_golden_test.go's
// own goldens describe. The captured values are exactly what that fixture
// input produces, not a value derived by intuition.
//
// This proves the incident family's FULL row output, field for field,
// against the Python authority, for both a fully-populated row
// (incidents_count with both MTTR percentiles present) and a sparse one (an
// incident counted but its only duration sample dropped for being
// negative, leaving both percentiles null) -- the percentile-kernel pin in
// TestIncidentPercentileMatchesPythonLinearInterpolation covers only the
// shared percentile function, not the family's row shape.
func TestComputeIncidentMetricsDailyMatchesFrozenPythonGolden(t *testing.T) {
	var golden incidentGoldenDocument
	data, err := os.ReadFile(filepath.Join("testdata", "incident_parity_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("decode incident_parity_golden.json: %v", err)
	}

	day := time.Date(2026, 2, 18, 0, 0, 0, 0, time.UTC)
	repoFull := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	repoSparse := uuid.MustParse("00000000-0000-4000-8000-000000000002")
	dt := func(hour, minute int) time.Time {
		return time.Date(2026, 2, 18, hour, minute, 0, 0, time.UTC)
	}

	incidents := []IncidentRow{
		{
			RepoID: repoFull, IncidentID: "i1", Status: "resolved",
			StartedAt: dt(8, 0), ResolvedAt: ptrTime(dt(12, 0)),
		},
		{
			RepoID: repoFull, IncidentID: "i2", Status: "resolved",
			StartedAt: dt(6, 0), ResolvedAt: ptrTime(dt(18, 0)),
		},
		{
			// resolved BEFORE started: counted toward incidents_count, its
			// MTTR sample dropped as negative -- the sparse_row case.
			RepoID: repoSparse, IncidentID: "i3", Status: "resolved",
			StartedAt: dt(11, 0), ResolvedAt: ptrTime(dt(10, 0)),
		},
		{
			// resolved the day before `day`: excluded entirely.
			RepoID: repoFull, IncidentID: "old", Status: "resolved",
			StartedAt:  time.Date(2026, 2, 17, 11, 0, 0, 0, time.UTC),
			ResolvedAt: ptrTime(time.Date(2026, 2, 17, 12, 0, 0, 0, time.UTC)),
		},
	}

	records := computeIncidentMetricsDaily(day, incidents)
	if len(records) != 2 {
		t.Fatalf("go produced %d rows, want 2: %#v", len(records), records)
	}

	byRepo := make(map[uuid.UUID]IncidentMetricsDailyRecord, len(records))
	for _, record := range records {
		byRepo[record.RepoID] = record
	}

	fullRow, ok := byRepo[repoFull]
	if !ok {
		t.Fatalf("no record for repoFull: %#v", records)
	}
	if got, want := toIncidentGoldenRow(fullRow), golden.FullRow; !reflect.DeepEqual(got, want) {
		t.Errorf("full_row mismatch:\n got  %+v\n want %+v", got, want)
	}

	sparseRow, ok := byRepo[repoSparse]
	if !ok {
		t.Fatalf("no record for repoSparse: %#v", records)
	}
	if got, want := toIncidentGoldenRow(sparseRow), golden.SparseRow; !reflect.DeepEqual(got, want) {
		t.Errorf("sparse_row mismatch:\n got  %+v\n want %+v", got, want)
	}
}

type incidentGoldenDocument struct {
	FullRow   incidentGoldenRow `json:"full_row"`
	SparseRow incidentGoldenRow `json:"sparse_row"`
}

type incidentGoldenRow struct {
	RepoID         string   `json:"repo_id"`
	IncidentsCount uint32   `json:"incidents_count"`
	MTTRP50Hours   *float64 `json:"mttr_p50_hours"`
	MTTRP90Hours   *float64 `json:"mttr_p90_hours"`
}

func toIncidentGoldenRow(record IncidentMetricsDailyRecord) incidentGoldenRow {
	return incidentGoldenRow{
		RepoID:         record.RepoID.String(),
		IncidentsCount: record.IncidentsCount,
		MTTRP50Hours:   record.MTTRP50Hours,
		MTTRP90Hours:   record.MTTRP90Hours,
	}
}
