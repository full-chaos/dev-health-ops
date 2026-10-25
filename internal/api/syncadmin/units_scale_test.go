package syncadmin

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// slimRollupReader serves the rollup read the way the SQL does (only the
// error_category and retry_exhausted keys of each stored result) and counts
// what the handler asks for.
type slimRollupReader struct {
	*unitsReader
	rollupReads  int
	pageReads    int
	pageRows     int
	unboundedGet bool
	largestLimit int64
}

func (s *slimRollupReader) runUnitRollupRows(context.Context, string, uuid.UUID) ([]runUnit, error) {
	s.rollupReads++
	slim := make([]runUnit, len(s.units))
	for i, unit := range s.units {
		unit.Result, unit.Error = nil, nil
		slim[i] = unit
	}
	return slim, nil
}

func (s *slimRollupReader) runUnitsPage(ctx context.Context, org string, run uuid.UUID, filter unitFilter, offset int64, limit *int64) ([]runUnit, error) {
	s.pageReads++
	if limit == nil {
		s.unboundedGet = true
	} else if *limit > s.largestLimit {
		s.largestLimit = *limit
	}
	rows, err := s.unitsReader.runUnitsPage(ctx, org, run, filter, offset, limit)
	s.pageRows += len(rows)
	return rows, err
}

// TestRunUnitsLargeRunScale drives the real getRunUnits handler over a run with
// 20k and 100k units and pins the cost of a paged read by what it fetches, not
// by wall time: one slim rollup scan, one page read bounded by the limit, no
// read of every full row, and a body under 1 MiB. Elapsed time is logged only.
func TestRunUnitsLargeRunScale(t *testing.T) {
	unsetForTest(t, "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS")
	unsetForTest(t, "SYNC_WATERMARK_OVERLAP")
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	datasets := []string{"commits", "files", "work-items", "repo-metadata", "pull-requests", "deployments", "incidents", "reviews", "tags", "branches"}
	statuses := []string{"success", "success", "success", "failed", "retrying", "pending"}
	result := `{"error_category":"budget_deferred","detail":"` + strings.Repeat("x", 600) + `"}`
	for _, count := range []int{20000, 100000} {
		sources := make([]uuid.UUID, 200)
		for i := range sources {
			sources[i] = uuid.New()
		}
		units := make([]runUnit, count)
		for i := range units {
			name := fmt.Sprintf("repo-%d", i%len(sources))
			full := "org/" + name
			res := result
			units[i] = runUnit{ID: uuid.New(), SyncRunID: faultRunID, SourceID: sources[i%len(sources)], OrgID: "org",
				Provider: "github", DatasetKey: datasets[i%len(datasets)], CostClass: "medium", Mode: "incremental",
				Status: statuses[i%len(statuses)], CreatedAt: now, UpdatedAt: now, HasSource: true,
				SourceName: &name, SourceFullName: &full, SourceExternalID: &full, Result: &res}
		}
		for _, query := range []string{"?offset=0&limit=500", "?status=failed,retrying&offset=500&limit=500&dataset_key=commits"} {
			reader := &slimRollupReader{unitsReader: &unitsReader{faultReader: &faultReader{}, units: units}}
			started := time.Now()
			_, status, body := serveUnits(t, reader, query, now)
			t.Logf("units=%d query=%q status=%d body_bytes=%d elapsed=%s", count, query, status, len(body), time.Since(started))
			if status != http.StatusOK {
				t.Fatalf("units=%d query=%q: status %d", count, query, status)
			}
			if len(body) > 1<<20 {
				t.Errorf("units=%d query=%q: paged body %d bytes, over 1 MiB", count, query, len(body))
			}
			if reader.rollupReads != 1 || reader.pageReads != 1 || reader.unboundedGet || reader.largestLimit != 500 {
				t.Errorf("units=%d query=%q: %d rollup scans, %d page reads, unbounded=%v, largest limit %d; want 1, 1, false, 500",
					count, query, reader.rollupReads, reader.pageReads, reader.unboundedGet, reader.largestLimit)
			}
			if listed := strings.Count(body, `"sync_run_id"`); listed != reader.pageRows || listed > 500 {
				t.Errorf("units=%d query=%q: %d units in the body, %d full rows fetched, want equal and at most 500", count, query, listed, reader.pageRows)
			}
			if query == "?offset=0&limit=500" && reader.pageRows != 500 {
				t.Errorf("units=%d: %d full rows fetched for a limit of 500", count, reader.pageRows)
			}
			if !strings.Contains(body, `"filtered_unit_count":`) {
				t.Errorf("units=%d query=%q: no filtered_unit_count in the paged body", count, query)
			}
		}
	}
}
