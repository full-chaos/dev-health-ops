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

// slimRollupReader serves the rollup read the way the SQL does: only the
// error_category and retry_exhausted keys of each stored result.
type slimRollupReader struct{ *unitsReader }

func (s slimRollupReader) runUnitRollupRows(context.Context, string, uuid.UUID) ([]runUnit, error) {
	slim := make([]runUnit, len(s.units))
	for i, unit := range s.units {
		unit.Result, unit.Error = nil, nil
		slim[i] = unit
	}
	return slim, nil
}

// TestRunUnitsLargeRunScale drives the real getRunUnits handler over a run with
// 20k and 100k units. The paged read (offset, limit) of a 100k-unit run must
// answer in under 1 s with under 1 MiB, and report the filtered total; the
// whole-run read is not timed here (its path is unchanged).
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
			reader := slimRollupReader{&unitsReader{faultReader: &faultReader{}, units: units}}
			started := time.Now()
			_, status, body := serveUnits(t, reader, query, now)
			elapsed := time.Since(started)
			t.Logf("units=%d query=%q status=%d body_bytes=%d elapsed=%s", count, query, status, len(body), elapsed)
			if status != http.StatusOK {
				t.Fatalf("units=%d query=%q: status %d", count, query, status)
			}
			budget := time.Second
			if scaleRaceEnabled {
				budget = 15 * time.Second
			}
			if count == 100000 && elapsed > budget {
				t.Errorf("units=%d query=%q: paged read took %s, over %s", count, query, elapsed, budget)
			}
			if len(body) > 1<<20 {
				t.Errorf("units=%d query=%q: paged body %d bytes, over 1 MiB", count, query, len(body))
			}
			if !strings.Contains(body, `"filtered_unit_count":`) {
				t.Errorf("units=%d query=%q: no filtered_unit_count in the paged body", count, query)
			}
		}
	}
}
