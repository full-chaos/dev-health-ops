//go:build integration

package syncadmin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

type splitItem struct {
	SyncRun struct {
		SyncRunID      string `json:"sync_run_id"`
		RequestedRange struct {
			Since string `json:"since"`
		} `json:"requested_range"`
		Split *struct {
			Since         string `json:"since"`
			Before        string `json:"before"`
			Units         int64  `json:"unit_count"`
			MedianSeconds int64  `json:"median_window_seconds"`
			LongerUnits   int64  `json:"longer_window_unit_count"`
			Longer        []struct {
				SourceID   string `json:"source_id"`
				DatasetKey string `json:"dataset_key"`
				Since      string `json:"since"`
				Before     string `json:"before"`
			} `json:"longer_window_units"`
		} `json:"requested_range_split"`
	} `json:"sync_run"`
}

// TestJobHistorySplitsAStuckUnitFromTheRequestedRange: a run whose units ask
// for the last hour plus one stuck unit asking for 30 days. requested_range
// stays the union (the stuck unit's since); requested_range_split gives the
// last hour and lists the stuck unit apart. A run of equal backfill chunks
// has no longer-window unit, a window of exactly twice the median is not
// longer, and a run lists at most ten longer-window units, earliest first.
func TestJobHistorySplitsAStuckUnitFromTheRequestedRange(t *testing.T) {
	v := startCascadeVenue(t, false)
	one := v.seed("github", `["git"]`, nil)
	source := func() uuid.UUID {
		id := uuid.New()
		v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repo', $4, $4, $4, '{}'::json, true, now(), now())`, id, v.org, one.integration, "acme/"+id.String()[:8])
		return id
	}
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	created := 0
	newRun := func(mode string) uuid.UUID {
		created++
		run, jobRun := uuid.New(), uuid.New()
		v.exec(`INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
VALUES ($1, $2, $3, 'schedule', $4, 'running', 0, 0, 0, now())`, run, v.org, one.integration, mode)
		v.exec(`INSERT INTO job_runs (id, job_id, status, result, triggered_by, created_at) VALUES ($1, $2, 1, $3::json, 'schedule', $4)`,
			jobRun, one.job, fmt.Sprintf(`{"sync_run_id": %q}`, run.String()), at.Add(time.Duration(created)*time.Minute))
		return run
	}
	unit := func(run, source uuid.UUID, dataset string, since, before time.Time) {
		v.exec(`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode,
status, attempts, since_at, before_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, 'github', $6, 'medium', 'incremental', 'planned', 0, $7, $8, now(), now())`,
			uuid.New(), v.org, run, one.integration, source, dataset, since, before)
	}

	hourly := newRun("incremental")
	for index := range 5 {
		unit(hourly, source(), "commits", at.Add(-time.Hour-time.Duration(index)*time.Minute), at)
	}
	stuckSource := source()
	unit(hourly, stuckSource, "work-items", at.AddDate(0, 0, -30), at)

	chunks := newRun("backfill")
	chunkSource := source()
	for day := range 4 {
		unit(chunks, chunkSource, "commits", at.AddDate(0, 0, -day-1), at.AddDate(0, 0, -day))
	}

	edge := newRun("incremental")
	edgeSource := source()
	unit(edge, edgeSource, "commits", at.Add(-time.Hour), at)
	unit(edge, edgeSource, "prs", at.Add(-time.Hour), at)
	unit(edge, edgeSource, "issues", at.Add(-2*time.Hour), at)

	many := newRun("incremental")
	for range 13 {
		unit(many, source(), "commits", at.Add(-time.Hour), at)
	}
	for index := range 12 {
		unit(many, source(), "commits", at.AddDate(0, 0, -index-1), at)
	}

	status, text := v.callAsOrg(v.org, "/api/v1/admin/sync-configs/"+one.config.String()+"/jobs")
	if status != http.StatusOK {
		t.Fatalf("jobs: %d %s", status, text)
	}
	var body []splitItem
	if err := json.Unmarshal([]byte(text), &body); err != nil {
		t.Fatal(err)
	}
	byRun := map[string]int{}
	for index, item := range body {
		byRun[item.SyncRun.SyncRunID] = index
	}
	get := func(run uuid.UUID) int {
		index, ok := byRun[run.String()]
		if !ok {
			t.Fatalf("run %s not in the page: %s", run, text)
		}
		if body[index].SyncRun.Split == nil {
			t.Fatalf("run %s has no requested_range_split: %s", run, text)
		}
		return index
	}

	item := body[get(hourly)].SyncRun
	if item.RequestedRange.Since != "2026-10-02T12:00:00Z" {
		t.Errorf("hourly requested_range.since = %s, want the stuck unit's (union kept)", item.RequestedRange.Since)
	}
	split := item.Split
	if split.Since != "2026-11-01T10:56:00Z" || split.Before != "2026-11-01T12:00:00Z" || split.Units != 5 ||
		split.MedianSeconds != 3720 || split.LongerUnits != 1 || len(split.Longer) != 1 {
		t.Fatalf("hourly split = %+v", split)
	}
	if got := split.Longer[0]; got.SourceID != stuckSource.String() || got.DatasetKey != "work-items" ||
		got.Since != "2026-10-02T12:00:00Z" || got.Before != "2026-11-01T12:00:00Z" {
		t.Errorf("stuck unit = %+v", got)
	}

	split = body[get(chunks)].SyncRun.Split
	if split.Since != "2026-10-28T12:00:00Z" || split.Before != "2026-11-01T12:00:00Z" || split.Units != 4 ||
		split.LongerUnits != 0 || len(split.Longer) != 0 {
		t.Errorf("backfill chunks split = %+v", split)
	}

	split = body[get(edge)].SyncRun.Split
	if split.Units != 3 || split.LongerUnits != 0 || split.Since != "2026-11-01T10:00:00Z" {
		t.Errorf("twice-the-median split = %+v", split)
	}

	split = body[get(many)].SyncRun.Split
	if split.Units != 13 || split.LongerUnits != 12 || len(split.Longer) != longerWindowLimit ||
		split.Longer[0].Since != "2026-10-20T12:00:00Z" || split.Longer[9].Since != "2026-10-29T12:00:00Z" {
		t.Errorf("many split: units %d longer %d listed %d, first %+v", split.Units, split.LongerUnits, len(split.Longer), split.Longer)
	}

	// Another org reads none of these runs' units.
	other := uuid.NewString()
	rows, err := store{pool: v.pool}.unitWindowSplits(t.Context(), other, []uuid.UUID{hourly, chunks, edge, many}, longerWindowLimit)
	if err != nil || len(rows) != 0 {
		t.Errorf("other org rows = %+v, %v", rows, err)
	}
}
