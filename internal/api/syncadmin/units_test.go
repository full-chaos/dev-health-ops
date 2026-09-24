package syncadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// unitsReader is faultReader with chosen units and watermark rows.
type unitsReader struct {
	*faultReader
	units        []runUnit
	watermarks   []schedsync.WatermarkRow
	askedSources []string
	askedLookups []string
}

func (u *unitsReader) runUnits(context.Context, string, uuid.UUID) ([]runUnit, error) {
	return u.units, u.hit("runUnits")
}

func (u *unitsReader) watermarkRows(_ context.Context, _ string, sources, lookups []string) ([]schedsync.WatermarkRow, error) {
	u.askedSources, u.askedLookups = sources, lookups
	return u.watermarks, u.hit("watermarkRows")
}

func serveUnits(t *testing.T, reader reader, query string, now time.Time) (*bytes.Buffer, int, string) {
	t.Helper()
	var logs bytes.Buffer
	h := &handlers{store: reader, features: faultFeatures{}, logger: slog.New(slog.NewTextHandler(&logs, nil)),
		clock: func() time.Time { return now }}
	recorder := serveAs(t, h, h.getRunUnits, "/x"+query, map[string]string{"run_id": faultRunID.String()})
	return &logs, recorder.Code, recorder.Body.String()
}

func TestPySliceEnd(t *testing.T) {
	for _, tc := range []struct {
		limit *big.Int
		n     int
		want  int
	}{
		{nil, 3, 3}, {big.NewInt(0), 3, 0}, {big.NewInt(2), 3, 2}, {big.NewInt(3), 3, 3}, {big.NewInt(9), 3, 3},
		{big.NewInt(-1), 3, 2}, {big.NewInt(-3), 3, 0}, {big.NewInt(-4), 3, 0}, {big.NewInt(-1), 0, 0},
		{new(big.Int).Lsh(big.NewInt(1), 80), 3, 3}, {new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 80)), 3, 0},
	} {
		if got := pySliceEnd(tc.limit, tc.n); got != tc.want {
			t.Errorf("seq[:%v] of %d = %d, want %d", tc.limit, tc.n, got, tc.want)
		}
	}
}

// TestRunUnitsAnswers pins get_sync_run_units' steps: the limit query is
// validated before the run lookup, an unknown run is 404, every store
// failure is a logged 500 at its step, no watermark read happens without
// an incremental dataset of a unit with a source, and the read asks for
// the sources' external ids and the datasets' lookup values.
func TestRunUnitsAnswers(t *testing.T) {
	unsetForTest(t, "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS")
	unsetForTest(t, "SYNC_WATERMARK_OVERLAP")
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	name, full, external := "repo", "org/repo", "org/repo"
	base := runUnit{ID: uuid.New(), SyncRunID: faultRunID, SourceID: uuid.New(), OrgID: "org", Provider: "github",
		DatasetKey: "commits", CostClass: "medium", Mode: "incremental", Status: "success",
		CreatedAt: now, UpdatedAt: now, HasSource: true, SourceName: &name, SourceFullName: &full, SourceExternalID: &external}

	if _, status, body := serveUnits(t, &faultReader{fail: "syncRunByID"}, "?limit=x", now); status != http.StatusUnprocessableEntity ||
		!strings.Contains(body, `"loc":["query","limit"]`) {
		t.Errorf("bad limit: %d %s", status, body)
	}
	for _, step := range []struct{ method, logged string }{
		{"syncRunByID", "step=get_sync_run"}, {"runUnits", "step=list_units"}, {"watermarkRows", "step=dataset_freshness"},
	} {
		reader := &unitsReader{faultReader: &faultReader{fail: step.method}, units: []runUnit{base}}
		logs, status, body := serveUnits(t, reader, "", now)
		if status != http.StatusInternalServerError || body != `{"detail":"Internal Server Error"}` || !strings.Contains(logs.String(), step.logged) {
			t.Errorf("failing %s: %d %s %s", step.method, status, body, logs)
		}
	}

	listFlags := base
	listFlags.DatasetKey = "work-items"
	flags := `["x"]`
	listFlags.ProcessorFlags = &flags
	if logs, status, _ := serveUnits(t, &unitsReader{faultReader: &faultReader{}, units: []runUnit{listFlags}}, "", now); status != http.StatusInternalServerError ||
		!strings.Contains(logs.String(), "step=dataset_freshness") || !strings.Contains(logs.String(), "not a dict") {
		t.Errorf("list-shaped family flags: %d %s", status, logs)
	}

	noSource := base
	noSource.HasSource = false
	noWatermark := base
	noWatermark.DatasetKey = "repo-metadata"
	for _, units := range [][]runUnit{nil, {noSource}, {noWatermark}} {
		reader := &unitsReader{faultReader: &faultReader{fail: "watermarkRows"}, units: units}
		if _, status, body := serveUnits(t, reader, "", now); status != http.StatusOK || reader.calls["watermarkRows"] != 0 ||
			!strings.Contains(body, `"dataset_freshness":[]`) {
			t.Errorf("units %v: %d %s, %d watermark reads", units, status, body, reader.calls["watermarkRows"])
		}
	}

	heavy := base
	heavy.DatasetKey, heavy.CostClass = "files", "heavy"
	at := now.Add(-30 * 24 * time.Hour)
	reader := &unitsReader{faultReader: &faultReader{}, units: []runUnit{base, heavy},
		watermarks: []schedsync.WatermarkRow{{SourceID: "org/repo", DatasetKey: "files", RepoID: "r", Target: "t", LastSyncedAt: &at}}}
	_, status, body := serveUnits(t, reader, "?limit=-1", now)
	if status != http.StatusOK {
		t.Fatalf("rich: %d %s", status, body)
	}
	if strings.Join(reader.askedSources, ",") != "org/repo" || strings.Join(reader.askedLookups, ",") != "commits,files,git" {
		t.Errorf("watermark read asked sources %v lookups %v", reader.askedSources, reader.askedLookups)
	}
	for _, want := range []string{
		`"dataset_key":"files","cost_class":"heavy","watermark_at":"2026-05-02T12:00:00Z","lag_seconds":2592000,"catching_up":true,"ticks_behind":5,"window_cap_days":7`,
		`"dataset_key":"commits","cost_class":"medium","watermark_at":null,"lag_seconds":null,"catching_up":false,"ticks_behind":null`,
		`"catching_up_dataset_count":1,"dataset_freshness_scope":"run"`,
		`"unit_count":2`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("rich body lacks %s:\n%s", want, body)
		}
	}
	if strings.Count(body, `"sync_run_id"`) != 1 {
		t.Errorf("limit=-1 of 2 units listed %d units", strings.Count(body, `"sync_run_id"`))
	}
}

// TestRunUnitsCountsAndFreshnessRules pins the route's own counting and
// freshness rules: next_retry_at is the earliest available_at of a
// retrying unit only; retry-exhausted counts a literal true flag or the
// worker-lost category; budget-blocked needs retrying and budget_deferred;
// one freshness entry per (source, dataset), first unit wins; a source's
// label is its full name, else its name; entries sort by label, then
// source id.
func TestRunUnitsCountsAndFreshnessRules(t *testing.T) {
	unsetForTest(t, "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS")
	unsetForTest(t, "SYNC_WATERMARK_OVERLAP")
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	t0, t1, t2 := now.Add(time.Hour), now.Add(2*time.Hour), now.Add(3*time.Hour)
	str := func(text string) *string { return &text }
	idA, idB, idC := uuid.MustParse("00000000-0000-0000-0000-00000000000a"), uuid.MustParse("00000000-0000-0000-0000-00000000000b"), uuid.MustParse("00000000-0000-0000-0000-00000000000c")
	unit := func(source uuid.UUID, name, full, dataset, status string, available *time.Time, result string) runUnit {
		return runUnit{ID: uuid.New(), SyncRunID: faultRunID, SourceID: source, OrgID: "org", Provider: "github",
			DatasetKey: dataset, CostClass: "medium", Mode: "incremental", Status: status, AvailableAt: available,
			Result: str(result), CreatedAt: now, UpdatedAt: now, HasSource: true, SourceName: str(name),
			SourceFullName: str(full), SourceExternalID: str(name)}
	}
	units := []runUnit{
		unit(idB, "b", "", "commits", "failed", &t0, `{"retry_exhausted": "yes", "error_category": "budget_deferred"}`),
		unit(idB, "b", "", "commits", "retrying", &t2, `{"retry_exhausted": true}`),
		unit(idC, "a", "", "commits", "retrying", &t1, `{"error_category": "budget_deferred"}`),
		unit(idA, "x", "a", "commits", "success", nil, `{"error_category": "worker_lost_retry_exhausted"}`),
	}
	reader := &unitsReader{faultReader: &faultReader{}, units: units}
	_, status, body := serveUnits(t, reader, "", now)
	if status != http.StatusOK {
		t.Fatalf("%d %s", status, body)
	}
	var summary map[string]any
	if err := json.Unmarshal([]byte(body), &summary); err != nil {
		t.Fatal(err)
	}
	if summary["next_retry_at"] != "2026-06-01T14:00:00Z" {
		t.Errorf("summary next_retry_at = %v, want the earliest retrying unit's 2026-06-01T14:00:00Z", summary["next_retry_at"])
	}
	for _, want := range []string{
		`"retry_exhausted_unit_count":2,"budget_blocked_unit_count":1,`,
		`"dataset_freshness":[{"source_id":"00000000-0000-0000-0000-00000000000a","source_name":"a",` +
			`"dataset_key":"commits","cost_class":"medium","watermark_at":null,"lag_seconds":null,"catching_up":false,` +
			`"ticks_behind":null,"window_cap_days":7},{"source_id":"00000000-0000-0000-0000-00000000000c","source_name":"a",` +
			`"dataset_key":"commits","cost_class":"medium","watermark_at":null,"lag_seconds":null,"catching_up":false,` +
			`"ticks_behind":null,"window_cap_days":7},{"source_id":"00000000-0000-0000-0000-00000000000b","source_name":"b",`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body lacks %s:\n%s", want, body)
		}
	}
	if got := strings.Count(body, `"source_name":"b","dataset_key"`); got != 1 {
		t.Errorf("source b has %d freshness entries, want 1 (first unit wins)", got)
	}
}
