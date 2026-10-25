package syncadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"slices"
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

func (u *unitsReader) runUnitRollupRows(context.Context, string, uuid.UUID) ([]runUnit, error) {
	return u.units, u.hit("runUnits")
}

func (u *unitsReader) kept(filter unitFilter) []runUnit {
	var out []runUnit
	for _, unit := range u.units {
		if len(filter.Statuses) > 0 && !slices.Contains(filter.Statuses, unit.Status) {
			continue
		}
		if filter.DatasetKey != nil && unit.DatasetKey != *filter.DatasetKey {
			continue
		}
		if filter.SourceID != nil && unit.SourceID != *filter.SourceID {
			continue
		}
		if filter.ErrorCategory != nil {
			var stored map[string]any
			if unit.Result == nil || json.Unmarshal([]byte(*unit.Result), &stored) != nil || stored["error_category"] != *filter.ErrorCategory {
				continue
			}
		}
		if filter.Since != nil && unit.BeforeAt != nil && unit.BeforeAt.Before(*filter.Since) {
			continue
		}
		if filter.Before != nil && unit.SinceAt != nil && unit.SinceAt.After(*filter.Before) {
			continue
		}
		out = append(out, unit)
	}
	return out
}

func (u *unitsReader) runUnitsPage(_ context.Context, _ string, _ uuid.UUID, filter unitFilter, offset int64, limit *int64) ([]runUnit, error) {
	kept := u.kept(filter)
	if offset > int64(len(kept)) {
		offset = int64(len(kept))
	}
	kept = kept[offset:]
	if limit != nil && *limit < int64(len(kept)) {
		kept = kept[:*limit]
	}
	return kept, u.hit("runUnitsPage")
}

func (u *unitsReader) countRunUnits(_ context.Context, _ string, _ uuid.UUID, filter unitFilter) (int64, error) {
	return int64(len(u.kept(filter))), u.hit("countRunUnits")
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

// unsetForTest removes key for the rest of the test and restores it after.
func unsetForTest(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
}

// A unit that lost the shared request-budget reservation reports those waits in
// budget_deferrals, added to the dispatch guard's own episode counter.
func TestRunUnitsBudgetDeferralsIncludeBudgetContentionWaits(t *testing.T) {
	unsetForTest(t, "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS")
	unsetForTest(t, "SYNC_WATERMARK_OVERLAP")
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	str := func(text string) *string { return &text }
	source := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	cases := []struct {
		name   string
		result string
		column int64
		want   string
	}{
		{"contention waits and a guard episode add", `{"provider_budget_contention_deferrals": 7}`, 2, `"budget_deferrals":9,`},
		{"contention waits alone", `{"provider_budget_contention_deferrals": 5}`, 0, `"budget_deferrals":5,`},
		{"no contention key keeps the column", `{"error_category": "budget_deferred"}`, 3, `"budget_deferrals":3,`},
		{"a non-integer value counts as none", `{"provider_budget_contention_deferrals": "many"}`, 1, `"budget_deferrals":1,`},
		{"a negative value counts as none", `{"provider_budget_contention_deferrals": -4}`, 1, `"budget_deferrals":1,`},
		{"a numeric string counts as none", `{"provider_budget_contention_deferrals": "7"}`, 1, `"budget_deferrals":1,`},
		{"a boolean counts as none", `{"provider_budget_contention_deferrals": true}`, 1, `"budget_deferrals":1,`},
		{"an integral float counts as none", `{"provider_budget_contention_deferrals": 7.0}`, 1, `"budget_deferrals":1,`},
		{"a null counts as none", `{"provider_budget_contention_deferrals": null}`, 1, `"budget_deferrals":1,`},
		{"the largest int64 adds without wrapping", `{"provider_budget_contention_deferrals": 9223372036854775807}`, 1, `"budget_deferrals":9223372036854775808,`},
		{"a count beyond int64 is kept exactly", `{"provider_budget_contention_deferrals": 18446744073709551616}`, 0, `"budget_deferrals":18446744073709551616,`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			unit := runUnit{ID: uuid.New(), SyncRunID: faultRunID, SourceID: source, OrgID: "org", Provider: "github",
				DatasetKey: "commits", CostClass: "medium", Mode: "incremental", Status: "dispatching",
				BudgetDeferrals: tc.column, Result: str(tc.result), CreatedAt: now, UpdatedAt: now, HasSource: true,
				SourceName: str("a"), SourceFullName: str("a"), SourceExternalID: str("a")}
			_, status, body := serveUnits(t, &unitsReader{faultReader: &faultReader{}, units: []runUnit{unit}}, "", now)
			if status != http.StatusOK || !strings.Contains(body, tc.want) {
				t.Fatalf("status %d, want %s in:\n%s", status, tc.want, body)
			}
		})
	}
}

// CHAOS-8801: "catching up" follows the planner's window ratchet, which covers
// the work-items family of every provider, while the reported cost_class
// stays the registry's.
func TestRunUnitsCatchingUpFollowsTheWorkItemWindowRatchet(t *testing.T) {
	unsetForTest(t, "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS")
	unsetForTest(t, "SYNC_WATERMARK_OVERLAP")
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	name, full, external := "repo", "org/repo", "org/repo"
	at := now.Add(-30 * 24 * time.Hour)
	for provider, wantCatchingUp := range map[string]bool{"github": true, "linear": true, "gitlab": true, "jira": true} {
		unit := runUnit{ID: uuid.New(), SyncRunID: faultRunID, SourceID: uuid.New(), OrgID: "org", Provider: provider,
			DatasetKey: "work-items", CostClass: "medium", Mode: "incremental", Status: "success",
			CreatedAt: now, UpdatedAt: now, HasSource: true, SourceName: &name, SourceFullName: &full, SourceExternalID: &external}
		reader := &unitsReader{faultReader: &faultReader{}, units: []runUnit{unit},
			watermarks: []schedsync.WatermarkRow{{SourceID: "org/repo", DatasetKey: "work-items", RepoID: "r", Target: "t", LastSyncedAt: &at}}}
		_, status, body := serveUnits(t, reader, "", now)
		if status != http.StatusOK {
			t.Fatalf("%s: %d %s", provider, status, body)
		}
		want := `"dataset_key":"work-items","cost_class":"medium"`
		if !strings.Contains(body, want) {
			t.Errorf("%s: reported cost class changed:\n%s", provider, body)
		}
		if got := strings.Contains(body, `"catching_up":true`); got != wantCatchingUp {
			t.Errorf("%s: catching_up = %v, want %v:\n%s", provider, got, wantCatchingUp, body)
		}
	}
}

// TestRunUnitsPagedQuery pins the paged read's parameters: any of offset,
// status, dataset_key, source_id, since or before pages the filtered units
// (limit default 100, at most 1000) and adds filtered_unit_count; a bad value
// is a 422 and reads nothing; without them the whole-run answer is unchanged.
func TestRunUnitsPagedQuery(t *testing.T) {
	unsetForTest(t, "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS")
	unsetForTest(t, "SYNC_WATERMARK_OVERLAP")
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	name := "repo"
	source := uuid.New()
	var units []runUnit
	for i, status := range []string{"success", "failed", "retrying", "failed", "success"} {
		units = append(units, runUnit{ID: uuid.New(), SyncRunID: faultRunID, SourceID: source, OrgID: "org", Provider: "github",
			DatasetKey: []string{"commits", "files"}[i%2], CostClass: "medium", Mode: "incremental", Status: status,
			CreatedAt: now, UpdatedAt: now, HasSource: true, SourceName: &name, SourceFullName: &name, SourceExternalID: &name})
	}
	reader := func() *unitsReader { return &unitsReader{faultReader: &faultReader{}, units: units} }

	for _, query := range []string{"?limit=1001&offset=0", "?offset=-1", "?offset=x", "?source_id=x", "?since=yesterday",
		"?before=2026-13-01&offset=0", "?status=", "?status=failed,,failed", "?dataset_key=&offset=0", "?limit=-1&offset=0",
		"?status=" + strings.Repeat("a", 129), "?status=failed%3BDROP", "?error_category=auth&offset=0", "?error_category=", "?status=bogus&offset=0", "?offset=1000001", "?offset=99999999999999999999",
		"?status=failed,failed,failed,failed,failed,failed,failed,failed,failed,failed,failed,failed,failed"} {
		r := reader()
		if _, status, body := serveUnits(t, r, query, now); status != http.StatusUnprocessableEntity || r.calls["runUnits"] != 0 {
			t.Errorf("%s: %d %s, %d unit reads, want 422 and none", query, status, body, r.calls["runUnits"])
		}
	}

	r := reader()
	_, status, body := serveUnits(t, r, "?status=failed,retrying&offset=1&limit=2", now)
	if status != http.StatusOK || !strings.Contains(body, `"filtered_unit_count":3`) || !strings.Contains(body, `"unit_count":5`) ||
		strings.Count(body, `"sync_run_id"`) != 2 {
		t.Errorf("paged: %d %s", status, body)
	}
	if !strings.Contains(body, `"by_status":{"success":2,"failed":2,"retrying":1}`) {
		t.Errorf("a filter narrowed the rollups: %s", body)
	}
	_, status, body = serveUnits(t, reader(), "?offset=0", now)
	if status != http.StatusOK || strings.Count(body, `"sync_run_id"`) != 5 || !strings.Contains(body, `"filtered_unit_count":5`) {
		t.Errorf("default page: %d %s", status, body)
	}
	_, status, body = serveUnits(t, reader(), "?dataset_key=files&source_id="+source.String()+"&since=2026-01-01&before=2026-12-31T00:00:00Z", now)
	if status != http.StatusOK || strings.Count(body, `"sync_run_id"`) != 2 || !strings.Contains(body, `"filtered_unit_count":2`) {
		t.Errorf("dataset and source filter: %d %s", status, body)
	}
	at := func(day int) *time.Time { d := time.Date(2026, 5, day, 0, 0, 0, 0, time.UTC); return &d }
	windowed := reader()
	windowed.units = append([]runUnit(nil), units...)
	// inside, ends before the window, starts after it, no bounds, spans it
	for i, bounds := range [][2]*time.Time{{at(10), at(20)}, {at(1), at(5)}, {at(25), at(28)}, {nil, nil}, {at(1), at(31)}} {
		windowed.units[i].SinceAt, windowed.units[i].BeforeAt = bounds[0], bounds[1]
	}
	_, status, body = serveUnits(t, windowed, "?since=2026-05-12&before=2026-05-20", now)
	if status != http.StatusOK || !strings.Contains(body, `"filtered_unit_count":3`) {
		t.Errorf("date window: %d %s", status, body)
	}
	_, status, body = serveUnits(t, reader(), "?limit=3", now)
	if status != http.StatusOK || strings.Contains(body, "filtered_unit_count") || strings.Count(body, `"sync_run_id"`) != 3 {
		t.Errorf("whole-run limit slice changed: %d %s", status, body)
	}
	for _, step := range []string{"countRunUnits", "runUnitsPage"} {
		_, status, body := serveUnits(t, &unitsReader{faultReader: &faultReader{fail: step}, units: units}, "?offset=0", now)
		if status != http.StatusInternalServerError || body != `{"detail":"Internal Server Error"}` {
			t.Errorf("failing %s: %d %s", step, status, body)
		}
	}
}

// TestRunUnitsFilterBindsEveryValue pins that every filter value travels as a
// bound SQL parameter: the clause holds only placeholders, the org scope
// terms come first and always, and a hostile value never reaches the text.
func TestRunUnitsFilterBindsEveryValue(t *testing.T) {
	hostile := "x'; DROP TABLE sync_run_units; --"
	source := uuid.New()
	since, before := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	clause, args := unitFilter{Statuses: []string{"failed", "retrying"}, DatasetKey: &hostile, SourceID: &source,
		ErrorCategory: &hostile, Since: &since, Before: &before}.where("org", faultRunID)
	if !strings.HasPrefix(clause, "u.sync_run_id = $1 AND u.org_id = $2") {
		t.Errorf("the org scope is not the first terms: %s", clause)
	}
	if strings.Contains(clause, hostile) || strings.Contains(clause, "DROP") || strings.Contains(clause, "failed") || strings.Contains(clause, source.String()) {
		t.Errorf("a value reached the SQL text: %s", clause)
	}
	if len(args) != 8 || args[0] != faultRunID || args[1] != "org" {
		t.Errorf("args %v, want run, org and 6 filter values", args)
	}
	if bare, bareArgs := (unitFilter{}).where("org", faultRunID); bare != "u.sync_run_id = $1 AND u.org_id = $2" || len(bareArgs) != 2 {
		t.Errorf("an empty filter changed the old scope: %s %v", bare, bareArgs)
	}
}

// TestRunUnitsPagedExtras pins the whole-run views a paged read carries: the
// label of every source (full name, else name, none without a source row) and
// the min/max window of all units and of the successful ones. They never
// follow a filter, and a read without paging does not carry them.
func TestRunUnitsPagedExtras(t *testing.T) {
	unsetForTest(t, "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS")
	unsetForTest(t, "SYNC_WATERMARK_OVERLAP")
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	day := func(d int) *time.Time { t := time.Date(2026, 5, d, 0, 0, 0, 0, time.UTC); return &t }
	full, shortName := "org/a", "b"
	srcA, srcB, srcGone := uuid.New(), uuid.New(), uuid.New()
	base := runUnit{SyncRunID: faultRunID, OrgID: "org", Provider: "github", DatasetKey: "commits", CostClass: "medium",
		Mode: "incremental", CreatedAt: now, UpdatedAt: now}
	mk := func(status string, source uuid.UUID, has bool, since, before *time.Time) runUnit {
		unit := base
		unit.ID, unit.Status, unit.SourceID, unit.HasSource, unit.SinceAt, unit.BeforeAt = uuid.New(), status, source, has, since, before
		if source == srcA {
			unit.SourceFullName, unit.SourceName = &full, &shortName
		} else if source == srcB {
			unit.SourceName = &shortName
		}
		return unit
	}
	units := []runUnit{
		mk("failed", srcA, true, day(1), day(30)),
		mk("success", srcB, true, day(10), day(20)),
		mk("success", srcGone, false, day(12), day(25)),
		mk("success", srcA, true, nil, nil),
	}
	_, status, body := serveUnits(t, &unitsReader{faultReader: &faultReader{}, units: units}, "?offset=0&limit=1&status=success", now)
	if status != http.StatusOK {
		t.Fatalf("paged: %d %s", status, body)
	}
	for _, want := range []string{
		`"source_names":{"` + srcA.String() + `":"org/a","` + srcB.String() + `":"b"}`,
		`"requested_window":{"since_at":"2026-05-01T00:00:00Z","before_at":"2026-05-30T00:00:00Z"}`,
		`"covered_window":{"since_at":"2026-05-10T00:00:00Z","before_at":"2026-05-25T00:00:00Z"}`,
		`"filtered_unit_count":3`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("paged body lacks %s", want)
		}
	}
	if names := body[strings.Index(body, `"source_names":`):]; strings.Contains(names[:strings.Index(names, "}")], srcGone.String()) {
		t.Errorf("a source without a source row got a name: %s", body)
	}
	_, status, body = serveUnits(t, &unitsReader{faultReader: &faultReader{}, units: units[:1]}, "?offset=0", now)
	if status != http.StatusOK || !strings.Contains(body, `"covered_window":{"since_at":null,"before_at":null}`) {
		t.Errorf("no successful unit: %d %s", status, body)
	}
	_, status, body = serveUnits(t, &unitsReader{faultReader: &faultReader{}, units: units}, "", now)
	if status != http.StatusOK || strings.Contains(body, "source_names") || strings.Contains(body, "requested_window") || strings.Contains(body, "covered_window") {
		t.Errorf("a read without paging carries the paged views: %d", status)
	}
	waiting := `{"error_category":"provider_budget_contention"}`
	units[1].Result = &waiting
	_, status, body = serveUnits(t, &unitsReader{faultReader: &faultReader{}, units: units}, "?error_category=provider_budget_contention&offset=0", now)
	if status != http.StatusOK || !strings.Contains(body, `"filtered_unit_count":1`) {
		t.Errorf("error_category filter: %d", status)
	}
}

func (u *unitsReader) unitsSnapshot(_ context.Context, fn func(unitsSnapshotReads) error) error {
	return fn(u)
}
