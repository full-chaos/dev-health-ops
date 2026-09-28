package recommendations

// Oracle: every fixture row and every assertion below is translated
// directly from tests/api/graphql/test_recommendations_resolver.py's
// FIXTURE_ROWS and its test_resolve_recommendations_* functions -- the
// Python test file this port's row-mapping and window-math logic is
// checked against, not a hand-invented Go-only expectation.

import (
	"context"
	"errors"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// --- fake QueryClient/RowScanner -------------------------------------------

type fakeRow struct {
	teamID, orgID, ruleID                                 string
	fired                                                 bool
	severity, title, rationale, successCrit, evidenceJSON string
	windowStart, windowEnd, computedAt                    time.Time
}

type fakeRowScanner struct {
	rows []fakeRow
	i    int
	err  error
}

func (f *fakeRowScanner) Next() bool {
	if f.i >= len(f.rows) {
		return false
	}
	f.i++
	return true
}

func (f *fakeRowScanner) Scan(dest ...any) error {
	r := f.rows[f.i-1]
	vals := []any{&r.teamID, &r.orgID, &r.ruleID, &r.fired, &r.severity, &r.title,
		&r.rationale, &r.successCrit, &r.evidenceJSON, &r.windowStart, &r.windowEnd, &r.computedAt}
	if len(dest) != len(vals) {
		return errors.New("fakeRowScanner: dest arity mismatch")
	}
	for i, d := range dest {
		switch v := d.(type) {
		case *string:
			*v = *(vals[i].(*string))
		case *bool:
			*v = *(vals[i].(*bool))
		case *time.Time:
			*v = *(vals[i].(*time.Time))
		default:
			return errors.New("fakeRowScanner: unsupported dest type")
		}
	}
	return nil
}

func (f *fakeRowScanner) Err() error   { return f.err }
func (f *fakeRowScanner) Close() error { return nil }

type fakeClient struct {
	scanner *fakeRowScanner
	err     error
}

func (c *fakeClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.scanner, nil
}

// --- fixture rows, translated from FIXTURE_ROWS -----------------------------

var evidenceListJSON = `[` +
	`{"team_id":"team-alpha","metric_table":"work_item_metrics_daily","field":"wip_count","window_start":"2026-04-01","window_end":"2026-04-07","value":14.0},` +
	`{"team_id":"team-alpha","metric_table":"work_item_metrics_daily","field":"throughput","window_start":"2026-04-01","window_end":"2026-04-07","value":2.0}` +
	`]`

var computedAtFixture = time.Date(2026, 4, 8, 3, 0, 0, 0, time.UTC)

func saturationRow() fakeRow {
	return fakeRow{
		teamID: "team-alpha", orgID: "test-org", ruleID: "saturation", fired: true,
		severity:     "critical",
		title:        "Team is saturating. Reduce active work before adding scope.",
		rationale:    "WIP has been rising while throughput remains flat for 2 cycles.",
		successCrit:  "WIP trend turns negative or throughput trend turns positive in 2 cycles",
		evidenceJSON: evidenceListJSON,
		windowStart:  time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		windowEnd:    time.Date(2026, 4, 7, 0, 0, 0, 0, time.UTC),
		computedAt:   computedAtFixture,
	}
}

func thrashRow() fakeRow {
	return fakeRow{
		teamID: "team-alpha", orgID: "test-org", ruleID: "thrash", fired: true,
		severity:     "warning",
		title:        "Thrash likely. Inspect hotspots and rework loops.",
		rationale:    "High churn detected with low delivery ratio over the last 7 days.",
		successCrit:  "Churn drops OR throughput rises in 2 cycles",
		evidenceJSON: `[{"team_id":"team-alpha","metric_table":"work_item_metrics_daily","field":"wip_count","window_start":"2026-04-01","window_end":"2026-04-07","value":14.0}]`,
		windowStart:  time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		windowEnd:    time.Date(2026, 4, 7, 0, 0, 0, 0, time.UTC),
		computedAt:   computedAtFixture,
	}
}

func weekWindow() model.WindowInput { return model.WindowInput{Value: 1, Unit: model.WindowUnitWeek} }

// --- tests, one per Python test_resolve_recommendations_* function ---------

// Port of test_resolve_recommendations_returns_list.
func TestResolve_ReturnsList(t *testing.T) {
	client := &fakeClient{scanner: &fakeRowScanner{rows: []fakeRow{saturationRow(), thrashRow()}}}
	got := Resolve(context.Background(), client, "test-org", "team-alpha", weekWindow(), computedAtFixture)
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
}

// Port of test_resolve_recommendations_field_mapping.
func TestResolve_FieldMapping(t *testing.T) {
	client := &fakeClient{scanner: &fakeRowScanner{rows: []fakeRow{saturationRow()}}}
	got := Resolve(context.Background(), client, "test-org", "team-alpha", weekWindow(), computedAtFixture)
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1", len(got))
	}
	rec := got[0]
	if rec.RuleID != "saturation" {
		t.Errorf("RuleID = %q, want saturation", rec.RuleID)
	}
	if rec.TeamID != "team-alpha" {
		t.Errorf("TeamID = %q, want team-alpha", rec.TeamID)
	}
	if rec.OrgID != "test-org" {
		t.Errorf("OrgID = %q, want test-org", rec.OrgID)
	}
	if rec.Severity != model.SeverityCritical {
		t.Errorf("Severity = %q, want CRITICAL", rec.Severity)
	}
	if rec.Title != "Team is saturating. Reduce active work before adding scope." {
		t.Errorf("Title = %q, unexpected", rec.Title)
	}
	if rec.SuccessCriterion == "" || rec.SuccessCriterion[:9] != "WIP trend" {
		t.Errorf("SuccessCriterion = %q, want it to start with %q", rec.SuccessCriterion, "WIP trend")
	}
	if rec.ComputedAt.IsZero() {
		t.Error("ComputedAt must not be zero")
	}
}

// Port of test_resolve_recommendations_evidence_references_resolve.
func TestResolve_EvidenceReferencesResolve(t *testing.T) {
	client := &fakeClient{scanner: &fakeRowScanner{rows: []fakeRow{saturationRow()}}}
	got := Resolve(context.Background(), client, "test-org", "team-alpha", weekWindow(), computedAtFixture)
	rec := got[0]
	if len(rec.Evidence) != 2 {
		t.Fatalf("len(Evidence) = %d, want 2", len(rec.Evidence))
	}
	ev := rec.Evidence[0]
	if ev.MetricTable != "work_item_metrics_daily" {
		t.Errorf("MetricTable = %q, want work_item_metrics_daily", ev.MetricTable)
	}
	if ev.Field != "wip_count" {
		t.Errorf("Field = %q, want wip_count", ev.Field)
	}
	if ev.Value != 14.0 {
		t.Errorf("Value = %v, want 14.0", ev.Value)
	}
	if ev.TeamID != "team-alpha" {
		t.Errorf("TeamID = %q, want team-alpha", ev.TeamID)
	}
}

// Port of test_resolve_recommendations_empty_on_no_rows.
func TestResolve_EmptyOnNoRows(t *testing.T) {
	client := &fakeClient{scanner: &fakeRowScanner{rows: nil}}
	got := Resolve(context.Background(), client, "test-org", "team-beta", model.WindowInput{Value: 7, Unit: model.WindowUnitDay}, computedAtFixture)
	if len(got) != 0 {
		t.Errorf("len(got) = %d, want 0", len(got))
	}
}

// Port of test_resolve_recommendations_tolerates_db_error.
func TestResolve_ToleratesDBError(t *testing.T) {
	client := &fakeClient{err: errors.New("ClickHouse unavailable")}
	got := Resolve(context.Background(), client, "test-org", "team-alpha", model.WindowInput{Value: 4, Unit: model.WindowUnitWeek}, computedAtFixture)
	if got == nil || len(got) != 0 {
		t.Errorf("got = %#v, want an empty (non-nil) slice", got)
	}
}

// Port of test_resolve_recommendations_multiple_rules.
func TestResolve_MultipleRules(t *testing.T) {
	client := &fakeClient{scanner: &fakeRowScanner{rows: []fakeRow{saturationRow(), thrashRow()}}}
	got := Resolve(context.Background(), client, "test-org", "team-alpha", model.WindowInput{Value: 2, Unit: model.WindowUnitCycle}, computedAtFixture)
	ids := map[string]bool{}
	for _, r := range got {
		ids[r.RuleID] = true
	}
	if !ids["saturation"] || !ids["thrash"] {
		t.Errorf("rule ids = %v, want both saturation and thrash present", ids)
	}
}

// Port of test_resolve_recommendations_unknown_severity_falls_back.
func TestResolve_UnknownSeverityFallsBack(t *testing.T) {
	bad := saturationRow()
	bad.severity = "ultra-critical"
	client := &fakeClient{scanner: &fakeRowScanner{rows: []fakeRow{bad}}}
	got := Resolve(context.Background(), client, "test-org", "team-alpha", weekWindow(), computedAtFixture)
	if got[0].Severity != model.SeverityWarning {
		t.Errorf("Severity = %q, want WARNING fallback", got[0].Severity)
	}
}

// Port of test_window_to_dates_caps_read_at_today_plus_one.
func TestWindowToDates_CapsReadAtTodayPlusOne(t *testing.T) {
	today := time.Date(2026, 4, 8, 15, 30, 0, 0, time.UTC) // arbitrary time-of-day; must truncate

	wantToday := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

	start, end := windowToDates(today, model.WindowInput{Value: 2, Unit: model.WindowUnitWeek})
	if !end.Equal(wantToday.AddDate(0, 0, 1)) {
		t.Errorf("week: end = %v, want today+1 = %v", end, wantToday.AddDate(0, 0, 1))
	}
	if !start.Equal(wantToday.AddDate(0, 0, -14)) {
		t.Errorf("week: start = %v, want today-14d", start)
	}

	start, end = windowToDates(today, model.WindowInput{Value: 7, Unit: model.WindowUnitDay})
	if !end.Equal(wantToday.AddDate(0, 0, 1)) || !start.Equal(wantToday.AddDate(0, 0, -7)) {
		t.Errorf("day: got start=%v end=%v, want today-7d/today+1", start, end)
	}

	start, end = windowToDates(today, model.WindowInput{Value: 2, Unit: model.WindowUnitCycle})
	if !end.Equal(wantToday.AddDate(0, 0, 1)) || !start.Equal(wantToday.AddDate(0, 0, -28)) {
		t.Errorf("cycle: got start=%v end=%v, want today-28d/today+1", start, end)
	}
}

// A JSON null entry inside an otherwise-valid evidence array must be
// skipped, as Python's _parse_evidence skips every non-dict entry -- never
// surfaced as a zero-valued evidence row (json.Unmarshal of null into a
// struct succeeds with zero values).
func TestParseEvidence_NullEntryIsSkipped(t *testing.T) {
	got := parseEvidence(context.Background(), `[null,{"team_id":"team-alpha","metric_table":"work_item_metrics_daily","field":"wip_count","window_start":"2026-04-01","window_end":"2026-04-07","value":14.0}]`)
	if len(got) != 1 || got[0].Field != "wip_count" {
		t.Errorf("evidence = %+v, want exactly the one real entry", got)
	}
	if only := parseEvidence(context.Background(), `[null]`); len(only) != 0 {
		t.Errorf("[null] evidence = %+v, want none", only)
	}
}
