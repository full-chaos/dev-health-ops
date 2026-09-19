package compoundingrisk

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

func fp(v float64) *float64 { return &v }

func mkRow(id string, score *float64, sev string) storedRow {
	return storedRow{scopeID: id, score: score, severity: sev, wChurn: .4, wComplexity: .3, wOwnership: .2, wReview: .1,
		thresholdElevated: .4, thresholdHigh: .7, computedAt: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
}

// The expected values were produced by running the Python
// _aggregate_repo_rows_to_team on the same rows.
func TestTeamPoints_MatchesPythonCells(t *testing.T) {
	r1, r2, r3, r4, r5, r6 := mkRow("r1", fp(0.1), "low"), mkRow("r2", fp(0.2), "low"), mkRow("r3", fp(0.3), "low"), mkRow("r4", fp(0.9), "high"), mkRow("r5", nil, "unknown"), mkRow("r6", fp(0.5), "elevated")
	r1.churnNorm, r1.busFactor = fp(0.1), fp(2.0)
	r2.churnNorm, r2.busFactor = fp(0.2), fp(3.0)
	r3.churnNorm = fp(0.3)
	r4.churnNorm, r4.wChurn, r4.thresholdElevated, r4.thresholdHigh = fp(0.6), .5, .5, .8
	r6.churnNorm = fp(0.7)
	rows := []storedRow{r1, r2, r3, r4, r5, r6}
	for _, id := range []string{"r7", "r8", "r9", "r10"} {
		v := map[string]float64{"r7": .1, "r8": .2, "r9": .3, "r10": .1}[id]
		rows = append(rows, mkRow(id, fp(v), "low"))
	}
	teamsOf := map[string][]string{"r1": {"tA"}, "r2": {"tA"}, "r3": {"tA"}, "r4": {"tB"}, "r5": {"tC"}, "r6": {"tB"},
		"r7": {"tD"}, "r8": {"tD"}, "r9": {"tD"}, "r10": {"tD"}}
	labels := map[string]string{"tA": "Alpha", "tB": "Beta"}
	got := teamPoints(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), rows, teamsOf, labels, nil, time.Unix(0, 0))
	type want struct {
		id, label  string
		score      *float64
		sev        model.CompoundingRiskSeverity
		churn, bus *float64
		wChurn     float64
		el, hi     float64
	}
	wants := []want{
		{"tB", "Beta", fp(0.7), model.CompoundingRiskSeverityElevated, fp(0.6499999999999999), nil, 0.5, 0.5, 0.8},
		{"tA", "Alpha", fp(0.19999999999999998), model.CompoundingRiskSeverityLow, fp(0.19999999999999998), fp(2.5), 0.4, 0.4, 0.7},
		{"tD", "tD", fp(0.175), model.CompoundingRiskSeverityLow, nil, nil, 0.4, 0.4, 0.7},
		{"tC", "tC", nil, model.CompoundingRiskSeverityUnknown, nil, nil, 0.4, 0.4, 0.7},
	}
	if len(got) != len(wants) {
		t.Fatalf("got %d points", len(got))
	}
	for i, w := range wants {
		g := got[i]
		if g.ScopeID != w.id || g.ScopeLabel != w.label || !reflect.DeepEqual(g.Score, w.score) || g.Severity != w.sev ||
			!reflect.DeepEqual(g.Components.ChurnNorm, w.churn) || !reflect.DeepEqual(g.Components.BusFactor, w.bus) ||
			g.Weights.Churn != w.wChurn || g.Thresholds.Elevated != w.el || g.Thresholds.High != w.hi || g.ScopeEntity.ID != w.id || g.ScopeEntity.DisplayName != w.label {
			t.Errorf("point %d: %#v (score %v) want %#v", i, g, g.Score, w)
		}
	}
	filtered := teamPoints(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), rows, teamsOf, nil, []string{"tB", "tZ"}, time.Unix(0, 0))
	if len(filtered) != 1 || filtered[0].ScopeID != "tB" {
		t.Errorf("team filter: %#v", filtered)
	}
}

func TestSeverityFrom_Table(t *testing.T) {
	cases := map[string]model.CompoundingRiskSeverity{"low": model.CompoundingRiskSeverityLow, "LOW": model.CompoundingRiskSeverityLow,
		"elevated": model.CompoundingRiskSeverityElevated, "high": model.CompoundingRiskSeverityHigh,
		"unknown": model.CompoundingRiskSeverityUnknown, "": model.CompoundingRiskSeverityUnknown, "bogus": model.CompoundingRiskSeverityUnknown}
	for in, want := range cases {
		if got := severityFrom(in); got != want {
			t.Errorf("severityFrom(%q) = %s", in, got)
		}
	}
}

type recordClient struct{ statements []string }

type noRows struct{}

func (noRows) Next() bool        { return false }
func (noRows) Err() error        { return nil }
func (noRows) Close() error      { return nil }
func (noRows) Scan(...any) error { return nil }

func (c *recordClient) Query(_ context.Context, st string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, st)
	return noRows{}, nil
}

func TestResolve_EmptyWindowShape(t *testing.T) {
	c := &recordClient{}
	got, err := Resolve(context.Background(), c, "o", nil, time.Date(2026, 3, 5, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got.OrgID != "o" || got.Breakout != model.CompoundingRiskScopeRepo || got.Rows == nil || got.Trend == nil || len(got.Rows) != 0 || got.GeneratedAt.IsZero() {
		t.Fatalf("%#v", got)
	}
	if len(c.statements) != 1 || !strings.Contains(c.statements[0], "org_id = {org_id:String}") {
		t.Fatalf("statements %v", c.statements)
	}
}

func TestResolve_EmptyFiltersAreNotWildcards(t *testing.T) {
	c := &recordClient{}
	f := &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeRepo, RepoIds: []string{}, TrendDays: 30}
	if _, err := Resolve(context.Background(), c, "o", f, time.Now()); err != nil {
		t.Fatal(err)
	}
	if len(c.statements) != 0 {
		t.Fatalf("an empty repo filter excludes everything and must not query: %v", c.statements)
	}
}

// Severity buckets of a derived team point are ">=" against the first row's
// thresholds; the expected buckets were produced by the Python
// _aggregate_repo_rows_to_team on the same rows.
func TestTeamPoints_SeverityBoundaryMatchesPython(t *testing.T) {
	cases := []struct {
		name  string
		score float64
		want  model.CompoundingRiskSeverity
	}{
		{"exactly high", 0.7, model.CompoundingRiskSeverityHigh},
		{"exactly elevated", 0.4, model.CompoundingRiskSeverityElevated},
		{"below elevated", 0.39, model.CompoundingRiskSeverityLow},
	}
	for _, c := range cases {
		rows := []storedRow{mkRow("a", fp(c.score), "low"), mkRow("b", fp(c.score), "low")}
		got := teamPoints(time.Unix(0, 0), rows, map[string][]string{"a": {"t"}, "b": {"t"}}, nil, nil, time.Unix(0, 0))
		if len(got) != 1 || got[0].Severity != c.want || *got[0].Score != c.score {
			t.Errorf("%s: %#v", c.name, got)
		}
	}
}

type valueRows struct {
	rows [][]any
	i    int
}

func (s *valueRows) Next() bool   { s.i++; return s.i <= len(s.rows) }
func (s *valueRows) Err() error   { return nil }
func (s *valueRows) Close() error { return nil }
func (s *valueRows) Scan(d ...any) error {
	for i, v := range s.rows[s.i-1] {
		switch p := d[i].(type) {
		case *string:
			*p = v.(string)
		}
	}
	return nil
}

type bindingClient struct {
	rows       [][]any
	statements []string
	bindings   [][]clickhouse.Binding
}

func (c *bindingClient) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, st)
	c.bindings = append(c.bindings, b)
	return &valueRows{rows: c.rows}, nil
}

func bindingOf(b []clickhouse.Binding, name string) any {
	for _, x := range b {
		if x.Name == name {
			return x.Value
		}
	}
	return nil
}

func TestRepoLabels_EmptyNameReadsAsID(t *testing.T) {
	c := &bindingClient{rows: [][]any{{"r1", "acme/web"}, {"r2", ""}}}
	got, err := repoLabels(context.Background(), c, "o", []string{"r1", "r2"})
	if err != nil {
		t.Fatal(err)
	}
	if got["r1"] != "acme/web" || got["r2"] != "r2" {
		t.Fatalf("%#v", got)
	}
}

// The trend window is clamped to 1..365 days ending today, and the first
// statement's start day shows it.
func TestResolve_TrendWindowClamp(t *testing.T) {
	now := time.Date(2026, 3, 10, 15, 0, 0, 0, time.UTC)
	cases := []struct {
		days      int
		wantStart string
	}{
		{0, "2026-03-10"}, {-4, "2026-03-10"}, {1, "2026-03-10"}, {30, "2026-02-09"},
		{365, "2025-03-11"}, {366, "2025-03-11"}, {1000, "2025-03-11"},
	}
	for _, c := range cases {
		cl := &bindingClient{}
		f := &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeRepo, TrendDays: c.days}
		if _, err := Resolve(context.Background(), cl, "o", f, now); err != nil {
			t.Fatal(err)
		}
		if got := bindingOf(cl.bindings[0], "start_day"); got != c.wantStart || bindingOf(cl.bindings[0], "end_day") != "2026-03-10" {
			t.Errorf("trendDays %d: start=%v", c.days, got)
		}
	}
}

// A filter carries at most maxRows ids to the store.
func TestIDList_BoundedAtMaxRows(t *testing.T) {
	long := make([]string, 501)
	for i := range long {
		long[i] = "r"
	}
	if got := idList(long).bounded(); len(got) != 500 {
		t.Fatalf("bounded to %d", len(got))
	}
	if got := idList(long[:500]).bounded(); len(got) != 500 {
		t.Fatalf("exactly maxRows must stay whole, got %d", len(got))
	}
	cl := &bindingClient{}
	f := &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeRepo, RepoIds: long, TrendDays: 30}
	if _, err := Resolve(context.Background(), cl, "o", f, time.Now()); err != nil {
		t.Fatal(err)
	}
	if ids, _ := bindingOf(cl.bindings[0], "repo_ids").([]string); len(ids) != 500 {
		t.Fatalf("the statement carried %d ids", len(ids))
	}
}
