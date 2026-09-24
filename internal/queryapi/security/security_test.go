package security

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

type scriptedRows struct {
	rows [][]any
	i    int
}

func (s *scriptedRows) Next() bool   { s.i++; return s.i <= len(s.rows) }
func (s *scriptedRows) Err() error   { return nil }
func (s *scriptedRows) Close() error { return nil }
func (s *scriptedRows) Scan(dest ...any) error {
	row := s.rows[s.i-1]
	for i, d := range dest {
		rv := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			rv.Set(reflect.Zero(rv.Type()))
			continue
		}
		v := reflect.ValueOf(row[i])
		if rv.Kind() == reflect.Ptr && v.Kind() != reflect.Ptr {
			p := reflect.New(rv.Type().Elem())
			p.Elem().Set(v)
			rv.Set(p)
			continue
		}
		rv.Set(v)
	}
	return nil
}

type recordingClient struct {
	statements []string
	bindings   [][]clickhouse.Binding
	responses  [][][]any
}

func (c *recordingClient) Query(_ context.Context, statement string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, b)
	var rows [][]any
	if len(c.responses) > 0 {
		rows, c.responses = c.responses[0], c.responses[1:]
	}
	return &scriptedRows{rows: rows}, nil
}

func bindingMap(b []clickhouse.Binding) map[string]any {
	m := map[string]any{}
	for _, x := range b {
		m[x.Name] = x.Value
	}
	return m
}

func date(s string) *graphqldate.Date {
	d, err := graphqldate.Parse(s)
	if err != nil {
		panic(err)
	}
	return &d
}

func strp(s string) *string { return &s }

func TestBuildFilter_Enumeration(t *testing.T) {
	sev := []model.SecuritySeverityInput{model.SecuritySeverityInputCritical, model.SecuritySeverityInputUnknown}
	src := []model.SecuritySourceInput{model.SecuritySourceInputGitlabDependency}
	st := []model.SecurityStateInput{model.SecurityStateInputResolved}
	cases := []struct {
		name     string
		f        *model.SecurityAlertFilterInput
		contains []string
		absent   []string
		want     map[string]any
	}{
		{name: "nil", f: nil, absent: []string{"sa.state", "sa.severity", "sa.source", "sa.created_at", "ilike", "repo_id IN"}, want: map[string]any{"org_id": "o"}},
		{name: "empty lists and empty search apply nothing", f: &model.SecurityAlertFilterInput{RepoIds: []string{}, Severities: []model.SecuritySeverityInput{}, Sources: []model.SecuritySourceInput{}, States: []model.SecurityStateInput{}, Search: strp("")},
			absent: []string{"sa.state", "sa.severity", "sa.source", "ilike", "repo_id IN"}, want: map[string]any{"org_id": "o"}},
		{name: "states", f: &model.SecurityAlertFilterInput{States: st}, contains: []string{"sa.state IN {states"}, want: map[string]any{"org_id": "o", "states": []string{"resolved"}}},
		{name: "openOnly wins over states", f: &model.SecurityAlertFilterInput{OpenOnly: true, States: st}, contains: []string{"sa.state IN {open_states"}, absent: []string{"{states"}, want: map[string]any{"org_id": "o", "open_states": []string{"open", "detected", "confirmed"}}},
		{name: "severities sources repos", f: &model.SecurityAlertFilterInput{Severities: sev, Sources: src, RepoIds: []string{"r1"}},
			contains: []string{"sa.severity IN", "sa.source IN", "toString(sa.repo_id) IN"},
			want:     map[string]any{"org_id": "o", "severities": []string{"critical", "unknown"}, "sources": []string{"gitlab_dependency"}, "repo_ids": []string{"r1"}}},
		{name: "since until", f: &model.SecurityAlertFilterInput{Since: date("2026-03-05"), Until: date("2026-03-06")}, contains: []string{"sa.created_at >=", "sa.created_at <="},
			want: map[string]any{"org_id": "o", "since": time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC), "until": time.Date(2026, 3, 6, 23, 59, 59, 0, time.UTC)}},
		{name: "search", f: &model.SecurityAlertFilterInput{Search: strp("lodash_")}, contains: []string{"ilike(sa.title", "ilike(sa.package_name", "ilike(sa.cve_id"}, want: map[string]any{"org_id": "o", "search_pattern": "%lodash_%"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := buildFilter("o", c.f)
			if !strings.HasPrefix(got.sql, "WHERE r.org_id = {org_id:String} AND sa.org_id = {org_id:String}") {
				t.Fatalf("org predicate missing: %s", got.sql)
			}
			for _, s := range c.contains {
				if !strings.Contains(got.sql, s) {
					t.Errorf("missing %q in %s", s, got.sql)
				}
			}
			for _, s := range c.absent {
				if strings.Contains(got.sql, s) {
					t.Errorf("unexpected %q in %s", s, got.sql)
				}
			}
			if !reflect.DeepEqual(bindingMap(got.bindings), c.want) {
				t.Errorf("bindings = %#v, want %#v", bindingMap(got.bindings), c.want)
			}
		})
	}
}

func TestDecodeCursor_Table(t *testing.T) {
	cases := []struct {
		in   *string
		want int64
		err  bool
	}{
		{nil, 0, false}, {strp(""), 0, false}, {strp("0"), 0, false}, {strp("50"), 50, false},
		{strp(" 7 "), 7, false}, {strp("+7"), 7, false}, {strp("-3"), 0, false}, {strp("1_000"), 1000, false},
		{strp("1__0"), 0, false}, {strp("_1"), 0, false}, {strp("1_"), 0, false}, {strp("abc"), 0, false},
		{strp("1.5"), 0, false}, {strp("5x"), 0, false}, {strp("-"), 0, false},
		{strp("99999999999999999999"), 0, true},
		{strp("١٢"), 12, false}, {strp("१_०"), 10, false}, {strp("𝟏𝟐"), 12, false}, {strp("１２"), 12, false}, {strp("1٢"), 12, false},
	}
	for _, c := range cases {
		got, err := decodeCursor(c.in)
		if (err != nil) != c.err || got != c.want {
			in := "<nil>"
			if c.in != nil {
				in = *c.in
			}
			t.Errorf("decodeCursor(%q) = %d, %v; want %d err=%v", in, got, err, c.want, c.err)
		}
	}
}

func TestSliceTo_Table(t *testing.T) {
	rows := []int{1, 2, 3}
	cases := []struct {
		n    int64
		want []int
	}{{0, []int{}}, {1, []int{1}}, {3, rows}, {4, rows}, {-1, []int{1, 2}}, {-3, []int{}}, {-9, []int{}}}
	for _, c := range cases {
		if got := sliceTo(rows, c.n); !reflect.DeepEqual(got, c.want) {
			t.Errorf("sliceTo(%d) = %v want %v", c.n, got, c.want)
		}
	}
}

func alertRow(id string, sev string) []any {
	ts := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	return []any{"repo-1", id, "acme/web", "dependabot", sev, "open", "", "CVE-1", "", "t", "", ts, nil, nil}
}

func TestResolveAlerts_Pagination(t *testing.T) {
	c := &recordingClient{responses: [][][]any{{alertRow("A-1", "high"), alertRow("A-2", "low"), alertRow("A-3", "low")}}}
	got, err := ResolveAlerts(context.Background(), c, "o", nil, &model.SecurityPaginationInput{First: 2, After: strp("10")})
	if err != nil {
		t.Fatal(err)
	}
	b := bindingMap(c.bindings[0])
	if b["limit"] != int64(3) || b["offset"] != int64(10) || b["org_id"] != "o" {
		t.Fatalf("bindings %#v", b)
	}
	if len(got.Edges) != 2 || got.Edges[0].Cursor != "11" || got.Edges[1].Cursor != "12" {
		t.Fatalf("edges %#v", got.Edges)
	}
	if got.TotalCount != 12 || !got.PageInfo.HasNextPage || !got.PageInfo.HasPreviousPage || *got.PageInfo.StartCursor != "11" || *got.PageInfo.EndCursor != "12" {
		t.Fatalf("page %#v total=%d", got.PageInfo, got.TotalCount)
	}
	n := got.Edges[0].Node
	if n.PackageName != nil || n.URL != nil || n.Description != nil || n.CveID == nil || *n.CveID != "CVE-1" || n.RepoURL != nil {
		t.Fatalf("empty text must read absent: %#v", n)
	}
}

func TestResolveAlerts_EmptyAndDefaults(t *testing.T) {
	c := &recordingClient{}
	got, err := ResolveAlerts(context.Background(), c, "o", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if b := bindingMap(c.bindings[0]); b["limit"] != int64(51) || b["offset"] != int64(0) {
		t.Fatalf("bindings %#v", b)
	}
	if got.Edges == nil || len(got.Edges) != 0 || got.TotalCount != 0 || got.PageInfo.HasNextPage || got.PageInfo.HasPreviousPage || got.PageInfo.StartCursor != nil || got.PageInfo.EndCursor != nil {
		t.Fatalf("empty page %#v", got)
	}
}

func TestResolveAlerts_FirstZeroAndNegative(t *testing.T) {
	c := &recordingClient{responses: [][][]any{{alertRow("A-1", "high")}}}
	got, _ := ResolveAlerts(context.Background(), c, "o", nil, &model.SecurityPaginationInput{First: 0})
	if len(got.Edges) != 0 || !got.PageInfo.HasNextPage {
		t.Fatalf("first=0: %#v", got)
	}
	c = &recordingClient{responses: [][][]any{{}}}
	got, _ = ResolveAlerts(context.Background(), c, "o", nil, &model.SecurityPaginationInput{First: -1})
	if len(got.Edges) != 0 || !got.PageInfo.HasNextPage {
		t.Fatalf("first=-1 mirrors an empty page with hasNext: %#v", got)
	}
}

func TestResolveOverview_ParsesAndScopes(t *testing.T) {
	nan := func() float64 { z := 0.0; return z / z }()
	c := &recordingClient{responses: [][][]any{
		{{uint64(5), uint64(2), uint64(1), nan, int64(-3)}},
		{{"high", uint64(3)}, {"low", uint64(2)}},
		{{"repo-1", "acme/web", uint64(4)}},
		{{time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), uint64(2), uint64(1)}},
	}}
	got, err := ResolveOverview(context.Background(), c, "o", &model.SecurityAlertFilterInput{Search: strp("x")})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.statements) != 4 {
		t.Fatalf("statements %d", len(c.statements))
	}
	for i, s := range c.statements {
		if !strings.Contains(s, "r.org_id = {org_id:String} AND sa.org_id = {org_id:String}") || bindingMap(c.bindings[i])["org_id"] != "o" || bindingMap(c.bindings[i])["search_pattern"] != "%x%" {
			t.Errorf("statement %d not scoped/filtered: %s", i, s)
		}
	}
	if strings.Count(c.statements[3], "sa.org_id = {org_id:String}") != 2 {
		t.Error("trend must scope both legs")
	}
	if got.Kpis.OpenTotal != 5 || got.Kpis.Critical != 2 || got.Kpis.High != 1 || got.Kpis.MeanDaysToFix30d != nil || got.Kpis.OpenDelta30d != -3 {
		t.Fatalf("kpis %#v", got.Kpis)
	}
	if len(got.SeverityBreakdown) != 2 || got.SeverityBreakdown[0].Count != 3 || got.TopRepos[0].Count != 4 || got.TopRepos[0].RepoURL != nil {
		t.Fatalf("aggregates %#v", got)
	}
	if got.Trend[0].Day.String() != "2026-03-01" || got.Trend[0].Opened != 2 || got.Trend[0].Fixed != 1 {
		t.Fatalf("trend %#v", got.Trend)
	}
}

func TestResolveOverview_EmptyListsAreNonNil(t *testing.T) {
	c := &recordingClient{}
	got, err := ResolveOverview(context.Background(), c, "o", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.SeverityBreakdown == nil || got.TopRepos == nil || got.Trend == nil || got.Kpis == nil {
		t.Fatalf("%#v", got)
	}
}

func TestResolveAlerts_ExactlyFullLastPageHasNoNext(t *testing.T) {
	c := &recordingClient{responses: [][][]any{{alertRow("A-1", "high"), alertRow("A-2", "low")}}}
	got, err := ResolveAlerts(context.Background(), c, "o", nil, &model.SecurityPaginationInput{First: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Edges) != 2 || got.PageInfo.HasNextPage {
		t.Fatalf("a page holding exactly first rows and no extra row has no next page: %#v", got.PageInfo)
	}
}

func TestResolveOverview_TopReposAreCappedAtTen(t *testing.T) {
	c := &recordingClient{}
	if _, err := ResolveOverview(context.Background(), c, "o", nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(c.statements[2], "\nLIMIT 10") {
		t.Fatalf("top repos statement lacks the ten-row cap: %s", c.statements[2])
	}
}

func TestResolveOverview_NaNMeanIsAbsentAndNumberIsKept(t *testing.T) {
	nan := func() float64 { z := 0.0; return z / z }()
	for _, tc := range []struct {
		mean float64
		want bool
	}{{nan, false}, {2.5, true}} {
		c := &recordingClient{responses: [][][]any{{{uint64(1), uint64(0), uint64(0), tc.mean, int64(0)}}}}
		got, err := ResolveOverview(context.Background(), c, "o", nil)
		if err != nil {
			t.Fatal(err)
		}
		if (got.Kpis.MeanDaysToFix30d != nil) != tc.want {
			t.Errorf("mean %v: present=%v", tc.mean, got.Kpis.MeanDaysToFix30d != nil)
		}
	}
}
