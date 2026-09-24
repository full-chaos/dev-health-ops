package producttelemetry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5"
)

type call struct {
	sql      string
	bindings map[string]any
}

// scriptedCH answers each section query from a table keyed by a fragment of
// its statement and records every call.
type scriptedCH struct {
	rows  map[string][][]any
	fail  string
	calls []call
}

func (c *scriptedCH) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	b := map[string]any{}
	for _, x := range bindings {
		b[x.Name] = x.Value
	}
	c.calls = append(c.calls, call{statement, b})
	if c.fail != "" && strings.Contains(statement, c.fail) {
		return nil, errors.New("clickhouse down")
	}
	for frag, rows := range c.rows {
		if strings.Contains(statement, frag) {
			return &scriptedRows{rows: rows}, nil
		}
	}
	return &scriptedRows{}, nil
}

type scriptedRows struct {
	rows [][]any
	i    int
}

func (r *scriptedRows) Next() bool   { r.i++; return r.i <= len(r.rows) }
func (r *scriptedRows) Err() error   { return nil }
func (r *scriptedRows) Close() error { return nil }
func (r *scriptedRows) Scan(dest ...any) error {
	row := r.rows[r.i-1]
	if len(row) != len(dest) {
		return fmt.Errorf("arity %d != %d", len(row), len(dest))
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *time.Time:
			*p = row[i].(time.Time)
		case *uint64:
			*p = row[i].(uint64)
		case *string:
			*p = row[i].(string)
		case **string:
			if v, ok := row[i].(string); ok {
				*p = &v
			}
		case *float64:
			*p = row[i].(float64)
		default:
			return fmt.Errorf("unsupported %T", d)
		}
	}
	return nil
}

var day = time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

func rangeOf() Range {
	return Range{Start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)}
}

// OrgHash is SHA-256 hex of the org id (value from hashlib.sha256(b"org-1")).
func TestOrgHash(t *testing.T) {
	if got := OrgHash("org-1"); got != "c24787a75cb363e4bc33871cff23a407c2dc0db631898c35b4604a833bfab5f8" {
		t.Fatalf("hash %s", got)
	}
	if OrgHash("org-1") == OrgHash("org-2") || OrgHash("") == OrgHash("x") {
		t.Fatal("hash collides")
	}
}

func TestCheckRange(t *testing.T) {
	a, b := day, day.AddDate(0, 0, 1)
	for name, c := range map[string]struct {
		s, e time.Time
		ok   bool
	}{"start before end": {a, b, true}, "equal": {a, a, true}, "start after end": {b, a, false}} {
		if err := CheckRange(c.s, c.e); (err == nil) != c.ok {
			t.Errorf("%s: err=%v", name, err)
		}
	}
}

func TestOrgDashboard_ScopeBindingsAndMapping(t *testing.T) {
	ch := &scriptedCH{rows: map[string][][]any{
		"toDate(occurred_at)":       {{day, uint64(3)}},
		"name = 'page_viewed'":      {{"/home", uint64(9), uint64(4), uint64(2)}, {nil, uint64(1), uint64(1), uint64(1)}},
		"name = 'feature_viewed'":   {{"f", "s", uint64(5), uint64(2)}},
		"name = 'filter_changed'":   {{"v", "k", uint64(2), 1.5}, {"v2", "k2", uint64(1), math.NaN()}},
		"name = 'chart_interacted'": {{"c", "a", "s", uint64(6), uint64(3)}},
		"name = 'client_error'":     {{nil, "b", "E", uint64(2), uint64(1)}},
		"name = 'session_ended'":    {{1200.9, 1500.0, 2000.0, 3000.5, 2.5, 4.0}},
	}}
	got, err := (&Reader{ClickHouse: ch}).Org(context.Background(), "org-1", rangeOf())
	if err != nil {
		t.Fatal(err)
	}
	if len(ch.calls) != 7 {
		t.Fatalf("%d queries, want 7", len(ch.calls))
	}
	for _, c := range ch.calls {
		if c.bindings["org_id_hash"] != OrgHash("org-1") || c.bindings["start"] != "2026-01-01" || c.bindings["end"] != "2026-01-08" {
			t.Fatalf("bindings %v", c.bindings)
		}
		if !strings.Contains(c.sql, "org_id_hash = {org_id_hash:String}") || !strings.Contains(c.sql, "occurred_at >= {start:Date}") || !strings.Contains(c.sql, "occurred_at < {end:Date}") {
			t.Fatalf("scope predicate missing:\n%s", c.sql)
		}
	}
	if len(got.DailyActiveUsers) != 1 || got.DailyActiveUsers[0].ActiveAnonymousUsers != 3 {
		t.Fatalf("daily %+v", got.DailyActiveUsers)
	}
	if len(got.TopRoutes) != 2 || got.TopRoutes[0].RoutePattern != "/home" || got.TopRoutes[1].RoutePattern != "" {
		t.Fatalf("routes %+v", got.TopRoutes)
	}
	if got.ClientErrors[0].RoutePattern != "" || got.ClientErrors[0].Errors != 2 {
		t.Fatalf("errors %+v", got.ClientErrors)
	}
	if got.FilterChanges[0].AvgValueCount == nil || *got.FilterChanges[0].AvgValueCount != 1.5 || got.FilterChanges[1].AvgValueCount != nil {
		t.Fatalf("filters %+v", got.FilterChanges)
	}
	s := got.SessionSummary
	if *s.P50DurationMs != 1200 || *s.P95DurationMs != 3000 || *s.AvgPagesViewed != 2.5 || *s.AvgInteractions != 4.0 {
		t.Fatalf("summary %+v", s)
	}
}

func TestOrgDashboard_EmptyAnswers(t *testing.T) {
	got, err := (&Reader{ClickHouse: &scriptedCH{}}).Org(context.Background(), "org-1", rangeOf())
	if err != nil {
		t.Fatal(err)
	}
	if got.DailyActiveUsers == nil || got.TopRoutes == nil || got.FeatureViews == nil || got.FilterChanges == nil || got.ChartInteractions == nil || got.ClientErrors == nil {
		t.Fatalf("lists must be empty, not null: %+v", got)
	}
	if got.SessionSummary == nil || got.SessionSummary.P50DurationMs != nil || got.SessionSummary.AvgPagesViewed != nil {
		t.Fatalf("summary %+v", got.SessionSummary)
	}
}

func TestSessionSummary_NonFiniteAndTruncation(t *testing.T) {
	ch := &scriptedCH{rows: map[string][][]any{"name = 'session_ended'": {{math.NaN(), math.Inf(1), 7.99, -1.5, math.NaN(), math.Inf(-1)}}}}
	got, err := (&Reader{ClickHouse: ch}).Org(context.Background(), "o", rangeOf())
	if err != nil {
		t.Fatal(err)
	}
	s := got.SessionSummary
	if s.P50DurationMs != nil || s.P75DurationMs != nil || *s.P90DurationMs != 7 || *s.P95DurationMs != -1 || s.AvgPagesViewed != nil || s.AvgInteractions != nil {
		t.Fatalf("summary %+v", s)
	}
}

func TestOrgDashboard_AnyQueryFailureFailsTheRead(t *testing.T) {
	for _, frag := range []string{"toDate(", "page_viewed", "feature_viewed", "filter_changed", "chart_interacted", "client_error", "session_ended"} {
		_, err := (&Reader{ClickHouse: &scriptedCH{fail: frag}}).Org(context.Background(), "o", rangeOf())
		if err == nil {
			t.Errorf("%s: failure was swallowed", frag)
		}
	}
}

func TestPlatform_NoOrgPredicateAndAssembly(t *testing.T) {
	ch := &scriptedCH{rows: map[string][][]any{
		"uniqExact(org_id_hash)": {{uint64(2), uint64(5), uint64(6), uint64(7)}},
		"GROUP BY org_id_hash":   {{OrgHash("org-1"), uint64(9), uint64(3), uint64(2)}, {"unknownhash", uint64(1), uint64(1), uint64(1)}, {OrgHash("org-3"), uint64(1), uint64(1), uint64(1)}},
	}}
	p, err := (&Reader{ClickHouse: ch}).PlatformSections(context.Background(), rangeOf())
	if err != nil {
		t.Fatal(err)
	}
	if len(ch.calls) != 9 {
		t.Fatalf("%d queries, want 9", len(ch.calls))
	}
	for _, c := range ch.calls {
		if strings.Contains(c.sql, "org_id_hash = {") {
			t.Fatalf("platform query carries an org predicate:\n%s", c.sql)
		}
		if _, ok := c.bindings["org_id_hash"]; ok {
			t.Fatalf("platform query binds an org: %v", c.bindings)
		}
	}
	out := p.Assemble([]OrgName{{ID: "org-1", Slug: "one", Name: "One"}, {ID: "org-3", Slug: "", Name: ""}})
	if out.Totals.ActiveOrgs != 2 || out.Totals.Events != 7 || len(out.TopOrgs) != 3 {
		t.Fatalf("out %+v", out)
	}
	first, second, third := out.TopOrgs[0], out.TopOrgs[1], out.TopOrgs[2]
	if *first.OrgID != "org-1" || *first.OrgSlug != "one" || *first.OrgName != "One" {
		t.Fatalf("first %+v", first)
	}
	if second.OrgID != nil || second.OrgSlug != nil || second.OrgName != nil || second.OrgIDHash != "unknownhash" {
		t.Fatalf("unmatched hash keeps its names null: %+v", second)
	}
	if *third.OrgID != "org-3" || third.OrgSlug != nil || third.OrgName != nil {
		t.Fatalf("empty slug and name read as null: %+v", third)
	}
}

func TestPlatform_EmptyTotalsAreZero(t *testing.T) {
	p, err := (&Reader{ClickHouse: &scriptedCH{}}).PlatformSections(context.Background(), rangeOf())
	if err != nil {
		t.Fatal(err)
	}
	out := p.Assemble(nil)
	if out.Totals == nil || out.Totals.Events != 0 || out.TopOrgs == nil || len(out.TopOrgs) != 0 {
		t.Fatalf("out %+v", out)
	}
}

func TestRequirePlatformAdmin_DecisionTable(t *testing.T) {
	cases := []struct {
		name string
		p    Principal
		want string
	}{
		{"absent", Principal{}, "Authentication required"},
		{"present, not superuser", Principal{Present: true}, "Platform admin access required"},
		{"superuser", Principal{Present: true, IsSuperuser: true}, ""},
		{"superuser impersonating", Principal{Present: true, IsSuperuser: true, ImpersonationActive: true}, "Platform admin access required"},
		{"impersonating, not superuser", Principal{Present: true, ImpersonationActive: true}, "Platform admin access required"},
		{"absent with flags", Principal{IsSuperuser: true}, "Authentication required"},
	}
	for _, tc := range cases {
		err := RequirePlatformAdmin(tc.p)
		if (tc.want == "") != (err == nil) || (err != nil && err.Error() != tc.want) {
			t.Errorf("%s: err=%v want %q", tc.name, err, tc.want)
		}
	}
}

type fakePG struct {
	rows [][]string
	err  error
	sql  string
}

func (f *fakePG) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	f.sql = sql
	if f.err != nil {
		return nil, f.err
	}
	return &fakePGRows{rows: f.rows}, nil
}

type fakePGRows struct {
	pgx.Rows
	rows [][]string
	i    int
}

func (r *fakePGRows) Next() bool { r.i++; return r.i <= len(r.rows) }
func (r *fakePGRows) Err() error { return nil }
func (r *fakePGRows) Close()     {}
func (r *fakePGRows) Scan(dest ...any) error {
	for i, d := range dest {
		*(d.(*string)) = r.rows[r.i-1][i]
	}
	return nil
}

func TestLoadOrgNames(t *testing.T) {
	pg := &fakePG{rows: [][]string{{"11111111-1111-1111-1111-111111111111", "acme", "Acme"}}}
	got, err := LoadOrgNames(context.Background(), pg)
	if err != nil || len(got) != 1 || got[0].Slug != "acme" || !strings.Contains(pg.sql, "FROM organizations") {
		t.Fatalf("got=%v err=%v sql=%s", got, err, pg.sql)
	}
	if _, err := LoadOrgNames(context.Background(), nil); err == nil {
		t.Fatal("no reader must fail, not answer empty")
	}
	if _, err := LoadOrgNames(context.Background(), &fakePG{err: errors.New("boom")}); err == nil {
		t.Fatal("query failure was swallowed")
	}
}

// The section queries are the behaviour: each is pinned for both scopes to the
// event name it filters, its grouping, its ordering and its row limit.
func TestSectionQueries_PinnedShape(t *testing.T) {
	for _, org := range []bool{true, false} {
		cases := []struct {
			name string
			sql  string
			want []string
		}{
			{"daily", dailySQL(org), []string{"GROUP BY day", "ORDER BY day"}},
			{"routes", routesSQL(org), []string{"name = 'page_viewed'", "GROUP BY route_pattern", "ORDER BY events DESC", "LIMIT 25"}},
			{"features", featureViewsSQL(org), []string{"name = 'feature_viewed'", "GROUP BY feature, surface", "ORDER BY views DESC"}},
			{"filters", filterChangesSQL(org), []string{"name = 'filter_changed'", "GROUP BY view, filter_key", "ORDER BY changes DESC", "avg(JSONExtractInt(payload_json, 'valueCount'))"}},
			{"charts", chartInteractionsSQL(org), []string{"name = 'chart_interacted'", "GROUP BY chart, action, surface", "ORDER BY interactions DESC"}},
			{"errors", clientErrorsSQL(org), []string{"name = 'client_error'", "GROUP BY route_pattern, boundary, error_class", "ORDER BY errors DESC"}},
			{"summary", sessionSummarySQL(org), []string{"name = 'session_ended'", "quantile(0.5)(", "quantile(0.75)(", "quantile(0.9)(", "quantile(0.95)(", "'pagesViewed'", "'interactions'"}},
		}
		for _, c := range cases {
			for _, w := range c.want {
				if !strings.Contains(c.sql, w) {
					t.Errorf("org=%v %s: missing %q in\n%s", org, c.name, w, c.sql)
				}
			}
			if strings.Contains(c.sql, "quantileExact") || strings.Contains(c.sql, "FINAL") {
				t.Errorf("org=%v %s: the reference implementation uses the approximate quantile and no FINAL", org, c.name)
			}
		}
	}
	if !strings.Contains(topOrgsSQL(), "LIMIT 50") || !strings.Contains(topOrgsSQL(), "GROUP BY org_id_hash") || !strings.Contains(topOrgsSQL(), "ORDER BY events DESC") {
		t.Errorf("top orgs query shape:\n%s", topOrgsSQL())
	}
	if !strings.Contains(totalsSQL(), "uniqExact(org_id_hash) AS active_orgs") {
		t.Errorf("totals query shape:\n%s", totalsSQL())
	}
}
