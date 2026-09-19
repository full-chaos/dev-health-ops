package analytics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

func dimPtr(d model.DimensionInput) *model.DimensionInput { return &d }

func catalogBindingValue(t *testing.T, q compiledQuery, name string) any {
	t.Helper()
	for _, b := range q.bindings {
		if b.Name == name {
			return b.Value
		}
	}
	t.Fatalf("binding %q not found in %+v", name, q.bindings)
	return nil
}

func TestResolveCatalog_StaticListsWithoutDimension(t *testing.T) {
	client := &routingFakeClient{}
	res, _, err := ResolveCatalog(context.Background(), client, "org-1", nil, nil)
	if err != nil {
		t.Fatalf("ResolveCatalog: %v", err)
	}
	if res.Values != nil {
		t.Fatalf("values must be nil when no dimension is requested, got %v", res.Values)
	}
	if len(client.calls) != 0 {
		t.Fatalf("no query expected without a dimension, got %v", client.calls)
	}
	wantDims := []string{"team", "repo", "author", "work_type", "theme", "subcategory"}
	if len(res.Dimensions) != len(wantDims) {
		t.Fatalf("dimensions = %d, want %d", len(res.Dimensions), len(wantDims))
	}
	for i, name := range wantDims {
		if res.Dimensions[i].Name != name || res.Dimensions[i].Description == "" {
			t.Fatalf("dimension %d = %+v, want name %q with a description", i, res.Dimensions[i], name)
		}
	}
	if len(res.Measures) != 21 || res.Measures[0].Name != "count" || res.Measures[20].Name != "flag_activation_rate" {
		t.Fatalf("measures = %+v", res.Measures)
	}
	for _, m := range res.Measures {
		if m.Description == "" {
			t.Fatalf("measure %q has no description", m.Name)
		}
	}
	want := model.CatalogLimits{MaxDays: 3650, MaxBuckets: 100, MaxTopN: 100, MaxSankeyNodes: 100, MaxSankeyEdges: 500, MaxSubRequests: 10}
	if *res.Limits != want {
		t.Fatalf("limits = %+v, want %+v", *res.Limits, want)
	}
}

func TestCompileCatalogValues_DecisionTable(t *testing.T) {
	teamScope := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"t1"}}}
	orgScope := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputOrg, Ids: []string{"o"}}}
	repoFilter := &model.FilterInput{What: &model.WhatFilterInput{Repos: []string{"r"}}}

	cases := []struct {
		name       string
		dim        Dimension
		limit      int
		filters    *model.FilterInput
		wantErr    bool
		wantSQL    []string
		wantNotSQL []string
		wantBind   []string
	}{
		{"repo no filters", DimensionRepo, 101, nil, false,
			[]string{"FROM repos FINAL", "lowerUTF8(trimBoth(repo))", "org_id = {org_id:String}", "ORDER BY value", "LIMIT {limit:UInt32}"}, nil, []string{"limit", "org_id"}},
		{"repo empty non-org filter object", DimensionRepo, 101, &model.FilterInput{}, false, []string{"FROM repos FINAL"}, nil, nil},
		{"repo org-level scope only", DimensionRepo, 101, orgScope, false, []string{"FROM repos FINAL"}, nil, nil},
		{"repo with team scope", DimensionRepo, 101, teamScope, true, nil, nil, nil},
		{"repo with what filter", DimensionRepo, 101, repoFilter, true, nil, nil, nil},
		{"team ignores filters", DimensionTeam, 100, teamScope, false,
			[]string{"FROM teams FINAL", "is_active = 1", "LEFT JOIN", "investment_metrics_daily", "ORDER BY count DESC, t.name ASC, t.id ASC"},
			[]string{"scope_ids"}, []string{"limit", "org_id"}},
		{"theme investment source", DimensionTheme, 100, nil, false,
			[]string{"splitByChar('.', subcategory_kv.1)[1]", "work_unit_investments.org_id = {org_id:String}", "ARRAY JOIN", "GROUP BY value", "ORDER BY count DESC, value ASC"}, nil, []string{"limit", "org_id"}},
		{"subcategory", DimensionSubcategory, 100, nil, false, []string{"subcategory_kv.1", "work_unit_investments.org_id"}, nil, nil},
		{"work_type", DimensionWorkType, 100, nil, false, []string{"work_unit_type", "work_unit_investments.org_id"}, nil, nil},
		{"work_type with team scope joins team", DimensionWorkType, 100, teamScope, false,
			[]string{"ut.team_label IN {scope_ids:Array(String)}", "work_unit_investments.org_id"}, nil, []string{"scope_ids"}},
		{"author rejected", DimensionAuthor, 100, nil, true, nil, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := CompileCatalogValues(tc.dim, tc.limit, "org-1", queryTimeoutSecs, tc.filters)
			if tc.wantErr {
				var ve *ValidationError
				if !errors.As(err, &ve) {
					t.Fatalf("want ValidationError, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			for _, s := range tc.wantSQL {
				if !strings.Contains(q.sql, s) {
					t.Errorf("sql missing %q:\n%s", s, q.sql)
				}
			}
			for _, s := range tc.wantNotSQL {
				if strings.Contains(q.sql, s) {
					t.Errorf("sql must not contain %q:\n%s", s, q.sql)
				}
			}
			if !strings.Contains(q.sql, "SETTINGS max_execution_time = 30") {
				t.Errorf("sql missing timeout literal:\n%s", q.sql)
			}
			if got := catalogBindingValue(t, q, "org_id"); got != "org-1" {
				t.Errorf("org_id binding = %v", got)
			}
			if got := catalogBindingValue(t, q, "limit"); got != uint32(tc.limit) {
				t.Errorf("limit binding = %v (%T), want %d", got, got, tc.limit)
			}
			for _, b := range tc.wantBind {
				catalogBindingValue(t, q, b)
			}
		})
	}
}

func TestCompileCatalogValues_NonInvestmentDimensionsUseOrgScopedSource(t *testing.T) {
	// team_id/repo_id/etc. are not offered as non-investment catalog
	// dimensions other than team; every SQL a caller can reach binds org_id.
	for _, dim := range []Dimension{DimensionRepo, DimensionTeam, DimensionWorkType, DimensionTheme, DimensionSubcategory} {
		q, err := CompileCatalogValues(dim, 100, "org-x", queryTimeoutSecs, nil)
		if err != nil {
			t.Fatalf("%s: %v", dim, err)
		}
		if catalogBindingValue(t, q, "org_id") != "org-x" || !strings.Contains(q.sql, "{org_id:String}") {
			t.Errorf("%s: org scope missing", dim)
		}
	}
}

func TestCanonicalRepositoryValues(t *testing.T) {
	rows := []catalogValueRow{
		{"Acme/Widget", 2},
		{" acme/widget ", 3},
		{"acme/other", 1},
		{"no-slash", 5},
		{"a/b/c", 5},
		{"-bad/name", 5},
		{"ok/na me", 5},
	}
	got, err := canonicalRepositoryValues(rows)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.CatalogValueItem{{Value: "acme/other", Count: 1}, {Value: "acme/widget", Count: 5}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	empty, err := canonicalRepositoryValues(nil)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("empty input must give a non-nil empty list, got %v %v", empty, err)
	}

	var many []catalogValueRow
	for i := 0; i < repositoryScopeLimit; i++ {
		many = append(many, catalogValueRow{fmt.Sprintf("org/repo%d", i), 1})
	}
	if _, err := canonicalRepositoryValues(many); err != nil {
		t.Fatalf("exactly %d slugs must be accepted: %v", repositoryScopeLimit, err)
	}
	many = append(many, catalogValueRow{"org/one-too-many", 1})
	var ve *ValidationError
	if _, err := canonicalRepositoryValues(many); !errors.As(err, &ve) {
		t.Fatalf("want ValidationError above the limit, got %v", err)
	}
}

func TestResolveCatalog_ValuesByDimension(t *testing.T) {
	t.Run("theme rows in query order", func(t *testing.T) {
		client := (&routingFakeClient{}).on("subcategory_kv", &fakeRowScanner{rows: [][]any{
			{"delivery", uint64(7)}, {"quality", uint64(2)},
		}})
		res, _, err := ResolveCatalog(context.Background(), client, "org-1", dimPtr(model.DimensionInputTheme), nil)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(res.Values) != fmt.Sprint([]model.CatalogValueItem{{Value: "delivery", Count: 7}, {Value: "quality", Count: 2}}) {
			t.Fatalf("values = %v", res.Values)
		}
	})
	t.Run("repo canonicalised", func(t *testing.T) {
		client := (&routingFakeClient{}).on("FROM repos FINAL", &fakeRowScanner{rows: [][]any{
			{"acme/widget", uint64(1)}, {"junk", uint64(1)},
		}})
		res, _, err := ResolveCatalog(context.Background(), client, "org-1", dimPtr(model.DimensionInputRepo), nil)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(res.Values) != fmt.Sprint([]model.CatalogValueItem{{Value: "acme/widget", Count: 1}}) {
			t.Fatalf("values = %v", res.Values)
		}
	})
	t.Run("repo over limit is an error", func(t *testing.T) {
		var rows [][]any
		for i := 0; i <= repositoryScopeLimit; i++ {
			rows = append(rows, []any{fmt.Sprintf("org/repo%d", i), uint64(1)})
		}
		client := (&routingFakeClient{}).on("FROM repos FINAL", &fakeRowScanner{rows: rows})
		_, _, err := ResolveCatalog(context.Background(), client, "org-1", dimPtr(model.DimensionInputRepo), nil)
		var ve *ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("want ValidationError, got %v", err)
		}
	})
	t.Run("team", func(t *testing.T) {
		client := (&routingFakeClient{}).on("FROM teams FINAL", &fakeRowScanner{rows: [][]any{{"team-a", uint64(0)}}})
		res, _, err := ResolveCatalog(context.Background(), client, "org-1", dimPtr(model.DimensionInputTeam), nil)
		if err != nil || len(res.Values) != 1 || res.Values[0].Count != 0 {
			t.Fatalf("values = %v err = %v", res.Values, err)
		}
	})
	t.Run("author is rejected before any query", func(t *testing.T) {
		client := &routingFakeClient{}
		_, _, err := ResolveCatalog(context.Background(), client, "org-1", dimPtr(model.DimensionInputAuthor), nil)
		var ve *ValidationError
		if !errors.As(err, &ve) || len(client.calls) != 0 {
			t.Fatalf("err = %v calls = %v", err, client.calls)
		}
	})
}

func TestResolveCatalog_QueryFailureAnswersEmptyValuesAndLogs(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)

	cases := map[string]*routingFakeClient{
		"query error": (&routingFakeClient{}).onErr("subcategory_kv", errors.New("boom")),
		"scan error":  (&routingFakeClient{}).on("subcategory_kv", &fakeRowScanner{rows: [][]any{{"delivery", "not-a-uint64"}}}),
	}
	for name, client := range cases {
		t.Run(name, func(t *testing.T) {
			buf.Reset()
			res, degraded, err := ResolveCatalog(context.Background(), client, "org-1", dimPtr(model.DimensionInputTheme), nil)
			if err != nil {
				t.Fatalf("a failed values query must not fail the field: %v", err)
			}
			if !degraded {
				t.Fatal("a failed values query must be reported as degraded")
			}
			if res.Values == nil || len(res.Values) != 0 {
				t.Fatalf("values must be a non-nil empty list, got %#v", res.Values)
			}
			if !strings.Contains(buf.String(), "level=WARN") || !strings.Contains(buf.String(), "operation=catalog") || !strings.Contains(buf.String(), "dimension=theme") {
				t.Fatalf("swallowed error must be logged with the operation name, got %q", buf.String())
			}
		})
	}
}

var _ clickhouse.RowScanner = (*fakeRowScanner)(nil)

type bindingCaptureClient struct{ bindings []clickhouse.Binding }

func (c *bindingCaptureClient) Query(_ context.Context, _ string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.bindings = bindings
	return &fakeRowScanner{}, nil
}

func TestResolveCatalog_LimitBindingPerDimension(t *testing.T) {
	cases := map[model.DimensionInput]uint32{
		model.DimensionInputRepo:  repositoryScopeLimit + 1,
		model.DimensionInputTeam:  defaultCatalogLimit,
		model.DimensionInputTheme: defaultCatalogLimit,
	}
	for dim, want := range cases {
		client := &bindingCaptureClient{}
		d := dim
		if _, _, err := ResolveCatalog(context.Background(), client, "org-9", &d, nil); err != nil {
			t.Fatalf("%s: %v", dim, err)
		}
		var limit, org any
		for _, b := range client.bindings {
			switch b.Name {
			case "limit":
				limit = b.Value
			case "org_id":
				org = b.Value
			}
		}
		if limit != want || org != "org-9" {
			t.Errorf("%s: limit=%v org=%v, want limit=%d org=org-9", dim, limit, org, want)
		}
	}
}
