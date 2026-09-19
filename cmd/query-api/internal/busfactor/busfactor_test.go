package busfactor

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
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
	statement string
	bindings  []clickhouse.Binding
	rows      [][]any
}

func (c *recordingClient) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statement, c.bindings = st, b
	return &scriptedRows{rows: c.rows}, nil
}

func sp(s string) *string { return &s }

const repoOne = "11111111-1111-1111-1111-111111111111"

type spec struct {
	email, name *string
	add, del    int32
}

func toRows(repo, name string, specs []spec) [][]any {
	var out [][]any
	for _, s := range specs {
		var e, n any
		if s.email != nil {
			e = *s.email
		}
		if s.name != nil {
			n = *s.name
		}
		out = append(out, []any{repo, name, e, n, s.add, s.del})
	}
	return out
}

// The expected values below were produced by running the Python
// compute_bus_factor and _top_maintainers on the same rows.
func TestComputation_MatchesPythonCells(t *testing.T) {
	cases := []struct {
		name  string
		specs []spec
		value int
		top3  []model.MaintainerShare
	}{
		{"empty", nil, 0, []model.MaintainerShare{}},
		{"single", []spec{{sp("a@x"), nil, 10, 0}}, 1, []model.MaintainerShare{{Author: "a@x", SharePercent: 100.0}}},
		{"zero", []spec{{sp("a@x"), nil, 0, 0}, {sp("b@x"), nil, 0, 0}}, 0, []model.MaintainerShare{}},
		{"even4", []spec{{sp("a@x"), nil, 5, 0}, {sp("b@x"), nil, 5, 0}, {sp("c@x"), nil, 5, 0}, {sp("d@x"), nil, 5, 0}}, 4,
			[]model.MaintainerShare{{Author: "a@x", SharePercent: 25}, {Author: "b@x", SharePercent: 25}, {Author: "c@x", SharePercent: 25}}},
		{"skew", []spec{{sp("a@x"), nil, 80, 0}, {sp("b@x"), nil, 10, 0}, {sp("c@x"), nil, 10, 0}}, 1,
			[]model.MaintainerShare{{Author: "a@x", SharePercent: 80}, {Author: "b@x", SharePercent: 10}, {Author: "c@x", SharePercent: 10}}},
		{"boundary80", []spec{{sp("a@x"), nil, 8, 0}, {sp("b@x"), nil, 2, 0}}, 1,
			[]model.MaintainerShare{{Author: "a@x", SharePercent: 80}, {Author: "b@x", SharePercent: 20}}},
		{"namefallback", []spec{{sp(""), sp("Bob"), 3, 1}, {nil, sp("Bob"), 2, 2}, {nil, nil, 1, 0}, {sp(""), sp(""), 1, 0}}, 1,
			[]model.MaintainerShare{{Author: "Bob", SharePercent: 80}, {Author: "unknown", SharePercent: 20}}},
		{"ties", []spec{{sp("a@x"), nil, 5, 0}, {sp("b@x"), nil, 5, 0}, {sp("c@x"), nil, 5, 0}, {sp("d@x"), nil, 5, 0}, {sp("e@x"), nil, 5, 0}, {sp("f@x"), nil, 5, 0}, {sp("g@x"), nil, 1, 0}}, 5,
			[]model.MaintainerShare{{Author: "a@x", SharePercent: 16.129032258064516}, {Author: "b@x", SharePercent: 16.129032258064516}, {Author: "c@x", SharePercent: 16.129032258064516}}},
		{"thirds", []spec{{sp("a@x"), nil, 1, 0}, {sp("b@x"), nil, 1, 0}, {sp("c@x"), nil, 1, 0}}, 3,
			[]model.MaintainerShare{{Author: "a@x", SharePercent: 33.33333333333333}, {Author: "b@x", SharePercent: 33.33333333333333}, {Author: "c@x", SharePercent: 33.33333333333333}}},
		{"roundshare", []spec{{sp("a@x"), nil, 1, 0}, {sp("b@x"), nil, 2, 0}}, 2,
			[]model.MaintainerShare{{Author: "b@x", SharePercent: 66.66666666666666}, {Author: "a@x", SharePercent: 33.33333333333333}}},
		{"negative", []spec{{sp("a@x"), nil, 10, 0}, {sp("b@x"), nil, -5, 0}}, 1, []model.MaintainerShare{{Author: "a@x", SharePercent: 100}}},
		{"zeroauthor", []spec{{sp("a@x"), nil, 10, 0}, {sp("b@x"), nil, 0, 0}}, 1, []model.MaintainerShare{{Author: "a@x", SharePercent: 100}}},
		{"four", []spec{{sp("a@x"), nil, 4, 0}, {sp("b@x"), nil, 3, 0}, {sp("c@x"), nil, 2, 0}, {sp("d@x"), nil, 1, 0}}, 3,
			[]model.MaintainerShare{{Author: "a@x", SharePercent: 40}, {Author: "b@x", SharePercent: 30}, {Author: "c@x", SharePercent: 20}}},
		{"big", []spec{{sp("a@x"), nil, 2147483647, 0}, {sp("b@x"), nil, 2147483647, 0}, {sp("c@x"), nil, 7, 0}}, 2,
			[]model.MaintainerShare{{Author: "a@x", SharePercent: 49.999999918509275}, {Author: "b@x", SharePercent: 49.999999918509275}, {Author: "c@x", SharePercent: 1.6298145036797336e-07}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cl := &recordingClient{rows: toRows(repoOne, "acme/web", c.specs)}
			got, err := Resolve(context.Background(), cl, "o", nil, time.Unix(0, 0))
			if err != nil {
				t.Fatal(err)
			}
			if got.Value != c.value {
				t.Errorf("value %d want %d", got.Value, c.value)
			}
			top3 := topMaintainers(evidence(cl.rows), 3)
			if !reflect.DeepEqual(top3, c.top3) {
				t.Errorf("top3 %#v want %#v", top3, c.top3)
			}
			if len(got.Repos) == 1 && !reflect.DeepEqual(got.Repos[0].TopMaintainers, c.top3) {
				t.Errorf("per-repo top %#v want %#v", got.Repos[0].TopMaintainers, c.top3)
			}
			if got.EvidenceSampleCount != len(c.specs) {
				t.Errorf("evidence %d", got.EvidenceSampleCount)
			}
		})
	}
}

func evidence(rows [][]any) []evidenceRow {
	var out []evidenceRow
	for _, r := range rows {
		var e, n *string
		if r[2] != nil {
			v := r[2].(string)
			e = &v
		}
		if r[3] != nil {
			v := r[3].(string)
			n = &v
		}
		out = append(out, evidenceRow{repoID: r[0].(string), repoName: r[1].(string), identity: identityOf(e, n), churn: int64(r[4].(int32)) + int64(r[5].(int32))})
	}
	return out
}

func TestParseRepoID_MatchesPythonCells(t *testing.T) {
	cases := map[string]string{
		"11111111-1111-1111-1111-111111111111":          repoOne,
		"11111111111111111111111111111111":              repoOne,
		"{11111111-1111-1111-1111-111111111111}":        repoOne,
		"urn:uuid:11111111-1111-1111-1111-111111111111": repoOne,
		"uuid:11111111-1111-1111-1111-111111111111":     repoOne,
		"1111-1111-11111111-1111-111111111111":          repoOne,
		"ABCDEF01-1111-1111-1111-111111111111":          "abcdef01-1111-1111-1111-111111111111",
		"--11111111111111111111111111111111--":          repoOne,
		"":                                              "",
		"not-a-uuid":                                    "",
		"11111111-1111-1111-1111-11111111111":           "",
		"11111111-1111-1111-1111-1111111111111":         "",
		"gggggggg-1111-1111-1111-111111111111":          "",
	}
	for in, want := range cases {
		in := in
		got, ok := parseRepoID(&in)
		if got != want || ok != (want != "") {
			t.Errorf("parseRepoID(%q) = %q,%v want %q", in, got, ok, want)
		}
	}
	if _, ok := parseRepoID(nil); ok {
		t.Error("nil scope id must not scope")
	}
}

func TestResolve_ReposSortedAndNamed(t *testing.T) {
	a := "22222222-2222-2222-2222-222222222222"
	rows := append(toRows(repoOne, "zeta", []spec{{sp("a@x"), nil, 5, 0}, {sp("b@x"), nil, 5, 0}}),
		toRows(a, "alpha", []spec{{sp("a@x"), nil, 5, 0}, {sp("b@x"), nil, 5, 0}})...)
	rows = append(rows, toRows("33333333-3333-3333-3333-333333333333", "", []spec{{sp("c@x"), nil, 9, 0}})...)
	got, err := Resolve(context.Background(), &recordingClient{rows: rows}, "o", nil, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, r := range got.Repos {
		names = append(names, r.RepoName)
	}
	// value ascending, then name: the single-author repo first; an empty name reads as its id.
	if strings.Join(names, ",") != "33333333-3333-3333-3333-333333333333,alpha,zeta" {
		t.Fatalf("order %v", names)
	}
	if got.Value != 3 || got.OrgID != "o" || got.Scope.RepoID != nil || got.Scope.TeamID != nil {
		t.Fatalf("%#v", got)
	}
}

func TestResolve_ScopeShapeAndStatement(t *testing.T) {
	team := "team-1"
	repo := "{" + strings.ToUpper(repoOne) + "}"
	cl := &recordingClient{}
	got, err := Resolve(context.Background(), cl, "org-1", &model.BusFactorScopeInput{RepoID: &repo, TeamID: &team}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != 0 || len(got.TopMaintainers) != 0 || len(got.Repos) != 0 || got.EvidenceSampleCount != 0 || got.Repos == nil || got.TopMaintainers == nil {
		t.Fatalf("empty window: %#v", got)
	}
	if *got.Scope.RepoID != repoOne || *got.Scope.TeamID != team {
		t.Fatalf("scope echo %#v", got.Scope)
	}
	for _, want := range []string{"gc.org_id = {org_id:String}", "gcs.org_id = {org_id:String}", "gc.repo_id = {repo_id:UUID}", "FROM team_repo_ownership AS o FINAL", "toString(gc.repo_id) IN ("} {
		if !strings.Contains(cl.statement, want) {
			t.Errorf("statement lacks %q", want)
		}
	}
	m := map[string]any{}
	for _, b := range cl.bindings {
		m[b.Name] = b.Value
	}
	if m["org_id"] != "org-1" || m["repo_id"] != repoOne || m["team_scope_org_id"] != "org-1" {
		t.Errorf("bindings %#v", m)
	}
}

func TestResolve_NoScopeAndBlankTeam(t *testing.T) {
	blank := ""
	for name, sc := range map[string]*model.BusFactorScopeInput{"nil": nil, "blank team": {TeamID: &blank}, "bad repo": {RepoID: &blank}} {
		cl := &recordingClient{}
		got, err := Resolve(context.Background(), cl, "o", sc, time.Unix(0, 0))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(cl.statement, "team_repo_ownership") || strings.Contains(cl.statement, "{repo_id:UUID}") {
			t.Errorf("%s: unscoped request carried a scope predicate", name)
		}
		if got.Scope.RepoID != nil {
			t.Errorf("%s: repo scope echoed", name)
		}
	}
	// The echo of a blank team id is the blank value itself.
	got, _ := Resolve(context.Background(), &recordingClient{}, "o", &model.BusFactorScopeInput{TeamID: &blank}, time.Unix(0, 0))
	if got.Scope.TeamID == nil || *got.Scope.TeamID != "" {
		t.Errorf("blank team echo %#v", got.Scope)
	}
}
