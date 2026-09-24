package aianalytics

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// scriptedRows replays fixed rows through Scan, wrapping a value for a
// pointer destination the way a Nullable column scans.
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

// fixtureClient answers each statement the resolvers issue from a case's
// scripted inputs, dispatching on a fragment unique to that statement.
type fixtureClient struct {
	c          oracleCase
	ds         dataset
	statements []string
	bindings   [][]clickhouse.Binding
}

type dataset struct {
	Daily      []map[string]any `json:"daily"`
	Engagement []map[string]any `json:"engagement"`
}

type oracleCase struct {
	Name    string    `json:"name"`
	Fn      string    `json:"fn"`
	Dataset string    `json:"dataset"`
	Loads   []float64 `json:"loads"`
	Scope   *struct {
		RepoID   *string  `json:"repoId"`
		TeamID   *string  `json:"teamId"`
		WorkType *string  `json:"workType"`
		Buckets  []string `json:"buckets"`
	} `json:"scope"`
	RepoLabels      map[string]string `json:"repoLabels"`
	TeamLabels      map[string]string `json:"teamLabels"`
	SlugRows        []string          `json:"slugRows"`
	TeamRows        []map[string]any  `json:"teamRows"`
	RepoRows        []map[string]any  `json:"repoRows"`
	EngagementError bool              `json:"engagementError"`
	TeamRepoIDs     []string          `json:"teamRepoIds"`
	Expected        map[string]any    `json:"expected"`
}

func fptr(m map[string]any, k string) any {
	v, ok := m[k]
	if !ok || v == nil {
		return nil
	}
	return v.(float64)
}

func u32(m map[string]any, k string) uint32 { return uint32(m[k].(float64)) }

func mustDay(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func (f *fixtureClient) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.statements = append(f.statements, st)
	f.bindings = append(f.bindings, b)
	switch {
	case strings.Contains(st, "FROM ai_impact_metrics_daily"):
		var rows [][]any
		for _, m := range f.ds.Daily {
			computed, err := time.Parse(time.RFC3339Nano, m["computed_at"].(string))
			if err != nil {
				return nil, err
			}
			rows = append(rows, []any{
				m["team_id"].(string), m["repo_id"].(string), mustDay(m["day"].(string)), m["attribution_bucket"].(string),
				u32(m, "prs_total"), u32(m, "prs_merged"), u32(m, "ai_assisted_prs"), u32(m, "agent_created_prs"), u32(m, "human_prs"), u32(m, "unknown_prs"),
				fptr(m, "ai_assisted_pr_ratio"), fptr(m, "cycle_time_avg_hours"), fptr(m, "ai_cycle_time_delta_hours"),
				fptr(m, "reviews_per_pr"), fptr(m, "ai_review_amplification"), fptr(m, "changes_requested_per_pr"),
				u32(m, "rework_prs"), fptr(m, "rework_drag_rate"), u32(m, "followup_commits_count"), u32(m, "revert_prs"), fptr(m, "revert_rate"),
				u32(m, "incidents_count"), fptr(m, "incident_drag_rate"), u32(m, "test_gap_prs"), fptr(m, "test_gap_rate"),
				m["leverage_prs_component"].(float64), fptr(m, "leverage_cycle_time_component"), fptr(m, "leverage_review_component"),
				fptr(m, "leverage_rework_component"), fptr(m, "leverage_test_component"), fptr(m, "leverage_incident_component"),
				computed,
			})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "AS label_id"):
		labels := f.c.RepoLabels
		if strings.Contains(st, "FROM teams") {
			labels = f.c.TeamLabels
		}
		var ids []string
		for _, bd := range b {
			if bd.Name == "ids" {
				ids = bd.Value.([]string)
			}
		}
		var rows [][]any
		for _, id := range ids {
			if l, ok := labels[id]; ok {
				rows = append(rows, []any{id, l})
			}
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "{slug:String}"):
		var rows [][]any
		for _, id := range f.c.SlugRows {
			rows = append(rows, []any{id})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "repo_patterns"):
		var rows [][]any
		for _, t := range f.c.TeamRows {
			var pats []string
			for _, p := range t["repo_patterns"].([]any) {
				pats = append(pats, p.(string))
			}
			rows = append(rows, []any{t["id"].(string), t["name"].(string), pats})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "AS full_name"):
		var rows [][]any
		for _, r := range f.c.RepoRows {
			rows = append(rows, []any{r["repo_id"].(string), r["full_name"].(string)})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "attr_map"):
		if f.c.EngagementError {
			return nil, errors.New("boom")
		}
		var rows [][]any
		for _, m := range f.ds.Engagement {
			var latency any
			if v, ok := m["pickup_latency_hours"]; ok && v != nil {
				latency = v.(float64)
			}
			rows = append(rows, []any{m["bucket"].(string), mustDay(m["day"].(string)), uint64(m["prs_with_first_review"].(float64)), latency, int64(m["review_comments_total"].(float64)), int64(m["loc_total"].(float64))})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "user_metrics_daily"):
		var rows [][]any
		for _, v := range f.c.Loads {
			rows = append(rows, []any{v})
		}
		return &scriptedRows{rows: rows}, nil
	}
	return nil, errors.New("fixtureClient: unscripted statement: " + st)
}

// normalize drops null entries, and parses the timestamp text of computedAt
// into an instant, so the two planes' JSON compares by value.
func normalize(v any, key string) any {
	switch t := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, x := range t {
			if x == nil {
				continue
			}
			out[k] = normalize(x, k)
		}
		return out
	case []any:
		out := make([]any, 0, len(t))
		for _, x := range t {
			out = append(out, normalize(x, key))
		}
		return out
	case string:
		if key == "computedAt" {
			ts, err := time.Parse(time.RFC3339Nano, t)
			if err != nil {
				return t
			}
			return ts.UTC().UnixNano()
		}
	}
	return v
}

func scopeInput(c oracleCase) *model.AIScopeInput {
	if c.Scope == nil {
		return nil
	}
	in := &model.AIScopeInput{RepoID: c.Scope.RepoID, TeamID: c.Scope.TeamID, WorkType: c.Scope.WorkType}
	for _, b := range c.Scope.Buckets {
		in.Buckets = append(in.Buckets, model.AIAttributionBucketInput(b))
	}
	return in
}

func loadOracle(t *testing.T) (map[string]dataset, []oracleCase) {
	t.Helper()
	raw, err := os.ReadFile("testdata/oracle_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Datasets map[string]dataset `json:"datasets"`
		Cases    []oracleCase       `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Datasets, doc.Cases
}

func day(s string) graphqldate.Date { return graphqldate.New(mustDay(s)) }

// TestResolvers_MatchPythonResolvers replays every captured Python
// resolver case (rollup rows, labels, raw-PR engagement, reviewer loads,
// repository and team catalogues) through the Go resolvers and requires the
// same response: every weighted average, ratio, half-even review total,
// ordering, missing state and swallow-to-empty path.
func TestResolvers_MatchPythonResolvers(t *testing.T) {
	datasets, cases := loadOracle(t)
	if len(cases) == 0 {
		t.Fatal("no captured cases")
	}
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			client := &fixtureClient{c: c, ds: datasets[c.Dataset]}
			var (
				got any
				err error
			)
			switch c.Fn {
			case "resolve_ai_impact_summary":
				got, err = ImpactSummary(context.Background(), client, "org-1", dr, scopeInput(c))
			case "resolve_ai_comparison":
				got, err = Comparison(context.Background(), client, "org-1", dr, scopeInput(c))
			case "resolve_ai_review_load":
				got, err = ReviewLoad(context.Background(), client, "org-1", dr, scopeInput(c))
			default:
				t.Fatalf("unknown fn %s", c.Fn)
			}
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var gotMap map[string]any
			if err := json.Unmarshal(raw, &gotMap); err != nil {
				t.Fatal(err)
			}
			delete(gotMap, "startDate")
			delete(gotMap, "endDate")
			want := map[string]any{}
			for k, v := range c.Expected {
				if k != "startDate" && k != "endDate" {
					want[k] = v
				}
			}
			g, w := normalize(gotMap, ""), normalize(want, "")
			if !reflect.DeepEqual(g, w) {
				gj, _ := json.MarshalIndent(g, "", " ")
				wj, _ := json.MarshalIndent(w, "", " ")
				t.Fatalf("go response differs from the python response\n--- go\n%s\n--- python\n%s", gj, wj)
			}
		})
	}
}

// TestTeamRepoIDs_MatchPython requires the repositories a team resolves to
// (the pattern resolver over the org's teams and repository names) to equal
// the set the Python resolver derives from the same catalogues.
func TestTeamRepoIDs_MatchPython(t *testing.T) {
	datasets, cases := loadOracle(t)
	checked := 0
	for _, c := range cases {
		if c.Scope == nil || c.Scope.TeamID == nil || *c.Scope.TeamID == "" || c.TeamRows == nil {
			continue
		}
		checked++
		client := &fixtureClient{c: c, ds: datasets[c.Dataset]}
		ids, ok := teamRepoIDs(context.Background(), client, "org-1", *c.Scope.TeamID, "test")
		if !ok {
			t.Fatalf("%s: catalogues unavailable", c.Name)
		}
		sort.Strings(ids)
		want := c.TeamRepoIDs
		if len(ids) == 0 && len(want) == 0 {
			continue
		}
		if !reflect.DeepEqual(ids, want) {
			t.Errorf("%s: go %v, python %v", c.Name, ids, want)
		}
	}
	if checked == 0 {
		t.Fatal("no team cases captured")
	}
	// a blank team or org has no repositories to resolve
	if _, ok := teamRepoIDs(context.Background(), &fixtureClient{}, "org-1", "", "test"); ok {
		t.Error("a blank team id must not resolve")
	}
	if _, ok := teamRepoIDs(context.Background(), &fixtureClient{}, "", "team-a", "test"); ok {
		t.Error("a blank org must not resolve")
	}
}
