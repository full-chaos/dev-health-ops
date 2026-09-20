package aianalytics

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

type oracleBCase struct {
	Name   string `json:"name"`
	Fn     string `json:"fn"`
	Limit  *int   `json:"limit"`
	Offset *int   `json:"offset"`
	Scope  *struct {
		RepoID   *string  `json:"repoId"`
		TeamID   *string  `json:"teamId"`
		WorkType *string  `json:"workType"`
		Buckets  []string `json:"buckets"`
	} `json:"scope"`
	Daily          []map[string]any `json:"daily"`
	Hotspot        []map[string]any `json:"hotspot"`
	Complexity     []map[string]any `json:"complexity"`
	Prs            []map[string]any `json:"prs"`
	Mix            []map[string]any `json:"mix"`
	Evidence       []map[string]any `json:"evidence"`
	TeamRows       []map[string]any `json:"teamRows"`
	RepoRows       []map[string]any `json:"repoRows"`
	SlugRows       []string         `json:"slugRows"`
	Fail           string           `json:"fail"`
	NameQueryFails bool             `json:"nameQueryFails"`
	Calls          map[string]any   `json:"calls"`
	Expected       map[string]any   `json:"expected"`
}

// fixtureClientB answers the statements the second group of AI fields
// issues; every statement it does not own goes to the shared fixtureClient.
type fixtureClientB struct {
	*fixtureClient
	c oracleBCase
}

func floatCell(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		if t == "NaN" {
			return math.NaN()
		}
		if t == "Inf" {
			return math.Inf(1)
		}
	case float64:
		return t
	}
	return v
}

func (f *fixtureClientB) Query(ctx context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c := f.c
	switch {
	case strings.Contains(st, "repo_patterns") && c.Fail == "teams":
		f.fixtureClient.statements = append(f.fixtureClient.statements, st)
		f.fixtureClient.bindings = append(f.fixtureClient.bindings, b)
		return nil, errors.New("boom")
	case strings.Contains(st, "AS full_name") && strings.Contains(st, "{repo_ids:Array(String)}"):
		f.fixtureClient.statements = append(f.fixtureClient.statements, st)
		f.fixtureClient.bindings = append(f.fixtureClient.bindings, b)
		if c.NameQueryFails {
			return nil, errors.New("boom")
		}
		var ids []string
		for _, bd := range b {
			if bd.Name == "repo_ids" {
				ids = bd.Value.([]string)
			}
		}
		var rows [][]any
		for _, r := range c.RepoRows {
			for _, id := range ids {
				if r["repo_id"].(string) == id {
					rows = append(rows, []any{id, r["full_name"].(string)})
				}
			}
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "hotspots AS ("):
		f.record(st, b)
		if c.Fail == "hotspot" {
			return nil, errors.New("boom")
		}
		var rows [][]any
		for _, m := range c.Hotspot {
			rows = append(rows, []any{m["bucket"].(string), uint64(m["prs_total"].(float64)), uint64(m["prs_touching_hotspots"].(float64)), floatCell(m["avg_hotspot_risk_score"])})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "complex_files AS ("):
		f.record(st, b)
		if c.Fail == "complexity" {
			return nil, errors.New("boom")
		}
		var rows [][]any
		for _, m := range c.Complexity {
			rows = append(rows, []any{m["bucket"].(string), uint64(m["prs_total"].(float64)), uint64(m["prs_touching_high_complexity"].(float64))})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "repo_id_str, number, kind, work_type"):
		f.record(st, b)
		var rows [][]any
		for _, m := range c.Prs {
			var title, merged any
			if v, ok := m["title"]; ok && v != nil {
				title = v.(string)
			}
			if v, ok := m["merged_at"]; ok && v != nil {
				ts, err := time.Parse("2006-01-02T15:04:05.999999", v.(string))
				if err != nil {
					return nil, err
				}
				merged = ts.UTC()
			}
			rows = append(rows, []any{m["repo_id"].(string), uint32(m["number"].(float64)), m["kind"].(string), m["work_type"].(string), title, merged})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "count() AS count"):
		f.record(st, b)
		var rows [][]any
		for _, m := range c.Mix {
			rows = append(rows, []any{m["kind"].(string), uint64(m["count"].(float64))})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "FROM ai_attribution_resolved") && strings.Contains(st, "subject_type,"):
		f.record(st, b)
		var rows [][]any
		for _, m := range c.Evidence {
			var repo, actor any
			if v := m["repo_id"]; v != nil {
				repo = v.(string)
			}
			if v := m["actor"]; v != nil {
				actor = v.(string)
			}
			conf := float32(0)
			if v := m["confidence"]; v != nil {
				conf = float32(v.(float64))
			}
			ts, err := time.Parse("2006-01-02T15:04:05.999999", m["observed_at"].(string))
			if err != nil {
				return nil, err
			}
			rows = append(rows, []any{m["subject_type"].(string), m["subject_id"].(string), repo, m["provider"].(string), m["kind"].(string), m["source"].(string), conf, actor, m["evidence"].(string), ts.UTC()})
		}
		return &scriptedRows{rows: rows}, nil
	}
	return f.fixtureClient.Query(ctx, st, b)
}

func (f *fixtureClientB) record(st string, b []clickhouse.Binding) {
	f.fixtureClient.statements = append(f.fixtureClient.statements, st)
	f.fixtureClient.bindings = append(f.fixtureClient.bindings, b)
}

func normalizeB(v any, key string) any {
	switch t := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, x := range t {
			if x == nil {
				continue
			}
			out[k] = normalizeB(x, k)
		}
		return out
	case []any:
		out := make([]any, 0, len(t))
		for _, x := range t {
			out = append(out, normalizeB(x, key))
		}
		return out
	case string:
		if key == "mergedAt" || key == "observedAt" {
			ts, err := time.Parse(time.RFC3339Nano, t)
			if err != nil {
				ts, err = time.Parse("2006-01-02T15:04:05.999999", t)
			}
			if err == nil {
				return ts.UTC().UnixNano()
			}
		}
	}
	return v
}

// TestGroupB_MatchPythonResolvers replays the captured Python cases of the
// risk breakdown, the attributed pull requests and the attribution overview
// and requires the same responses, and the same page and scope arguments
// reaching the readers.
func TestGroupB_MatchPythonResolvers(t *testing.T) {
	raw, err := os.ReadFile("testdata/oracle_b_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []oracleBCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Cases) == 0 {
		t.Fatal("no captured cases")
	}
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	for _, c := range doc.Cases {
		t.Run(c.Name, func(t *testing.T) {
			client := &fixtureClientB{
				fixtureClient: &fixtureClient{
					c:  oracleCase{SlugRows: c.SlugRows, TeamRows: c.TeamRows, RepoRows: c.RepoRows},
					ds: dataset{Daily: c.Daily},
				},
				c: c,
			}
			var scope *model.AIScopeInput
			var aScope *model.AIAttributionScopeInput
			if c.Scope != nil {
				scope = &model.AIScopeInput{RepoID: c.Scope.RepoID, TeamID: c.Scope.TeamID, WorkType: c.Scope.WorkType}
				aScope = &model.AIAttributionScopeInput{RepoID: c.Scope.RepoID, TeamID: c.Scope.TeamID}
				for _, b := range c.Scope.Buckets {
					scope.Buckets = append(scope.Buckets, model.AIAttributionBucketInput(b))
					aScope.Buckets = append(aScope.Buckets, model.AIAttributionBucketInput(b))
				}
			}
			limit, offset := 0, 0
			if c.Limit != nil {
				limit, offset = *c.Limit, *c.Offset
			}
			var got any
			switch c.Fn {
			case "resolve_ai_risk_breakdown":
				got, err = RiskBreakdown(context.Background(), client, "org-1", dr, scope)
			case "resolve_ai_attributed_prs":
				got, err = AttributedPrs(context.Background(), client, "org-1", dr, scope, limit, offset)
			case "resolve_ai_attribution_overview":
				got, err = AttributionOverview(context.Background(), client, "org-1", dr, aScope, limit, offset)
			default:
				t.Fatalf("unknown fn %s", c.Fn)
			}
			if err != nil {
				t.Fatal(err)
			}
			enc, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var gotMap map[string]any
			if err := json.Unmarshal(enc, &gotMap); err != nil {
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
			g, w := normalizeB(gotMap, ""), normalizeB(want, "")
			if !reflect.DeepEqual(g, w) {
				gj, _ := json.MarshalIndent(g, "", " ")
				wj, _ := json.MarshalIndent(w, "", " ")
				t.Fatalf("go response differs from the python response\n--- go\n%s\n--- python\n%s", gj, wj)
			}
			checkCalls(t, c, client.fixtureClient)
		})
	}
}

// checkCalls compares the page and scope arguments the Python readers were
// called with against the bindings the Go statements carry.
func checkCalls(t *testing.T, c oracleBCase, f *fixtureClient) {
	t.Helper()
	find := func(marker string) []clickhouse.Binding {
		for i, st := range f.statements {
			if strings.Contains(st, marker) {
				return f.bindings[i]
			}
		}
		return nil
	}
	num := func(bs []clickhouse.Binding, name string) any {
		for _, b := range bs {
			if b.Name == name {
				switch v := b.Value.(type) {
				case uint32:
					return float64(v)
				default:
					return v
				}
			}
		}
		return nil
	}
	if want, ok := c.Calls["prs"].(map[string]any); ok {
		bs := find("repo_id_str, number, kind, work_type")
		if bs == nil {
			t.Fatalf("the attributed pull request read did not run")
		}
		for _, k := range []string{"limit", "offset"} {
			if got := num(bs, k); got != want[k] {
				t.Errorf("%s = %v, want %v", k, got, want[k])
			}
		}
		if ids, _ := want["repo_ids"].([]any); ids != nil {
			got, _ := num(bs, "repo_ids").([]string)
			if len(got) != len(ids) {
				t.Errorf("repo_ids = %v, want %v", got, ids)
			}
		} else if num(bs, "repo_ids") != nil {
			t.Errorf("repo_ids bound, python passed none")
		}
	}
	if want, ok := c.Calls["evidence"].(map[string]any); ok {
		bs := find("subject_type,")
		for _, k := range []string{"limit", "offset"} {
			if got := num(bs, k); got != want[k] {
				t.Errorf("%s = %v, want %v", k, got, want[k])
			}
		}
	}
	if want, ok := c.Calls["mix"].(map[string]any); ok {
		bs := find("count() AS count")
		got, _ := num(bs, "kinds").([]string)
		wantKinds, _ := want["kinds"].([]any)
		if len(got) != len(wantKinds) {
			t.Errorf("kinds = %v, want %v", got, wantKinds)
		}
	}
	if want, ok := c.Calls["hotspot"].(map[string]any); ok {
		bs := find("hotspots AS (")
		if bs == nil {
			t.Fatalf("the hotspot read did not run")
		}
		if (want["repo_ids"] != nil) != (num(bs, "repo_ids") != nil) {
			t.Errorf("hotspot team narrowing differs: python %v go %v", want["repo_ids"], num(bs, "repo_ids"))
		}
	}
}
