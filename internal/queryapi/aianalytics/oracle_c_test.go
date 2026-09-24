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
)

type oracleCCase struct {
	Name  string `json:"name"`
	Kind  string `json:"kind"`
	Limit int    `json:"limit"`
	Scope *struct {
		RepoID *string `json:"repoId"`
		TeamID *string `json:"teamId"`
	} `json:"scope"`
	Coverage   []map[string]any   `json:"coverage"`
	Violations []map[string]any   `json:"violations"`
	SlugRows   []string           `json:"slugRows"`
	Calls      json.RawMessage    `json:"calls"`
	RootType   string             `json:"rootType"`
	RootID     string             `json:"rootId"`
	Depth      int                `json:"depth"`
	Hops       [][]map[string]any `json:"hops"`
	Error      *string            `json:"error"`
	Expected   map[string]any     `json:"expected"`
}

type fixtureClientC struct {
	c          oracleCCase
	statements []string
	bindings   [][]clickhouse.Binding
	hop        int
}

func str(m map[string]any, k string) any {
	if v, ok := m[k]; ok && v != nil {
		return v.(string)
	}
	return nil
}

func (f *fixtureClientC) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.statements = append(f.statements, st)
	f.bindings = append(f.bindings, b)
	switch {
	case strings.Contains(st, "{slug:String}"):
		var rows [][]any
		for _, id := range f.c.SlugRows {
			rows = append(rows, []any{id})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "FROM ai_governance_coverage_daily"):
		var rows [][]any
		for _, m := range f.c.Coverage {
			rows = append(rows, []any{str(m, "team_id"), str(m, "repo_id"), mustDay(m["day"].(string)),
				uint64(m["ai_artifacts"].(float64)), uint64(m["declared_artifacts"].(float64)), uint64(m["human_reviewed_prs"].(float64)),
				uint64(m["security_scanned_prs"].(float64)), uint64(m["in_policy_artifacts"].(float64))})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "FROM ai_policy_events FINAL"):
		var rows [][]any
		for _, m := range f.c.Violations {
			ts, err := time.Parse(time.RFC3339Nano, m["observed_at"].(string))
			if err != nil {
				ts, err = time.Parse("2006-01-02T15:04:05.999999", m["observed_at"].(string))
				if err != nil {
					return nil, err
				}
			}
			rows = append(rows, []any{str(m, "team_id"), str(m, "repo_id"), m["rule_id"].(string), m["severity"].(string),
				m["subject_type"].(string), m["subject_id"].(string), ts.UTC(), m["evidence"].(string)})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "FROM ai_workflow_issue_edges FINAL"):
		var rows [][]any
		if f.hop < len(f.c.Hops) {
			for _, m := range f.c.Hops[f.hop] {
				var repo any
				if v := m["repo_id"]; v != nil {
					repo = v.(string)
				}
				rows = append(rows, []any{m["edge_id"].(string), m["source_type"].(string), m["source_id"].(string), m["target_type"].(string), m["target_id"].(string),
					m["edge_type"].(string), float32(m["confidence"].(float64)), m["source"].(string), m["evidence"].(string), m["provider"].(string), repo})
			}
		}
		f.hop++
		return &scriptedRows{rows: rows}, nil
	}
	return nil, errors.New("fixtureClientC: unscripted statement: " + st)
}

func normalizeC(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, x := range t {
			if x == nil || k == "day" {
				continue
			}
			out[k] = normalizeC(x)
		}
		return out
	case []any:
		out := make([]any, 0, len(t))
		for _, x := range t {
			out = append(out, normalizeC(x))
		}
		return out
	}
	return v
}

func TestGroupC_MatchPythonResolvers(t *testing.T) {
	raw, err := os.ReadFile("testdata/oracle_c_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []oracleCCase `json:"cases"`
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
			client := &fixtureClientC{c: c}
			var got any
			switch c.Kind {
			case "governance":
				var in *model.AIScopeInput
				if c.Scope != nil {
					in = &model.AIScopeInput{RepoID: c.Scope.RepoID, TeamID: c.Scope.TeamID}
				}
				got, err = GovernanceSummary(context.Background(), client, "org-1", dr, in, c.Limit)
			case "workflow":
				got, err = WorkflowDrilldown(context.Background(), client, "org-1", model.AIWorkflowRootTypeInput(c.RootType), c.RootID, c.Depth, c.Limit)
			}
			if c.Error != nil {
				if err == nil || err.Error() != *c.Error {
					t.Fatalf("error = %v, want %q", err, *c.Error)
				}
				return
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
			g, w := normalizeC(gotMap), normalizeC(want)
			// instants: observedAt text differs by plane, the instant must not
			normalizeInstants(g)
			normalizeInstants(w)
			if !reflect.DeepEqual(g, w) {
				gj, _ := json.MarshalIndent(g, "", " ")
				wj, _ := json.MarshalIndent(w, "", " ")
				t.Fatalf("go response differs from the python response\n--- go\n%s\n--- python\n%s", gj, wj)
			}
			if r, ok := got.(*model.AIGovernanceSummary); ok {
				wantDays, _ := c.Expected["coverage"].([]any)
				if len(wantDays) != len(r.Coverage) {
					t.Fatalf("coverage rows %d, want %d", len(r.Coverage), len(wantDays))
				}
				for i, row := range r.Coverage {
					if row.Day.String() != wantDays[i].(map[string]any)["day"].(string) {
						t.Errorf("coverage day %d = %s", i, row.Day.String())
					}
				}
			}
			checkCallsC(t, c, client)
		})
	}
}

func normalizeInstants(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			if s, ok := x.(string); ok && k == "observedAt" {
				ts, err := time.Parse(time.RFC3339Nano, s)
				if err != nil {
					ts, err = time.Parse("2006-01-02T15:04:05.999999", s)
				}
				if err == nil {
					t[k] = ts.UTC().UnixNano()
				}
				continue
			}
			normalizeInstants(x)
		}
	case []any:
		for _, x := range t {
			normalizeInstants(x)
		}
	}
}

// checkCallsC compares the reader arguments Python used with the bindings the
// Go statements carry.
func checkCallsC(t *testing.T, c oracleCCase, f *fixtureClientC) {
	t.Helper()
	val := func(bs []clickhouse.Binding, name string) any {
		for _, b := range bs {
			if b.Name == name {
				return b.Value
			}
		}
		return nil
	}
	if c.Kind == "governance" {
		var calls map[string]map[string]any
		_ = json.Unmarshal(c.Calls, &calls)
		for i, st := range f.statements {
			if strings.Contains(st, "FROM ai_governance_coverage_daily") {
				want := calls["coverage"]
				if want != nil {
					team, _ := want["team_id"].(string)
					repo, _ := want["repo_id"].(string)
					if val(f.bindings[i], "team_id") != team || val(f.bindings[i], "repo_id") != repo {
						t.Errorf("coverage args team=%v repo=%v, python team=%q repo=%q", val(f.bindings[i], "team_id"), val(f.bindings[i], "repo_id"), team, repo)
					}
				}
			}
			if strings.Contains(st, "FROM ai_policy_events FINAL") {
				if want := calls["violations"]; want != nil {
					if float64(val(f.bindings[i], "limit").(uint32)) != want["limit"].(float64) {
						t.Errorf("violation limit %v, python %v", val(f.bindings[i], "limit"), want["limit"])
					}
				}
			}
		}
		return
	}
	var calls []map[string]any
	_ = json.Unmarshal(c.Calls, &calls)
	hop := 0
	for i, st := range f.statements {
		if !strings.Contains(st, "FROM ai_workflow_issue_edges FINAL") {
			continue
		}
		if hop >= len(calls) {
			t.Fatalf("go issued more edge reads than python (%d)", len(calls))
		}
		for k, wantV := range calls[hop] {
			if k == "org_id" {
				continue
			}
			got := val(f.bindings[i], k)
			switch w := wantV.(type) {
			case []any:
				g, _ := got.([]string)
				sort.Strings(g)
				var ws []string
				for _, x := range w {
					ws = append(ws, x.(string))
				}
				if !(len(g) == 0 && len(ws) == 0) && !reflect.DeepEqual(g, ws) {
					t.Errorf("hop %d %s = %v, python %v", hop, k, g, ws)
				}
			case float64:
				if float64(got.(uint32)) != w {
					t.Errorf("hop %d %s = %v, python %v", hop, k, got, w)
				}
			}
		}
		hop++
	}
	if hop != len(calls) {
		t.Errorf("go issued %d edge reads, python %d", hop, len(calls))
	}
}
