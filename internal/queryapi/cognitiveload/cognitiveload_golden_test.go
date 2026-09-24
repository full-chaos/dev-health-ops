package cognitiveload

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// The golden holds, for neutral synthetic requests over the three request
// paths (team only, team and repository, organisation wide), the exact
// queries the reference resolver issued, in order, with their parameters, and
// the exact signals it built from scripted rows: the merge of per-user and
// per-team days, empty and null handling, repository ownership resolution
// (native ownership rows, ranking winner, several candidate repositories) and
// the pattern fallback across every team. Queries are matched by the table
// they read. The tables read, their order and their parameters are compared
// for every call, and the query text is compared with whitespace collapsed.
// The ownership and team-pattern queries are compared from the FROM clause
// on: the Go queries select only the columns they use, and qualify the
// ranking columns with the table alias, which orders identically.

type goldenCase struct {
	Name          string                      `json:"name"`
	TeamID        *string                     `json:"team_id"`
	RepoID        *string                     `json:"repo_id"`
	Src           map[string][]map[string]any `json:"src"`
	PythonQueries []string                    `json:"python_queries"`
	PythonParams  []map[string]any            `json:"python_params"`
	Expected      struct {
		OrgID     string  `json:"org_id"`
		TeamID    *string `json:"team_id"`
		TotalDays int     `json:"total_days"`
		Signals   []struct {
			Day                   string   `json:"day"`
			PrInterruptionLoad    float64  `json:"pr_interruption_load"`
			ContextSpreadCount    float64  `json:"context_spread_count"`
			ReviewRequestLoad     float64  `json:"review_request_load"`
			AfterHoursCommitRatio *float64 `json:"after_hours_commit_ratio"`
			WeekendCommitRatio    *float64 `json:"weekend_commit_ratio"`
		} `json:"signals"`
	} `json:"expected"`
}

// queryKind names the table a statement reads, in the order the reference
// resolver's markers were checked.
func queryKind(statement string) string {
	q := strings.Join(strings.Fields(statement), " ")
	switch {
	case strings.Contains(q, "FROM team_repo_ownership"):
		return "own"
	case strings.Contains(q, "FROM teams FINAL"):
		return "teams"
	case strings.Contains(q, "repo = {repo_id:String}") && strings.Contains(q, "FROM repos FINAL"):
		return "cand"
	case strings.Contains(q, "FROM team_cognitive_load_daily"):
		return "tcl"
	case strings.Contains(q, "FROM user_metrics_daily"):
		return "user"
	case strings.Contains(q, "FROM team_metrics_daily AS t"):
		return "team_repo"
	case strings.Contains(q, "FROM team_metrics_daily"):
		return "team"
	}
	return "unmatched"
}

// comparableText collapses whitespace and, for the two queries whose
// projection differs, drops the projection and the ranking-column alias.
func comparableText(kind, statement string) string {
	q := strings.Join(strings.Fields(statement), " ")
	if kind == "own" || kind == "teams" {
		if i := strings.Index(q, " FROM "); i >= 0 {
			q = q[i:]
		}
		q = strings.ReplaceAll(q, "o.is_primary DESC, o.specificity DESC, o.updated_at DESC", "is_primary DESC, specificity DESC, updated_at DESC")
	}
	return q
}

func gDay(v any) time.Time {
	t, err := time.Parse("2006-01-02", v.(string))
	if err != nil {
		panic(err)
	}
	return t
}

func gF(v any) float64 {
	f, _ := v.(float64)
	return f
}

func gNullF(v any) any {
	if v == nil {
		return nil
	}
	return v.(float64)
}

func scriptedRows(kind string, rows []map[string]any) [][]any {
	var out [][]any
	for _, r := range rows {
		switch kind {
		case "tcl":
			out = append(out, []any{gDay(r["day"]), gF(r["pr_interruption_load"]), gF(r["context_spread_count"]), gF(r["review_request_load"]), gNullF(r["after_hours_commit_ratio"]), gNullF(r["weekend_commit_ratio"])})
		case "user":
			out = append(out, []any{gDay(r["day"]), uint64(gF(r["pr_interruption_load"])), uint64(gF(r["context_spread_count"])), uint64(gF(r["review_request_load"]))})
		case "team", "team_repo":
			out = append(out, []any{gDay(r["day"]), gF(r["after_hours_commit_ratio"]), gF(r["weekend_commit_ratio"])})
		case "cand":
			out = append(out, []any{r["id"], r["repo"]})
		case "own":
			out = append(out, []any{r["resolved_repo_id"], r["team_id"]})
		case "teams":
			var pats []string
			for _, p := range r["repo_patterns"].([]any) {
				pats = append(pats, p.(string))
			}
			out = append(out, []any{r["id"], pats})
		}
	}
	return out
}

type goldenClient struct {
	src        map[string][]map[string]any
	kinds      []string
	statements []string
	bindings   [][]clickhouse.Binding
}

func (c *goldenClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	kind := queryKind(statement)
	c.kinds = append(c.kinds, kind)
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, bindings)
	return &fakeRowScanner{rows: scriptedRows(kind, c.src[kind])}, nil
}

func TestCognitiveLoadMatchesTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/cognitive_load_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 26 {
		t.Fatalf("golden holds %d cases, want 26", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			since, err := graphqldate.Parse("2026-01-05")
			if err != nil {
				t.Fatal(err)
			}
			until, err := graphqldate.Parse("2026-01-11")
			if err != nil {
				t.Fatal(err)
			}
			client := &goldenClient{src: tc.Src}
			got, err := Resolve(context.Background(), client, "org-1", since, until, tc.TeamID, tc.RepoID)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}

			var wantKinds []string
			for _, q := range tc.PythonQueries {
				wantKinds = append(wantKinds, queryKind(q))
			}
			if !reflect.DeepEqual(client.kinds, wantKinds) {
				t.Fatalf("tables read %v, want %v", client.kinds, wantKinds)
			}
			for i := range client.kinds {
				if got, want := comparableText(client.kinds[i], client.statements[i]), comparableText(client.kinds[i], tc.PythonQueries[i]); got != want {
					t.Errorf("query %d (%s) text differs:\n got  %s\n want %s", i, client.kinds[i], got, want)
				}
				gotParams := map[string]any{}
				for _, b := range client.bindings[i] {
					gotParams[b.Name] = b.Value
				}
				wantParams := map[string]any{}
				for k, v := range tc.PythonParams[i] {
					if list, ok := v.([]any); ok {
						ss := make([]string, len(list))
						for j, e := range list {
							ss[j] = e.(string)
						}
						wantParams[k] = ss
						continue
					}
					wantParams[k] = v
				}
				if !reflect.DeepEqual(gotParams, wantParams) {
					t.Errorf("query %d (%s) bindings %#v, want %#v", i, client.kinds[i], gotParams, wantParams)
				}
			}

			if got.OrgID != tc.Expected.OrgID || !reflect.DeepEqual(got.TeamID, tc.Expected.TeamID) || got.TotalDays != tc.Expected.TotalDays || len(got.Signals) != len(tc.Expected.Signals) {
				t.Fatalf("result %s/%v/%d/%d signals, want %s/%v/%d/%d", got.OrgID, got.TeamID, got.TotalDays, len(got.Signals), tc.Expected.OrgID, tc.Expected.TeamID, tc.Expected.TotalDays, len(tc.Expected.Signals))
			}
			for i, want := range tc.Expected.Signals {
				s := got.Signals[i]
				if s.Day.String() != want.Day || s.PrInterruptionLoad != want.PrInterruptionLoad || s.ContextSpreadCount != want.ContextSpreadCount ||
					s.ReviewRequestLoad != want.ReviewRequestLoad || !reflect.DeepEqual(s.AfterHoursCommitRatio, want.AfterHoursCommitRatio) ||
					!reflect.DeepEqual(s.WeekendCommitRatio, want.WeekendCommitRatio) {
					t.Errorf("signal %d = %+v, want %+v", i, s, want)
				}
			}
		})
	}
}
