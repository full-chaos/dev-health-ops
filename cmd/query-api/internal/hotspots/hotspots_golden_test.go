package hotspots

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

// The golden holds, for neutral synthetic requests (repo filters, row caps,
// offset datetimes, missing labels, null blame concentration), the exact query
// text and parameters the reference resolver issued and the exact rows it
// built from scripted rows. The label query is compared whole. For the row
// query the source table with its predicates, the grouping and the ordering
// with its row cap are compared with whitespace collapsed; the projection is
// not: the reference took each column's latest value by compute time alone,
// while the Go query takes every column from the one row that is latest by
// (day, compute time), a declared difference in which the Go query is the
// correct one.

type goldenCase struct {
	Name          string           `json:"name"`
	Since         string           `json:"since"`
	Until         string           `json:"until"`
	RepoIDs       []string         `json:"repo_ids"`
	Limit         *float64         `json:"limit"`
	Rows          []map[string]any `json:"rows"`
	Labels        []map[string]any `json:"labels"`
	PythonQueries []string         `json:"python_queries"`
	PythonParams  []map[string]any `json:"python_params"`
	Expected      []map[string]any `json:"expected"`
}

var goldenLimit = regexp.MustCompile(`\{limit:UInt32\}|\{limit:UInt64\}`)

var (
	goldenWhere = regexp.MustCompile(`FROM file_hotspot_daily (WHERE .*?) GROUP BY repo_id, file_path`)
	goldenOrder = regexp.MustCompile(`(ORDER BY risk_score.*)$`)
)

// rowQuerySegments keeps the parts of the row query the golden pins.
func rowQuerySegments(norm string) (string, bool) {
	w := goldenWhere.FindStringSubmatch(norm)
	o := goldenOrder.FindStringSubmatch(norm)
	if w == nil || o == nil {
		return "", false
	}
	return w[1] + " | GROUP BY repo_id, file_path | " + o[1], true
}

func gf(v any) float64 {
	f, _ := v.(float64)
	return f
}

func TestHotspotsMatchesTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/hotspots_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 18 {
		t.Fatalf("golden holds %d cases, want 18", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			since, err := time.Parse(time.RFC3339, tc.Since)
			if err != nil {
				t.Fatal(err)
			}
			until, err := time.Parse(time.RFC3339, tc.Until)
			if err != nil {
				t.Fatal(err)
			}
			var scripted [][]any
			for _, r := range tc.Rows {
				var blame any
				if r["blame_concentration"] != nil {
					blame = gf(r["blame_concentration"])
				}
				scripted = append(scripted, []any{r["repo_id"], r["file_path"], uint64(gf(r["churn_loc_30d"])), uint32(gf(r["churn_commits_30d"])), uint32(gf(r["cyclomatic_total"])), gf(r["cyclomatic_avg"]), blame, gf(r["risk_score"])})
			}
			responses := []*fakeRowScanner{{rows: scripted}}
			if len(tc.Rows) > 0 {
				var labels [][]any
				for _, l := range tc.Labels {
					labels = append(labels, []any{l["repo_id"], l["full_name"]})
				}
				responses = append(responses, &fakeRowScanner{rows: labels})
			}
			var limit *int
			if tc.Limit != nil {
				l := int(*tc.Limit)
				limit = &l
			}
			client := &fakeClient{responses: responses, errs: make([]error, len(responses))}
			got, err := Resolve(context.Background(), client, "org-1", since, until, tc.RepoIDs, limit)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}

			if len(client.statements) != len(tc.PythonQueries) {
				t.Fatalf("%d queries, want %d", len(client.statements), len(tc.PythonQueries))
			}
			for i, statement := range client.statements {
				gotParams := map[string]any{}
				var limitValue any
				for _, b := range client.bindings[i] {
					gotParams[b.Name] = b.Value
					if b.Name == "limit" {
						limitValue = b.Value
					}
				}
				norm := strings.Join(strings.Fields(statement), " ")
				if _, bound := tc.PythonParams[i]["limit"]; !bound && limitValue != nil {
					norm = goldenLimit.ReplaceAllString(norm, fmt.Sprint(limitValue))
					delete(gotParams, "limit")
				}
				want := tc.PythonQueries[i]
				if strings.Contains(want, "FROM file_hotspot_daily") {
					gotSeg, gotOK := rowQuerySegments(norm)
					wantSeg, wantOK := rowQuerySegments(want)
					if !gotOK || !wantOK {
						t.Fatalf("query %d: row query segments not found (got %v, want %v)", i, gotOK, wantOK)
					}
					norm, want = gotSeg, wantSeg
				}
				if norm != want {
					t.Errorf("query %d text differs:\n got  %s\n want %s", i, norm, want)
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
					t.Errorf("query %d bindings %#v, want %#v", i, gotParams, wantParams)
				}
			}

			if len(got.Rows) != len(tc.Expected) {
				t.Fatalf("%d rows, want %d", len(got.Rows), len(tc.Expected))
			}
			for i, want := range tc.Expected {
				r := got.Rows[i]
				var blame any
				if r.BlameConcentration != nil {
					blame = *r.BlameConcentration
				}
				var evidence any
				if r.EvidenceURL != nil {
					evidence = *r.EvidenceURL
				}
				gotRow := map[string]any{
					"file_path": r.FilePath, "repo_id": r.RepoID, "repo_name": r.RepoName,
					"churn_loc_30d": float64(r.ChurnLoc30d), "churn_commits_30d": float64(r.ChurnCommits30d),
					"cyclomatic_total": float64(r.CyclomaticTotal), "cyclomatic_avg": r.CyclomaticAvg,
					"blame_concentration": blame, "risk_score": r.RiskScore, "evidence_url": evidence,
				}
				if !reflect.DeepEqual(gotRow, want) {
					t.Errorf("row %d = %#v, want %#v", i, gotRow, want)
				}
			}
		})
	}
}
