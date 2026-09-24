package complexitytimeseries

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

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// The golden holds, for neutral synthetic requests (repo and file scope, day
// and week buckets, repo filters, row caps, windows of different lengths,
// offset datetimes), the exact query text and parameters the reference
// resolver issued and the exact points it built from scripted rows. Query
// text is compared with whitespace collapsed and an inline row cap taken from
// the bound value.

type goldenCase struct {
	Name          string           `json:"name"`
	Scope         string           `json:"scope"`
	Gran          string           `json:"gran"`
	Since         string           `json:"since"`
	Until         string           `json:"until"`
	RepoIDs       []string         `json:"repo_ids"`
	Limit         *float64         `json:"limit"`
	Rows          []map[string]any `json:"rows"`
	Labels        []map[string]any `json:"labels"`
	PythonQueries []string         `json:"python_queries"`
	PythonParams  []map[string]any `json:"python_params"`
	Expected      struct {
		TotalScope int              `json:"total_scope"`
		Points     []map[string]any `json:"points"`
	} `json:"expected"`
}

func gDay(v any) time.Time {
	t, err := time.Parse("2006-01-02", v.(string))
	if err != nil {
		panic(err)
	}
	return t
}

func gU64(v any) any {
	if v == nil {
		return nil
	}
	return uint64(v.(float64))
}

func gU32(v any) any {
	if v == nil {
		return nil
	}
	return uint32(v.(float64))
}

func gF64(v any) any {
	if v == nil {
		return nil
	}
	return v.(float64)
}

func gPtr[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

var goldenLimit = regexp.MustCompile(`\{limit:UInt32\}`)

func TestComplexityTimeseriesMatchesTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/complexity_timeseries_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 29 {
		t.Fatalf("golden holds %d cases, want 29", len(cases))
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
			scope := model.ComplexityScopeRepo
			var scripted [][]any
			for _, r := range tc.Rows {
				if tc.Scope == "file" {
					scripted = append(scripted, []any{gDay(r["day"]), r["repo_id"], r["file_path"], gU32(r["cyclomatic_total"]), gF64(r["cyclomatic_avg"]), gU32(r["high_complexity_functions"]), gU32(r["very_high_complexity_functions"])})
				} else {
					scripted = append(scripted, []any{gDay(r["day"]), r["repo_id"], gU64(r["loc_total"]), gU64(r["cyclomatic_total"]), gF64(r["cyclomatic_per_kloc"]), gU64(r["high_complexity_functions"]), gU64(r["very_high_complexity_functions"])})
				}
			}
			responses := []*fakeRowScanner{{rows: normaliseNil(scripted)}}
			if tc.Scope == "file" {
				scope = model.ComplexityScopeFile
			} else if len(tc.Rows) > 0 {
				var labels [][]any
				for _, l := range tc.Labels {
					labels = append(labels, []any{l["repo_id"], l["full_name"]})
				}
				responses = append(responses, &fakeRowScanner{rows: labels})
			}
			gran := model.TimeGranularityDay
			if tc.Gran == "week" {
				gran = model.TimeGranularityWeek
			}
			var limit *int
			if tc.Limit != nil {
				l := int(*tc.Limit)
				limit = &l
			}
			client := &fakeClient{responses: responses, errs: make([]error, len(responses))}
			got, err := Resolve(context.Background(), client, "org-1", since, until, gran, scope, tc.RepoIDs, limit)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}

			if len(client.statements) != len(tc.PythonQueries) {
				t.Fatalf("%d queries, want %d", len(client.statements), len(tc.PythonQueries))
			}
			for i, statement := range client.statements {
				params := map[string]any{}
				var limitValue any
				for _, b := range client.bindings[i] {
					params[b.Name] = b.Value
					if b.Name == "limit" {
						limitValue = b.Value
					}
				}
				norm := strings.Join(strings.Fields(statement), " ")
				want := tc.PythonQueries[i]
				if _, bound := tc.PythonParams[i]["limit"]; !bound && limitValue != nil {
					norm = goldenLimit.ReplaceAllString(norm, fmt.Sprint(limitValue))
					delete(params, "limit")
				}
				if norm != want {
					t.Errorf("query %d text differs:\n got  %s\n want %s", i, norm, want)
				}
				wantParams := map[string]any{}
				for k, v := range tc.PythonParams[i] {
					switch x := v.(type) {
					case float64:
						wantParams[k] = int(x)
					case []any:
						ss := make([]string, len(x))
						for j, e := range x {
							ss[j] = e.(string)
						}
						wantParams[k] = ss
					default:
						wantParams[k] = v
					}
				}
				gotParams := map[string]any{}
				for k, v := range params {
					switch x := v.(type) {
					case uint32:
						gotParams[k] = int(x)
					case uint64:
						gotParams[k] = int(x)
					default:
						gotParams[k] = v
					}
				}
				if !reflect.DeepEqual(gotParams, wantParams) {
					t.Errorf("query %d bindings %#v, want %#v", i, gotParams, wantParams)
				}
			}

			if got.TotalScope != tc.Expected.TotalScope || len(got.Points) != len(tc.Expected.Points) {
				t.Fatalf("total scope %d points %d, want %d/%d", got.TotalScope, len(got.Points), tc.Expected.TotalScope, len(tc.Expected.Points))
			}
			for i, want := range tc.Expected.Points {
				p := got.Points[i]
				gotPoint := map[string]any{
					"point_date": p.Date.String(), "scope_id": p.ScopeID, "scope_name": p.ScopeName,
					"loc_total": intAny(p.LocTotal), "cyclomatic_per_kloc": gPtr(p.CyclomaticPerKloc), "cyclomatic_total": intAny(p.CyclomaticTotal),
					"cyclomatic_avg": gPtr(p.CyclomaticAvg), "high_complexity_functions": intAny(p.HighComplexityFunctions),
					"very_high_complexity_functions": intAny(p.VeryHighComplexityFunctions),
				}
				wantPoint := map[string]any{}
				for k, v := range want {
					if f, ok := v.(float64); ok && k != "cyclomatic_per_kloc" && k != "cyclomatic_avg" {
						wantPoint[k] = int(f)
						continue
					}
					wantPoint[k] = v
				}
				if !reflect.DeepEqual(gotPoint, wantPoint) {
					t.Errorf("point %d = %#v, want %#v", i, gotPoint, wantPoint)
				}
			}
		})
	}
}

func intAny(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

func normaliseNil(rows [][]any) [][]any { return rows }
