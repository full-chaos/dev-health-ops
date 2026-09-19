package reviewedges

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The golden holds, for eleven neutral synthetic requests, the exact query text
// and parameters the reference resolver issued and the exact result it built
// from scripted rows. Query text is compared with whitespace collapsed and
// the row cap inlined as the reference did. A change to a predicate, the
// ordering, the deduplication, the cap or the row mapping goes red here.

type goldenEdge struct {
	Reviewer     string  `json:"reviewer"`
	Author       string  `json:"author"`
	ReviewsCount int     `json:"reviews_count"`
	Day          string  `json:"day"`
	RepoID       *string `json:"repo_id"`
}

type goldenCase struct {
	Name         string         `json:"name"`
	RepoIDs      []string       `json:"repo_ids"`
	Limit        int            `json:"limit"`
	Rows         [][]any        `json:"rows"`
	PythonQuery  string         `json:"python_query"`
	PythonParams map[string]any `json:"python_params"`
	Expected     struct {
		TotalCount int          `json:"total_count"`
		Edges      []goldenEdge `json:"edges"`
	} `json:"expected"`
}

func TestReviewEdgesMatchesTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/review_edges_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 11 {
		t.Fatalf("golden holds %d cases, want 11", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			var scripted [][]any
			for _, r := range tc.Rows {
				day, err := time.Parse("2006-01-02", r[3].(string))
				if err != nil {
					t.Fatal(err)
				}
				scripted = append(scripted, []any{r[0].(string), r[1].(string), uint32(r[2].(float64)), day, r[4].(string)})
			}
			client := &fakeClient{response: &fakeRowScanner{rows: scripted}}
			got, err := Resolve(context.Background(), client, "org-1", mustDate(t, "2026-01-01"), mustDate(t, "2026-01-31"), tc.RepoIDs, tc.Limit)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}

			params := map[string]any{}
			limit := -1
			for _, b := range client.bindings {
				if b.Name == "limit" {
					limit = b.Value.(int)
					continue
				}
				params[b.Name] = b.Value
			}
			statement := strings.Join(strings.Fields(client.statement), " ")
			statement = strings.Replace(statement, "{limit:UInt64}", fmt.Sprint(limit), 1)
			if statement != tc.PythonQuery {
				t.Errorf("query text differs from the golden:\n got  %s\n want %s", statement, tc.PythonQuery)
			}
			wantParams := map[string]any{}
			for k, v := range tc.PythonParams {
				if list, ok := v.([]any); ok {
					strs := make([]string, len(list))
					for i, item := range list {
						strs[i] = item.(string)
					}
					wantParams[k] = strs
					continue
				}
				wantParams[k] = v
			}
			if !reflect.DeepEqual(params, wantParams) {
				t.Errorf("bindings %#v, want %#v", params, wantParams)
			}

			if got.TotalCount != tc.Expected.TotalCount || len(got.Edges) != len(tc.Expected.Edges) {
				t.Fatalf("total %d edges %d, want %d/%d", got.TotalCount, len(got.Edges), tc.Expected.TotalCount, len(tc.Expected.Edges))
			}
			for i, want := range tc.Expected.Edges {
				edge := got.Edges[i]
				if edge.Reviewer != want.Reviewer || edge.Author != want.Author || edge.ReviewsCount != want.ReviewsCount || edge.Day.String() != want.Day {
					t.Errorf("edge %d = %+v, want %+v", i, edge, want)
				}
				if !reflect.DeepEqual(edge.RepoID, want.RepoID) {
					t.Errorf("edge %d repo id %v, want %v", i, edge.RepoID, want.RepoID)
				}
			}
		})
	}
}
