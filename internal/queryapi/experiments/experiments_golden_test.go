package experiments

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/opportunities"
)

type goldenCard struct {
	ID        string   `json:"id"`
	Title     string   `json:"title"`
	Suggested []string `json:"suggested"`
}

type goldenCall struct {
	OrgID       string   `json:"org_id"`
	Level       string   `json:"level"`
	IDs         []string `json:"ids"`
	RangeDays   int      `json:"range_days"`
	CompareDays int      `json:"compare_days"`
}

type goldenItem struct {
	ID            string  `json:"id"`
	OpportunityID string  `json:"opportunity_id"`
	Hypothesis    string  `json:"hypothesis"`
	Metric        string  `json:"metric"`
	Owner         string  `json:"owner"`
	StopCondition string  `json:"stop_condition"`
	Status        string  `json:"status"`
	StartDate     *string `json:"start_date"`
	StopDate      *string `json:"stop_date"`
	Outcome       *string `json:"outcome"`
}

type goldenCase struct {
	Name        string `json:"name"`
	HasFilters  bool   `json:"has_filters"`
	FilterScope *struct {
		Level string   `json:"level"`
		IDs   []string `json:"ids"`
	} `json:"filter_scope"`
	Cards       []goldenCard `json:"cards"`
	Fail        bool         `json:"fail"`
	PythonCalls []goldenCall `json:"python_calls"`
	Expected    struct {
		Derived bool         `json:"derived"`
		Items   []goldenItem `json:"items"`
	} `json:"expected"`
}

// TestExperimentsMatchesTheFrozenGolden runs the Go resolver over the requests
// the deleted reference resolver was run over: it compares the scope and window
// handed to the opportunity build, the organisation, and every field of the
// answer, including an empty non-nil item list and the failure disposition.
func TestExperimentsMatchesTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/experiments_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 21 {
		t.Fatalf("golden holds %d cases, want at least 21", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			var calls []goldenCall
			build := func(_ context.Context, org string, f home.Filters, _ time.Time) (*opportunities.Response, error) {
				calls = append(calls, goldenCall{OrgID: org, Level: f.Scope.Level, IDs: append([]string{}, f.Scope.IDs...), RangeDays: f.Time.RangeDays, CompareDays: f.Time.CompareDays})
				if tc.Fail {
					return nil, errors.New("boom")
				}
				resp := &opportunities.Response{}
				for _, c := range tc.Cards {
					resp.Items = append(resp.Items, opportunities.Card{ID: c.ID, Title: c.Title, SuggestedExperiments: c.Suggested})
				}
				return resp, nil
			}
			var in *model.FilterInput
			if tc.HasFilters {
				in = &model.FilterInput{}
				if tc.FilterScope != nil {
					in.Scope = &model.ScopeFilterInput{Level: model.ScopeLevelInput(upper(tc.FilterScope.Level)), Ids: tc.FilterScope.IDs}
				}
			}
			got := Resolve(context.Background(), build, "org-1", in, time.Unix(0, 0))

			if len(calls) != len(tc.PythonCalls) {
				t.Fatalf("%d builds, want %d", len(calls), len(tc.PythonCalls))
			}
			for i := range calls {
				if !sameCall(calls[i], tc.PythonCalls[i]) {
					t.Errorf("build %d = %+v, want %+v", i, calls[i], tc.PythonCalls[i])
				}
			}
			if got.DerivedFromOpportunities != tc.Expected.Derived {
				t.Errorf("derived = %v, want %v", got.DerivedFromOpportunities, tc.Expected.Derived)
			}
			if got.Items == nil {
				t.Fatal("items is nil, want a non-nil list")
			}
			if len(got.Items) != len(tc.Expected.Items) {
				t.Fatalf("%d items, want %d", len(got.Items), len(tc.Expected.Items))
			}
			for i, want := range tc.Expected.Items {
				g := got.Items[i]
				if g.ID != want.ID || g.OpportunityID != want.OpportunityID || g.Hypothesis != want.Hypothesis || g.Metric != want.Metric ||
					g.Owner != want.Owner || g.StopCondition != want.StopCondition || string(g.Status) != upper(want.Status) ||
					g.StartDate != nil || g.StopDate != nil || g.Outcome != nil || want.StartDate != nil || want.StopDate != nil || want.Outcome != nil {
					t.Errorf("item %d = %+v, want %+v", i, g, want)
				}
			}
		})
	}
}

func sameCall(a, b goldenCall) bool {
	if a.IDs == nil {
		a.IDs = []string{}
	}
	if b.IDs == nil {
		b.IDs = []string{}
	}
	return reflect.DeepEqual(a, b)
}

func upper(s string) string {
	out := []byte(s)
	for i, c := range out {
		if c >= 'a' && c <= 'z' {
			out[i] = c - 32
		}
	}
	return string(out)
}
