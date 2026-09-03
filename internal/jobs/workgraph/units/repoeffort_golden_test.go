package units

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
)

type repoEffortGoldenCase struct {
	Label        string             `json:"label"`
	CommitIDs    []string           `json:"commit_ids"`
	PRIDs        []string           `json:"pr_ids"`
	CommitChurn  map[string]any     `json:"commit_churn"`
	PRChurn      map[string]any     `json:"pr_churn"`
	EffortMetric string             `json:"effort_metric"`
	EffortValue  any                `json:"effort_value"`
	Allocations  []repoAllocationJS `json:"allocations"`
}

type repoAllocationJS struct {
	RepoID           *string `json:"repo_id"`
	RepoEffort       any     `json:"repo_effort"`
	AllocationWeight any     `json:"allocation_weight"`
	AllocationSource string  `json:"allocation_source"`
}

type repoEffortGoldenDocument struct {
	Cases []repoEffortGoldenCase `json:"cases"`
}

// jsonNumberToFloat undoes the golden generator's _num encoding: "nan" and
// "inf"/"-inf" as strings (JSON has no literal for them), everything else a
// plain JSON number.
func jsonNumberToFloat(t *testing.T, value any) float64 {
	t.Helper()
	switch v := value.(type) {
	case float64:
		return v
	case string:
		switch v {
		case "nan":
			return math.NaN()
		case "inf":
			return math.Inf(1)
		case "-inf":
			return math.Inf(-1)
		}
		t.Fatalf("unrecognised numeric string %q in golden", v)
	}
	t.Fatalf("unrecognised numeric encoding %#v (%T) in golden", value, value)
	return 0
}

func churnMap(t *testing.T, raw map[string]any) map[string]float64 {
	t.Helper()
	out := make(map[string]float64, len(raw))
	for key, value := range raw {
		out[key] = jsonNumberToFloat(t, value)
	}
	return out
}

func loadRepoEffortGolden(t *testing.T) repoEffortGoldenDocument {
	t.Helper()
	path := filepath.Join(repositoryRootPath(t), "tests", "fixtures", "repo_effort_allocation_python_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var doc repoEffortGoldenDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	return doc
}

// TestAllocateRepoEffortMatchesPythonGolden is the EXHAUSTIVE test: every
// case in the frozen golden, including the sort order the golden itself
// pins (assertions below compare allocations SLICE-POSITIONALLY, not as an
// unordered set, so a wrong sort would fail here even if the set of rows
// were otherwise correct).
func TestAllocateRepoEffortMatchesPythonGolden(t *testing.T) {
	doc := loadRepoEffortGolden(t)
	if len(doc.Cases) == 0 {
		t.Fatal("golden carries zero cases -- a vacuous corpus proves nothing")
	}
	for _, testCase := range doc.Cases {
		t.Run(testCase.Label, func(t *testing.T) {
			input := AllocateRepoEffortInput{
				CommitIDs:    testCase.CommitIDs,
				PRIDs:        testCase.PRIDs,
				CommitChurn:  churnMap(t, testCase.CommitChurn),
				PRChurn:      churnMap(t, testCase.PRChurn),
				EffortMetric: testCase.EffortMetric,
				EffortValue:  jsonNumberToFloat(t, testCase.EffortValue),
			}

			got := AllocateRepoEffort(input)

			if len(got) != len(testCase.Allocations) {
				t.Fatalf("len(allocations) = %d, want %d\ngot:  %+v\nwant: %+v",
					len(got), len(testCase.Allocations), got, testCase.Allocations)
			}
			for i, want := range testCase.Allocations {
				row := got[i]
				gotRepoID := (*string)(nil)
				if row.RepoID != nil {
					s := row.RepoID.String()
					gotRepoID = &s
				}
				if !equalStringPointers(gotRepoID, want.RepoID) {
					t.Fatalf("row %d repo_id = %v, want %v", i, derefOrNil(gotRepoID), derefOrNil(want.RepoID))
				}
				wantEffort := jsonNumberToFloat(t, want.RepoEffort)
				if row.RepoEffort != wantEffort {
					t.Fatalf("row %d repo_effort = %v, want %v", i, row.RepoEffort, wantEffort)
				}
				wantWeight := jsonNumberToFloat(t, want.AllocationWeight)
				if row.AllocationWeight != wantWeight {
					t.Fatalf("row %d allocation_weight = %v, want %v", i, row.AllocationWeight, wantWeight)
				}
				if row.AllocationSource != want.AllocationSource {
					t.Fatalf("row %d allocation_source = %q, want %q", i, row.AllocationSource, want.AllocationSource)
				}
			}
		})
	}
}

func equalStringPointers(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func derefOrNil(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}
