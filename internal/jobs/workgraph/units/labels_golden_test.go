package units

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type labelGoldenWorkItem struct {
	Title string `json:"title"`
	Type  string `json:"type"`
}

type labelGoldenPR struct {
	Title string `json:"title"`
}

type labelGoldenCommit struct {
	Message string `json:"message"`
}

type labelGoldenCase struct {
	Label        string                         `json:"label"`
	IssueIDs     []string                       `json:"issue_ids"`
	PRIDs        []string                       `json:"pr_ids"`
	CommitIDs    []string                       `json:"commit_ids"`
	WorkItemMap  map[string]labelGoldenWorkItem `json:"work_item_map"`
	PRMap        map[string]labelGoldenPR       `json:"pr_map"`
	CommitMap    map[string]labelGoldenCommit   `json:"commit_map"`
	ExpectedType *string                        `json:"expected_type"`
	ExpectedName *string                        `json:"expected_name"`
}

type labelGoldenDocument struct {
	Cases []labelGoldenCase `json:"cases"`
}

func loadLabelGolden(t *testing.T) labelGoldenDocument {
	t.Helper()
	path := filepath.Join(repositoryRootPath(t), "tests", "fixtures", "work_unit_label_python_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var doc labelGoldenDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	return doc
}

// TestResolveWorkUnitLabelMatchesPythonGolden is the EXHAUSTIVE test: every
// case in the frozen golden, not a hand-picked subset.
func TestResolveWorkUnitLabelMatchesPythonGolden(t *testing.T) {
	doc := loadLabelGolden(t)
	if len(doc.Cases) == 0 {
		t.Fatal("golden carries zero cases -- a vacuous corpus proves nothing")
	}
	for _, testCase := range doc.Cases {
		t.Run(testCase.Label, func(t *testing.T) {
			workItems := make(map[string]WorkItemLabelFields, len(testCase.WorkItemMap))
			for id, item := range testCase.WorkItemMap {
				workItems[id] = WorkItemLabelFields{Title: item.Title, Type: item.Type}
			}
			prs := make(map[string]PRLabelFields, len(testCase.PRMap))
			for id, pr := range testCase.PRMap {
				prs[id] = PRLabelFields{Title: pr.Title}
			}
			commits := make(map[string]CommitLabelFields, len(testCase.CommitMap))
			for id, commit := range testCase.CommitMap {
				commits[id] = CommitLabelFields{Message: commit.Message}
			}

			gotType, gotName := ResolveWorkUnitLabel(ResolveWorkUnitLabelInput{
				IssueIDs: testCase.IssueIDs, PRIDs: testCase.PRIDs, CommitIDs: testCase.CommitIDs,
				WorkItems: workItems, PRs: prs, Commits: commits,
			})

			if !equalStringPointers(gotType, testCase.ExpectedType) {
				t.Fatalf("type = %v, want %v", derefOrNil(gotType), derefOrNil(testCase.ExpectedType))
			}
			if !equalStringPointers(gotName, testCase.ExpectedName) {
				t.Fatalf("name = %v, want %v", derefOrNil(gotName), derefOrNil(testCase.ExpectedName))
			}
		})
	}
}
