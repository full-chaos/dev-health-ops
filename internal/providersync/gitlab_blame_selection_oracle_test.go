package providersync

import "testing"

type gitLabBlameSelectionOracleRow struct {
	Paths []string `json:"paths"`
}

func TestGitLabBlameSelectionMatchesLivePython(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "gitlab/blame/selection", []oracleCase{
			{ID: "empty", Input: map[string]any{
				"file_paths": []string{}, "blamed_paths": []string{}, "max_files": 3,
			}},
			{ID: "partial", Input: map[string]any{
				"file_paths":   []string{"z.go", "a.go", "m.go", "n.go"},
				"blamed_paths": []string{"a.go"}, "max_files": 2,
			}},
			{ID: "complete", Input: map[string]any{
				"file_paths":   []string{"a.go", "b.go"},
				"blamed_paths": []string{"a.go", "b.go"}, "max_files": 3,
			}},
			{ID: "duplicate_tree_entry", Input: map[string]any{
				"file_paths":   []string{"a.go", "b.go", "a.go"},
				"blamed_paths": []string{"b.go"}, "max_files": 2,
			}},
		},
		func(t *testing.T, input map[string]any) gitLabBlameSelectionOracleRow {
			filePaths := stringsFromOracleInput(t, input["file_paths"])
			blamedPaths := stringsFromOracleInput(t, input["blamed_paths"])
			maxFiles := int(input["max_files"].(int))
			paths, _, err := selectNextGitLabBlamePaths(filePaths, blamedPaths, maxFiles)
			if err != nil {
				t.Fatal(err)
			}
			return gitLabBlameSelectionOracleRow{Paths: paths}
		}, nil,
	)
}
