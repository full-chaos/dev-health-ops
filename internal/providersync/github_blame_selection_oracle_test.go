package providersync

import "testing"

type gitHubBlameSelectionOracleRow struct {
	Paths []string `json:"paths"`
}

func TestGitHubBlameSelectionMatchesLivePython(t *testing.T) {
	cases := []oracleCase{
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
		{ID: "bounded", Input: map[string]any{
			"file_paths":   []string{"a.go", "b.go", "c.go", "d.go", "e.go"},
			"blamed_paths": []string{"b.go"}, "max_files": 3,
		}},
	}
	compareRowsAgainstPythonOracle(
		t, "github/blame/selection", cases,
		func(t *testing.T, input map[string]any) gitHubBlameSelectionOracleRow {
			t.Helper()
			filePaths := stringsFromOracleInput(t, input["file_paths"])
			blamedPaths := stringsFromOracleInput(t, input["blamed_paths"])
			maxFiles := int(input["max_files"].(int))
			paths, _, err := selectNextGitHubBlamePaths(filePaths, blamedPaths, maxFiles)
			if err != nil {
				t.Fatal(err)
			}
			return gitHubBlameSelectionOracleRow{Paths: paths}
		}, nil,
	)
}

func stringsFromOracleInput(t *testing.T, value any) []string {
	t.Helper()
	items, ok := value.([]string)
	if !ok {
		t.Fatalf("oracle string-list input has type %T", value)
	}
	return append([]string(nil), items...)
}
