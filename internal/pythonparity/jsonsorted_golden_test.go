package pythonparity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// jsonSortedGolden mirrors jsonIndentSortedGolden's shape (same tagged
// input scheme, defined in jsonindentsorted_golden_test.go, reused here
// since both goldens are generated the same way for the same reason).
type jsonSortedGolden struct {
	Case   string      `json:"case"`
	Input  taggedValue `json:"input"`
	Output string      `json:"output"`
}

func TestMarshalPythonJSONSortedMatchesPythonGolden(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "json_sorted__*.json"))
	if err != nil {
		t.Fatalf("glob goldens: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no json_sorted__*.json goldens found -- run generate_json_sorted_golden.py")
	}

	for _, path := range files {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			var golden jsonSortedGolden
			if err := json.Unmarshal(data, &golden); err != nil {
				t.Fatalf("decode golden: %v", err)
			}

			input, err := golden.Input.decode()
			if err != nil {
				t.Fatalf("decode tagged input: %v", err)
			}

			got, err := MarshalPythonJSONSorted(input)
			if err != nil {
				t.Fatalf("MarshalPythonJSONSorted: %v", err)
			}

			if string(got) != golden.Output {
				t.Fatalf("case %q mismatch:\n--- want (python) ---\n%s\n--- got (go) ---\n%s", golden.Case, golden.Output, got)
			}
		})
	}
}
