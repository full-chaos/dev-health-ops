package pythonparity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// taggedValue decodes testdata/generate_json_indent_sorted_golden.py's
// {"type": ..., "value": ...} tree back into the exact Go type each case
// needs -- json.Unmarshal alone can't recover the int-vs-float distinction
// a bare JSON number erases, which is precisely what
// MarshalPythonJSONIndentSorted exists to get right, so the golden's input
// must carry that distinction explicitly rather than let decoding guess it
// away.
type taggedValue struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

func (t taggedValue) decode() (any, error) {
	switch t.Type {
	case "null":
		return nil, nil
	case "bool":
		var v bool
		err := json.Unmarshal(t.Value, &v)
		return v, err
	case "int":
		var v int64
		if err := json.Unmarshal(t.Value, &v); err != nil {
			return nil, err
		}
		return int(v), nil
	case "float":
		var v float64
		err := json.Unmarshal(t.Value, &v)
		return v, err
	case "string":
		var v string
		err := json.Unmarshal(t.Value, &v)
		return v, err
	case "list":
		var raw []taggedValue
		if err := json.Unmarshal(t.Value, &raw); err != nil {
			return nil, err
		}
		out := make([]any, len(raw))
		for i, elem := range raw {
			decoded, err := elem.decode()
			if err != nil {
				return nil, err
			}
			out[i] = decoded
		}
		return out, nil
	case "map":
		var raw map[string]taggedValue
		if err := json.Unmarshal(t.Value, &raw); err != nil {
			return nil, err
		}
		out := make(map[string]any, len(raw))
		for k, elem := range raw {
			decoded, err := elem.decode()
			if err != nil {
				return nil, err
			}
			out[k] = decoded
		}
		return out, nil
	default:
		panic("unknown tagged value type: " + t.Type)
	}
}

type jsonIndentSortedGolden struct {
	Case   string      `json:"case"`
	Input  taggedValue `json:"input"`
	Output string      `json:"output"`
}

func TestMarshalPythonJSONIndentSortedMatchesPythonGolden(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "json_indent_sorted__*.json"))
	if err != nil {
		t.Fatalf("glob goldens: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no json_indent_sorted__*.json goldens found -- run generate_json_indent_sorted_golden.py")
	}

	for _, path := range files {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			var golden jsonIndentSortedGolden
			if err := json.Unmarshal(data, &golden); err != nil {
				t.Fatalf("decode golden: %v", err)
			}

			input, err := golden.Input.decode()
			if err != nil {
				t.Fatalf("decode tagged input: %v", err)
			}

			got, err := MarshalPythonJSONIndentSorted(input)
			if err != nil {
				t.Fatalf("MarshalPythonJSONIndentSorted: %v", err)
			}

			if string(got) != golden.Output {
				t.Fatalf("case %q mismatch:\n--- want (python) ---\n%s\n--- got (go) ---\n%s", golden.Case, golden.Output, got)
			}
		})
	}
}
