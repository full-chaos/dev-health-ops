package investmentexplain

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// repositoryRoot resolves the repo root from this test file's own path --
// same pattern as internal/jobs/investment/categorize/prompts_golden_test.go's
// categorizeRepositoryRoot.
func repositoryRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("could not determine caller for repository root resolution")
	}
	// cmd/query-api/internal/investmentexplain/prompt_golden_test.go -> repo
	// root is four directories up.
	return filepath.Join(filepath.Dir(file), "..", "..", "..", ".."), nil
}

// taggedValue decodes generate_build_prompt_golden.py's {"type": ...,
// "value": ...} tree back into the exact Go type each case needs -- same
// scheme and same reason as pythonparity's own golden test: JSON erases
// the int-vs-float distinction a bare number carries, which is exactly
// what BuildPrompt's payload encoding needs to get right.
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

type buildPromptGolden struct {
	Case       string      `json:"case"`
	BasePrompt string      `json:"base_prompt"`
	Payload    taggedValue `json:"payload"`
	Rendered   string      `json:"rendered"`
}

func TestBuildPromptMatchesPythonGolden(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "build_prompt__*.json"))
	if err != nil {
		t.Fatalf("glob goldens: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no build_prompt__*.json goldens found -- run generate_build_prompt_golden.py")
	}

	for _, path := range files {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			var golden buildPromptGolden
			if err := json.Unmarshal(data, &golden); err != nil {
				t.Fatalf("decode golden: %v", err)
			}

			if golden.BasePrompt != LoadPrompt() {
				t.Fatalf("golden's captured base_prompt no longer matches LoadPrompt() -- "+
					"prompts/investment_mix_explain_prompt.txt and the Python source have "+
					"diverged, or the golden is stale; re-run generate_build_prompt_golden.py"+
					"\n--- golden ---\n%s\n--- LoadPrompt() ---\n%s", golden.BasePrompt, LoadPrompt())
			}

			decoded, err := golden.Payload.decode()
			if err != nil {
				t.Fatalf("decode tagged payload: %v", err)
			}
			payload, ok := decoded.(map[string]any)
			if !ok {
				t.Fatalf("golden payload decoded to %T, want map[string]any", decoded)
			}

			got, err := BuildPrompt(LoadPrompt(), payload)
			if err != nil {
				t.Fatalf("BuildPrompt: %v", err)
			}
			if got != golden.Rendered {
				t.Fatalf("case %q mismatch:\n--- want (python) ---\n%s\n--- got (go) ---\n%s", golden.Case, golden.Rendered, got)
			}
		})
	}
}

func TestPromptTextMatchesPythonSource(t *testing.T) {
	// See prompt.go's promptText doc comment: this file is a byte-for-byte
	// copy of src/dev_health_ops/llm/prompts/investment_mix_explain_prompt.txt,
	// embedded rather than read at runtime. TestBuildPromptMatchesPythonGolden
	// already checks the copy against a frozen golden capture, but that
	// golden goes stale the moment someone edits the Python source without
	// re-running the generator -- this test reads the Python source
	// directly, so it catches that drift even when nobody remembers to
	// regenerate.
	repoRoot, err := repositoryRoot()
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	pythonPath := filepath.Join(repoRoot, "src", "dev_health_ops", "llm", "prompts", "investment_mix_explain_prompt.txt")
	pythonSource, err := os.ReadFile(pythonPath)
	if err != nil {
		t.Fatalf("read python source %s: %v", pythonPath, err)
	}
	if string(pythonSource) != LoadPrompt() {
		t.Fatalf("embedded prompts/investment_mix_explain_prompt.txt no longer matches %s -- "+
			"re-copy the file", pythonPath)
	}
}
