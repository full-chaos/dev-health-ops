package investmentexplain

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

type resolveModelNameGolden struct {
	Case  string `json:"case"`
	Input struct {
		Provider string            `json:"provider"`
		Model    *string           `json:"model"`
		Env      map[string]string `json:"env"`
	} `json:"input"`
	Result *string `json:"result"`
}

func TestResolveModelNameMatchesPythonGolden(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "resolve_model_name__*.json"))
	if err != nil {
		t.Fatalf("glob goldens: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no resolve_model_name__*.json goldens found -- run generate_resolve_model_name_golden.py")
	}

	// Every env var any case sets, cleared up front so a case that omits
	// one doesn't inherit a value a PRIOR subtest set via t.Setenv (t.Setenv
	// restores per-test, but subtests share the parent's process-wide env
	// until their own cleanup runs, so an explicit clear keeps cases
	// order-independent rather than relying on subtest isolation timing).
	for _, name := range []string{"LLM_MODEL", "LLM_MODEL_OPENAI", "LLM_MODEL_LOCAL", "LOCAL_LLM_MODEL"} {
		t.Setenv(name, "")
		_ = os.Unsetenv(name)
	}

	for _, path := range files {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			var golden resolveModelNameGolden
			if err := json.Unmarshal(data, &golden); err != nil {
				t.Fatalf("decode golden: %v", err)
			}

			for _, name := range []string{"LLM_MODEL", "LLM_MODEL_OPENAI", "LLM_MODEL_LOCAL", "LOCAL_LLM_MODEL"} {
				t.Setenv(name, "")
				_ = os.Unsetenv(name)
			}
			for name, value := range golden.Input.Env {
				t.Setenv(name, value)
			}

			model := ""
			if golden.Input.Model != nil {
				model = *golden.Input.Model
			}

			got, found := ResolveModelName(categorize.ProviderKind(golden.Input.Provider), model)

			if golden.Result == nil {
				if found {
					t.Fatalf("case %q: want not-found, got %q", golden.Case, got)
				}
				return
			}
			if !found || got != *golden.Result {
				t.Fatalf("case %q: want %q, got %q (found=%v)", golden.Case, *golden.Result, got, found)
			}
		})
	}
}
