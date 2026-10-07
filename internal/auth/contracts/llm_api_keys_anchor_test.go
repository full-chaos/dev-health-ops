package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The llm_api_keys class names the Go guard that keeps an LLM API key out of
// fmt and slog. Each file below must exist and hold the symbol the class
// claims; a missing file or symbol fails, it is never skipped.
var llmAPIKeyHolders = map[string][]string{
	"internal/platform/secrets/hidden.go":                   {"type Hidden struct"},
	"internal/jobs/investment/categorize/openaiprovider.go": {"APIKey secrets.Hidden"},
	"internal/jobs/investment/categorize/localprovider.go":  {"APIKey          secrets.Hidden"},
	"internal/jobs/investment/categorize/ollamaprovider.go": {"APIKey          secrets.Hidden"},
	"internal/jobs/investment/categorize/typesafeclient.go": {"APIKey secrets.Hidden"},
	"internal/llmorgsettings/resolve.go":                    {"APIKey  secrets.Hidden"},
	"internal/apiservice/admin/llmsettingsreadiness.go":     {"apiKey   secrets.Hidden"},
	"internal/adminops/secretflag.go":                       {"value secrets.Hidden"},
}

func TestLLMAPIKeyClassAnchorsAndHoldersResolve(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Classes []map[string]any `json:"classes"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	var class map[string]any
	for _, candidate := range document.Classes {
		if candidate["class_id"] == "llm_api_keys" {
			class = candidate
		}
	}
	if class == nil {
		t.Fatal("credential-classes.json has no llm_api_keys class")
	}
	anchors := 0
	var walk func(node any)
	walk = func(node any) {
		switch typed := node.(type) {
		case map[string]any:
			if path, ok := typed["path"].(string); ok {
				if line, ok := typed["line"].(float64); ok {
					anchors++
					checkAnchor(t, root, path, int(line), typed["line_end"])
				}
			}
			for _, child := range typed {
				walk(child)
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(class)
	if anchors == 0 {
		t.Fatal("the llm_api_keys class carries no anchors: nothing was measured")
	}
	for path, symbols := range llmAPIKeyHolders {
		source, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Errorf("holder file %s does not resolve: %v", path, err)
			continue
		}
		for _, symbol := range symbols {
			if !strings.Contains(string(source), symbol) {
				t.Errorf("%s no longer holds %q", path, symbol)
			}
		}
	}
}

func checkAnchor(t *testing.T, root, path string, line int, lineEnd any) {
	t.Helper()
	source, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Errorf("anchor %s does not resolve: %v", path, err)
		return
	}
	lines := strings.Count(string(source), "\n")
	end := line
	if value, ok := lineEnd.(float64); ok {
		end = int(value)
	}
	if line < 1 || end < line || end > lines {
		t.Errorf("anchor %s:%d-%d is outside the file (%d lines)", path, line, end, lines)
	}
}
