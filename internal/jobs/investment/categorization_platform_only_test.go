package investment

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Categorization has no URL/token organization config client: no non-test file
// of this package may name the retired org config client or the worker
// llm-config route. An org's own model reaches categorization only through the
// settings path (categorize_byo_enabled, D6279; see nativeexecutor.go).
func TestCategorizationHasNoURLOrTokenOrgConfigClient(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no source files found: %v", err)
	}
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for _, banned := range []string{"orgllmconfig", "llm-config", "ORG_LLM_CONFIG", "SetOrgLLMConfig"} {
			if strings.Contains(string(body), banned) {
				t.Errorf("%s names %q: categorization has no URL/token org config client (BYO goes through the settings path)", file, banned)
			}
		}
	}
	if checked < 5 {
		t.Fatalf("only %d source files checked: the census did not run", checked)
	}
}
