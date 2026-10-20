package investment

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Categorization is served by the platform provider only. No non-test file of
// this package may name an organization config client or the worker llm-config
// route, so an org's BYO config cannot reach the categorization call.
func TestCategorizationHasNoOrgConfigClient(t *testing.T) {
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
				t.Errorf("%s names %q: categorization must not read an organization's BYO config", file, banned)
			}
		}
	}
	if checked < 5 {
		t.Fatalf("only %d source files checked: the census did not run", checked)
	}
}
