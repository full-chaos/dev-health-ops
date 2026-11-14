package contracts

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

const (
	modelCacheInvalidates = "invalidates the in-process model list cache"
	modelCacheFile        = "internal/apiservice/admin/llmsettings.go"
	modelCacheCall        = "invalidateOrg"
)

// The symbol each llm-settings write row must cite for its cache invalidation.
var modelCacheSymbolByMethod = map[string]string{"PUT": "handlers.applyLLMSettings", "DELETE": "handlers.deleteLLMSettings"}

var modelCacheLineCite = regexp.MustCompile(`llmsettings\.go:\d+`)

// modelCacheCiteProblems judges the cache-invalidation cite of one row text: it names llmsettings.go by symbol (never by line,
// the two calls read the same), the symbol is the handler of the row's method, and the quoted anchor holds the call.
func modelCacheCiteProblems(root, method, text string) []string {
	var problems []string
	if cite := modelCacheLineCite.FindString(text); cite != "" {
		problems = append(problems, "cites "+cite+" by line: the two invalidateOrg calls read the same, cite the symbol with the call as its anchor")
	}
	symbols := map[symbolCite]bool{}
	refused := map[string]string{}
	takeSymbolCites(text, symbols, refused)
	for token, why := range refused {
		problems = append(problems, token+" "+why)
	}
	found := false
	for c := range symbols {
		if c.file != modelCacheFile {
			continue
		}
		found = true
		if want := modelCacheSymbolByMethod[method]; c.symbol != want {
			problems = append(problems, "cites symbol "+c.symbol+", the "+method+" row must cite "+want)
		}
		if !strings.Contains(c.anchor, modelCacheCall) {
			problems = append(problems, "the anchor "+c.anchor+" does not hold "+modelCacheCall)
			continue
		}
		source, err := os.ReadFile(filepath.Join(root, modelCacheFile))
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		if _, err := routescan.ResolveSymbol(source, c.file, c.symbol, c.anchor); err != nil {
			problems = append(problems, err.Error())
		}
	}
	if !found {
		problems = append(problems, "names no symbol cite of "+modelCacheFile)
	}
	return problems
}

// CHAOS-13218. The PUT and DELETE llm-settings rows say they invalidate the model list cache. Each cites the handler that makes the
// call, by symbol with the call as the anchor; a line cite there read the wrong line for two commits and no check noticed.
func TestEveryModelListCacheInvalidationCiteAnchorsTheInvalidateOrgCall(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "endpoint-profiles.go-api.json"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, item := range loadGoAPIDocument(t)["rows"].([]any) {
		row := item.(map[string]any)
		text, _ := row["current_state_cache_behavior"].(string)
		if !strings.Contains(text, modelCacheInvalidates) {
			continue
		}
		method, _ := row["method"].(string)
		id, _ := row["id"].(string)
		seen[method] = true
		at := strings.Index(string(raw), strings.ReplaceAll(strings.ReplaceAll(text, `\`, `\\`), `"`, `\"`))
		line := 1 + strings.Count(string(raw[:max(at, 0)]), "\n")
		for _, problem := range modelCacheCiteProblems(root, method, text) {
			t.Errorf("%s (endpoint-profiles.go-api.json:%d): %s", id, line, problem)
		}
	}
	if len(seen) != 2 || !seen["PUT"] || !seen["DELETE"] {
		t.Fatalf("the rows that invalidate the model list cache are %v, want the PUT and the DELETE row: the check would measure nothing", seen)
	}
}

// The cell itself refuses the three wrong forms, so a green run on the real rows is not a vacuous one.
func TestModelListCacheCiteCellRefusesTheWrongForms(t *testing.T) {
	root := testRoot(t)
	good := "the route invalidates the in-process model list cache for the organization (" + modelCacheFile + "#handlers.applyLLMSettings`h.modelCache.invalidateOrg(orgID)`; cache type internal/apiservice/admin/llmmodels.go:62)"
	if problems := modelCacheCiteProblems(root, "PUT", good); len(problems) != 0 {
		t.Fatalf("the correct PUT cite was refused: %v", problems)
	}
	cases := map[string]struct{ method, text string }{
		"a line cite":                      {"PUT", "invalidates the in-process model list cache (" + modelCacheFile + ":548)"},
		"the right anchor, a wrong symbol": {"PUT", "invalidates the in-process model list cache (" + modelCacheFile + "#handlers.getLLMSettings`h.modelCache.invalidateOrg(orgID)`)"},
		"the other row's symbol":           {"PUT", "invalidates the in-process model list cache (" + modelCacheFile + "#handlers.deleteLLMSettings`h.modelCache.invalidateOrg(orgID)`)"},
		"an anchor without the call":       {"DELETE", "invalidates the in-process model list cache (" + modelCacheFile + "#handlers.deleteLLMSettings`credentialsDeleted == 0`)"},
		"no symbol cite":                   {"DELETE", "invalidates the in-process model list cache"},
	}
	for name, c := range cases {
		if problems := modelCacheCiteProblems(root, c.method, c.text); len(problems) == 0 {
			t.Errorf("%s was accepted: %s", name, c.text)
		}
	}
}
