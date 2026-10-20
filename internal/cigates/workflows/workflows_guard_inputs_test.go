package workflows_test

import "testing"

// Inputs of the Go guards in internal/goapicli/routing (docs_guards_test.go): the SDL and the digest pin, the history doc the
// digest must be a row of, the runbook whose enable commands are checked, and the verb source whose flags define the vocabulary.
// A pull request that changes only one of them must run the Go tests, so each one must match go.yml's own push and pull_request
// path filters. The doc and the runbook are Markdown, which the path-filter scan of Go string literals does not read.
var routingGuardInputs = []string{
	"contracts/graphql/v1/schema.graphql",
	"contracts/graphql/v1/schema-digest.json",
	"docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md",
	"docs/operate/runbooks/query-api-bootstrap.md",
	"internal/goapicli/routing/enable.go",
}

func filterMisses(t *testing.T, files, patterns []string) []string {
	t.Helper()
	var missed []string
	for _, f := range files {
		if !matchesAny(t, f, patterns) {
			missed = append(missed, f)
		}
	}
	return missed
}

func TestRoutingGuardInputsAreInGoYmlPathFilters(t *testing.T) {
	root := repoRoot(t)
	for _, event := range []string{"push", "pull_request"} {
		patterns := goPaths(t, root, event)
		if len(patterns) == 0 {
			t.Fatalf("go.yml declares no %s paths", event)
		}
		if missed := filterMisses(t, routingGuardInputs, patterns); len(missed) != 0 {
			t.Errorf("go.yml's %s path filter does not select inputs of the routing docs guards: %v", event, missed)
		}
	}
}

// Planted defect: a filter without the history doc must be reported, and one that covers every input must not be.
func TestRoutingGuardInputsPlantedDefect(t *testing.T) {
	without := []string{"**/*.go", "contracts/**", "docs/operate/runbooks/query-api-bootstrap.md"}
	missed := filterMisses(t, routingGuardInputs, without)
	if len(missed) != 1 || missed[0] != "docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md" {
		t.Fatalf("a filter without the history doc reported %v, want exactly the history doc", missed)
	}
	with := append([]string{"docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md"}, without...)
	if missed := filterMisses(t, routingGuardInputs, with); len(missed) != 0 {
		t.Fatalf("a filter naming every input still reported %v", missed)
	}
}
