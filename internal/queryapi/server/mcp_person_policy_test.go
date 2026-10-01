package server

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

func allowlistedRoots() []string {
	var roots []string
	for root := range mcpRootFieldAllowlist {
		roots = append(roots, root)
	}
	sort.Strings(roots)
	return roots
}

// lintMCPInputClasses returns what is wrong with a class table: a position
// whose name carries a person word but is not classed person.
func lintMCPInputClasses(classes map[string]string) []string {
	var bad []string
	for key, class := range classes {
		position := strings.Fields(key)[1]
		name := position[strings.LastIndex(position, ".")+1:]
		if mcpIsPersonName(name) && class != "person" {
			bad = append(bad, key+" is classed "+class+" but its name carries a person word")
		}
	}
	sort.Strings(bad)
	return bad
}

// Every position a request value can land on, reachable from an allow-listed
// root field, has an explicit class in mcp_input_classes.txt. A schema change
// that adds one -- under ANY name -- fails here, and at runtime the listener
// refuses the position as unclassified until a PR classifies it. There is no
// human gate outside the PR: the table is the source of truth, the build is
// the gate.
func TestMCPInputClassesCoverEveryReachablePositionExactly(t *testing.T) {
	es := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})
	reachable := mcpReachableInputMembers(es.Schema(), allowlistedRoots())
	inSchema := map[string]bool{}
	for _, line := range reachable {
		inSchema[line] = true
		if mcpInputClasses[line] == "" {
			t.Errorf("reachable position has no class (the listener refuses it as unclassified): %s", line)
		}
	}
	for key := range mcpInputClasses {
		if !inSchema[key] {
			t.Errorf("class for a position no allow-listed root reaches any more: %s", key)
		}
	}
	if bad := lintMCPInputClasses(mcpInputClasses); len(bad) > 0 {
		t.Errorf("person-named positions not classed person:\n%s", strings.Join(bad, "\n"))
	}
}

func TestMCPPersonPositionsAreExactlyTheReviewedFour(t *testing.T) {
	var person []string
	for key, class := range mcpInputClasses {
		if class == "person" {
			person = append(person, key)
		}
	}
	sort.Strings(person)
	want := []string{"enum DimensionInput.AUTHOR", "enum ScopeLevelInput.DEVELOPER", "input FilterInput.who", "input WhoFilterInput.developers"}
	if strings.Join(person, "|") != strings.Join(want, "|") {
		t.Fatalf("person positions = %v, want %v", person, want)
	}
}

// Plants: a person selector under a NEUTRAL name, and a classified-other
// person-worded name. Both must be caught.
func TestMCPUnclassifiedPositionIsRefusedAndAPersonNamedOtherIsLinted(t *testing.T) {
	schema, err := gqlparser.LoadSchema(&ast.Source{Name: "plant", Input: `
		type Query { people(filter: HandleFilter, kind: Kind): String }
		input HandleFilter { handle: String }
		enum Kind { ENGINEER_OF_RECORD }
	`})
	if err != nil {
		t.Fatal(err)
	}
	field := schema.Query.Fields.ForName("people")
	filter := field.Arguments.ForName("filter")
	if got := mcpClassifyValue(schema, filter.Type, map[string]any{"handle": "ada"}); got != mcpInputUnclassified {
		t.Fatalf("an unlisted input field under a neutral name = verdict %d, want unclassified (fail closed)", got)
	}
	if got := mcpClassifyValue(schema, field.Arguments.ForName("kind").Type, "ENGINEER_OF_RECORD"); got != mcpInputUnclassified {
		t.Fatalf("an unlisted enum value = verdict %d, want unclassified", got)
	}
	if got := mcpClassifyArgument(schema, "Query", "people", filter, map[string]any{}); got != mcpInputUnclassified {
		t.Fatalf("an unlisted argument position = verdict %d, want unclassified", got)
	}
	if bad := lintMCPInputClasses(map[string]string{"input HandleFilter.reviewerHandle": "other"}); len(bad) != 1 {
		t.Fatalf("a person-worded name classed other must be linted, got %v", bad)
	}
	if got := fmt.Sprint(mcpReachableInputMembers(schema, []string{"people"})); !strings.Contains(got, "input HandleFilter.handle") || !strings.Contains(got, "enum Kind.ENGINEER_OF_RECORD") || !strings.Contains(got, "arg Query.people.filter") {
		t.Fatalf("the reachable lister missed a position: %s", got)
	}
}

func TestMCPPersonNamesAreRecognisedPerToken(t *testing.T) {
	for name, want := range map[string]bool{
		"DEVELOPER": true, "AUTHOR": true, "who": true, "developers": true,
		"AUTHOR_MEMBERSHIP": true, "reviewerId": true, "assigneeEmail": true, "userLogin": true,
		"REPO": false, "teamId": false, "repoIds": false, "authority": false, "memberless": false,
		"search": false, "WORK_TYPE": false, "whole": false, "reviewLoad": false,
	} {
		if got := mcpIsPersonName(name); got != want {
			t.Errorf("mcpIsPersonName(%q) = %v, want %v", name, got, want)
		}
	}
}

// The runtime half: with one reachable position missing from the table, the
// MCP listener refuses a request that uses it, before any ClickHouse call.
func TestMCPListenerRefusesAnUnclassifiedPositionAtRuntime(t *testing.T) {
	const key = "input ScopeFilterInput.ids"
	class, ok := mcpInputClasses[key]
	if !ok {
		t.Fatalf("%s is not in the table", key)
	}
	delete(mcpInputClasses, key)
	defer func() { mcpInputClasses[key] = class }()
	ch := &countingMCPClient{}
	l := newMCPTestListeners(t, ch, allMCPRootsEnabled(), mcpDefaultLimits())
	query := fmt.Sprintf("query { catalog(orgId: %q, filters: {scope: {level: TEAM, ids: [\"t\"]}}) { values { value } } }", mcpTestOrg)
	rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, query, nil))
	assertMCPRefused(t, rec, ch, http.StatusForbidden, mcpReasonUnclassifiedInput)
}

// A selected field the validator left without its definitions cannot be
// classified; it is refused, never skipped.
func TestMCPCheckRequestInputsRefusesAFieldWithoutDefinitions(t *testing.T) {
	es := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})
	op := &ast.OperationDefinition{
		Operation: ast.Query,
		SelectionSet: ast.SelectionSet{&ast.Field{
			Name:      "catalog",
			Arguments: ast.ArgumentList{{Name: "orgId", Value: &ast.Value{Kind: ast.StringValue, Raw: mcpTestOrg}}},
		}},
	}
	status, reason := mcpCheckRequestInputs(es.Schema(), op, nil, nil, nil, mcpTestOrg)
	if status != http.StatusForbidden || reason != mcpReasonUnclassifiedInput {
		t.Fatalf("status %d reason %q, want 403 %s", status, reason, mcpReasonUnclassifiedInput)
	}
}
