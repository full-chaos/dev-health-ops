package server

import (
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

// The guard CHAOS-7330 asks for. What selects a person is derived from the
// schema (mcp_person_policy.go); this pins what the derivation sees. Every
// argument, input-object field and enum value reachable from an allow-listed
// root field's arguments is in the golden with its classification. A schema
// change that adds or renames one -- under ANY name, person word or not --
// fails this test until a reviewer classifies the new member in the golden.
func TestMCPReachableInputsMatchTheReviewedGolden(t *testing.T) {
	es := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})
	var roots []string
	for root := range mcpRootFieldAllowlist {
		roots = append(roots, root)
	}
	sort.Strings(roots)
	got := mcpReachableInputMembers(es.Schema(), roots)
	raw, err := os.ReadFile("testdata/mcp_reachable_inputs.golden")
	if err != nil {
		t.Fatalf("the reviewed golden is missing, which is a failure, not a skip: %v", err)
	}
	want := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	gotSet, wantSet := map[string]bool{}, map[string]bool{}
	for _, line := range got {
		gotSet[line] = true
	}
	for _, line := range want {
		wantSet[line] = true
	}
	for _, line := range got {
		if !wantSet[line] {
			t.Errorf("new or reclassified reachable input member not in the golden (review whether it selects a person): %s", line)
		}
	}
	for _, line := range want {
		if !gotSet[line] {
			t.Errorf("golden member no longer in the schema: %s", line)
		}
	}
}

// The person set the golden classifies, pinned by name: the four members the
// MCP class refuses today. Adding a person-named member to the schema changes
// this list through the golden test above.
func TestMCPPersonMembersAreExactlyTheReviewedFour(t *testing.T) {
	raw, err := os.ReadFile("testdata/mcp_reachable_inputs.golden")
	if err != nil {
		t.Fatal(err)
	}
	var person []string
	for _, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		if strings.HasSuffix(line, " person") {
			person = append(person, line)
		}
	}
	want := []string{
		"enum DimensionInput.AUTHOR person",
		"enum ScopeLevelInput.DEVELOPER person",
		"input FilterInput.who person",
		"input WhoFilterInput.developers person",
	}
	if strings.Join(person, "|") != strings.Join(want, "|") {
		t.Fatalf("person members = %v, want %v", person, want)
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
