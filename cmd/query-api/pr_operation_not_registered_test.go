package main

// CHAOS-4980 condition 2 (team-lead ruling): the Pr resolver ships in this
// PR as a PARTIAL port (linkedIssues + a nil-for-unknown existence check
// only -- see schema.resolvers.go's Pr doc comment and workgraph/pr.go's).
// It must not be reachable as a served GraphQL operation until the PR
// core row, reviews, and commits are also ported (a follow-up ticket).
//
// FIXED (codex round 1 on #2190, P2): the first version of this guard
// only checked four candidate STRINGS ("pr"/"Pr"/"prDetail"/"PrDetail")
// against digestByOperation's KEYS -- but routeswitch dispatches purely by
// an operation's registered document/digest, never by which GraphQL
// fields that document happens to SELECT. A future document registered
// under an entirely different operation name (e.g. "getPullRequest")
// whose query text still selects the `pr` field would pass the old
// candidate-string check while making this partial resolver reachable --
// codex constructed exactly this case and confirmed it slips past the old
// version. This version instead walks every registered document's
// selection set (reusing registered_document_field_gate_test.go's own
// gqlparser-based `collectSelectedFields`/schema-loading machinery, same
// package/directory, same discipline: derive from source, never
// hand-copy) and fails on ANY document -- under ANY operation name --
// whose root selection includes `Query.pr`. This closes the gap
// structurally: it no longer matters what a future operation is named.
import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vektah/gqlparser/v2"
	gqlast "github.com/vektah/gqlparser/v2/ast"
)

func TestPrOperationIsNotRegistered(t *testing.T) {
	repoRoot := documentFieldGateRepoRoot(t)

	sdlPath := filepath.Join(repoRoot, "contracts", "graphql", "v1", "schema.graphql")
	sdlBytes, err := os.ReadFile(sdlPath)
	if err != nil {
		t.Fatalf("read SDL pin %s: %v", sdlPath, err)
	}
	schema, gqlErr := gqlparser.LoadSchema(&gqlast.Source{Name: "schema.graphql", Input: string(sdlBytes)})
	if gqlErr != nil {
		t.Fatalf("parse SDL pin %s: %v", sdlPath, gqlErr)
	}

	documentConstants, operationToIdentifier := parseQueryRouteDocuments(t, repoRoot)

	checkedDocuments := 0
	for _, operation := range sortedKeys(operationToIdentifier) {
		identifier := operationToIdentifier[operation]
		text, ok := documentConstants[identifier]
		if !ok {
			t.Fatalf("digestByOperation[%q] references identifier %q, but no `const %s = ...` document was found", operation, identifier, identifier)
		}
		doc, gqlErr := gqlparser.LoadQuery(schema, text)
		if gqlErr != nil {
			t.Fatalf("operation %q (document %s): failed to validate against the published SDL: %v", operation, identifier, gqlErr)
		}
		for _, f := range collectSelectedFields(t, operation, doc) {
			checkedDocuments++
			if f.typeName == "Query" && f.fieldName == "pr" {
				t.Fatalf("operation %q (document %s) selects Query.pr -- this makes the Pr resolver's "+
					"PARTIAL port (see schema.resolvers.go's Pr doc comment) reachable via routeswitch "+
					"before the PR core row/reviews/commits ports land; do not register any operation "+
					"selecting Query.pr until that follow-up ticket lands", operation, identifier)
			}
		}
	}
	if checkedDocuments == 0 {
		t.Fatal("checked zero selected fields across zero registered documents -- extraction is broken, not the registry (this gate proves nothing if it silently walks nothing)")
	}

	// Cheap, redundant belt-and-braces: also reject the obvious candidate
	// names directly, so a reader diffing this file against the previous
	// version can see the original check is still subsumed, not dropped.
	for _, candidate := range []string{"pr", "Pr", "prDetail", "PrDetail"} {
		if identifier, registered := operationToIdentifier[candidate]; registered {
			t.Fatalf("operation %q is registered against %q in query_route.go's digestByOperation -- "+
				"do not register it until the PR core row/reviews/commits follow-up ticket lands",
				candidate, identifier)
		}
	}
}
