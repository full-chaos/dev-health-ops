package routing

// CHAOS-7214: the MCP class rows in the routing verbs.
//
// A class decision's operation is "mcp:<rootField>" (internal/mcpclass). It has no registered document, so each
// verb checks the class's own preconditions (allowlisted root, root is a Query field of THIS binary's SDL, the class
// digest). The verbs take class operations only.

import (
	"strings"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// classScope is what -operations resolved to when it names MCP class rows.
type classScope struct {
	// Operations is the sorted "mcp:<root>" list.
	Operations []string
	// Digests maps each operation to the class document digest.
	Digests map[string]string
}

// resolveClassScope returns (scope, true, nil) when -operations names class
// rows ("mcp:<root>" or "all-mcp"), (_, false, nil) when it names none, and a
// refusal when it mixes the two kinds or names an unknown root.
func resolveClassScope(raw string) (classScope, bool, error) {
	names, err := goapiproof.SplitOperations(raw)
	if err != nil || names == nil {
		// The document path owns the empty / default / malformed cases and its
		// own refusals.
		return classScope{}, false, nil
	}
	var class, document int
	for _, name := range names {
		if name == "all-mcp" || mcpclass.IsOperation(name) {
			class++
		} else {
			document++
		}
	}
	switch {
	case class == 0:
		return classScope{}, false, nil
	case document > 0:
		return classScope{}, true, refuse("-operations mixes MCP class rows (%s) with document operations: they are admitted by different receipts, so run them separately", strings.Join(names, ","))
	}
	operations, err := mcpclass.ResolveOperations(raw)
	if err != nil {
		return classScope{}, true, refuse("%v", err)
	}
	digests := make(map[string]string, len(operations))
	for _, operation := range operations {
		digests[operation] = mcpclass.DocumentDigest()
	}
	return classScope{Operations: operations, Digests: digests}, true, nil
}

// requireClassRootsServed refuses a class operation whose root field is not a
// Query field of this binary's own SDL: a row for it would be dead on arrival.
func requireClassRootsServed(operations []string) error {
	served, err := mcpclass.ServedRoots(schemav1.SDL)
	if err != nil {
		return refuse("%v -- the MCP class roots cannot be checked without this binary's SDL", err)
	}
	var missing []string
	for _, operation := range operations {
		root, _ := mcpclass.Root(operation)
		if !served[root] {
			missing = append(missing, operation)
		}
	}
	if len(missing) > 0 {
		return refuse("not a Query root field of this binary's SDL (or not allowlisted): %v", missing)
	}
	return nil
}

// classKinds declares every class operation's kind for the proof reader.
func classKinds(operations []string) map[string]string {
	kinds := make(map[string]string, len(operations))
	for _, operation := range operations {
		kinds[operation] = goapiproof.OperationKindMCPClass
	}
	return kinds
}

const classOperationsUsage = "comma-separated MCP class operations (mcp:<root>), or 'all-mcp' (required)"

// requireClassScope resolves -operations to MCP class operations and refuses anything else: a catalog operation has
// no routing state, because query-api serves every registered operation without a row.
func requireClassScope(verb, raw string) (classScope, error) {
	names, err := goapiproof.SplitOperations(raw)
	if err != nil || names == nil {
		return classScope{}, refuse("-operations is required: %s", classOperationsUsage)
	}
	scope, isClass, err := resolveClassScope(raw)
	if err != nil {
		return classScope{}, err
	}
	if !isClass {
		return classScope{}, refuse("%s applies to MCP class roots (-operations mcp:<root> or all-mcp) only: query-api serves every registered catalog operation without a routing row, so there is nothing to %s for: %s",
			verb, verb, strings.Join(names, ","))
	}
	return scope, nil
}
