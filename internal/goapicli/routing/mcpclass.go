package routing

// CHAOS-7214: the MCP class rows in the routing verbs.
//
// A class row's operation is "mcp:<rootField>" (internal/mcpclass). It has no
// registered document, so the document-operation checks that read the edge
// catalog or the deployed /registry cannot apply to it; each verb that learns
// the class swaps those checks for the class's own (allowlisted root, root is
// a Query field of THIS binary's SDL, the class digest) and keeps every other
// preflight. One run is one class: document operations and class operations
// are never mixed, because they are admitted by different receipts.

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

// catalogOperationRefusal is the answer of enable, disable and seed to a document (catalog) operation:
// query-api serves every registered operation and reads no routing row for it (CHAOS-8702), so there is
// nothing to turn on, off or create. Only MCP class rows (mcp:<root> or all-mcp) are still decided by a row.
const catalogOperationRefusal = "`%s` takes MCP class rows only (-operations mcp:<root> or all-mcp). A catalog operation is served by query-api whatever its routing row says, so there is nothing to turn on, off or create for it; to hold one dark, change the code and deploy"

// requireClassOperations refuses, before any credential, database or network step, a request that does
// not name MCP class rows.
func requireClassOperations(verb, operations string) error {
	if _, isClass, err := resolveClassScope(operations); err != nil {
		return err
	} else if !isClass {
		return refuse(catalogOperationRefusal, verb)
	}
	return nil
}
