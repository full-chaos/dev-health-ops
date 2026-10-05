// Package mcpclass is the ONE home of the facts that define the MCP caller
// class's routing decisions (CHAOS-7085 listener, CHAOS-7214 routing verbs).
//
// The listener (internal/queryapi/server) reads a class decision per root field
// from go_api_class_decision; the routing verbs (internal/goapicli/routing,
// internal/goapiproof) write, repoint and prove them. Both sides
// import these definitions, so the key the verbs write is by construction
// the key the listener reads.
//
// A free-form MCP query has no registered document, so no per-document row
// can exist for it. The class row stands in, one per root field:
//
//	schema_digest      = the live query-api schema digest
//	document_digest    = DocumentDigest() (SDL-independent, a function of
//	                     DocumentKey only)
//	selected_operation = "mcp:<rootField>"
package mcpclass

import (
	"fmt"
	"sort"
	"strings"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
)

// DocumentKey names the free-form class. Bump the version only together
// with the tooling that writes the rows.
const DocumentKey = "dev-health-ops/mcp-freeform-class/v1"

// OperationPrefix prefixes a root field to form its row's
// selected_operation. No registered operation name contains ':', so a class
// row can never collide with a per-document row.
const OperationPrefix = "mcp:"

// DocumentDigest is the class's document_digest: sha256 of DocumentKey.
func DocumentDigest() string { return goapidigest.Document(DocumentKey) }

// rootFields is the ceiling of what the class may ever call: the SDL Query
// root fields of design r5 D.3's first-slice operations for an unrestricted
// caller. Reviewed by PR, never by configuration. See the server's
// mcpRootFieldAllowlist doc comment for what is out on purpose and why.
var rootFields = map[string]bool{
	"analytics":            true,
	"capacityForecast":     true,
	"capacityForecasts":    true,
	"catalog":              true,
	"cognitiveLoad":        true,
	"complexityTimeseries": true,
	"compoundingRisk":      true,
	"hotspots":             true,
	"securityAlerts":       true,
	"securityOverview":     true,
	"throughputForecast":   true,
	"workGraphArtifacts":   true,
	"workGraphEdges":       true,
	"workGraphFlow":        true,
}

// AllowedRoots returns a fresh copy of the allowlist.
func AllowedRoots() map[string]bool {
	out := make(map[string]bool, len(rootFields))
	for root := range rootFields {
		out[root] = true
	}
	return out
}

// SortedRoots returns the allowlisted root fields, sorted.
func SortedRoots() []string {
	out := make([]string, 0, len(rootFields))
	for root := range rootFields {
		out = append(out, root)
	}
	sort.Strings(out)
	return out
}

// Operation is the selected_operation of a root field's class row.
func Operation(root string) string { return OperationPrefix + root }

// IsOperation reports whether a selected_operation has the class shape.
func IsOperation(operation string) bool { return strings.HasPrefix(operation, OperationPrefix) }

// Root returns the root field of a class operation.
func Root(operation string) (string, bool) {
	if !IsOperation(operation) {
		return "", false
	}
	return strings.TrimPrefix(operation, OperationPrefix), true
}

// IsClassRow reports whether (operation, documentDigest) is a class row key:
// the prefix AND the class digest. Both clauses are required: a row with the
// prefix under another digest, or the digest under another operation, is not
// a class row and must never be treated as one.
func IsClassRow(operation, documentDigest string) bool {
	return IsOperation(operation) && documentDigest == DocumentDigest()
}

// AllowedOperation reports whether a class operation names an allowlisted
// root field.
func AllowedOperation(operation string) bool {
	root, ok := Root(operation)
	return ok && rootFields[root]
}

// Digests is the operation -> document digest map of every class row the
// listener may look up.
func Digests() map[string]string {
	digest := DocumentDigest()
	out := make(map[string]string, len(rootFields))
	for root := range rootFields {
		out[Operation(root)] = digest
	}
	return out
}

// ResolveOperations turns a comma-separated -operations value into class
// operations: "mcp:<root>" names, bare root names, or "all-mcp". An unknown
// root is refused by name; an empty list is refused, never widened.
func ResolveOperations(value string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == "all-mcp" {
			for _, root := range SortedRoots() {
				if !seen[Operation(root)] {
					seen[Operation(root)] = true
					out = append(out, Operation(root))
				}
			}
			continue
		}
		root := strings.TrimPrefix(part, OperationPrefix)
		if !rootFields[root] {
			return nil, fmt.Errorf("mcpclass: %q is not an allowlisted MCP root field (allowed: %v)", part, SortedRoots())
		}
		if !seen[Operation(root)] {
			seen[Operation(root)] = true
			out = append(out, Operation(root))
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("mcpclass: no MCP root field named")
	}
	sort.Strings(out)
	return out, nil
}

// QueryRootFields returns the Query type's field names of an SDL.
func QueryRootFields(sdl []byte) (map[string]bool, error) {
	schema, err := gqlparser.LoadSchema(&ast.Source{Name: "schema.graphql", Input: string(sdl)})
	if err != nil {
		return nil, fmt.Errorf("mcpclass: load the schema: %w", err)
	}
	if schema.Query == nil {
		return nil, fmt.Errorf("mcpclass: the schema has no Query type")
	}
	out := make(map[string]bool, len(schema.Query.Fields))
	for _, field := range schema.Query.Fields {
		out[field.Name] = true
	}
	return out, nil
}

// ServedRoots is the set of class root fields an image serves: the class
// allowlist intersected with the Query root fields of that image's SDL.
func ServedRoots(sdl []byte) (map[string]bool, error) {
	fields, err := QueryRootFields(sdl)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for root := range rootFields {
		if fields[root] {
			out[root] = true
		}
	}
	return out, nil
}
