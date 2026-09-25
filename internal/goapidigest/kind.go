package goapidigest

import (
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// Document kinds, as DocumentKind reports them and as the edge's operation
// catalog (go_api_operations.json) carries them: an entry with no kind is a
// query.
const (
	KindQuery    = "query"
	KindMutation = "mutation"
)

// DocumentKind reports whether a registered GraphQL document is a query or a
// mutation. It reads the operation type from the parsed document and nothing
// else: it never reprints the text, so it is not the parse-and-reprint class
// the package comment rules out, and the digest above still hashes the raw
// bytes.
//
// It answers only for a document with exactly one operation, and refuses
// anything else -- unparseable text, no operation, more than one, or a
// subscription -- so a registered document whose kind cannot be stated fails
// the tool that asks instead of being counted as a query.
func DocumentKind(text string) (string, error) {
	doc, err := parser.ParseQuery(&ast.Source{Name: "registered document", Input: text})
	if err != nil {
		return "", fmt.Errorf("document does not parse: %w", err)
	}
	if len(doc.Operations) != 1 {
		return "", fmt.Errorf("document has %d operations, want exactly 1", len(doc.Operations))
	}
	switch doc.Operations[0].Operation {
	case ast.Query:
		return KindQuery, nil
	case ast.Mutation:
		return KindMutation, nil
	default:
		return "", fmt.Errorf("document operation type %q is not served by query-api", doc.Operations[0].Operation)
	}
}
