package graph

import (
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
	"github.com/vektah/gqlparser/v2/validator"
)

// A nullable variable feeding a defaulted non-null argument validates; every
// other incompatible position is still refused.
func TestLocationDefaultVariableRule(t *testing.T) {
	schema, err := validator.LoadSchema(validator.Prelude, &ast.Source{Name: "s", Input: `
type Query { withDefault(n: Int! = 5): Int  withoutDefault(n: Int!): Int  nullDefault(n: Int! = null): Int  list(n: [Int!]! = []): Int }`})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		query string
		valid bool
	}{
		{`query($n: Int) { withDefault(n: $n) }`, true},
		{`query($n: Int) { withoutDefault(n: $n) }`, false},
		{`query($n: Int!) { withoutDefault(n: $n) }`, true},
		{`query($n: Int = 1) { withoutDefault(n: $n) }`, true},
		{`query($n: String) { withDefault(n: $n) }`, false},
		{`query($n: Int) { nullDefault(n: $n) }`, false},
		{`query($n: [Int]) { list(n: $n) }`, false},
		{`query($n: [Int!]) { list(n: $n) }`, true},
		{`query($n: Int) { a: withDefault(n: $n) b: withoutDefault(n: $n) }`, false},
		{`query($n: Int) { ...F } fragment F on Query { withoutDefault(n: $n) }`, false},
		{`query($n: Int) { ...F } fragment F on Query { withDefault(n: $n) }`, true},
	} {
		doc, perr := parser.ParseQuery(&ast.Source{Input: c.query})
		if perr != nil {
			t.Fatal(perr)
		}
		errs := validator.Validate(schema, doc)
		if (len(errs) == 0) != c.valid {
			t.Errorf("%s: valid=%v, errors=%v", c.query, len(errs) == 0, errs)
		}
	}
}
