package routescan

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
)

// ConstLines returns the lines of the top-level const and var specs of a Go source whose value is the string literal
// value: the declaration a row cites when it names its route's pattern constant.
func ConstLines(src []byte, value string) ([]int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "", src, 0)
	if err != nil {
		return nil, err
	}
	var out []int
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || (gen.Tok != token.CONST && gen.Tok != token.VAR) {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, ident := range vs.Names {
				if i >= len(vs.Values) {
					continue
				}
				if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if got, err := strconv.Unquote(lit.Value); err == nil && got == value {
						out = append(out, fset.Position(ident.Pos()).Line)
					}
				}
			}
		}
	}
	return out, nil
}

// RowSymbol is the place a row's source anchor names in its file, found by symbol. A registration in the file wins:
// the one registration of the row's method and route, or the one whose pattern is the identifier ident (a constant
// from another package). With no registration in the file, the one const or var spec whose value is the route (the
// row cites its pattern constant; the registration is in another file). Two registrations, or two constants and no
// registration, are ambiguous; nothing is stale.
func RowSymbol(regs []Registration, src []byte, file, method, route, ident string) (int, error) {
	var at []int
	for _, reg := range regs {
		if reg.File != file || !reg.Accepts(method) {
			continue
		}
		if (reg.Pattern != "" && reg.Pattern == route) || (reg.Pattern == "" && ident != "" && reg.PatternIdent == ident) {
			at = append(at, reg.Line)
		}
	}
	what := "registrations"
	if len(at) == 0 {
		consts, err := ConstLines(src, route)
		if err != nil {
			return 0, fmt.Errorf("source %s does not parse: %v", file, err)
		}
		at, what = consts, "constants"
	}
	switch len(at) {
	case 1:
		return at[0], nil
	case 0:
		return 0, fmt.Errorf("source anchor stale: %s holds no registration or constant of %s %s", file, method, route)
	}
	lines := make([]string, len(at))
	for i, n := range at {
		lines[i] = strconv.Itoa(n)
	}
	return 0, fmt.Errorf("source anchor ambiguous: %s holds %d %s of %s %s (lines %s)", file, len(at), what, method, route, strings.Join(lines, ", "))
}
