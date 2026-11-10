package routescan

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
)

// Decl is one top-level declaration of a Go file and the lines it spans (1-based, inclusive).
type Decl struct {
	Name   string
	Lo, Hi int
}

// Decls returns every top-level declaration of a Go source by symbol name (DeclLines spans funcs, methods and types
// only; a credential-class pin can also sit on a const, a var, a declaration's doc comment or the package clause):
//   - "package" for the package clause and every line above it (the file and package doc comments);
//   - a func by its name, a method as Recv.Name (the receiver's type name, pointer and type parameters dropped),
//     from its doc comment to its closing brace;
//   - a const, var or type spec by its names joined with ",", the spec only;
//   - the whole const, var or type declaration, doc comment included, as "(" + every spec name joined with "," + ")".
func Decls(src []byte) ([]Decl, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "", src, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	line := func(pos token.Pos) int { return fset.Position(pos).Line }
	out := []Decl{{"package", 1, line(file.Name.End())}}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			name := d.Name.Name
			if recv := receiverName(d); recv != "" {
				name = recv + "." + name
			}
			lo := d.Pos()
			if d.Doc != nil {
				lo = d.Doc.Pos()
			}
			out = append(out, Decl{name, line(lo), line(d.End())})
		case *ast.GenDecl:
			var all []string
			for _, spec := range d.Specs {
				var names []string
				switch s := spec.(type) {
				case *ast.ValueSpec:
					for _, ident := range s.Names {
						names = append(names, ident.Name)
					}
				case *ast.TypeSpec:
					names = append(names, s.Name.Name)
				default:
					continue
				}
				all = append(all, names...)
				out = append(out, Decl{strings.Join(names, ","), line(spec.Pos()), line(spec.End())})
			}
			lo := d.Pos()
			if d.Doc != nil {
				lo = d.Doc.Pos()
			}
			out = append(out, Decl{"(" + strings.Join(all, ",") + ")", line(lo), line(d.End())})
		}
	}
	return out, nil
}

// Enclosing returns the smallest declaration that holds line; false when none does (a blank or comment line between
// declarations).
func Enclosing(decls []Decl, line int) (Decl, bool) {
	var best Decl
	found := false
	for _, d := range decls {
		if d.Lo <= line && line <= d.Hi && (!found || d.Hi-d.Lo < best.Hi-best.Lo) {
			best, found = d, true
		}
	}
	return best, found
}

// Named returns every declaration called name.
func Named(decls []Decl, name string) []Decl {
	var out []Decl
	for _, d := range decls {
		if d.Name == name {
			out = append(out, d)
		}
	}
	return out
}
