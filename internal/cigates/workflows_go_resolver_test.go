package cigates_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// CHAOS-10177: the path-filter oracle reads Go SOURCE, so it parses it. Every string literal ("..." and `...`) is
// unquoted by strconv.Unquote, filepath.Join / path.Join are resolved from their call arguments, and //go:embed
// directives come from the parsed comments. A Join argument that is not a literal (other than a leading root such as
// repoRoot) is REPORTED as unresolved, never skipped: a path built at run time cannot be proved covered.

var packageClause = regexp.MustCompile(`(?m)^package\s+\w+`)

// parsedGo is one Go source; line numbers are those of the original text.
type parsedGo struct {
	fset   *token.FileSet
	file   *ast.File
	offset int
}

// parseGoSource parses a whole file, or wraps a statement snippet in a function first.
func parseGoSource(source string) (*parsedGo, error) {
	fset := token.NewFileSet()
	src, offset := source, 0
	if !packageClause.MatchString(source) {
		src, offset = "package p\nfunc _() {\n"+source+"\n}\n", 2
	}
	f, err := parser.ParseFile(fset, "src.go", src, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	return &parsedGo{fset, f, offset}, nil
}

func (p *parsedGo) line(n ast.Node) int { return p.fset.Position(n.Pos()).Line - p.offset }

func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	v, err := strconv.Unquote(lit.Value)
	return v, err == nil
}

// isPathCall reports whether the call is filepath.Join or path.Join.
func isPathCall(c *ast.CallExpr) bool {
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Join" {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && (id.Name == "filepath" || id.Name == "path")
}

func isReadDirCall(c *ast.CallExpr) bool {
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "ReadDir" {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == "os"
}

// joinFacts splits a Join call into its literal segments and reports whether an argument other than the first (the
// root) is not a literal.
func joinFacts(c *ast.CallExpr) (segs []string, nonLiteralBeyondRoot bool) {
	for i, a := range c.Args {
		if v, ok := stringLit(a); ok {
			segs = append(segs, v)
		} else if i > 0 {
			nonLiteralBeyondRoot = true
		}
	}
	return segs, nonLiteralBeyondRoot
}

// resolveNamedFiles resolves every file the Go source names, relative to its own directory and to the repo root.
func resolveNamedFiles(source, base, anchor string, index *fileIndex) map[string]bool {
	pg, err := parseGoSource(source)
	if err != nil {
		return map[string]bool{}
	}
	return resolveParsed(pg, base, anchor, index)
}

func resolveParsed(pg *parsedGo, base, anchor string, index *fileIndex) map[string]bool {
	found := map[string]bool{}
	add := func(candidate string) {
		rel, err := filepath.Rel(anchor, filepath.Clean(candidate))
		if err != nil || strings.HasPrefix(rel, "..") {
			return
		}
		if isFile(candidate) && index.tracked(anchor, candidate) && filepath.Ext(candidate) != ".go" {
			found[filepath.ToSlash(rel)] = true
		}
	}
	ast.Inspect(pg.file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			lit, ok := stringLit(x)
			if !ok || !hasDataSuffix(lit) {
				return true
			}
			before := len(found)
			add(filepath.Join(base, lit))
			add(filepath.Join(anchor, lit))
			if len(found) == before && looksLikeAPath(lit) {
				if c := index.byBase[path.Base(lit)]; len(c) == 1 {
					add(filepath.Join(anchor, c[0]))
				}
			}
		case *ast.CallExpr:
			if isPathCall(x) {
				if segs, _ := joinFacts(x); len(segs) > 0 && hasDataSuffix(segs[len(segs)-1]) {
					add(filepath.Join(append([]string{base}, segs...)...))
					add(filepath.Join(append([]string{anchor}, segs...)...))
				}
			}
			if isReadDirCall(x) && len(x.Args) == 1 {
				if dir, ok := stringLit(x.Args[0]); ok {
					target := filepath.Join(base, dir)
					if entries, err := os.ReadDir(target); err == nil {
						for _, e := range entries {
							add(filepath.Join(target, e.Name()))
						}
					}
				}
			}
		}
		return true
	})
	for _, cg := range pg.file.Comments {
		for _, c := range cg.List {
			rest, ok := strings.CutPrefix(c.Text, "//go:embed")
			if !ok {
				continue
			}
			for _, pattern := range strings.Fields(rest) {
				includeDot := strings.HasPrefix(pattern, "all:")
				pattern = strings.TrimPrefix(pattern, "all:")
				matches, _ := filepath.Glob(filepath.Join(base, pattern))
				for _, match := range matches {
					st, err := os.Stat(match)
					if err != nil {
						continue
					}
					if !st.IsDir() {
						add(match)
						continue
					}
					_ = filepath.WalkDir(match, func(p string, d fs.DirEntry, err error) error {
						if err != nil || !d.Type().IsRegular() {
							return nil
						}
						rel, _ := filepath.Rel(match, p)
						if !includeDot {
							for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
								if strings.HasPrefix(part, ".") || strings.HasPrefix(part, "_") {
									return nil
								}
							}
						}
						add(p)
						return nil
					})
				}
			}
		}
	}
	return found
}

type unresolved struct {
	line       int
	literal    string
	candidates []string
	call       bool // a path call with a non-literal argument: no candidate list, never covered
}

// unresolvableInSource reports (a) every data-suffixed literal naming a real tracked file that the resolver could not
// locate, and (b) every path call with a non-literal argument beyond its root, which cannot be resolved at all.
func unresolvableInSource(source, base, root string, index *fileIndex) []unresolved {
	pg, err := parseGoSource(source)
	if err != nil {
		return nil
	}
	return unresolvableParsed(pg, base, root, index)
}

func unresolvableParsed(pg *parsedGo, base, root string, index *fileIndex) []unresolved {
	var out []unresolved
	ast.Inspect(pg.file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			lit, ok := stringLit(x)
			if !ok || !hasDataSuffix(lit) || !looksLikeAPath(lit) {
				return true
			}
			if _, fab := fabricatedPathLiterals[lit]; fab {
				return true
			}
			cands := index.byBase[path.Base(lit)]
			if len(cands) == 0 || (isFile(filepath.Join(base, lit)) && index.tracked(root, filepath.Join(base, lit))) || (isFile(filepath.Join(root, lit)) && index.tracked(root, filepath.Join(root, lit))) || len(cands) == 1 {
				return true
			}
			out = append(out, unresolved{line: pg.line(x), literal: lit, candidates: cands})
		case *ast.CallExpr:
			if isPathCall(x) {
				if _, bad := joinFacts(x); bad {
					// the literal directory prefix of a call whose last arguments are computed: the file is somewhere
					// under that directory, so every tracked data file under it must be covered
					lead := leadingLiterals(x)
					if len(lead) == 0 {
						return true
					}
					var cands []string
					for _, anchorDir := range []string{base, root} {
						dir := filepath.Join(append([]string{anchorDir}, lead...)...)
						rel, err := filepath.Rel(root, dir)
						if err != nil || strings.HasPrefix(rel, "..") {
							continue
						}
						cands = append(cands, index.underDir(filepath.ToSlash(rel))...)
					}
					if len(cands) > 0 {
						out = append(out, unresolved{line: pg.line(x), literal: fmt.Sprintf("%s(... computed argument ..., %q)", callName(x), lead), candidates: cands, call: true})
					}
				}
			}
		}
		return true
	})
	return out
}

// leadingLiterals is the run of literal arguments at the start of a Join, after an optional computed root.
func leadingLiterals(c *ast.CallExpr) []string {
	var lead []string
	for i, a := range c.Args {
		v, ok := stringLit(a)
		if !ok {
			if i == 0 {
				continue
			}
			break
		}
		lead = append(lead, v)
	}
	return lead
}

func callName(c *ast.CallExpr) string {
	sel := c.Fun.(*ast.SelectorExpr)
	return sel.X.(*ast.Ident).Name + "." + sel.Sel.Name
}

// mayNamePaths is a cheap text test run before parsing: a file with no data-suffixed literal, path call, embed or
// ReadDir cannot name a path, so parsing it would only cost time (the guard reads thousands of files, and runs under -race).
func mayNamePaths(src string) bool {
	for _, k := range []string{"go:embed", "filepath.Join(", "path.Join(", "os.ReadDir("} {
		if strings.Contains(src, k) {
			return true
		}
	}
	for _, suf := range goDataSuffixes {
		if strings.Contains(src, suf+"\"") || strings.Contains(src, suf+"`") {
			return true
		}
	}
	return false
}
