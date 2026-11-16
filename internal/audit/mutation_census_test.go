package audit

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const auditImportPath = "github.com/full-chaos/dev-health-ops/internal/audit"

// mutationFinding is one Mutation that Commit's validate refuses for having neither Events nor OffStream, or one Commit
// site whose Mutation the walk cannot trace.
type mutationFinding struct{ site, why string }

// mutationFindings walks every non-test Go file under root (source overrides a file's content by its slash path) and
// reports every audit.Mutation literal that names neither Events nor OffStream (after the field assignments of the
// variable it is bound to, in the same function) and every Commit site whose Mutation is neither such a literal, a
// variable bound to one, nor a parameter of the enclosing function (a forwarder: its callers' literals are walked too).
func mutationFindings(t *testing.T, root string, source map[string][]byte) []mutationFinding {
	t.Helper()
	var findings []mutationFinding
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		src, ok := source[rel]
		if !ok {
			if src, err = os.ReadFile(path); err != nil {
				return err
			}
		}
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			return err
		}
		findings = append(findings, fileMutationFindings(fset, rel, file)...)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].site < findings[j].site })
	return findings
}

func fileMutationFindings(fset *token.FileSet, rel string, file *ast.File) []mutationFinding {
	inAudit := file.Name.Name == "audit" && strings.HasPrefix(rel, "internal/audit/")
	name := ""
	for _, spec := range file.Imports {
		if path, _ := strconv.Unquote(spec.Path.Value); path == auditImportPath {
			name = "audit"
			if spec.Name != nil {
				name = spec.Name.Name
			}
		}
	}
	if name == "" && !inAudit {
		return nil
	}
	isAudit := func(expr ast.Expr, ident string) bool {
		switch x := expr.(type) {
		case *ast.SelectorExpr:
			pkg, ok := x.X.(*ast.Ident)
			return ok && name != "" && pkg.Name == name && x.Sel.Name == ident
		case *ast.Ident:
			return inAudit && x.Name == ident
		}
		return false
	}
	mutationLit := func(expr ast.Expr) *ast.CompositeLit {
		if u, ok := expr.(*ast.UnaryExpr); ok && u.Op == token.AND {
			expr = u.X
		}
		if lit, ok := expr.(*ast.CompositeLit); ok && lit.Type != nil && isAudit(lit.Type, "Mutation") {
			return lit
		}
		return nil
	}
	var findings []mutationFinding
	site := func(pos token.Pos, fn string) string {
		return rel + ":" + strconv.Itoa(fset.Position(pos).Line) + " " + fn
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		named := map[string]bool{}     // variable names that carry Events or OffStream by a later field assignment
		bound := map[string]bool{}     // variable names bound to a Mutation literal
		boundLit := map[*ast.CompositeLit]string{}
		params := map[string]bool{}
		addParams := func(ft *ast.FuncType) {
			for _, field := range ft.Params.List {
				for _, n := range field.Names {
					params[n.Name] = true
				}
			}
		}
		addParams(fn.Type)
		bind := func(lhs ast.Expr, rhs ast.Expr) {
			if id, ok := lhs.(*ast.Ident); ok {
				if lit := mutationLit(rhs); lit != nil {
					bound[id.Name], boundLit[lit] = true, id.Name
				}
			}
			if sel, ok := lhs.(*ast.SelectorExpr); ok && (sel.Sel.Name == "Events" || sel.Sel.Name == "OffStream") {
				if id, ok := sel.X.(*ast.Ident); ok {
					named[id.Name] = true
				}
			}
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncLit:
				addParams(x.Type)
			case *ast.AssignStmt:
				for i := range x.Lhs {
					if i < len(x.Rhs) {
						bind(x.Lhs[i], x.Rhs[i])
					} else if sel, ok := x.Lhs[i].(*ast.SelectorExpr); ok {
						bind(sel, nil)
					}
				}
			case *ast.ValueSpec:
				for i, n := range x.Names {
					if i < len(x.Values) {
						bind(n, x.Values[i])
					}
				}
			}
			return true
		})
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CompositeLit:
				if mutationLit(x) == nil {
					return true
				}
				events, off := false, false
				for _, elt := range x.Elts {
					if kv, ok := elt.(*ast.KeyValueExpr); ok {
						if key, ok := kv.Key.(*ast.Ident); ok {
							events, off = events || key.Name == "Events", off || key.Name == "OffStream"
						}
					}
				}
				if v, ok := boundLit[x]; ok && named[v] {
					events = true
				}
				if !events && !off {
					findings = append(findings, mutationFinding{site(x.Pos(), fn.Name.Name), "a Mutation with neither Events nor OffStream"})
				}
			case *ast.CallExpr:
				if !isAudit(x.Fun, "Commit") || len(x.Args) < 4 {
					return true
				}
				arg := x.Args[3]
				if mutationLit(arg) != nil {
					return true
				}
				if id, ok := arg.(*ast.Ident); ok && (bound[id.Name] || params[id.Name]) {
					return true
				}
				findings = append(findings, mutationFinding{site(x.Pos(), fn.Name.Name), "a Commit whose Mutation the census cannot trace"})
			}
			return true
		})
	}
	return findings
}

// Every audit.Mutation in the repo names its outbox events or why it has none, and every Commit site's Mutation is
// traceable: Commit's validate refuses a Mutation with neither at run time (audit.go, "names its outbox events or why it
// has none"), so a write path built that way fails every call. The SSO provider store shipped so (five writes answered
// 503); this census is the build-time check.
func TestEveryMutationNamesItsOutboxEventsOrWhyItHasNone(t *testing.T) {
	for _, f := range mutationFindings(t, repoRoot(t), nil) {
		t.Errorf("%s: %s", f.site, f.why)
	}
}

// The census sees a dropped OffStream: the SSO provider store's Create with its OffStream line taken out is a finding,
// and a planted file with a bare Mutation literal and a Commit of an untraceable value are findings too.
func TestTheMutationCensusFindsADroppedOffStream(t *testing.T) {
	root := repoRoot(t)
	const target = "internal/auth/ssoprovider/ssoprovider.go"
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(target)))
	if err != nil {
		t.Fatal(err)
	}
	const line = "\t\tOffStream: audit.OffStreamSSOProviderConfig,\n"
	at := strings.Index(string(raw), line)
	if at < 0 {
		t.Fatalf("%s has no %q line to drop", target, strings.TrimSpace(line))
	}
	dropped := append(append([]byte{}, raw[:at]...), raw[at+len(line):]...)
	if got := mutationFindings(t, root, map[string][]byte{target: dropped}); len(got) != 1 || !strings.HasPrefix(got[0].site, target+":") {
		t.Fatalf("with one OffStream dropped the census found %v, want exactly one finding in %s", got, target)
	}
	planted := []byte(`package planted

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func bare(ctx context.Context) error {
	return audit.Commit(ctx, nil, schema, audit.Mutation{Apply: apply})
}

func untraced(ctx context.Context) error {
	return audit.Commit(ctx, nil, schema, build())
}
`)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "planted.go", planted, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := fileMutationFindings(fset, "internal/planted/planted.go", file)
	if len(got) != 2 || !strings.Contains(got[0].why, "neither") || !strings.Contains(got[1].why, "cannot trace") {
		t.Fatalf("planted findings %v, want the bare literal and the untraced Commit", got)
	}
}
