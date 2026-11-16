package audit

import (
	"context"
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

// censusFinding is one audit write site that cannot commit: a Mutation that Commit's validate refuses for having neither
// Events nor OffStream, a Commit or public projection site the census cannot trace, or a public projection event type
// outside publicProjectionEventTypes or whose off-stream reason is outside offStreamReasons.
type censusFinding struct{ site, why string }

// censusRules are what a public projection site is checked against (the live tables, or a planted copy).
type censusRules struct {
	types   map[string]string
	reasons map[string]bool
	consts  map[string]string // the audit package's string constants, by name
}

func liveCensusRules(t *testing.T, root string) censusRules {
	t.Helper()
	consts := map[string]string{}
	entries, err := os.ReadDir(filepath.Join(root, "internal", "audit"))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(root, "internal", "audit", e.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, n := range vs.Names {
					if i < len(vs.Values) {
						if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
							consts[n.Name], _ = strconv.Unquote(lit.Value)
						}
					}
				}
			}
		}
	}
	return censusRules{types: publicProjectionEventTypes, reasons: offStreamReasons, consts: consts}
}

// auditCensus walks every non-test Go file under root (source overrides a file's content by its slash path).
func auditCensus(t *testing.T, root string, rules censusRules, source map[string][]byte) []censusFinding {
	t.Helper()
	type parsed struct {
		rel  string
		file *ast.File
	}
	byDir := map[string][]parsed{}
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
		byDir[filepath.Dir(rel)] = append(byDir[filepath.Dir(rel)], parsed{rel, file})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	var findings []censusFinding
	for _, files := range byDir {
		aliases := map[string]bool{}
		for _, f := range files {
			for name := range projectionAliases(f.file) {
				aliases[name] = true
			}
		}
		for _, f := range files {
			findings = append(findings, fileCensus(fset, f.rel, f.file, aliases, rules)...)
		}
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].site < findings[j].site })
	return findings
}

// auditName is the file's name for the audit package ("" when it does not import it), and whether the file is in it.
func auditName(rel string, file *ast.File) (string, bool) {
	inAudit := file.Name.Name == "audit" && strings.HasPrefix(rel, "internal/audit/")
	for _, spec := range file.Imports {
		if path, _ := strconv.Unquote(spec.Path.Value); path == auditImportPath {
			if spec.Name != nil {
				return spec.Name.Name, inAudit
			}
			return "audit", inAudit
		}
	}
	return "", inAudit
}

// projectionAliases are the package-level variables a file binds to audit.CommitPublicProjection(With) (a seam a test
// replaces): a call of one of them is a public projection site.
func projectionAliases(file *ast.File) map[string]bool {
	name, _ := auditName("", file)
	out := map[string]bool{}
	if name == "" {
		return out
	}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, n := range vs.Names {
				if i < len(vs.Values) {
					if sel, ok := vs.Values[i].(*ast.SelectorExpr); ok {
						if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == name && strings.HasPrefix(sel.Sel.Name, "CommitPublicProjection") {
							out[n.Name] = true
						}
					}
				}
			}
		}
	}
	return out
}

func fileCensus(fset *token.FileSet, rel string, file *ast.File, aliases map[string]bool, rules censusRules) []censusFinding {
	name, inAudit := auditName(rel, file)
	if name == "" && !inAudit {
		return nil
	}
	isAudit := func(expr ast.Expr, prefix string, exact bool) bool {
		match := func(s string) bool { return s == prefix || (!exact && strings.HasPrefix(s, prefix)) }
		switch x := expr.(type) {
		case *ast.SelectorExpr:
			pkg, ok := x.X.(*ast.Ident)
			return ok && name != "" && pkg.Name == name && match(x.Sel.Name)
		case *ast.Ident:
			return inAudit && match(x.Name)
		}
		return false
	}
	mutationLit := func(expr ast.Expr) *ast.CompositeLit {
		if u, ok := expr.(*ast.UnaryExpr); ok && u.Op == token.AND {
			expr = u.X
		}
		if lit, ok := expr.(*ast.CompositeLit); ok && lit.Type != nil && isAudit(lit.Type, "Mutation", true) {
			return lit
		}
		return nil
	}
	var findings []censusFinding
	site := func(pos token.Pos, fn string) string {
		return rel + ":" + strconv.Itoa(fset.Position(pos).Line) + " " + fn
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		named := map[string]bool{} // variables that carry Events or OffStream by a later field assignment
		bound := map[string]bool{} // variables bound to a Mutation literal
		boundLit := map[*ast.CompositeLit]string{}
		params := map[string]bool{}
		type assignment struct {
			rhs        ast.Expr
			pos        token.Pos
			start, end token.Pos // the block the assignment is in
			define     bool
		}
		assigned := map[string][]assignment{} // the values assigned to a local name, for the event type walk
		var stack []ast.Node
		scope := func() (token.Pos, token.Pos) {
			for i := len(stack) - 1; i >= 0; i-- {
				switch b := stack[i].(type) {
				case *ast.BlockStmt, *ast.CaseClause, *ast.CommClause:
					return b.Pos(), b.End()
				}
			}
			return fn.Body.Pos(), fn.Body.End()
		}
		addParams := func(ft *ast.FuncType) {
			for _, field := range ft.Params.List {
				for _, n := range field.Names {
					params[n.Name] = true
				}
			}
		}
		addParams(fn.Type)
		bind := func(lhs, rhs ast.Expr, define bool) {
			if id, ok := lhs.(*ast.Ident); ok && rhs != nil {
				start, end := scope()
				assigned[id.Name] = append(assigned[id.Name], assignment{rhs, lhs.Pos(), start, end, define})
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
			if n == nil {
				stack = stack[:len(stack)-1]
				return false
			}
			switch x := n.(type) {
			case *ast.FuncLit:
				addParams(x.Type)
			case *ast.AssignStmt:
				for i := range x.Lhs {
					if len(x.Lhs) == len(x.Rhs) {
						bind(x.Lhs[i], x.Rhs[i], x.Tok == token.DEFINE)
					} else {
						bind(x.Lhs[i], nil, false)
					}
				}
			case *ast.ValueSpec:
				for i, n := range x.Names {
					if i < len(x.Values) {
						bind(n, x.Values[i], true)
					}
				}
			}
			stack = append(stack, n)
			return true
		})
		// eventTypes is every string an event expression can name as its type: a literal, an audit constant, a local
		// variable's assigned values, an AuditEvent literal's EventType, or the first argument of a call that names one.
		// A local name resolves to its definitions in a block that holds the site, and to every plain reassignment before it.
		var eventTypes func(expr ast.Expr, at token.Pos, depth int) []string
		eventTypes = func(expr ast.Expr, at token.Pos, depth int) []string {
			if depth > 6 {
				return nil
			}
			switch x := expr.(type) {
			case *ast.BasicLit:
				if x.Kind == token.STRING {
					v, _ := strconv.Unquote(x.Value)
					return []string{v}
				}
			case *ast.SelectorExpr:
				if isAudit(x, "Event", false) {
					if v, ok := rules.consts[x.Sel.Name]; ok {
						return []string{v}
					}
				}
			case *ast.Ident:
				if inAudit {
					if v, ok := rules.consts[x.Name]; ok {
						return []string{v}
					}
				}
				var out []string
				for _, a := range assigned[x.Name] {
					if a.pos < at && (!a.define || (a.start <= at && at <= a.end)) {
						out = append(out, eventTypes(a.rhs, a.pos, depth+1)...)
					}
				}
				return out
			case *ast.CompositeLit:
				for _, elt := range x.Elts {
					if kv, ok := elt.(*ast.KeyValueExpr); ok {
						if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "EventType" {
							return eventTypes(kv.Value, at, depth+1)
						}
					}
				}
			case *ast.CallExpr:
				// The event type is the first argument of every event builder (localAudit, auditEvent, withChangedFields).
				if len(x.Args) > 0 {
					return eventTypes(x.Args[0], at, depth+1)
				}
			}
			return nil
		}
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
					findings = append(findings, censusFinding{site(x.Pos(), fn.Name.Name), "a Mutation with neither Events nor OffStream"})
				}
			case *ast.CallExpr:
				alias := false
				if id, ok := x.Fun.(*ast.Ident); ok && aliases[id.Name] {
					alias = true
				}
				if alias || isAudit(x.Fun, "CommitPublicProjection", false) {
					if len(x.Args) < 4 {
						findings = append(findings, censusFinding{site(x.Pos(), fn.Name.Name), "a public projection commit the census cannot read"})
						return true
					}
					if id, ok := x.Args[3].(*ast.Ident); ok && params[id.Name] && len(assigned[id.Name]) == 0 {
						return true // a forwarder: its callers' sites are walked
					}
					types := eventTypes(x.Args[3], x.Pos(), 0)
					if len(types) == 0 {
						findings = append(findings, censusFinding{site(x.Pos(), fn.Name.Name), "a public projection commit whose event type the census cannot trace"})
					}
					for _, typ := range types {
						reason, ok := rules.types[typ]
						switch {
						case !ok:
							findings = append(findings, censusFinding{site(x.Pos(), fn.Name.Name), "public projection event type " + strconv.Quote(typ) + " is not in publicProjectionEventTypes"})
						case !rules.reasons[reason]:
							findings = append(findings, censusFinding{site(x.Pos(), fn.Name.Name), "public projection event type " + strconv.Quote(typ) + " has off-stream reason " + strconv.Quote(reason) + " outside offStreamReasons"})
						}
					}
					return true
				}
				if !isAudit(x.Fun, "Commit", true) || len(x.Args) < 4 {
					return true
				}
				arg := x.Args[3]
				if mutationLit(arg) != nil {
					return true
				}
				if id, ok := arg.(*ast.Ident); ok && (bound[id.Name] || params[id.Name]) {
					return true
				}
				findings = append(findings, censusFinding{site(x.Pos(), fn.Name.Name), "a Commit whose Mutation the census cannot trace"})
			}
			return true
		})
	}
	return findings
}

// Every audit write site can commit: every audit.Mutation names its outbox events or why it has none (Commit's validate
// refuses one with neither), every Commit site's Mutation is traceable, and every public projection commit names an event
// type of publicProjectionEventTypes whose off-stream reason is in offStreamReasons. The SSO provider store shipped with
// writes that could not commit (all five answered 503); this census is the build-time check.
func TestEveryAuditWriteSiteCanCommit(t *testing.T) {
	root := repoRoot(t)
	for _, f := range auditCensus(t, root, liveCensusRules(t, root), nil) {
		t.Errorf("%s: %s", f.site, f.why)
	}
}

// Planted-red cells: the census finds a Mutation with its OffStream dropped at a real site, a bare Mutation literal and an
// untraceable Commit, a public projection commit of an unknown type, and a type whose reason is outside the set.
func TestTheAuditWriteCensusFindsEachPlantedDefect(t *testing.T) {
	root := repoRoot(t)
	rules := liveCensusRules(t, root)
	drop := func(target, text, with string) map[string][]byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(target)))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), text) {
			t.Fatalf("%s has no %q", target, text)
		}
		return map[string][]byte{target: []byte(strings.Replace(string(raw), text, with, 1))}
	}
	expectOne := func(cell string, got []censusFinding, file, why string) {
		t.Helper()
		if len(got) != 1 || !strings.HasPrefix(got[0].site, file+":") || !strings.Contains(got[0].why, why) {
			t.Errorf("%s: census found %v, want exactly one %q finding in %s", cell, got, why, file)
		}
	}
	const users = "internal/auth/identity/adminusers.go"
	expectOne("OffStream dropped at a real Mutation site",
		auditCensus(t, root, rules, drop(users, "OffStream: audit.OffStreamPublicProfile,", "")), users, "neither Events nor OffStream")
	const store = "internal/auth/ssoprovider/ssoprovider.go"
	expectOne("a public projection type that is not in the map",
		auditCensus(t, root, rules, drop(store, "audit.EventSSOProviderDeleted", `"auth.sso_provider.purged"`)), store, "is not in publicProjectionEventTypes")
	wrong := map[string]string{}
	for k, v := range publicProjectionEventTypes {
		wrong[k] = v
	}
	wrong[EventSSOProviderCreated] = "sso_provider_made_up"
	expectOne("a type whose reason is outside the set",
		auditCensus(t, root, censusRules{types: wrong, reasons: offStreamReasons, consts: rules.consts}, nil), store, "outside offStreamReasons")
	missing := map[string]string{}
	for k, v := range publicProjectionEventTypes {
		if k != EventSSOProviderUpdated {
			missing[k] = v
		}
	}
	expectOne("a type dropped from the map",
		auditCensus(t, root, censusRules{types: missing, reasons: offStreamReasons, consts: rules.consts}, nil), store, "is not in publicProjectionEventTypes")

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

func projected(ctx context.Context) error {
	return audit.CommitPublicProjection(ctx, nil, schema, audit.AuditEvent{EventType: "auth.planted.changed"}, apply)
}
`)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "planted.go", planted, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := fileCensus(fset, "internal/planted/planted.go", file, nil, rules)
	if len(got) != 3 || !strings.Contains(got[0].why, "neither") || !strings.Contains(got[1].why, "cannot trace") ||
		!strings.Contains(got[2].why, `"auth.planted.changed" is not in publicProjectionEventTypes`) {
		t.Errorf("planted findings %v, want the bare literal, the untraced Commit and the unknown public projection type", got)
	}
}

// An SSO provider write's event types are public projection types with the SSO provider config reason, which is in the
// permitted set; Commit (validate) still refuses them, so only CommitPublicProjection can write them.
func TestTheSSOProviderTypesArePublicProjectionTypes(t *testing.T) {
	for _, typ := range []string{EventSSOProviderCreated, EventSSOProviderUpdated, EventSSOProviderDeleted, EventSSOProviderActivated, EventSSOProviderDeactivated} {
		if reason := publicProjectionEventTypes[typ]; reason != OffStreamSSOProviderConfig || !offStreamReasons[reason] {
			t.Errorf("%s: reason %q (in the set: %v), want %q", typ, reason, offStreamReasons[reason], OffStreamSSOProviderConfig)
		}
		org := "7d3a2d1e-3c41-4c1e-9a52-0d5f0d7c2b11"
		m := Mutation{
			Apply:     func(context.Context, TxOps) error { return nil },
			Audit:     AuditEvent{EventType: typ, Outcome: OutcomeAllowed, OrganizationID: &org},
			OffStream: OffStreamSSOProviderConfig,
		}
		if err := m.validate(); err == nil {
			t.Errorf("%s: Commit's validate admitted a public projection type", typ)
		}
	}
}
