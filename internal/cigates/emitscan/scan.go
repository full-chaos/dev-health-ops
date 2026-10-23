// Package emitscan is the static half of the telemetry hygiene gate. It reads
// the Go source of the emit paths (slog calls, OpenTelemetry attributes and
// metric labels, River client construction) and reports a finding as a rule id
// and a file:line only. The redaction rules themselves live in
// internal/platform/logging; this package keeps no copy of them. It is a best-effort census and it fails closed:
// scopes and declaration order are not tracked. A name bound anywhere in the package (:=, var, =, a copy, a range, a
// map entry or struct field in a literal) to a watched constructor, directly or through other names, makes every call through that name count
// as that constructor; a name that holds several constructors counts as each of them. A With call on any receiver is
// read as a log call, and every argument of it is checked. A same-named non-constructor or a non-logger With is a
// false finding (noise accepted). Only conversions (string(x), a local string type, attribute.Key(x)) and
// strconv.Itoa / fmt.Sprint of constants are folded into a key; any other call in key position is a dynamic key (R2 on
// a metric label or a span attribute). Named limits (not seen by the scan): a constant key taken from another package (for example
// tracing.StageAttribute); a constructor returned by a function call; a constant whose folding exceeds 8 values, 256 bytes, 64 passes or 4096 expression nodes, or that has a declaration the scan cannot read (it, and every constant built from it, is marked saturated and a key that uses it is an R1 finding); a package that exhausts the work budget (1,000,000 steps plus 100 per syntax node; one R1 "census incomplete: work budget" finding at its package clause, and its scan stops); a constructor reached through a function parameter, an append, or a map or struct field read under another name; a constructor converted to a function type (T(attribute.String)); a constructor assigned to a map entry or struct field after its literal (m[k] = f, x.f = f); a call through a dereferenced pointer ((*p)(...)); and a key that is a variable and not a constant.
// The runtime sentinel tests are the primary defence.
package emitscan

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

const (
	RuleProtectedKey  = "R1" // span or metric attribute key the shared redactor protects
	RuleMetricLabel   = "R2" // unbounded metric label key, free-text value or too many labels
	RuleErrorText     = "R3" // error text as a span or metric attribute value, or RecordError
	RuleRawHandler    = "R4" // slog handler built outside platform/logging
	RuleRiverLogger   = "R5" // river client without a Logger
	RuleRawPath       = "R6" // raw request path or query in a log attribute
	RuleIdentityHash  = "R7" // bare digest of a value used as an emit attribute
	RuleDotImport     = "R8" // dot-import of a watched package: its calls cannot be matched
	RuleUnparsable    = "R0" // a file the scan could not read or parse: always a failure
	maxMetricLabels   = 6
	maxConstValues    = 8    // folded values kept per constant name
	maxConstBytes     = 256  // longest folded value kept
	maxConstPasses    = 64   // passes of the constant fixpoint
	maxFoldWork       = 4096 // expression nodes one fold may visit before it fails closed
	rawHandlerAllowed = "internal/platform/logging/"
)

// Finding is one place that breaks a rule. Where is file:line; Detail is a
// rule-specific name taken from the source text (a key name or a callee), never
// a value.
type Finding struct {
	Rule   string
	File   string
	Line   int
	Func   string // enclosing function name, "-" outside any function
	Detail string
}

func (f Finding) Where() string { return fmt.Sprintf("%s:%d", f.File, f.Line) }

func (f Finding) String() string {
	if f.Detail == "" {
		return f.Rule + " " + f.Where() + " " + f.Func
	}
	return f.Rule + " " + f.Where() + " " + f.Func + " " + f.Detail
}

var watchedImports = map[string]bool{
	"log/slog": true, "go.opentelemetry.io/otel/attribute": true, "go.opentelemetry.io/otel/metric": true,
	"github.com/riverqueue/river": true,
}

var attrConstructors = map[string]bool{
	"String": true, "Int": true, "Int64": true, "Float64": true, "Bool": true, "StringSlice": true,
	"Int64Slice": true, "IntSlice": true, "Float64Slice": true, "BoolSlice": true,
}

var slogAttrConstructors = map[string]bool{
	"String": true, "Any": true, "Int": true, "Int64": true, "Uint64": true, "Float64": true,
	"Bool": true, "Duration": true, "Time": true,
}

var logMethods = map[string]bool{
	"Debug": true, "Info": true, "Warn": true, "Error": true,
	"DebugContext": true, "InfoContext": true, "WarnContext": true, "ErrorContext": true,
}

var digestFuncs = map[string]bool{
	"sha256.Sum256": true, "sha256.Sum224": true, "sha512.Sum512": true, "sha1.Sum": true, "md5.Sum": true,
	"sha256.New": true, "sha1.New": true, "md5.New": true,
}

// strongLabelWords make a metric label key unbounded whatever word ends it.
var strongLabelWords = map[string]bool{
	"id": true, "ids": true, "uuid": true, "email": true, "path": true, "url": true, "uri": true, "query": true,
	"ip": true, "address": true, "session": true, "user": true, "username": true, "request": true, "trace": true,
	"span": true,
}

// weakLabelWords are free text unless a bounded final word (kind, class, ...) says the value is a class.
var weakLabelWords = map[string]bool{"message": true, "msg": true, "text": true, "error": true, "err": true, "name": true}

// classLast are final words that turn an unbounded thing into a class of it (path_class): a bounded set by construction.
var classLast = map[string]bool{"kind": true, "class": true, "category": true}

// labelAllowedLast are final words that name a bounded class of a weak-word thing.
var labelAllowedLast = map[string]bool{"kind": true, "class": true, "type": true, "category": true, "code": true, "count": true, "status": true}

type parsedFile struct {
	rel     string
	fset    *token.FileSet
	file    *ast.File
	imports map[string]string
}

// Scan walks dirs under root (non-test .go files only) and returns every
// finding, ordered by file and line. A file that cannot be read or parsed is a
// finding of rule R0: the scan fails closed. Files are read per package
// directory first, so constants and helper functions of a package are known
// before any of its files is scanned.
func Scan(root string, dirs []string) ([]Finding, error) {
	var out []Finding
	byDir := map[string][]parsedFile{}
	files := 0
	for _, dir := range dirs {
		base := filepath.Join(root, dir)
		err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				rel, _ := filepath.Rel(root, path)
				out = append(out, Finding{Rule: RuleUnparsable, File: filepath.ToSlash(rel), Line: 1, Func: "-"})
				return nil
			}
			if entry.IsDir() {
				if name := entry.Name(); name == "testdata" || strings.HasPrefix(name, ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			files++
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			source, err := os.ReadFile(path)
			if err != nil {
				out = append(out, Finding{Rule: RuleUnparsable, File: rel, Line: 1, Func: "-"})
				return nil
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, rel, source, 0)
			if err != nil {
				out = append(out, Finding{Rule: RuleUnparsable, File: rel, Line: 1, Func: "-"})
				return nil
			}
			dirKey := filepath.ToSlash(filepath.Dir(rel))
			byDir[dirKey] = append(byDir[dirKey], parsedFile{rel: rel, fset: fset, file: file, imports: importNames(file)})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if files == 0 {
		return nil, fmt.Errorf("emitscan: no source files read under %v", dirs)
	}
	for _, group := range byDir {
		info := packageInfo(group)
		var found []Finding
		for _, parsed := range group {
			if info.budget.over {
				break
			}
			found = append(found, scanFile(parsed, info)...)
		}
		if info.budget.over {
			first := group[0]
			out = append(out, Finding{Rule: RuleProtectedKey, File: first.rel, Line: first.fset.Position(first.file.Package).Line,
				Func: "-", Detail: "census incomplete: work budget"})
			continue
		}
		out = append(out, found...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Line < out[j].Line
	})
	return out, nil
}

// pkgInfo is what one package directory knows across its files.
// The work budget of a package scan is workBase plus workPerNode for every syntax node of the package: a scan costs a
// small multiple of the package size, and an input that makes it cost more (a blowup in some loop) ends as unscannable.
// A step is a fold node, a key byte, an alias or flow step, or an expression node a walk visits.
var (
	workBase    = 1000000
	workPerNode = 100
)

// inspectCharged is ast.Inspect that charges one step per node and stops at the budget.
func inspectCharged(b *budget, root ast.Node, visit func(ast.Node) bool) {
	ast.Inspect(root, func(node ast.Node) bool {
		if node != nil && !b.charge(1) {
			return false
		}
		return visit(node)
	})
}

// budget is the one counter of a package scan. Every fold step, key byte, alias step and fixpoint pass charges it; when
// it runs out the package is unscannable: it reports one finding and its scan stops (fail closed).
type budget struct {
	used  int
	limit int
	over  bool
}

func (b *budget) charge(n int) bool {
	if b.over {
		return false
	}
	if b.used += n; b.used > b.limit {
		b.over = true
		return false
	}
	return true
}

type pkgInfo struct {
	consts      map[string][]string    // string constants, every value a name holds (function-local names can repeat)
	rawFuncs    map[string]bool        // functions whose result carries a raw request path or query
	constNames  map[string]bool        // every constant name declared in the package
	stringTypes map[string]bool        // package types whose underlying type is string (conversions of a constant keep its text)
	aliases     map[string][][2]string // every watched constructor a name is ever bound to anywhere in the package, deduped
	saturated   map[string]bool        // constants whose folding hit a bound: no value is kept, a use is an unfoldable const key
	budget      *budget
}

func packageInfo(group []parsedFile) *pkgInfo {
	nodes := 0
	for _, parsed := range group {
		ast.Inspect(parsed.file, func(node ast.Node) bool {
			if node != nil {
				nodes++
			}
			return true
		})
	}
	info := &pkgInfo{consts: map[string][]string{}, rawFuncs: map[string]bool{}, constNames: map[string]bool{},
		stringTypes: map[string]bool{}, aliases: map[string][][2]string{}, saturated: map[string]bool{}, budget: &budget{limit: workBase + workPerNode*nodes}}
	for _, parsed := range group {
		ast.Inspect(parsed.file, func(node ast.Node) bool {
			if spec, ok := node.(*ast.TypeSpec); ok {
				if ident, isIdent := spec.Type.(*ast.Ident); isIdent && ident.Name == "string" {
					info.stringTypes[spec.Name.Name] = true
				}
			}
			return true
		})
	}
	saturate := func(name string) {
		info.saturated[name] = true
		delete(info.consts, name)
	}
	for pass, changed := 0, true; changed && !info.budget.over; pass++ {
		changed = false
		growing := map[string]bool{}
		for _, parsed := range group {
			ast.Inspect(parsed.file, func(node ast.Node) bool {
				gen, ok := node.(*ast.GenDecl)
				if !ok || gen.Tok != token.CONST || !info.budget.charge(1) {
					return true
				}
				var inherited []ast.Expr // a spec without values repeats the expression list of the one before it
				for specIndex, spec := range gen.Specs {
					value := spec.(*ast.ValueSpec)
					values := value.Values
					if len(values) == 0 {
						values = inherited
					} else {
						inherited = values
					}
					for i, name := range value.Names {
						info.constNames[name.Name] = true
						if i >= len(values) || info.saturated[name.Name] {
							continue
						}
						if info.refsSaturated(values[i]) {
							saturate(name.Name)
							changed = true
							continue
						}
						ctx := newFoldCtx(info.budget, info.consts, importsOf(parsed), info.stringTypes)
						folds := foldString(values[i], ctx, specIndex)
						if ctx.over {
							saturate(name.Name)
							changed = true
							continue
						}
						for _, folded := range folds {
							if contains(info.consts[name.Name], folded) {
								continue
							}
							if len(folded) > maxConstBytes || len(info.consts[name.Name]) >= maxConstValues {
								saturate(name.Name)
								changed = true
								break
							}
							info.consts[name.Name] = append(info.consts[name.Name], folded)
							growing[name.Name] = true
							changed = true
						}
					}
				}
				return true
			})
		}
		if changed && pass >= maxConstPasses {
			for name := range growing {
				saturate(name)
			}
			break
		}
	}
	info.saturateEmptyDecls(group)
	info.saturateDependants(group)
	info.collectAliases(group)
	for changed := true; changed && !info.budget.over; {
		changed = false
		for _, parsed := range group {
			for _, decl := range parsed.file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil || info.rawFuncs[fn.Name.Name] || !info.budget.charge(1) {
					continue
				}
				taint := rawPathIdents(fn.Body, info.rawFuncs, info.budget)
				inspectCharged(info.budget, fn.Body, func(node ast.Node) bool {
					if ret, ok := node.(*ast.ReturnStmt); ok {
						for _, result := range ret.Results {
							if rawRequestPart(result, info.rawFuncs, info.budget) || carriesTaint(result, taint, info.budget) {
								info.rawFuncs[fn.Name.Name] = true
							}
						}
					}
					return true
				})
				if info.rawFuncs[fn.Name.Name] {
					changed = true
				}
			}
		}
	}
	return info
}

// saturateEmptyDecls saturates a constant with a declaration that folds to nothing (a function-local redeclaration, an
// expression the scan cannot read), even when another declaration of the same name folds: the name has a value the scan
// cannot see. It runs after the fixpoint, so a declaration that waited for a later one is not mistaken for empty.
func (info *pkgInfo) saturateEmptyDecls(group []parsedFile) {
	for _, parsed := range group {
		imports := importsOf(parsed)
		ast.Inspect(parsed.file, func(node ast.Node) bool {
			gen, ok := node.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST || info.budget.over {
				return true
			}
			var inherited []ast.Expr
			for specIndex, spec := range gen.Specs {
				value := spec.(*ast.ValueSpec)
				values := value.Values
				if len(values) == 0 {
					values = inherited
				} else {
					inherited = values
				}
				for i, name := range value.Names {
					if i >= len(values) || info.saturated[name.Name] || name.Name == "_" {
						continue
					}
					ctx := newFoldCtx(info.budget, info.consts, imports, info.stringTypes)
					if folds := foldString(values[i], ctx, specIndex); ctx.over || len(folds) == 0 {
						info.saturated[name.Name] = true
						delete(info.consts, name.Name)
					}
				}
			}
			return true
		})
	}
}

// saturateDependants closes saturation over the reference graph of the constants: a constant built, directly or through
// other constants, from a saturated one is saturated too, whatever partial values it holds.
func (info *pkgInfo) saturateDependants(group []parsedFile) {
	dependants := map[string][]string{} // referenced name -> constants whose expression names it
	for _, parsed := range group {
		ast.Inspect(parsed.file, func(node ast.Node) bool {
			gen, ok := node.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				return true
			}
			var inherited []ast.Expr
			for _, spec := range gen.Specs {
				value := spec.(*ast.ValueSpec)
				values := value.Values
				if len(values) == 0 {
					values = inherited
				} else {
					inherited = values
				}
				for i, name := range value.Names {
					if i >= len(values) {
						continue
					}
					inspectCharged(info.budget, values[i], func(inner ast.Node) bool {
						if ident, ok := inner.(*ast.Ident); ok {
							dependants[ident.Name] = append(dependants[ident.Name], name.Name)
						}
						return true
					})
				}
			}
			return true
		})
	}
	var queue []string
	for name := range info.saturated {
		queue = append(queue, name)
	}
	for len(queue) > 0 && info.budget.charge(1) {
		name := queue[0]
		queue = queue[1:]
		for _, dependant := range dependants[name] {
			if !info.saturated[dependant] {
				info.saturated[dependant] = true
				delete(info.consts, dependant)
				queue = append(queue, dependant)
			}
		}
	}
}

// refsSaturated reports an expression that names a saturated constant: the constant built from it is saturated too.
func (info *pkgInfo) refsSaturated(expr ast.Expr) bool {
	found := false
	inspectCharged(info.budget, expr, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && info.saturated[ident.Name] {
			found = true
		}
		return !found
	})
	return found
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

// foldCtx is what folding a constant expression needs from its package and file.
type foldCtx struct {
	consts      map[string][]string
	imports     map[string]string
	stringTypes map[string]bool
	work        int     // nodes still allowed
	over        bool    // a bound was hit: the result is not trustworthy
	budget      *budget // the package counter
}

func newFoldCtx(b *budget, consts map[string][]string, imports map[string]string, stringTypes map[string]bool) *foldCtx {
	return &foldCtx{consts: consts, imports: imports, stringTypes: stringTypes, work: maxFoldWork, budget: b}
}

// bytesOver charges the bytes of a folded value and reports one longer than a key can be.
func (ctx *foldCtx) bytesOver(values []string) bool {
	for _, value := range values {
		if len(value) > maxConstBytes || !ctx.budget.charge(len(value)+1) {
			ctx.over = true
			return true
		}
	}
	return false
}

func importsOf(parsed parsedFile) map[string]string { return parsed.imports }

// foldString evaluates a string constant expression: literals, constant names, + and conversions (string(x), a local
// string type, attribute.Key(x)) and strconv.Itoa / fmt.Sprint of a folded argument. Any other call is not folded.
func foldString(expr ast.Expr, ctx *foldCtx, iota int) []string {
	values := foldNode(expr, ctx, iota)
	if ctx.over || ctx.bytesOver(values) {
		return nil
	}
	return values
}

func foldNode(expr ast.Expr, ctx *foldCtx, iota int) []string {
	if ctx.work--; ctx.work < 0 || !ctx.budget.charge(1) {
		ctx.over = true
		return nil
	}
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind == token.STRING {
			return []string{unquote(e.Value)}
		}
	case *ast.ParenExpr:
		return foldString(e.X, ctx, iota)
	case *ast.Ident:
		if e.Name == "iota" {
			return []string{strconv.Itoa(iota)}
		}
		return ctx.consts[e.Name]
	case *ast.CallExpr:
		if len(e.Args) != 1 {
			return nil
		}
		switch fun := e.Fun.(type) {
		case *ast.Ident:
			if fun.Name == "string" || ctx.stringTypes[fun.Name] {
				return foldString(e.Args[0], ctx, iota)
			}
		case *ast.SelectorExpr:
			if x, ok := fun.X.(*ast.Ident); ok {
				switch ctx.imports[x.Name] + "." + fun.Sel.Name {
				case "attribute.Key", "strconv.Itoa", "fmt.Sprint":
					if lit, ok := e.Args[0].(*ast.BasicLit); ok && lit.Kind == token.INT {
						return []string{lit.Value}
					}
					return foldString(e.Args[0], ctx, iota)
				}
			}
		}
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return nil
		}
		var out []string
		lefts, rights := foldString(e.X, ctx, iota), foldString(e.Y, ctx, iota)
		for _, left := range lefts {
			for _, right := range rights {
				if len(out) < 8 {
					out = append(out, left+right)
				}
			}
		}
		return out
	}
	return nil
}

// aliasEdge is one binding of a name to an expression (:=, var, =, a range, a composite literal field).
type aliasEdge struct {
	name    string
	rhs     ast.Expr
	imports map[string]string
}

// collectAliases computes, over the whole package, every watched constructor each name is ever bound to, in any scope
// and in any order, directly or through other names. It is a deliberate over-approximation (fail closed): scopes and
// order are ignored, the sets are deduplicated and the loop runs until nothing is added.
func (info *pkgInfo) collectAliases(group []parsedFile) {
	var edges []aliasEdge
	for _, parsed := range group {
		imports := importsOf(parsed)
		add := func(name ast.Expr, rhs ast.Expr) {
			if ident, ok := name.(*ast.Ident); ok && ident.Name != "_" && rhs != nil {
				edges = append(edges, aliasEdge{ident.Name, rhs, imports})
			}
		}
		ast.Inspect(parsed.file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				if len(n.Lhs) == len(n.Rhs) {
					for i, lhs := range n.Lhs {
						add(lhs, n.Rhs[i])
					}
				}
			case *ast.ValueSpec:
				for i, name := range n.Names {
					if i < len(n.Values) {
						add(name, n.Values[i])
					}
				}
			case *ast.RangeStmt:
				add(n.Key, n.X)
				add(n.Value, n.X)
			case *ast.KeyValueExpr:
				add(n.Key, n.Value)
			}
			return true
		})
	}
	dependents := map[string][]int{} // name -> edges whose right side reads that name's alternatives
	for index, edge := range edges {
		for _, name := range readNames(edge.rhs, edge.imports) {
			if !info.budget.charge(1) {
				return
			}
			dependents[name] = append(dependents[name], index)
		}
	}
	seen := map[string]map[[2]string]bool{}
	var queue []int
	queued := map[int]bool{}
	for index := range edges {
		queue = append(queue, index)
		queued[index] = true
	}
	for len(queue) > 0 {
		index := queue[0]
		queue = queue[1:]
		queued[index] = false
		edge := edges[index]
		grew := false
		alts := info.altsOf(edge.rhs, edge.imports)
		if !info.budget.charge(1 + len(alts)) {
			return
		}
		for _, alt := range alts {
			if seen[edge.name] == nil {
				seen[edge.name] = map[[2]string]bool{}
			}
			if !seen[edge.name][alt] {
				seen[edge.name][alt] = true
				info.aliases[edge.name] = append(info.aliases[edge.name], alt)
				grew = true
			}
		}
		if grew {
			for _, dependent := range dependents[edge.name] {
				if !queued[dependent] {
					queued[dependent] = true
					queue = append(queue, dependent)
				}
			}
		}
	}
}

// readNames lists the names whose alternatives altsOf reads for an expression: it mirrors altsOf.
func readNames(expr ast.Expr, imports map[string]string) []string {
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return readNames(e.X, imports)
	case *ast.StarExpr:
		return readNames(e.X, imports)
	case *ast.UnaryExpr:
		return readNames(e.X, imports)
	case *ast.IndexExpr:
		return readNames(e.X, imports)
	case *ast.Ident:
		return []string{e.Name}
	case *ast.SelectorExpr:
		if x, ok := e.X.(*ast.Ident); ok {
			if _, imported := imports[x.Name]; imported {
				return nil
			}
		}
		if e.Sel.Name == "With" {
			return nil
		}
		return []string{e.Sel.Name}
	case *ast.CompositeLit:
		var out []string
		for _, element := range e.Elts {
			if kv, ok := element.(*ast.KeyValueExpr); ok {
				element = kv.Value
			}
			out = append(out, readNames(element, imports)...)
		}
		return out
	}
	return nil
}

// altsOf returns the watched constructors an expression holds: a selector of a watched package, a name already known to
// hold some, a method value With, or the elements of a composite literal, a parenthesis or an index.
func (info *pkgInfo) altsOf(expr ast.Expr, imports map[string]string) [][2]string {
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return info.altsOf(e.X, imports)
	case *ast.StarExpr:
		return info.altsOf(e.X, imports)
	case *ast.UnaryExpr:
		return info.altsOf(e.X, imports)
	case *ast.IndexExpr:
		return info.altsOf(e.X, imports)
	case *ast.Ident:
		return info.aliases[e.Name]
	case *ast.SelectorExpr:
		if x, ok := e.X.(*ast.Ident); ok {
			if pkg, imported := imports[x.Name]; imported {
				return [][2]string{{pkg, e.Sel.Name}}
			}
		}
		if e.Sel.Name == "With" {
			return [][2]string{{"", "With"}} // a method value
		}
		return info.aliases[e.Sel.Name] // a field holding a constructor
	case *ast.CompositeLit:
		var out [][2]string
		for _, element := range e.Elts {
			if kv, ok := element.(*ast.KeyValueExpr); ok {
				element = kv.Value
			}
			alts := info.altsOf(element, imports)
			if !info.budget.charge(1 + 2*len(alts)) { // the copy below allocates: weigh it double
				return out
			}
			out = append(out, alts...)
		}
		return out
	}
	return nil
}

func scanFile(parsed parsedFile, info *pkgInfo) []Finding {
	file := parsed.file
	s := &fileScan{fset: parsed.fset, rel: parsed.rel, metricAttrs: map[*ast.CallExpr]bool{}, reported: map[string]bool{}, pkg: info,
		imports: importNames(file)}
	for _, spec := range file.Imports {
		if spec.Name != nil && spec.Name.Name == "." {
			if path, err := strconv.Unquote(spec.Path.Value); err == nil && watchedImports[path] {
				s.add(RuleDotImport, spec.Pos(), path)
			}
		}
	}
	s.markMetricFunctions(file)
	ast.Walk(s, file)
	return s.out
}

type fileScan struct {
	fset        *token.FileSet
	rel         string
	out         []Finding
	metricAttrs map[*ast.CallExpr]bool
	reported    map[string]bool
	pkg         *pkgInfo
	imports     map[string]string
	nodes       []ast.Node
	taints      []map[string]bool
	funcs       []string
}

// Visit keeps a stack of the open nodes and, per function, the identifiers that hold a raw request path.
func (s *fileScan) Visit(node ast.Node) ast.Visitor {
	if node != nil && s.pkg.budget.over {
		return nil
	}
	if node == nil {
		top := s.nodes[len(s.nodes)-1]
		s.nodes = s.nodes[:len(s.nodes)-1]
		switch top.(type) {
		case *ast.FuncDecl:
			s.funcs = s.funcs[:len(s.funcs)-1]
			s.taints = s.taints[:len(s.taints)-1]
		case *ast.FuncLit:
			s.taints = s.taints[:len(s.taints)-1]
		}
		return nil
	}
	s.nodes = append(s.nodes, node)
	switch fn := node.(type) {
	case *ast.FuncDecl:
		s.funcs = append(s.funcs, fn.Name.Name)
		s.taints = append(s.taints, rawPathIdents(fn.Body, s.pkg.rawFuncs, s.pkg.budget))
	case *ast.FuncLit:
		if len(s.taints) > 0 { // the enclosing function's pass already walked this body
			s.taints = append(s.taints, s.taints[len(s.taints)-1])
		} else {
			s.taints = append(s.taints, rawPathIdents(fn.Body, s.pkg.rawFuncs, s.pkg.budget))
		}
	case *ast.CallExpr:
		s.call(fn)
	}
	return s
}

func (s *fileScan) tainted(name string) bool {
	return len(s.taints) > 0 && s.taints[len(s.taints)-1][name]
}

// rawPathIdents returns the identifiers a function assigns from a raw request path or query, directly or through other
// tainted identifiers. Each right side is read once (the raw part, and the identifiers whose taint it would carry); the
// taint then spreads over those links with a worklist, so a long chain costs no repeated walk.
func rawPathIdents(body *ast.BlockStmt, rawFuncs map[string]bool, b *budget) map[string]bool {
	out := map[string]bool{}
	if body == nil {
		return out
	}
	type link struct {
		lhs     string
		carried []string
	}
	var links []link
	var queue []string
	add := func(lhs ast.Expr, rhs ast.Expr) {
		ident, ok := lhs.(*ast.Ident)
		if !ok {
			return
		}
		if rawRequestPart(rhs, rawFuncs, b) {
			if !out[ident.Name] {
				out[ident.Name] = true
				queue = append(queue, ident.Name)
			}
			return
		}
		links = append(links, link{ident.Name, carriedIdents(rhs, b, nil)})
	}
	ast.Inspect(body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.AssignStmt:
			if !b.charge(1 + len(n.Rhs)) {
				return false
			}
			for i, rhs := range n.Rhs {
				if i < len(n.Lhs) {
					add(n.Lhs[i], rhs)
				}
			}
		case *ast.ValueSpec:
			if !b.charge(1 + len(n.Values)) {
				return false
			}
			for i, value := range n.Values {
				if i < len(n.Names) {
					add(n.Names[i], value)
				}
			}
		}
		return true
	})
	dependents := map[string][]int{}
	for index, l := range links {
		for _, name := range l.carried {
			dependents[name] = append(dependents[name], index)
		}
	}
	for len(queue) > 0 && b.charge(1) {
		name := queue[0]
		queue = queue[1:]
		for _, index := range dependents[name] {
			if lhs := links[index].lhs; !out[lhs] {
				out[lhs] = true
				queue = append(queue, lhs)
			}
		}
	}
	return out
}

// carriedIdents lists the identifiers whose taint an expression would carry: it mirrors carriesTaint.
func carriedIdents(expr ast.Expr, b *budget, acc []string) []string {
	if !b.charge(1) {
		return acc
	}
	switch e := expr.(type) {
	case *ast.Ident:
		return append(acc, e.Name)
	case *ast.ParenExpr:
		return carriedIdents(e.X, b, acc)
	case *ast.BinaryExpr:
		return carriedIdents(e.Y, b, carriedIdents(e.X, b, acc))
	case *ast.UnaryExpr:
		return carriedIdents(e.X, b, acc)
	case *ast.SelectorExpr:
		return carriedIdents(e.X, b, acc)
	case *ast.SliceExpr:
		return carriedIdents(e.X, b, acc)
	case *ast.IndexExpr:
		return carriedIdents(e.X, b, acc)
	case *ast.CompositeLit:
		for _, element := range e.Elts {
			if kv, ok := element.(*ast.KeyValueExpr); ok {
				element = kv.Value
			}
			acc = carriedIdents(element, b, acc)
		}
	case *ast.CallExpr:
		shaping := false
		switch fun := e.Fun.(type) {
		case *ast.Ident:
			shaping = fun.Name == "string"
		case *ast.SelectorExpr:
			if ident, ok := fun.X.(*ast.Ident); ok && (ident.Name == "fmt" || ident.Name == "strings" || ident.Name == "path") {
				shaping = true
			} else if fun.Sel.Name == "String" || fun.Sel.Name == "Query" || fun.Sel.Name == "EscapedPath" {
				acc = carriedIdents(fun.X, b, acc)
			}
		}
		if shaping {
			for _, arg := range e.Args {
				acc = carriedIdents(arg, b, acc)
			}
		}
	}
	return acc
}

// carriesTaint reports an expression whose value keeps the text of a tainted identifier: a copy, a concatenation, a
// conversion, a slice, a field of it, or the result of a string-shaping call. Any other call is a new value.
func carriesTaint(expr ast.Expr, tainted map[string]bool, b *budget) bool {
	if !b.charge(1) {
		return false
	}
	switch e := expr.(type) {
	case *ast.Ident:
		return tainted[e.Name]
	case *ast.ParenExpr:
		return carriesTaint(e.X, tainted, b)
	case *ast.BinaryExpr:
		return carriesTaint(e.X, tainted, b) || carriesTaint(e.Y, tainted, b)
	case *ast.UnaryExpr:
		return carriesTaint(e.X, tainted, b)
	case *ast.SelectorExpr:
		return carriesTaint(e.X, tainted, b)
	case *ast.SliceExpr:
		return carriesTaint(e.X, tainted, b)
	case *ast.IndexExpr:
		return carriesTaint(e.X, tainted, b)
	case *ast.CompositeLit:
		for _, element := range e.Elts {
			if kv, ok := element.(*ast.KeyValueExpr); ok {
				element = kv.Value
			}
			if carriesTaint(element, tainted, b) {
				return true
			}
		}
	case *ast.CallExpr:
		shaping := false
		switch fun := e.Fun.(type) {
		case *ast.Ident:
			shaping = fun.Name == "string"
		case *ast.SelectorExpr:
			if ident, ok := fun.X.(*ast.Ident); ok && (ident.Name == "fmt" || ident.Name == "strings" || ident.Name == "path") {
				shaping = true
			} else if fun.Sel.Name == "String" || fun.Sel.Name == "Query" || fun.Sel.Name == "EscapedPath" {
				if carriesTaint(fun.X, tainted, b) {
					return true
				}
			}
		}
		if shaping {
			for _, arg := range e.Args {
				if carriesTaint(arg, tainted, b) {
					return true
				}
			}
		}
	}
	return false
}

// importNames maps each local import name to the package's name: the last element of its path, without a /vN suffix.
func importNames(file *ast.File) map[string]string {
	out := map[string]string{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		elements := strings.Split(path, "/")
		name := elements[len(elements)-1]
		if len(elements) > 1 && isMajorVersion(name) {
			name = elements[len(elements)-2]
		}
		local := name
		if spec.Name != nil {
			local = spec.Name.Name
		}
		out[local] = name
	}
	return out
}

func isMajorVersion(element string) bool {
	if len(element) < 2 || element[0] != 'v' {
		return false
	}
	for _, r := range element[1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// markMetricFunctions marks every attribute constructor inside a function that builds metric options or an
// attribute set: a label assembled in a slice or a set is a metric label too.
func (s *fileScan) markMetricFunctions(file *ast.File) {
	flagged := map[ast.Node]bool{} // a function with a metric-options or attribute-set call anywhere in its body, nested closures included
	var stack []ast.Node
	isFunc := func(node ast.Node) bool {
		switch fn := node.(type) {
		case *ast.FuncDecl:
			return fn.Body != nil
		case *ast.FuncLit:
			return true
		}
		return false
	}
	inspectCharged(s.pkg.budget, file, func(node ast.Node) bool {
		if node == nil {
			top := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if isFunc(top) && flagged[top] {
				for i := len(stack) - 1; i >= 0; i-- {
					if isFunc(stack[i]) {
						flagged[stack[i]] = true
						break
					}
				}
			}
			return true
		}
		stack = append(stack, node)
		if call, ok := node.(*ast.CallExpr); ok {
			pkg, name, ok := s.qualified(call.Fun)
			if ok && ((pkg == "metric" && (name == "WithAttributes" || name == "WithAttributeSet")) ||
				(pkg == "attribute" && name == "NewSet")) {
				for i := len(stack) - 1; i >= 0; i-- {
					if isFunc(stack[i]) {
						flagged[stack[i]] = true
						break
					}
				}
			}
		}
		return true
	})
	stack = stack[:0]
	inFlagged := 0
	inspectCharged(s.pkg.budget, file, func(node ast.Node) bool {
		if node == nil {
			top := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if isFunc(top) && flagged[top] {
				inFlagged--
			}
			return true
		}
		stack = append(stack, node)
		if isFunc(node) && flagged[node] {
			inFlagged++
		}
		if call, ok := node.(*ast.CallExpr); ok && inFlagged > 0 {
			if _, _, ok := s.attrParts(call); ok {
				s.metricAttrs[call] = true
			}
		}
		return true
	})
}

func (s *fileScan) add(rule string, pos token.Pos, detail string) {
	dedupe := fmt.Sprintf("%s|%d|%s", rule, s.fset.Position(pos).Line, detail)
	if s.reported[dedupe] {
		return
	}
	s.reported[dedupe] = true
	fn := "-"
	if len(s.funcs) > 0 {
		fn = s.funcs[len(s.funcs)-1]
	}
	s.out = append(s.out, Finding{Rule: rule, File: s.rel, Line: s.fset.Position(pos).Line, Func: fn, Detail: detail})
}

func unquote(literal string) string {
	if value, err := strconv.Unquote(literal); err == nil {
		return value
	}
	return literal
}

// keysOf returns the key texts an attribute key expression can hold: a string literal, or a constant (folded across the
// package, concatenations included). ok is false for any other expression.
func (s *fileScan) keysOf(expr ast.Expr) (keys []string, ok, over bool) {
	ctx := newFoldCtx(s.pkg.budget, s.pkg.consts, s.imports, s.pkg.stringTypes)
	values := foldString(expr, ctx, 0)
	if ctx.over {
		return nil, false, true
	}
	return values, len(values) > 0, false
}

// qualified names the package (by its import path's last element, whatever the local alias) and the function of a
// selector call. pkg is the identifier itself when it is not an import (a variable receiver).
func (s *fileScan) qualified(fun ast.Expr) (pkg, name string, ok bool) {
	if _, isIdent := fun.(*ast.Ident); isIdent {
		return "", "", false
	}
	sel, isSel := fun.(*ast.SelectorExpr)
	if !isSel {
		return "", "", false
	}
	if ident, isIdent := sel.X.(*ast.Ident); isIdent {
		if canonical, imported := s.imports[ident.Name]; imported {
			return canonical, sel.Sel.Name, true
		}
		return ident.Name, sel.Sel.Name, true
	}
	return "", sel.Sel.Name, true
}

// targets lists every (package, function) a call target can be: one for a selector, every alternative of a function
// variable, a field or an indexed holder (union over the package, fail closed).
func (s *fileScan) targets(fun ast.Expr) [][2]string {
	var out [][2]string
	switch f := fun.(type) {
	case *ast.ParenExpr:
		return s.targets(f.X)
	case *ast.Ident:
		if !s.pkg.budget.charge(1 + len(s.pkg.aliases[f.Name])) {
			return nil
		}
		return s.pkg.aliases[f.Name]
	case *ast.IndexExpr:
		alts := s.pkg.altsOf(f.X, s.imports)
		if !s.pkg.budget.charge(1 + len(alts)) {
			return nil
		}
		return alts
	case *ast.SelectorExpr:
		if pkg, name, ok := s.qualified(f); ok {
			out = append(out, [2]string{pkg, name})
		}
		if x, ok := f.X.(*ast.Ident); !ok || s.imports[x.Name] == "" {
			if !s.pkg.budget.charge(1 + len(s.pkg.aliases[f.Sel.Name])) {
				return out
			}
			out = append(out, s.pkg.aliases[f.Sel.Name]...)
		}
	}
	return out
}

// attrParts returns the key and value expressions of an OpenTelemetry attribute constructor call:
// attribute.String(k, v), an alias of it, or attribute.Key(k).String(v).
func (s *fileScan) attrParts(call *ast.CallExpr) (key, value ast.Expr, ok bool) {
	for _, target := range s.targets(call.Fun) {
		if target[0] == "attribute" && attrConstructors[target[1]] && len(call.Args) >= 2 {
			return call.Args[0], call.Args[1], true
		}
	}
	if sel, isSel := call.Fun.(*ast.SelectorExpr); isSel && attrConstructors[sel.Sel.Name] && len(call.Args) >= 1 {
		if ident, isIdent := sel.X.(*ast.Ident); isIdent && s.pkg.constNames[ident.Name] {
			return ident, call.Args[0], true // const k = attribute.Key("..."); k.String(v)
		}
		if inner, isCall := sel.X.(*ast.CallExpr); isCall && len(inner.Args) == 1 {
			if pkg, name, found := s.qualified(inner.Fun); found && pkg == "attribute" && name == "Key" {
				return inner.Args[0], call.Args[0], true
			}
		}
	}
	return nil, nil, false
}

func (s *fileScan) call(call *ast.CallExpr) {
	if key, value, ok := s.attrParts(call); ok {
		s.attribute(call, key, value)
	}
	for _, target := range s.targets(call.Fun) {
		if !s.pkg.budget.charge(1) {
			return
		}
		s.callTarget(call, target[0], target[1])
	}
}

func (s *fileScan) callTarget(call *ast.CallExpr, pkg, name string) {
	switch {
	case pkg == "slog" && slogAttrConstructors[name] && len(call.Args) >= 2:
		s.emitValue(call.Args[1], call.Pos())
	case pkg == "metric" && name == "WithAttributes":
		s.metricAttributes(call)
	case name == "RecordError" && len(call.Args) >= 1:
		s.add(RuleErrorText, call.Pos(), "RecordError")
	case pkg == "slog" && (name == "NewJSONHandler" || name == "NewTextHandler"):
		if !strings.HasPrefix(s.rel, rawHandlerAllowed) && !s.discardsOutput(call) {
			s.add(RuleRawHandler, call.Pos(), "slog."+name)
		}
	case pkg == "river" && name == "NewClient":
		s.riverClient(call)
	case logMethods[name] && len(call.Args) >= 3:
		s.logCall(call, strings.HasSuffix(name, "Context"))
	case name == "With":
		s.logPairs(call.Args, 0)
	}
}

func exprName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return exprName(e.X) + "." + e.Sel.Name
	case *ast.CallExpr:
		return exprName(e.Fun) + "()"
	}
	return ""
}

// logCall inspects the loose key/value arguments of a log call.
func (s *fileScan) logCall(call *ast.CallExpr, withContext bool) {
	skip := 1
	if withContext {
		skip = 2
	}
	s.logPairs(call.Args, skip)
}

// logPairs checks every argument from the given index as an emitted value: a key is never a raw request path, and a
// key held in a variable, a spread or a method value would otherwise hide its value (fail closed).
func (s *fileScan) logPairs(args []ast.Expr, from int) {
	for i := from; i < len(args); i++ {
		s.emitValue(args[i], args[i].Pos())
	}
}

// emitValue applies the value rules shared by every emit path.
func (s *fileScan) emitValue(value ast.Expr, pos token.Pos) {
	if rawRequestPart(value, s.pkg.rawFuncs, s.pkg.budget) || s.usesTaintedPath(value) {
		s.add(RuleRawPath, pos, "")
	}
	if s.containsDigest(value) {
		s.add(RuleIdentityHash, pos, "")
	}
}

func (s *fileScan) usesTaintedPath(expr ast.Expr) bool {
	found := false
	inspectCharged(s.pkg.budget, expr, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && s.tainted(ident.Name) {
			found = true
		}
		return !found
	})
	return found
}

func rawRequestPart(expr ast.Expr, rawFuncs map[string]bool, b *budget) bool {
	found := false
	inspectCharged(b, expr, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				if rawFuncs[fun.Name] {
					found = true
					return false
				}
				// A named classifier (pathClass(r.URL.Path)) turns the path into a bounded class.
				if fun.Name != "string" {
					return false
				}
			case *ast.SelectorExpr:
				if inner, ok := fun.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "URL" &&
					(fun.Sel.Name == "String" || fun.Sel.Name == "EscapedPath" || fun.Sel.Name == "Query") {
					found = true
					return false
				}
				if _, isIdent := fun.X.(*ast.Ident); !isIdent && rawFuncs[fun.Sel.Name] {
					found = true
					return false
				}
				if ident, isIdent := fun.X.(*ast.Ident); isIdent && rawFuncs[fun.Sel.Name] && ident.Name != "fmt" && ident.Name != "strings" {
					found = true
					return false
				}
			}
		}
		sel, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "RawQuery", "RequestURI", "RawPath":
			found = true
		case "Path":
			if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "URL" {
				found = true
			}
		}
		return !found
	})
	return found
}

func (s *fileScan) containsDigest(expr ast.Expr) bool {
	found := false
	inspectCharged(s.pkg.budget, expr, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if pkg, name, ok := s.qualified(call.Fun); ok && digestFuncs[pkg+"."+name] {
				found = true
			}
		}
		return !found
	})
	return found
}

// errorTextValue reports an expression that reads error text: x.Error() or a
// fmt formatting call.
func (s *fileScan) errorTextValue(expr ast.Expr) bool {
	found := false
	inspectCharged(s.pkg.budget, expr, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		pkg, name, ok := s.qualified(call.Fun)
		if !ok {
			return true
		}
		if name == "Error" && len(call.Args) == 0 {
			found = true
		}
		if pkg == "fmt" && (name == "Sprint" || name == "Sprintf" || name == "Sprintln" || name == "Errorf") {
			found = true
		}
		return !found
	})
	return found
}

func (s *fileScan) attribute(call *ast.CallExpr, keyExpr, value ast.Expr) {
	keys, literal, over := s.keysOf(keyExpr)
	if !literal && (over || s.unfoldableConst(keyExpr)) {
		// a constant (or a conversion of one) the scan cannot fold: fail closed rather than skip
		s.add(RuleProtectedKey, call.Pos(), "unfoldable const key")
	}
	label := "-"
	for _, key := range keys {
		if logging.ProtectedKey(key) {
			s.add(RuleProtectedKey, call.Pos(), key)
		}
		label = key
	}
	if s.errorTextValue(value) {
		s.add(RuleErrorText, call.Pos(), label)
	}
	s.emitValue(value, call.Pos())
	if s.metricAttrs[call] {
		s.metricLabel(call, keys, literal)
	} else if !literal && s.dynamicCallInKey(keyExpr) {
		s.add(RuleMetricLabel, call.Pos(), "dynamic key")
	}
}

// dynamicCallInKey reports a key expression with a call that is neither a conversion nor foldable (strings.ToLower(x),
// fmt.Sprint of a variable): the key text is computed at run time.
func (s *fileScan) dynamicCallInKey(expr ast.Expr) bool {
	found := false
	inspectCharged(s.pkg.budget, expr, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || found {
			return !found
		}
		if s.isConversion(call) {
			return true
		}
		if len(foldString(call, newFoldCtx(s.pkg.budget, s.pkg.consts, s.imports, s.pkg.stringTypes), 0)) == 0 {
			found = true
		}
		return !found
	})
	return found
}

func (s *fileScan) isConversion(call *ast.CallExpr) bool {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name == "string" || s.pkg.stringTypes[fun.Name]
	case *ast.SelectorExpr:
		if x, ok := fun.X.(*ast.Ident); ok {
			return s.imports[x.Name]+"."+fun.Sel.Name == "attribute.Key"
		}
	}
	return false
}

// unfoldableConst reports a key expression that names a package constant yet folds to no text.
func (s *fileScan) unfoldableConst(expr ast.Expr) bool {
	found := false
	inspectCharged(s.pkg.budget, expr, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && s.pkg.constNames[ident.Name] {
			found = true
		}
		return !found
	})
	return found
}

func (s *fileScan) metricAttributes(call *ast.CallExpr) {
	attrs := 0
	for _, arg := range call.Args {
		inner, ok := arg.(*ast.CallExpr)
		if !ok {
			continue
		}
		if _, _, ok := s.attrParts(inner); ok {
			s.metricAttrs[inner] = true
			attrs++
		}
	}
	if attrs > maxMetricLabels {
		s.add(RuleMetricLabel, call.Pos(), fmt.Sprintf("%d labels", attrs))
	}
}

func (s *fileScan) metricLabel(call *ast.CallExpr, keys []string, literal bool) {
	if !literal {
		s.add(RuleMetricLabel, call.Pos(), "dynamic key")
		return
	}
	for _, key := range keys {
		if unboundedLabel(key) {
			s.add(RuleMetricLabel, call.Pos(), key)
			return
		}
	}
}

func unboundedLabel(key string) bool {
	words := splitWords(key)
	if len(words) == 0 {
		return false
	}
	if !classLast[words[len(words)-1]] {
		for _, word := range words {
			if strongLabelWords[word] {
				return true
			}
		}
	}
	if labelAllowedLast[words[len(words)-1]] {
		return false
	}
	for _, word := range words {
		if weakLabelWords[word] {
			return true
		}
	}
	return false
}

// splitWords lower-cases a key and splits it at separators and at camel-case boundaries (orgId -> org, id).
func splitWords(key string) []string {
	runes := []rune(key)
	var words []string
	var current []rune
	flush := func() {
		if len(current) > 0 {
			words = append(words, strings.ToLower(string(current)))
			current = current[:0]
		}
	}
	for i, r := range runes {
		if r == '_' || r == '.' || r == '-' || r == ' ' {
			flush()
			continue
		}
		if unicode.IsUpper(r) && len(current) > 0 {
			previous := runes[i-1]
			nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || (unicode.IsUpper(previous) && nextLower) {
				flush()
			}
		}
		current = append(current, r)
	}
	flush()
	return words
}

func (s *fileScan) riverClient(call *ast.CallExpr) {
	if len(call.Args) < 2 {
		s.add(RuleRiverLogger, call.Pos(), "config")
		return
	}
	config := call.Args[1]
	if unary, ok := config.(*ast.UnaryExpr); ok {
		config = unary.X
	}
	lit, ok := config.(*ast.CompositeLit)
	if !ok {
		s.add(RuleRiverLogger, call.Pos(), "config not a literal")
		return
	}
	for _, element := range lit.Elts {
		if kv, ok := element.(*ast.KeyValueExpr); ok {
			if ident, ok := kv.Key.(*ast.Ident); ok && ident.Name == "Logger" {
				return
			}
		}
	}
	s.add(RuleRiverLogger, call.Pos(), "no Logger")
}

// discardsOutput reports a handler that writes to io.Discard: it emits nothing.
func (s *fileScan) discardsOutput(call *ast.CallExpr) bool {
	if len(call.Args) == 0 {
		return false
	}
	sel, ok := call.Args[0].(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Discard" {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && s.imports[ident.Name] == "io"
}
