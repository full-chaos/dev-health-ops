// Package emitscan is the static half of the telemetry hygiene gate. It reads
// the Go source of the emit paths (slog calls, OpenTelemetry attributes and
// metric labels, River client construction) and reports a finding as a rule id
// and a file:line only. The redaction rules themselves live in
// internal/platform/logging; this package keeps no copy of them.
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
	rel  string
	fset *token.FileSet
	file *ast.File
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
			byDir[dirKey] = append(byDir[dirKey], parsedFile{rel: rel, fset: fset, file: file})
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
		for _, parsed := range group {
			out = append(out, scanFile(parsed, info)...)
		}
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
type pkgInfo struct {
	consts   map[string][]string // string constants, every value a name holds (function-local names can repeat)
	rawFuncs map[string]bool     // functions whose result carries a raw request path or query
}

func packageInfo(group []parsedFile) *pkgInfo {
	info := &pkgInfo{consts: map[string][]string{}, rawFuncs: map[string]bool{}}
	for changed := true; changed; {
		changed = false
		for _, parsed := range group {
			ast.Inspect(parsed.file, func(node ast.Node) bool {
				gen, ok := node.(*ast.GenDecl)
				if !ok || gen.Tok != token.CONST {
					return true
				}
				for _, spec := range gen.Specs {
					value := spec.(*ast.ValueSpec)
					for i, name := range value.Names {
						if i >= len(value.Values) {
							continue
						}
						for _, folded := range foldString(value.Values[i], info.consts) {
							if !contains(info.consts[name.Name], folded) {
								info.consts[name.Name] = append(info.consts[name.Name], folded)
								changed = true
							}
						}
					}
				}
				return true
			})
		}
	}
	for changed := true; changed; {
		changed = false
		for _, parsed := range group {
			for _, decl := range parsed.file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil || info.rawFuncs[fn.Name.Name] {
					continue
				}
				taint := rawPathIdents(fn.Body, nil, info.rawFuncs)
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					if ret, ok := node.(*ast.ReturnStmt); ok {
						for _, result := range ret.Results {
							if rawRequestPart(result, info.rawFuncs) || carriesTaint(result, taint) {
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

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

// foldString evaluates a string constant expression: literals, constant names and +.
func foldString(expr ast.Expr, consts map[string][]string) []string {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind == token.STRING {
			return []string{unquote(e.Value)}
		}
	case *ast.ParenExpr:
		return foldString(e.X, consts)
	case *ast.Ident:
		return consts[e.Name]
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return nil
		}
		var out []string
		for _, left := range foldString(e.X, consts) {
			for _, right := range foldString(e.Y, consts) {
				if len(out) < 8 {
					out = append(out, left+right)
				}
			}
		}
		return out
	}
	return nil
}

func scanFile(parsed parsedFile, info *pkgInfo) []Finding {
	file := parsed.file
	s := &fileScan{fset: parsed.fset, rel: parsed.rel, metricAttrs: map[*ast.CallExpr]bool{}, pkg: info,
		imports: importNames(file), aliases: map[string][2]string{}}
	s.collectAliases(file)
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
	pkg         *pkgInfo
	imports     map[string]string
	aliases     map[string][2]string // local function variable -> (package, function) of a watched constructor
	nodes       []ast.Node
	taints      []map[string]bool
	funcs       []string
}

// Visit keeps a stack of the open nodes and, per function, the identifiers that hold a raw request path.
func (s *fileScan) Visit(node ast.Node) ast.Visitor {
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
		s.taints = append(s.taints, rawPathIdents(fn.Body, nil, s.pkg.rawFuncs))
	case *ast.FuncLit:
		var parent map[string]bool
		if len(s.taints) > 0 {
			parent = s.taints[len(s.taints)-1]
		}
		s.taints = append(s.taints, rawPathIdents(fn.Body, parent, s.pkg.rawFuncs))
	case *ast.CallExpr:
		s.call(fn)
	}
	return s
}

func (s *fileScan) tainted(name string) bool {
	return len(s.taints) > 0 && s.taints[len(s.taints)-1][name]
}

// rawPathIdents returns the identifiers a function (with the identifiers its enclosing function already holds) assigns
// from a raw request path or query, directly or through other tainted identifiers, to a fixpoint.
func rawPathIdents(body *ast.BlockStmt, seed map[string]bool, rawFuncs map[string]bool) map[string]bool {
	out := map[string]bool{}
	for name := range seed {
		out[name] = true
	}
	if body == nil {
		return out
	}
	holds := func(expr ast.Expr) bool { return rawRequestPart(expr, rawFuncs) || carriesTaint(expr, out) }
	for changed := true; changed; {
		changed = false
		ast.Inspect(body, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				for i, rhs := range n.Rhs {
					if i < len(n.Lhs) && holds(rhs) {
						if ident, ok := n.Lhs[i].(*ast.Ident); ok && !out[ident.Name] {
							out[ident.Name] = true
							changed = true
						}
					}
				}
			case *ast.ValueSpec:
				for i, value := range n.Values {
					if i < len(n.Names) && holds(value) && !out[n.Names[i].Name] {
						out[n.Names[i].Name] = true
						changed = true
					}
				}
			}
			return true
		})
	}
	return out
}

// carriesTaint reports an expression whose value keeps the text of a tainted identifier: a copy, a concatenation, a
// conversion, a slice, a field of it, or the result of a string-shaping call. Any other call is a new value.
func carriesTaint(expr ast.Expr, tainted map[string]bool) bool {
	switch e := expr.(type) {
	case *ast.Ident:
		return tainted[e.Name]
	case *ast.ParenExpr:
		return carriesTaint(e.X, tainted)
	case *ast.BinaryExpr:
		return carriesTaint(e.X, tainted) || carriesTaint(e.Y, tainted)
	case *ast.UnaryExpr:
		return carriesTaint(e.X, tainted)
	case *ast.SelectorExpr:
		return carriesTaint(e.X, tainted)
	case *ast.SliceExpr:
		return carriesTaint(e.X, tainted)
	case *ast.IndexExpr:
		return carriesTaint(e.X, tainted)
	case *ast.CompositeLit:
		for _, element := range e.Elts {
			if kv, ok := element.(*ast.KeyValueExpr); ok {
				element = kv.Value
			}
			if carriesTaint(element, tainted) {
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
				if carriesTaint(fun.X, tainted) {
					return true
				}
			}
		}
		if shaping {
			for _, arg := range e.Args {
				if carriesTaint(arg, tainted) {
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

// collectAliases records function variables that hold a watched constructor (var str = attribute.String).
func (s *fileScan) collectAliases(file *ast.File) {
	note := func(name ast.Expr, value ast.Expr) {
		ident, ok := name.(*ast.Ident)
		sel, isSel := value.(*ast.SelectorExpr)
		if !ok || !isSel {
			return
		}
		x, ok2 := sel.X.(*ast.Ident)
		if !ok2 {
			return
		}
		if pkg, imported := s.imports[x.Name]; imported {
			s.aliases[ident.Name] = [2]string{pkg, sel.Sel.Name}
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.ValueSpec:
			for i, value := range n.Values {
				if i < len(n.Names) {
					note(n.Names[i], value)
				}
			}
		case *ast.AssignStmt:
			for i, rhs := range n.Rhs {
				if i < len(n.Lhs) {
					note(n.Lhs[i], rhs)
				}
			}
		}
		return true
	})
}

// markMetricFunctions marks every attribute constructor inside a function that builds metric options or an
// attribute set: a label assembled in a slice or a set is a metric label too.
func (s *fileScan) markMetricFunctions(file *ast.File) {
	ast.Inspect(file, func(node ast.Node) bool {
		var body *ast.BlockStmt
		switch fn := node.(type) {
		case *ast.FuncDecl:
			body = fn.Body
		case *ast.FuncLit:
			body = fn.Body
		}
		if body == nil || !s.buildsMetricLabels(body) {
			return true
		}
		ast.Inspect(body, func(inner ast.Node) bool {
			if call, ok := inner.(*ast.CallExpr); ok {
				if _, _, ok := s.attrParts(call); ok {
					s.metricAttrs[call] = true
				}
			}
			return true
		})
		return true
	})
}

func (s *fileScan) buildsMetricLabels(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			pkg, name, ok := s.qualified(call.Fun)
			if ok && ((pkg == "metric" && (name == "WithAttributes" || name == "WithAttributeSet")) ||
				(pkg == "attribute" && name == "NewSet")) {
				found = true
			}
		}
		return !found
	})
	return found
}

func (s *fileScan) add(rule string, pos token.Pos, detail string) {
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
func (s *fileScan) keysOf(expr ast.Expr) ([]string, bool) {
	values := foldString(expr, s.pkg.consts)
	return values, len(values) > 0
}

// qualified names the package (by its import path's last element, whatever the local alias) and the function of a
// selector call. pkg is the identifier itself when it is not an import (a variable receiver).
func (s *fileScan) qualified(fun ast.Expr) (pkg, name string, ok bool) {
	if ident, isIdent := fun.(*ast.Ident); isIdent {
		if alias, ok := s.aliases[ident.Name]; ok {
			return alias[0], alias[1], true
		}
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

// attrParts returns the key and value expressions of an OpenTelemetry attribute constructor call:
// attribute.String(k, v), an alias of it, or attribute.Key(k).String(v).
func (s *fileScan) attrParts(call *ast.CallExpr) (key, value ast.Expr, ok bool) {
	if pkg, name, found := s.qualified(call.Fun); found && pkg == "attribute" && attrConstructors[name] && len(call.Args) >= 2 {
		return call.Args[0], call.Args[1], true
	}
	if sel, isSel := call.Fun.(*ast.SelectorExpr); isSel && attrConstructors[sel.Sel.Name] && len(call.Args) >= 1 {
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
		return
	}
	pkg, name, ok := s.qualified(call.Fun)
	if !ok {
		return
	}
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
	case name == "With" && len(call.Args) >= 2 && pkg != "attribute":
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

// logPairs checks the value of every key/value pair that starts with a string key.
func (s *fileScan) logPairs(args []ast.Expr, from int) {
	for i := from; i < len(args); i++ {
		if _, isKey := s.keysOf(args[i]); isKey && i+1 < len(args) {
			s.emitValue(args[i+1], args[i+1].Pos())
			i++
		}
	}
}

// emitValue applies the value rules shared by every emit path.
func (s *fileScan) emitValue(value ast.Expr, pos token.Pos) {
	if rawRequestPart(value, s.pkg.rawFuncs) || s.usesTaintedPath(value) {
		s.add(RuleRawPath, pos, "")
	}
	if s.containsDigest(value) {
		s.add(RuleIdentityHash, pos, "")
	}
}

func (s *fileScan) usesTaintedPath(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && s.tainted(ident.Name) {
			found = true
		}
		return !found
	})
	return found
}

func rawRequestPart(expr ast.Expr, rawFuncs map[string]bool) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
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
	ast.Inspect(expr, func(node ast.Node) bool {
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
	ast.Inspect(expr, func(node ast.Node) bool {
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
	keys, literal := s.keysOf(keyExpr)
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
	}
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
