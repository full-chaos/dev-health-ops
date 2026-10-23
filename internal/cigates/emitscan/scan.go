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
	"strings"

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
	Detail string
}

func (f Finding) Where() string { return fmt.Sprintf("%s:%d", f.File, f.Line) }

func (f Finding) String() string {
	if f.Detail == "" {
		return f.Rule + " " + f.Where()
	}
	return f.Rule + " " + f.Where() + " " + f.Detail
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

// labelWords are words that make a metric label key unbounded or free text.
var labelWords = map[string]bool{
	"id": true, "ids": true, "uuid": true, "email": true, "path": true, "url": true, "uri": true, "query": true,
	"ip": true, "address": true, "session": true, "user": true, "username": true, "message": true, "msg": true,
	"text": true, "error": true, "err": true, "request": true, "trace": true, "span": true, "name": true,
}

// labelAllowedLast are final words that name a bounded class of an unbounded thing.
var labelAllowedLast = map[string]bool{"kind": true, "class": true, "type": true, "category": true, "code": true, "count": true, "status": true}

// Scan walks dirs under root (non-test .go files only) and returns every
// finding, ordered by file and line. A file that cannot be read or parsed is a
// finding of rule R0: the scan fails closed.
func Scan(root string, dirs []string) ([]Finding, error) {
	var out []Finding
	files := 0
	for _, dir := range dirs {
		base := filepath.Join(root, dir)
		err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				rel, _ := filepath.Rel(root, path)
				out = append(out, Finding{Rule: RuleUnparsable, File: filepath.ToSlash(rel), Line: 1})
				return nil
			}
			if entry.IsDir() {
				if name := entry.Name(); name == "testdata" || name == "testsupport" || strings.HasPrefix(name, ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			files++
			rel, _ := filepath.Rel(root, path)
			out = append(out, scanFile(path, filepath.ToSlash(rel))...)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if files == 0 {
		return nil, fmt.Errorf("emitscan: no source files read under %v", dirs)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Line < out[j].Line
	})
	return out, nil
}

func scanFile(path, rel string) []Finding {
	source, err := os.ReadFile(path)
	if err != nil {
		return []Finding{{Rule: RuleUnparsable, File: rel, Line: 1}}
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, rel, source, 0)
	if err != nil {
		return []Finding{{Rule: RuleUnparsable, File: rel, Line: 1}}
	}
	s := &fileScan{fset: fset, rel: rel, metricAttrs: map[*ast.CallExpr]bool{}, consts: stringConsts(file)}
	ast.Inspect(file, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			s.call(call)
		}
		return true
	})
	return s.out
}

type fileScan struct {
	fset        *token.FileSet
	rel         string
	out         []Finding
	metricAttrs map[*ast.CallExpr]bool
	consts      map[string]string
}

func (s *fileScan) add(rule string, pos token.Pos, detail string) {
	s.out = append(s.out, Finding{Rule: rule, File: s.rel, Line: s.fset.Position(pos).Line, Detail: detail})
}

func stringConsts(file *ast.File) map[string]string {
	out := map[string]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if i < len(value.Values) {
					if lit, ok := value.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						out[name.Name] = unquote(lit.Value)
					}
				}
			}
		}
	}
	return out
}

func unquote(literal string) string {
	if len(literal) >= 2 {
		return literal[1 : len(literal)-1]
	}
	return literal
}

// keyOf returns the key text of an attribute key expression: a string literal or
// a string constant of the same file. ok is false for any other expression.
func (s *fileScan) keyOf(expr ast.Expr) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind == token.STRING {
			return unquote(e.Value), true
		}
	case *ast.Ident:
		value, ok := s.consts[e.Name]
		return value, ok
	}
	return "", false
}

func qualified(fun ast.Expr) (pkg, name string, ok bool) {
	sel, isSel := fun.(*ast.SelectorExpr)
	if !isSel {
		return "", "", false
	}
	if ident, isIdent := sel.X.(*ast.Ident); isIdent {
		return ident.Name, sel.Sel.Name, true
	}
	return "", sel.Sel.Name, true
}

func (s *fileScan) call(call *ast.CallExpr) {
	pkg, name, ok := qualified(call.Fun)
	if !ok {
		return
	}
	switch {
	case pkg == "attribute" && attrConstructors[name] && len(call.Args) >= 2:
		s.attribute(call)
	case pkg == "slog" && slogAttrConstructors[name] && len(call.Args) >= 2:
		s.emitValue(call.Args[1], call.Pos())
	case pkg == "metric" && name == "WithAttributes":
		s.metricAttributes(call)
	case name == "RecordError" && len(call.Args) >= 1:
		s.add(RuleErrorText, call.Pos(), "RecordError")
	case pkg == "slog" && (name == "NewJSONHandler" || name == "NewTextHandler"):
		if !strings.HasPrefix(s.rel, rawHandlerAllowed) && !discardsOutput(call) {
			s.add(RuleRawHandler, call.Pos(), "slog."+name)
		}
	case pkg == "river" && name == "NewClient":
		s.riverClient(call)
	case logMethods[name] && len(call.Args) >= 3:
		s.logCall(call, strings.HasSuffix(name, "Context"))
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
	for i := skip; i < len(call.Args); i++ {
		if lit, ok := call.Args[i].(*ast.BasicLit); ok && lit.Kind == token.STRING && i+1 < len(call.Args) {
			s.emitValue(call.Args[i+1], call.Args[i+1].Pos())
			i++
		}
	}
}

// emitValue applies the value rules shared by every emit path.
func (s *fileScan) emitValue(value ast.Expr, pos token.Pos) {
	if rawRequestPart(value) {
		s.add(RuleRawPath, pos, "")
	}
	if containsDigest(value) {
		s.add(RuleIdentityHash, pos, "")
	}
}

func rawRequestPart(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			// A named classifier (pathClass(r.URL.Path)) turns the path into a bounded class.
			if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name != "string" {
				return false
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

func containsDigest(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if pkg, name, ok := qualified(call.Fun); ok && digestFuncs[pkg+"."+name] {
				found = true
			}
		}
		return !found
	})
	return found
}

// errorTextValue reports an expression that reads error text: x.Error() or a
// fmt formatting call.
func errorTextValue(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		pkg, name, ok := qualified(call.Fun)
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

func (s *fileScan) attribute(call *ast.CallExpr) {
	key, literal := s.keyOf(call.Args[0])
	if literal && logging.ProtectedKey(key) {
		s.add(RuleProtectedKey, call.Pos(), key)
	}
	if errorTextValue(call.Args[1]) {
		s.add(RuleErrorText, call.Pos(), key)
	}
	s.emitValue(call.Args[1], call.Pos())
	if s.metricAttrs[call] {
		s.metricLabel(call, key, literal)
	}
}

func (s *fileScan) metricAttributes(call *ast.CallExpr) {
	attrs := 0
	for _, arg := range call.Args {
		inner, ok := arg.(*ast.CallExpr)
		if !ok {
			continue
		}
		if pkg, name, ok := qualified(inner.Fun); ok && pkg == "attribute" && attrConstructors[name] {
			s.metricAttrs[inner] = true
			attrs++
		}
	}
	if attrs > maxMetricLabels {
		s.add(RuleMetricLabel, call.Pos(), fmt.Sprintf("%d labels", attrs))
	}
}

func (s *fileScan) metricLabel(call *ast.CallExpr, key string, literal bool) {
	if !literal {
		s.add(RuleMetricLabel, call.Pos(), "dynamic key")
		return
	}
	words := splitWords(key)
	if len(words) == 0 {
		return
	}
	if labelAllowedLast[words[len(words)-1]] {
		return
	}
	for _, word := range words {
		if labelWords[word] {
			s.add(RuleMetricLabel, call.Pos(), key)
			return
		}
	}
}

func splitWords(key string) []string {
	return strings.FieldsFunc(strings.ToLower(key), func(r rune) bool { return r == '_' || r == '.' || r == '-' })
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

// discardsOutput reports a handler that writes to io.Discard or a local
// discard type: it emits nothing, so it carries nothing.
func discardsOutput(call *ast.CallExpr) bool {
	if len(call.Args) == 0 {
		return false
	}
	switch arg := call.Args[0].(type) {
	case *ast.SelectorExpr:
		return exprName(arg) == "io.Discard"
	case *ast.CompositeLit:
		if ident, ok := arg.Type.(*ast.Ident); ok {
			return ident.Name == "discard"
		}
	}
	return false
}
