package routescan

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Registration is one route registration found in the source: where it is written and which guard level its handler
// expression names.
type Registration struct {
	File    string
	Line    int      // the line of the pattern expression
	Method  string   // "GET", ... ; "" when the method is not a literal
	Methods []string // a Methods: list, when the registration names one
	Pattern string   // fully resolved path; "" when it is not
	Tail    string   // the literal tail of the path expression
	Head    string   // the literal head of a path expression whose tail is a variable
	Level   string   // Public, Optional, Authenticated, Admin, AdminOrg, Superuser, or "" for none named
	Mux     string   // the receiver of a Handle/HandleFunc registration (ScanWithMux), e.g. "mcpMux"
}

var methodNames = map[string]string{
	"MethodGet": "GET", "MethodHead": "HEAD", "MethodPost": "POST", "MethodPut": "PUT",
	"MethodPatch": "PATCH", "MethodDelete": "DELETE", "MethodOptions": "OPTIONS",
}

var guardLevels = map[string]bool{"Public": true, "Optional": true, "Authenticated": true, "Admin": true, "AdminOrg": true, "Superuser": true}

// Scan parses the non-test Go files under dirs and returns every httpapi.Route-style literal (a Pattern: key) and
// every sso-style route(method, path, handler) call. Path expressions are resolved through the string constants and
// constant-initialized locals of the file's directory.
func Scan(dirs ...string) ([]Registration, error) { return scan(false, dirs...) }

// ScanWithMux is Scan plus every Handle/HandleFunc call whose first argument resolves to a path (method "": any).
func ScanWithMux(dirs ...string) ([]Registration, error) { return scan(true, dirs...) }

func scan(mux bool, dirs ...string) ([]Registration, error) {
	var out []Registration
	for _, root := range dirs {
		byDir := map[string][]string{}
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			byDir[filepath.Dir(path)] = append(byDir[filepath.Dir(path)], path)
			return nil
		})
		if err != nil {
			return nil, err
		}
		dirNames := make([]string, 0, len(byDir))
		for dir := range byDir {
			dirNames = append(dirNames, dir)
		}
		sort.Strings(dirNames)
		for _, dir := range dirNames {
			regs, err := scanDir(byDir[dir], mux)
			if err != nil {
				return nil, err
			}
			out = append(out, regs...)
		}
	}
	return out, nil
}

func scanDir(files []string, mux bool) ([]Registration, error) {
	sort.Strings(files)
	fset := token.NewFileSet()
	parsed := map[string]*ast.File{}
	consts := map[string]string{}
	for _, path := range files {
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil, err
		}
		parsed[path] = file
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.ValueSpec:
				for i, ident := range n.Names {
					if i < len(n.Values) {
						if value, ok := literalString(n.Values[i], consts); ok {
							consts[ident.Name] = value
						}
					}
				}
			case *ast.AssignStmt:
				if n.Tok == token.DEFINE && len(n.Lhs) == len(n.Rhs) {
					for i, lhs := range n.Lhs {
						if ident, ok := lhs.(*ast.Ident); ok {
							if value, ok := literalString(n.Rhs[i], consts); ok {
								consts[ident.Name] = value
							}
						}
					}
				}
			}
			return true
		})
	}
	closureLevel := map[string]string{}
	for _, path := range files {
		ast.Inspect(parsed[path], func(node ast.Node) bool {
			if decl, ok := node.(*ast.FuncDecl); ok && decl.Body != nil {
				if level := levelIn(decl.Body, nil); level != "" {
					closureLevel[decl.Name.Name] = level
				}
				return true
			}
			assign, ok := node.(*ast.AssignStmt)
			if !ok || assign.Tok != token.DEFINE || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
				return true
			}
			lit, ok := assign.Rhs[0].(*ast.FuncLit)
			ident, isIdent := assign.Lhs[0].(*ast.Ident)
			if !ok || !isIdent {
				return true
			}
			if level := levelIn(lit.Body, nil); level != "" {
				closureLevel[ident.Name] = level
			}
			return true
		})
	}
	var out []Registration
	for _, path := range files {
		ast.Inspect(parsed[path], func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CompositeLit:
				var methodExpr, methodsExpr, patternExpr, handlerExpr ast.Expr
				for _, element := range n.Elts {
					kv, ok := element.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, _ := kv.Key.(*ast.Ident)
					if key == nil {
						continue
					}
					switch key.Name {
					case "Method":
						methodExpr = kv.Value
					case "Methods":
						methodsExpr = kv.Value
					case "Pattern":
						patternExpr = kv.Value
					case "Handler":
						handlerExpr = kv.Value
					}
				}
				if patternExpr != nil {
					reg := buildRegistration(fset, path, methodExpr, patternExpr, handlerExpr, consts, closureLevel)
					reg.Methods = methodList(methodsExpr, consts)
					out = append(out, reg)
				}
			case *ast.CallExpr:
				if ident, ok := n.Fun.(*ast.Ident); ok && ident.Name == "route" && len(n.Args) == 3 {
					reg := buildRegistration(fset, path, n.Args[0], n.Args[1], n.Args[2], consts, closureLevel)
					if reg.Pattern != "" {
						reg.Pattern = consts["prefix"] + reg.Pattern
					}
					out = append(out, reg)
				}
				if selector, ok := n.Fun.(*ast.SelectorExpr); mux && ok && len(n.Args) == 2 && (selector.Sel.Name == "Handle" || selector.Sel.Name == "HandleFunc") {
					reg := buildRegistration(fset, path, nil, n.Args[0], n.Args[1], consts, closureLevel)
					if receiver, ok := selector.X.(*ast.Ident); ok {
						reg.Mux = receiver.Name
					}
					if reg.Pattern != "" {
						out = append(out, reg)
					}
				}
			}
			return true
		})
	}
	return out, nil
}

func methodList(expr ast.Expr, consts map[string]string) []string {
	list, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil
	}
	var out []string
	for _, element := range list.Elts {
		if selector, ok := element.(*ast.SelectorExpr); ok && methodNames[selector.Sel.Name] != "" {
			out = append(out, methodNames[selector.Sel.Name])
		} else if value, ok := literalString(element, consts); ok {
			out = append(out, value)
		}
	}
	return out
}

func literalString(expr ast.Expr, consts map[string]string) (string, bool) {
	switch typed := expr.(type) {
	case *ast.BasicLit:
		if typed.Kind == token.STRING {
			value, err := strconv.Unquote(typed.Value)
			return value, err == nil
		}
	case *ast.Ident:
		value, ok := consts[typed.Name]
		return value, ok
	case *ast.BinaryExpr:
		if typed.Op == token.ADD {
			left, lok := literalString(typed.X, consts)
			right, rok := literalString(typed.Y, consts)
			return left + right, lok && rok
		}
	case *ast.ParenExpr:
		return literalString(typed.X, consts)
	}
	return "", false
}

// tailString is the right-most string literal of a path expression.
func tailString(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.BasicLit:
		if typed.Kind == token.STRING {
			value, _ := strconv.Unquote(typed.Value)
			return value
		}
	case *ast.BinaryExpr:
		if typed.Op == token.ADD {
			return tailString(typed.Y)
		}
	}
	return ""
}

func buildRegistration(fset *token.FileSet, path string, methodExpr, patternExpr, handlerExpr ast.Expr, consts map[string]string, closures map[string]string) Registration {
	reg := Registration{File: filepath.ToSlash(path), Line: fset.Position(patternExpr.Pos()).Line}
	if selector, ok := methodExpr.(*ast.SelectorExpr); ok {
		reg.Method = methodNames[selector.Sel.Name]
	} else if methodExpr != nil {
		if value, ok := literalString(methodExpr, consts); ok {
			reg.Method = value
		}
	}
	if value, ok := literalString(patternExpr, consts); ok {
		reg.Pattern = value
	}
	reg.Tail = tailString(patternExpr)
	if binary, ok := patternExpr.(*ast.BinaryExpr); ok && reg.Pattern == "" && binary.Op == token.ADD {
		reg.Head, _ = literalString(binary.X, consts)
	}
	if handlerExpr != nil {
		reg.Level = levelIn(handlerExpr, closures)
	}
	return reg
}

// levelIn is the first guard level a subtree names: a policy.<Level> selector, or a call of a local closure whose
// body names one.
func levelIn(root ast.Node, closures map[string]string) string {
	level := ""
	ast.Inspect(root, func(node ast.Node) bool {
		if level != "" {
			return false
		}
		switch n := node.(type) {
		case *ast.SelectorExpr:
			if pkg, ok := n.X.(*ast.Ident); ok && pkg.Name == "policy" && guardLevels[n.Sel.Name] {
				level = n.Sel.Name
			}
		case *ast.CallExpr:
			// The platform wraps name an action, not a level: WrapPlatform is the Superuser level, WrapAdminOr the
			// Admin level.
			if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
				switch sel.Sel.Name {
				case "WrapPlatform", "BodyFirstPlatform", "bodyFirstPlatform":
					level = "Superuser"
				case "WrapAdminOr", "BodyFirstAdminOr", "bodyFirstAdminOr":
					level = "Admin"
				}
			}
			switch fun := n.Fun.(type) {
			case *ast.Ident:
				if closure, ok := closures[fun.Name]; ok {
					level = closure
				}
			case *ast.SelectorExpr:
				if closure, ok := closures[fun.Sel.Name]; ok {
					level = closure
				}
			}
		}
		return true
	})
	return level
}

// Accepts reports whether the registration serves method: a literal method equal to it, a Methods list holding it, or
// neither (any method).
func (r Registration) Accepts(method string) bool {
	switch {
	case r.Method != "":
		return r.Method == method
	case len(r.Methods) > 0:
		for _, m := range r.Methods {
			if m == method {
				return true
			}
		}
		return false
	}
	return true
}

// Resolve returns the one registration in file that serves method and pattern exactly. A row's source anchor is
// that symbol, not a line: zero or two registrations is an error that says the anchor is stale or ambiguous.
func Resolve(regs []Registration, file, method, pattern string) (Registration, error) {
	return ResolveWhere(regs, file, method, pattern, nil)
}

// ResolveWhere is Resolve over the registrations keep accepts (nil keeps all): a symbol can name the mux too.
func ResolveWhere(regs []Registration, file, method, pattern string, keep func(Registration) bool) (Registration, error) {
	var found []Registration
	for _, reg := range regs {
		if reg.File == file && reg.Pattern == pattern && reg.Accepts(method) && (keep == nil || keep(reg)) {
			found = append(found, reg)
		}
	}
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return Registration{}, fmt.Errorf("source anchor stale: %s holds no registration of %s %s", file, method, pattern)
	}
	lines := make([]string, len(found))
	for i, reg := range found {
		lines[i] = strconv.Itoa(reg.Line)
	}
	return Registration{}, fmt.Errorf("source anchor ambiguous: %s registers %s %s %d times (lines %s)", file, method, pattern, len(found), strings.Join(lines, ", "))
}

// NoteLines returns the lines (1-based) of source that hold note. A note that ends in an identifier character must
// not run on into another one: "func (g *Guard) Wrap" is not on the line of "func (g *Guard) WrapAdminOr(".
func NoteLines(source []byte, note string) []int {
	var out []int
	if note == "" {
		return out
	}
	identEnd := isIdentByte(note[len(note)-1])
	for i, line := range strings.Split(string(source), "\n") {
		for from := 0; ; {
			at := strings.Index(line[from:], note)
			if at < 0 {
				break
			}
			end := from + at + len(note)
			if !identEnd || end == len(line) || !isIdentByte(line[end]) {
				out = append(out, i+1)
				break
			}
			from += at + 1
		}
	}
	return out
}

func isIdentByte(b byte) bool {
	return b == '_' || ('0' <= b && b <= '9') || ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z')
}

// ResolveNote finds the one place a validator anchor's note names in source (the file at path): the one line that
// holds it, or, when it is on several lines and the anchor sits in the row's own source file (rowFile), the row's
// registration (own). problem is empty when it resolves; drifted says the cited line is not that place.
func ResolveNote(source []byte, path, note string, line int, rowFile string, own func() (Registration, error)) (problem string, drifted bool) {
	at := NoteLines(source, note)
	switch {
	case len(at) == 1:
		return "", at[0] != line
	case len(at) == 0:
		return fmt.Sprintf("anchor stale: %s holds no line with %q (cited at line %d)", path, note, line), false
	case rowFile != path:
		return fmt.Sprintf("anchor ambiguous: %q is on %d lines of %s and the row is registered elsewhere: anchor a unique text", note, len(at), path), false
	}
	reg, err := own()
	if err != nil {
		return fmt.Sprintf("anchor %q on the row's registration: %v", note, err), false
	}
	for _, n := range at {
		if n == reg.Line {
			return "", reg.Line != line
		}
	}
	return fmt.Sprintf("anchor stale: the row's registration (%s:%d) does not hold %q", path, reg.Line, note), false
}
