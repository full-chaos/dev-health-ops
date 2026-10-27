//go:build ignore

// gotestfuncs prints, for each Go test file named on the command line, the top-level tests, the other names go test
// runs and the hooks that ci/check_go.sh needs to plan the integration shards. It parses the files (go/parser) instead
// of matching text, so a comment or a string literal can neither hide nor invent a function. One record per line,
// fields split by U+001F:
//
//	T <path> <name>   a top-level test by the rule cmd/go applies (load.isTest + isTestFunc): no receiver, no type
//	                  parameters, named "Test" or "Test" followed by a rune that is not lower case, one parameter of
//	                  type *T or *x.T, no results; TestMain is a hook, not a test. A Test-named function with another
//	                  signature is no test (go test refuses to build such a package: "wrong signature").
//	X <path> <name>   another name go test -run selects, by cmd/go's load: a fuzz target ("Fuzz" or "Fuzz" followed by a
//	                  rune that is not lower case, one parameter of type *F or *x.F, no results, no type parameters) or
//	                  an example with an output comment (go/doc.Examples, as cmd/go; an example without one is compiled
//	                  but never run, so it is no X record)
//	H <path> <what>   the file changes the process its package's other tests run in: the first top-level `func init`
//	                  or `func TestMain`, else the first package-level side effect no reviewed row covers. Effects:
//	                  var-call:<callee> (a package-level `var` initializer calls anything outside the pure list in
//	                  pureCalls, or passes a func literal to any call; a called func literal is always one; a call on a
//	                  computed value is "?.name", a parenthesized callee that is not provably a type is "(...)"; funclit, "?.name", "(...)" and "(func-arg)" findings end in #<n>, their
//	                  occurrence in the file, so a row covers one of them), builtin-shadow:<name> (a file of the
//	                  directory declares a builtin's name at package level), deny-import:<path> (a named import of a path in
//	                  denyImports, whose init registers into a process-wide registry; a row
//	                  `<package dir><TAB>deny-import:<path><TAB><reason>` holds only while the package's plain test binary
//	                  (go list -test, no tags) loads that path), blank-import:<path>
//	                  and dot-import:<path>. A reviewed row is `<repo path><TAB><what><TAB><reason>` in the -reviewed table.
//	F <path> <what>   with -findings only: every distinct finding of the file, reviewed or not, and no T/H records
//
// Usage: gotestfuncs [-root DIR -reviewed FILE | -findings] FILE...   (the table's repo paths are joined to -root; file
// arguments may be relative, both sides are made absolute before they are compared)
//
// It is run with `go run` / `go build` on this file alone (build tag ignore keeps it out of ./...).
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// isTest is cmd/go's name rule: prefix alone, or prefix followed by a rune that is not lower case.
func isTest(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	if len(name) == len(prefix) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(name[len(prefix):])
	return !unicode.IsLower(r)
}

// isTestFunc is cmd/go's check: one parameter of type *<arg> or *x.<arg> (it cannot know the import name of testing),
// no results, no type parameters.
func isTestFunc(fn *ast.FuncDecl, arg string) bool {
	if fn.Type.TypeParams != nil && fn.Type.TypeParams.NumFields() > 0 {
		return false
	}
	if fn.Type.Results != nil && len(fn.Type.Results.List) > 0 || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 || len(fn.Type.Params.List[0].Names) > 1 {
		return false
	}
	ptr, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	if name, ok := ptr.X.(*ast.Ident); ok && name.Name == arg {
		return true
	}
	sel, ok := ptr.X.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == arg
}

// pureCalls are the calls a package-level initializer may make without a review row: import path to function names
// ("*" = any function of the package, "Parse*" = a name prefix). Adding to this list needs the acp's word; a single
// call goes in the reviewed table instead.
// pureCalls are the calls a package-level initializer may make without a review row. The rule is a class: a call is pure
// only when it cannot write through an argument, cannot register anywhere and cannot do I/O, that is, it reads its value
// arguments and returns new values. Everything not on this list is a finding. Why each entry cannot mutate:
//
//	time.Date, Unix, FixedZone, Parse*   value arguments, a new time.Time or *Location (Parse* returns a value and an error)
//	uuid.MustParse, New                  a string argument, a new UUID (New reads the random source, no package state)
//	regexp.MustCompile                   a string argument, a new *Regexp
//	errors.New, Join                     strings or error values, a new error
//	fmt.Sprintf, Errorf                  formats its arguments into a new string or error; a String or Error method of an
//	                                     argument runs, which is a call a reader of the argument expression judges
//	strings: Join Repeat ToLower ToUpper TrimSpace TrimPrefix TrimSuffix Replace ReplaceAll Split SplitN Fields Contains
//	         HasPrefix HasSuffix Index Count   string and []string values in, new values out, no callback parameter
//	strconv: Itoa Atoi FormatInt FormatUint FormatBool FormatFloat ParseInt ParseUint ParseBool ParseFloat Quote Unquote
//	         string and number values in, new values out; every Append* function writes into its first argument and is not listed
//	reflect.TypeOf                       reads the dynamic type of its argument
//	path/filepath.Join                   string values in, a new string
//
// The builtins and conversions below follow the same rule: len, cap, min, max read; make and new allocate; a conversion
// to a predeclared type yields a new value. append and copy write into their arguments' backing arrays and are not pure.
var pureCalls = map[string][]string{
	"time":                   {"Date", "Unix", "FixedZone", "Parse*"},
	"github.com/google/uuid": {"MustParse", "New"},
	"regexp":                 {"MustCompile"},
	"errors":                 {"New", "Join"},
	"fmt":                    {"Sprintf", "Errorf"},
	"strings": {"Join", "Repeat", "ToLower", "ToUpper", "TrimSpace", "TrimPrefix", "TrimSuffix", "Replace", "ReplaceAll",
		"Split", "SplitN", "Fields", "Contains", "HasPrefix", "HasSuffix", "Index", "Count"},
	"strconv": {"Itoa", "Atoi", "FormatInt", "FormatUint", "FormatBool", "FormatFloat", "ParseInt", "ParseUint", "ParseBool",
		"ParseFloat", "Quote", "Unquote"},
	"reflect":       {"TypeOf"},
	"path/filepath": {"Join"},
}

var pureBuiltins = map[string]bool{
	"len": true, "cap": true, "make": true, "new": true, "min": true, "max": true,
	"bool": true, "byte": true, "rune": true, "string": true, "error": true, "any": true,
	"int": true, "int8": true, "int16": true, "int32": true, "int64": true,
	"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true, "uintptr": true,
	"float32": true, "float64": true, "complex64": true, "complex128": true,
}

// literalArgsOnly are the pure-list entries that run a method of an argument (a String or Error method under %v): pure
// only when every argument is a constant or a basic literal.
var literalArgsOnly = map[string]map[string]bool{"fmt": {"Sprintf": true, "Errorf": true}}

func literalExpr(x ast.Expr, shadowed map[string]bool) bool {
	switch x := x.(type) {
	case *ast.BasicLit:
		return true
	case *ast.Ident:
		return (x.Name == "true" || x.Name == "false" || x.Name == "nil") && !shadowed[x.Name]
	case *ast.ParenExpr:
		return literalExpr(x.X, shadowed)
	case *ast.UnaryExpr:
		return literalExpr(x.X, shadowed)
	case *ast.BinaryExpr:
		return literalExpr(x.X, shadowed) && literalExpr(x.Y, shadowed)
	}
	return false
}

func pureCall(name string, imports map[string]string, sel string) bool {
	for _, allowed := range pureCalls[imports[name]] {
		if allowed == sel || strings.HasSuffix(allowed, "*") && strings.HasPrefix(sel, strings.TrimSuffix(allowed, "*")) {
			return true
		}
	}
	return false
}

// callFinding names a call in a package-level initializer that is not pure, or "" when it is. A call that is pure by
// name but receives a func literal is not: the literal's body runs inside the call or is kept for later.
func callFinding(call *ast.CallExpr, imports map[string]string, declTypes, shadowed map[string]bool) string {
	name, pure := calleeName(call, imports, declTypes, shadowed)
	if _, lit := call.Fun.(*ast.FuncLit); lit {
		return name
	}
	for _, arg := range call.Args {
		if _, ok := arg.(*ast.FuncLit); ok {
			return name + "(func-arg)"
		}
	}
	if pure {
		return ""
	}
	return name
}

// calleeName is the finding text of a call and whether the callee is on the pure list. A type conversion to a
// composite type is pure.
func calleeName(call *ast.CallExpr, imports map[string]string, declTypes, shadowed map[string]bool) (string, bool) {
	switch fun := call.Fun.(type) {
	case *ast.FuncLit:
		return "funclit", false
	case *ast.Ident:
		return fun.Name, pureBuiltins[fun.Name]
	case *ast.SelectorExpr:
		if x, ok := fun.X.(*ast.Ident); ok {
			pure := imports[x.Name] == "encoding/json" && fun.Sel.Name == "RawMessage" || pureCall(x.Name, imports, fun.Sel.Name)
			if pure && literalArgsOnly[imports[x.Name]][fun.Sel.Name] {
				for _, arg := range call.Args {
					pure = pure && literalExpr(arg, shadowed)
				}
			}
			return x.Name + "." + fun.Sel.Name, pure
		}
		return "?." + fun.Sel.Name, false
	case *ast.ArrayType, *ast.MapType, *ast.ChanType, *ast.FuncType, *ast.InterfaceType, *ast.StructType:
		return "type", true
	case *ast.ParenExpr:
		if star, ok := fun.X.(*ast.StarExpr); ok && provablyType(star.X, declTypes) {
			return "type", true
		}
		return "(...)", false
	}
	return "?", false
}

var predeclaredTypes = map[string]bool{
	"bool": true, "byte": true, "rune": true, "string": true, "error": true, "any": true,
	"int": true, "int8": true, "int16": true, "int32": true, "int64": true,
	"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true, "uintptr": true,
	"float32": true, "float64": true, "complex64": true, "complex128": true,
}

// provablyType: the expression names a type for certain, so `(*x)(v)` is a conversion and not a call of the function
// that the pointer x points to. A type literal, a predeclared type name and a type declared in the package are types; a
// plain identifier or a package-qualified name could be a variable (fail closed: a call).
func provablyType(x ast.Expr, types map[string]bool) bool {
	switch x := x.(type) {
	case *ast.ArrayType, *ast.MapType, *ast.ChanType, *ast.FuncType, *ast.InterfaceType, *ast.StructType:
		return true
	case *ast.Ident:
		return predeclaredTypes[x.Name] || types[x.Name]
	case *ast.StarExpr:
		return provablyType(x.X, types)
	case *ast.ParenExpr:
		return provablyType(x.X, types)
	}
	return false
}

func plainIdent(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !(r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)) {
			return false
		}
	}
	return true
}

// sideEffects lists, in source order and without repeats, what the file runs at package load besides func init: the
// non-pure calls of package-level var initializers (a func literal that is not called is not walked), and the blank
// and dot imports.
func sideEffects(file *ast.File, decls dirDecls) []string {
	shadowed := decls.shadowed
	imports := map[string]string{}
	var out []string
	seen := map[string]bool{}
	add := func(what string) {
		if !seen[what] {
			seen[what] = true
			out = append(out, what)
		}
	}
	// A finding that stands for a body or an expression the name does not identify (a func literal, a call on a
	// computed value, a call that takes a func literal, append and copy, whose effect is on their destination
	// argument) is keyed by its occurrence in the file, so one reviewed row covers one of them and never a second one.
	// A named callee is keyed by name: its body is what the row reviews.
	occurrences := map[string]int{}
	addCall := func(what string) {
		// A callee that is a func declaration of the package is keyed by name and the hash of its source: the row reviews
		// that body, and a body edit re-opens it. Any other plain identifier (a func-typed var, a parameter, a builtin) and
		// every call whose effect depends on its arguments or on an expression is keyed by its occurrence.
		if hash, ok := decls.funcs[what]; ok && plainIdent(what) {
			add("var-call:" + what + "@" + hash)
			return
		}
		if strings.HasPrefix(what, "funclit") || strings.HasPrefix(what, "?") || strings.HasPrefix(what, "(") || strings.Contains(what, ".") || plainIdent(what) || strings.HasSuffix(what, "(func-arg)") {
			occurrences[what]++
			what += "#" + strconv.Itoa(occurrences[what])
		}
		add("var-call:" + what)
	}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			path = spec.Path.Value
		}
		local := filepath.Base(path)
		if spec.Name != nil {
			switch spec.Name.Name {
			case "_":
				add("blank-import:" + path)
				continue
			case ".":
				add("dot-import:" + path)
				continue
			}
			local = spec.Name.Name
		}
		imports[local] = path
		if _, listed := denyImports[path]; listed {
			add("deny-import:" + path)
		}
	}
	// A package that declares a builtin's name makes that name in any initializer a call of its own function, which
	// the pure list cannot judge: fail closed for every file of such a package.
	var names []string
	for name := range shadowed {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		add("builtin-shadow:" + name)
	}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			for _, value := range spec.(*ast.ValueSpec).Values {
				ast.Inspect(value, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.FuncLit:
						return false
					case *ast.CallExpr:
						if what := callFinding(n, imports, decls.types, decls.shadowed); what != "" {
							addCall(what)
						}
						if _, lit := n.Fun.(*ast.FuncLit); lit {
							return false
						}
					}
					return true
				})
			}
		}
	}
	return out
}

// denyImports are the import paths whose init registers into a process-wide registry; a test file that imports one by
// name (a blank or dot import is a finding anyway) is a side effect. The list is frozen by the acp: a change needs its
// word. Each value is the cite of the registration, in the standard library of Go 1.27.0 or in the module version go.mod
// pins. github.com/lib/pq and github.com/mattn/go-sqlite3 are not in go.mod, so no pinned init can be cited: not listed.
var denyImports = map[string]string{
	"net/http/pprof": "net/http/pprof/pprof.go:95-104 init: http.HandleFunc on http.DefaultServeMux",
	"expvar":         "expvar/expvar.go:380-387 init: http.HandleFunc \"/debug/vars\" and Publish(\"cmdline\", \"memstats\")",
	"image/png":      "image/png/reader.go:1053 init: image.RegisterFormat",
	"image/jpeg":     "image/jpeg/reader.go:836 init: image.RegisterFormat",
	"image/gif":      "image/gif/reader.go:658 init: image.RegisterFormat",

	"github.com/jackc/pgx/v5/stdlib":                 "pgx v5.10.0 stdlib/sql.go:95-105 init: sql.Register(\"pgx\") and (\"pgx/v5\")",
	"github.com/ClickHouse/clickhouse-go/v2":         "clickhouse-go v2.49.0 clickhouse_std.go:96-97 init: sql.Register(\"clickhouse\")",
	"github.com/prometheus/client_golang/prometheus": "client_golang v1.23.2 prometheus/registry.go:60-63 init: MustRegister(process and Go collectors)",
}

// dirDeclarations: shadowed are the names of pureBuiltins that a file of the directory declares at package level (func, var,
// const or type): a call of such a name is that declaration, not the builtin. Cached per directory.
type dirDecls struct {
	shadowed, types map[string]bool
	// funcs are the package-level func declarations of package pkg (no receiver) by name, each as the first 8 hex digits
	// of the sha256 of its source text; a name that any file also declares as a var, const or type is not one.
	funcs map[string]string
}

var shadowCache = map[string]dirDecls{}

// shadowable: every identifier of the universe scope (go/types), that is the builtin functions, the predeclared types and
// the constants true, false, iota and nil. A package-level redeclaration of any of them changes what the name means in an
// initializer, whether the pure list names it or not.
func shadowable(name string) bool { return types.Universe.Lookup(name) != nil }

// dirDeclarations scans every .go file of dir, whatever its package clause or build tags. shadowed are the names of
// shadowable builtins that a file declares at package level (func, var, const or type). types are the type names that
// files of package pkg declare, minus every name that any file of the directory also declares as a var, const or func:
// a parenthesized `(*T)(x)` is a conversion only for a name that is a type for certain.
func dirDeclarations(dir, pkg string) dirDecls {
	key := dir + "\x00" + pkg
	if d, ok := shadowCache[key]; ok {
		return d
	}
	found := map[string]bool{}
	declared := map[string]bool{}
	notType := map[string]bool{}
	notFunc := map[string]bool{}
	funcSource := map[string][]string{}
	funcs := map[string]string{}
	shadowCache[key] = dirDecls{shadowed: found, types: declared, funcs: funcs}
	names, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return shadowCache[key]
	}
	for _, name := range names {
		src, err := os.ReadFile(name)
		if err != nil {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil {
					notType[d.Name.Name] = true
					if file.Name.Name == pkg {
						funcSource[d.Name.Name] = append(funcSource[d.Name.Name], string(src[fset.Position(d.Pos()).Offset:fset.Position(d.End()).Offset]))
					}
					if shadowable(d.Name.Name) {
						found[d.Name.Name] = true
					}
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch sp := spec.(type) {
					case *ast.ValueSpec:
						for _, id := range sp.Names {
							notType[id.Name] = true
							notFunc[id.Name] = true
							if shadowable(id.Name) {
								found[id.Name] = true
							}
						}
					case *ast.TypeSpec:
						notFunc[sp.Name.Name] = true
						if file.Name.Name == pkg {
							declared[sp.Name.Name] = true
						}
						if shadowable(sp.Name.Name) {
							found[sp.Name.Name] = true
						}
					}
				}
			}
		}
	}
	for name := range notType {
		delete(declared, name)
	}
	for name, sources := range funcSource {
		if notFunc[name] {
			continue
		}
		sort.Strings(sources)
		sum := sha256.Sum256([]byte(strings.Join(sources, "\n")))
		funcs[name] = hex.EncodeToString(sum[:4])
	}
	return shadowCache[key]
}

// loadReviewed reads the table: `<repo path><TAB><what><TAB><reason>` per line, `#` lines and empty lines skipped, an
// empty reason or a repeated row refused. Keys are "<absolute path>\t<what>".
func loadReviewed(root, table string) map[string]bool {
	rows := map[string]bool{}
	f, err := os.Open(table)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gotestfuncs: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := strings.Split(line, "\t")
		if len(cols) != 3 || cols[0] == "" || cols[1] == "" || strings.TrimSpace(cols[2]) == "" {
			fmt.Fprintf(os.Stderr, "gotestfuncs: %s:%d: want <path><TAB><what><TAB><reason>\n", table, n)
			os.Exit(1)
		}
		key := filepath.Join(root, cols[0]) + "\t" + cols[1]
		if rows[key] {
			fmt.Fprintf(os.Stderr, "gotestfuncs: %s:%d: repeated row\n", table, n)
			os.Exit(1)
		}
		rows[key] = true
	}
	return rows
}

// reviewedFinding reports whether a reviewed row covers a finding of the file. A deny-import row is keyed by the PACKAGE
// directory and holds only while the package's plain test binary (go list -test, no tags) still loads that import:
// then the tagged binary registers nothing the untagged tests did not already see. Once the plain binary drops the
// import, the row is stale and the finding returns.
func reviewedFinding(reviewed map[string]bool, repoRoot, absPath, what string) bool {
	if rest, ok := strings.CutPrefix(what, "deny-import:"); ok {
		dir := filepath.Dir(absPath)
		return reviewed[dir+"\t"+what] && plainBinaryLoads(repoRoot, dir, rest)
	}
	return reviewed[absPath+"\t"+what]
}

var plainLoadCache = map[string]map[string]bool{}

// plainBinaryLoads: the plain test binary of the package in dir loads the import path.
func plainBinaryLoads(repoRoot, dir, importPath string) bool {
	loaded, ok := plainLoadCache[dir]
	if !ok {
		loaded = map[string]bool{}
		plainLoadCache[dir] = loaded
		rel, err := filepath.Rel(repoRoot, dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gotestfuncs: %v\n", err)
			os.Exit(1)
		}
		cmd := exec.Command("go", "list", "-e", "-test", "-mod=readonly", "-f", "{{.ImportPath}}|{{join .Deps \",\"}}", "./"+filepath.ToSlash(rel))
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=")
		out, err := cmd.Output()
		if err != nil {
			fmt.Fprintf(os.Stderr, "gotestfuncs: go list -test in %s: %v\n", dir, err)
			os.Exit(1)
		}
		for _, line := range strings.Split(string(out), "\n") {
			if imp, deps, _ := strings.Cut(line, "|"); strings.HasSuffix(imp, ".test") {
				for _, dep := range strings.Split(deps, ",") {
					dep, _, _ = strings.Cut(dep, " [")
					loaded[dep] = true
				}
			}
		}
	}
	return loaded[importPath]
}

func main() {
	root := flag.String("root", "", "repository root the reviewed table's paths are relative to")
	reviewedFile := flag.String("reviewed", "", "reviewed side-effect table")
	findings := flag.Bool("findings", false, "print every side-effect finding (F records) and nothing else")
	denyList := flag.Bool("deny-list", false, "print the deny list of registering imports with the cite of each init, and nothing else")
	flag.Parse()
	if *denyList {
		var paths []string
		for path := range denyImports {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, path := range paths {
			fmt.Printf("%s\t%s\n", path, denyImports[path])
		}
		return
	}
	files := flag.Args()
	if len(files) == 0 || (*reviewedFile != "") != (*root != "") || *findings && *reviewedFile != "" {
		fmt.Fprintln(os.Stderr, "usage: gotestfuncs [-root DIR -reviewed FILE | -findings] FILE...")
		os.Exit(2)
	}
	reviewed := map[string]bool{}
	repoRoot := ""
	if *reviewedFile != "" {
		absRoot, err := filepath.Abs(*root)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gotestfuncs: %v\n", err)
			os.Exit(1)
		}
		repoRoot = absRoot
		reviewed = loadReviewed(absRoot, *reviewedFile)
	}
	out := &strings.Builder{}
	for _, path := range files {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gotestfuncs: %v\n", err)
			os.Exit(1)
		}
		absPath, err := filepath.Abs(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gotestfuncs: %v\n", err)
			os.Exit(1)
		}
		effects := sideEffects(file, dirDeclarations(filepath.Dir(absPath), file.Name.Name))
		if *findings {
			for _, what := range effects {
				fmt.Fprintf(out, "F\x1f%s\x1f%s\n", path, what)
			}
			continue
		}
		hooked := false
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			name := fn.Name.Name
			switch {
			case name == "init" || name == "TestMain":
				if !hooked {
					hooked = true
					fmt.Fprintf(out, "H\x1f%s\x1f%s\n", path, name)
				}
			case isTest(name, "Test") && isTestFunc(fn, "T"):
				fmt.Fprintf(out, "T\x1f%s\x1f%s\n", path, name)
			case isTest(name, "Fuzz") && isTestFunc(fn, "F"):
				fmt.Fprintf(out, "X\x1f%s\x1f%s\n", path, name)
			}
		}
		for _, e := range doc.Examples(file) {
			if e.Output != "" || e.EmptyOutput {
				fmt.Fprintf(out, "X\x1f%s\x1fExample%s\n", path, e.Name)
			}
		}
		if !hooked {
			for _, what := range effects {
				if !reviewedFinding(reviewed, repoRoot, absPath, what) {
					fmt.Fprintf(out, "H\x1f%s\x1f%s\n", path, what)
					break
				}
			}
		}
	}
	fmt.Print(out.String())
}
