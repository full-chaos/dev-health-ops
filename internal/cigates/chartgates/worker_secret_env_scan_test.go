package chartgates_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/dsnclass"
)

// A name is read by a Go source when a non-test file holds it as a string constant or literal in code (a comment is not
// code) and the same file calls an environment lookup. A call is a lookup by its identity, not by its name alone:
//   - a function of an imported package in envLookupFuncs, matched by import path, so any alias counts and a same-named
//     function of another package does not;
//   - inside the secrets package, the same functions called unqualified;
//   - a lookup passed in: a one-argument call of a parameter, variable or struct field whose declared type is
//     func(string) string or func(string) (string, bool), or a named type or alias of one (secrets.LookupEnv), unless
//     it is a non-env holder (nonEnvLookupHolders by package and identifier, or a redactor word of nonEnvLookupWords).
//     A same-named method or a call with other arguments is not a lookup.
//
// A name that appears only in a comment, or in a file that never looks anything up, is not read.
const (
	secretsImportPath = "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	secretsDir        = "internal/platform/secrets"
)

// nonEnvLookupWords are the name words of string-lookup-shaped holders that redact a value and read no environment.
var nonEnvLookupWords = map[string]string{
	"scrub":    "redacts a value",
	"redact":   "redacts a value",
	"mask":     "masks a value",
	"sanitize": "cleans a value",
}

type lookupHolder struct{ dir, name string }

// nonEnvLookupHolders are the string-lookup-shaped holders and types of the tree that read no environment, each by its
// package directory and identifier, with the reason. A holder of the same name in another package still counts. Every
// other func(string) string or func(string) (string, bool) holder counts as an environment lookup.
var nonEnvLookupHolders = map[lookupHolder]string{
	{"internal/auth/identity/oidc", "Cookie"}:                  "reads a request cookie",
	{"internal/auth/identity/saml", "cookie"}:                  "reads a request cookie",
	{"internal/websmoke", "Documents"}:                         "reads a registered GraphQL document",
	{"internal/cigates/docsbuild", "Revision"}:                 "reads a git revision",
	{"internal/testsupport/authrows", "Rewrite"}:               "rewrites a recorded row value",
	{"internal/queryapi/analytics", "rewrite"}:                 "rewrites an SQL expression",
	{"internal/jobs/metrics/aiworkflow", "find"}:               "matches a body",
	{"internal/syncdispatchruntime", "build"}:                  "builds an SQL predicate",
	{"internal/platform/logging", "cut"}:                       "cuts a value",
	{"internal/goapiproof", "digestFn"}:                        "digests a document",
	{"internal/cigates/affected", "importPath"}:                "maps an import path",
	{"internal/queryapi/investmentexplain", "label"}:           "labels a key",
	{"internal/streamrunner", "read"}:                          "reads a stored row field",
	{"internal/llmorgsettings", "value"}:                       "reads a stored setting value",
	{"internal/jobs/metrics/daily/icfinalize", "TeamNames"}:    "lists team names",
	{"internal/jobs/metrics/daily/icfinalize", "TeamResolver"}: "resolves a team name",
}

var envLookupFuncs = map[string]map[string]bool{
	"os":      {"Getenv": true, "LookupEnv": true},
	"syscall": {"Getenv": true},
	secretsImportPath: {
		"GetenvSecret": true, "GetenvNamed": true, "GetenvLogin": true, "GetenvDSN": true, "LookupSecretEnv": true,
		"ProcessLookup": true, "ProcessGetenv": true, "Resolve": true, "ResolveSecret": true, "ResolveDSNSetting": true,
		"ResolveFlag": true,
	},
}

type envScan struct {
	lookupTypes map[string]bool            // named types (and aliases) of a string lookup, by type name
	fields      map[string]bool            // struct fields of a string-lookup type, by field name, tree-wide
	pkgValues   map[string]map[string]bool // package-level variables of a string-lookup type, by directory then name
}

func scanEnvReaders(sources map[string]string) map[string][]string {
	readers := map[string][]string{}
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for path, src := range sources {
		if file, err := parser.ParseFile(fset, path, src, 0); err == nil {
			files[path] = file
		}
	}
	scan := newEnvScan(files)
	for path, file := range files {
		imports := importNames(file)
		dir := filepath.ToSlash(filepath.Dir(path))
		lookups := false
		names := map[string]bool{}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CallExpr:
				lookups = lookups || scan.isEnvLookup(typed, imports, dir)
			case *ast.BasicLit:
				if typed.Kind == token.STRING {
					if value, err := strconv.Unquote(typed.Value); err == nil {
						names[value] = true
					}
				}
			}
			return true
		})
		if !lookups {
			continue
		}
		for name := range names {
			readers[name] = append(readers[name], path)
		}
	}
	for name := range readers {
		sort.Strings(readers[name])
	}
	return readers
}

func newEnvScan(files map[string]*ast.File) *envScan {
	scan := &envScan{lookupTypes: map[string]bool{}, fields: map[string]bool{}, pkgValues: map[string]map[string]bool{}}
	for changed := true; changed; {
		changed = false
		for path, file := range files {
			dir := filepath.ToSlash(filepath.Dir(path))
			ast.Inspect(file, func(node ast.Node) bool {
				if spec, ok := node.(*ast.TypeSpec); ok && !scan.lookupTypes[spec.Name.Name] && !nonEnvLookupHolder(dir, spec.Name.Name) && scan.isLookupType(spec.Type) {
					scan.lookupTypes[spec.Name.Name], changed = true, true
				}
				return true
			})
		}
	}
	for path, file := range files {
		dir := filepath.ToSlash(filepath.Dir(path))
		imports := importNames(file)
		ast.Inspect(file, func(node ast.Node) bool {
			if st, ok := node.(*ast.StructType); ok {
				for _, field := range st.Fields.List {
					for _, name := range field.Names {
						if scan.isLookupHolder(dir, name.Name, field.Type) {
							scan.fields[name.Name] = true
						}
					}
				}
			}
			return true
		})
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				value := spec.(*ast.ValueSpec)
				for i, name := range value.Names {
					if scan.isLookupValueSpec(value, i, imports, dir) {
						if scan.pkgValues[dir] == nil {
							scan.pkgValues[dir] = map[string]bool{}
						}
						scan.pkgValues[dir][name.Name] = true
					}
				}
			}
		}
	}
	return scan
}

// isLookupHolder reports whether a parameter, variable or field holds an environment lookup: its type is a string lookup
// (or a named type or alias of one) and it is not a non-env holder. The scan errs toward counting a read.
func (s *envScan) isLookupHolder(dir, name string, typ ast.Expr) bool {
	return s.isLookupType(typ) && !nonEnvLookupHolder(dir, name)
}

// nonEnvLookupHolder reports whether a holder or type is in nonEnvLookupHolders, or its name has a word of
// nonEnvLookupWords and no word that names the environment (env, getenv, environment, lookup): redactEnv still counts.
func nonEnvLookupHolder(dir, name string) bool {
	if _, ok := nonEnvLookupHolders[lookupHolder{dir, name}]; ok {
		return true
	}
	exempt := false
	for _, word := range nameWords.FindAllString(name, -1) {
		word = strings.ToLower(word)
		switch word {
		case "env", "getenv", "environment", "lookup":
			return false
		}
		if _, ok := nonEnvLookupWords[word]; ok {
			exempt = true
		}
	}
	return exempt
}

// nameWords splits a Go name into its words: KeyScrub -> Key Scrub, lookupEnv -> lookup Env, team_names -> team names.
var nameWords = regexp.MustCompile(`[A-Z]+[a-z0-9]*|[a-z0-9]+`)

// isLookupType reports whether a type expression is a string lookup: func(string) string, func(string) (string, bool),
// or a named type or alias of one.
func (s *envScan) isLookupType(expr ast.Expr) bool {
	switch typed := expr.(type) {
	case *ast.ParenExpr:
		return s.isLookupType(typed.X)
	case *ast.Ident:
		return s.lookupTypes[typed.Name]
	case *ast.SelectorExpr:
		return s.lookupTypes[typed.Sel.Name]
	case *ast.FuncType:
		params, results := fieldTypes(typed.Params), fieldTypes(typed.Results)
		if len(params) != 1 || !isIdent(params[0], "string") {
			return false
		}
		switch len(results) {
		case 1:
			return isIdent(results[0], "string")
		case 2:
			return isIdent(results[0], "string") && isIdent(results[1], "bool")
		}
	}
	return false
}

func (s *envScan) isLookupValueSpec(value *ast.ValueSpec, i int, imports map[string]string, dir string) bool {
	if value.Type != nil {
		return s.isLookupHolder(dir, value.Names[i].Name, value.Type)
	}
	return i < len(value.Values) && isEnvFuncRef(value.Values[i], imports)
}

// isEnvFuncRef reports whether an expression names a package environment lookup without calling it (lookup := os.LookupEnv).
func isEnvFuncRef(expr ast.Expr, imports map[string]string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	x, ok := sel.X.(*ast.Ident)
	return ok && x.Obj == nil && envLookupFuncs[imports[x.Name]][sel.Sel.Name]
}

// isLookupFieldRef reports whether an expression takes a lookup field of a value without calling it (lookup := env.Lookup).
func (s *envScan) isLookupFieldRef(expr ast.Expr, imports map[string]string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if x, ok := sel.X.(*ast.Ident); ok && x.Obj == nil {
		if _, imported := imports[x.Name]; imported {
			return false
		}
	}
	return s.fields[sel.Sel.Name]
}

func (s *envScan) isEnvLookup(call *ast.CallExpr, imports map[string]string, dir string) bool {
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		if x, ok := fun.X.(*ast.Ident); ok && x.Obj == nil {
			if path, imported := imports[x.Name]; imported {
				return envLookupFuncs[path][fun.Sel.Name]
			}
		}
		return len(call.Args) == 1 && s.fields[fun.Sel.Name]
	case *ast.Ident:
		if fun.Obj == nil {
			if dir == secretsDir && envLookupFuncs[secretsImportPath][fun.Name] {
				return true
			}
			return len(call.Args) == 1 && s.pkgValues[dir][fun.Name]
		}
		return len(call.Args) == 1 && s.isLookupObject(fun.Obj, imports, dir)
	}
	return false
}

// isLookupObject reports whether a local declaration (a parameter, a variable, an assignment) holds a string lookup.
func (s *envScan) isLookupObject(obj *ast.Object, imports map[string]string, dir string) bool {
	if obj.Kind != ast.Var {
		return false
	}
	switch decl := obj.Decl.(type) {
	case *ast.Field:
		return s.isLookupHolder(dir, obj.Name, decl.Type)
	case *ast.ValueSpec:
		for i, name := range decl.Names {
			if name.Name == obj.Name {
				return s.isLookupValueSpec(decl, i, imports, dir)
			}
		}
	case *ast.AssignStmt:
		for i, lhs := range decl.Lhs {
			if id, ok := lhs.(*ast.Ident); ok && id.Name == obj.Name && len(decl.Rhs) == len(decl.Lhs) {
				return isEnvFuncRef(decl.Rhs[i], imports) || s.isLookupFieldRef(decl.Rhs[i], imports)
			}
		}
	}
	return false
}

func importNames(file *ast.File) map[string]string {
	out := map[string]string{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := pathpkg.Base(path)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		out[name] = path
	}
	return out
}

func fieldTypes(list *ast.FieldList) []ast.Expr {
	var out []ast.Expr
	if list == nil {
		return out
	}
	for _, field := range list.List {
		for n := max(len(field.Names), 1); n > 0; n-- {
			out = append(out, field.Type)
		}
	}
	return out
}

func isIdent(expr ast.Expr, name string) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == name
}

// realEnvReaders scans the non-test Go sources of the tree under g.root.
func realEnvReaders(g *guard) map[string][]string {
	sources := map[string]string{}
	for _, dir := range []string{"internal", "cmd"} {
		_ = filepath.WalkDir(g.path(dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if entry.IsDir() && entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			if data, rerr := os.ReadFile(path); rerr == nil {
				rel, _ := filepath.Rel(g.root, path)
				sources[filepath.ToSlash(rel)] = string(data)
			}
			return nil
		})
	}
	g.expect(len(sources) > 0, "no Go source was found under %s, so the reader scan measured nothing", g.root)
	return scanEnvReaders(sources)
}

func TestWorkerSecretKeyReaderScan(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want bool
	}{
		{"E01-name-only-in-a-comment", "package x\nimport \"os\"\n// reads \"SOME_KEY\"\nfunc f() string { return os.Getenv(\"OTHER\") }\n", false},
		{"E02-name-in-a-file-with-no-lookup", "package x\nvar table = map[string]string{\"SOME_KEY\": \"v\"}\n", false},
		{"E03-os-lookupenv-literal", "package x\nimport \"os\"\nfunc f() { _, _ = os.LookupEnv(\"SOME_KEY\") }\n", true},
		{"E04-constant-name-and-getenv", "package x\nimport \"os\"\nconst envKey = \"SOME_KEY\"\nfunc f() string { return os.Getenv(envKey) }\n", true},
		{"E05-secrets-getenv-family", "package x\nimport envsecrets \"" + secretsImportPath + "\"\nfunc f() string { return envsecrets.GetenvSecret(\"SOME_KEY\") }\n", true},
		{"E06-lookup-function-passed-in", "package x\nfunc f(lookup func(string) (string, bool)) { lookup(\"SOME_KEY\") }\n", true},
		// CHAOS-12688: a call is a lookup by its identity, not by its name.
		{"E07-same-named-method-two-args", "package x\ntype S struct{}\nfunc (S) lookup(ctx, key string) string { return key }\nfunc f(s S) string { return s.lookup(\"ctx\", \"SOME_KEY\") }\n", false},
		{"E08-same-named-method-one-arg", "package x\ntype S struct{}\nfunc (S) lookup(key string) string { return key }\nfunc f(s S) string { return s.lookup(\"SOME_KEY\") }\n", false},
		{"E09-getenv-of-another-package", "package x\nimport \"example.com/fake\"\nfunc f() string { return fake.Getenv(\"SOME_KEY\") }\n", false},
		{"E10-aliased-secrets-import", "package x\nimport (\n\"os\"\nplatformsecrets \"" + secretsImportPath + "\"\n)\nfunc f() { _, _, _ = platformsecrets.ResolveSecret(\"SOME_KEY\", os.LookupEnv) }\n", true},
		{"E11-injected-struct-field", "package x\ntype H struct{ Getenv func(string) string }\nfunc (h H) f() string { return h.Getenv(\"SOME_KEY\") }\n", true},
		{"E12-syscall-getenv", "package x\nimport \"syscall\"\nfunc f() { _, _ = syscall.Getenv(\"SOME_KEY\") }\n", true},
		{"E13-named-func-type-param", "package x\ntype lookupFn func(string) (string, bool)\nfunc f(get lookupFn) { get(\"SOME_KEY\") }\n", true},
		{"E14-func-type-alias-param", "package x\ntype getenvFn = func(string) string\nfunc f(get getenvFn) string { return get(\"SOME_KEY\") }\n", true},
		{"E15-name-default-helper-reads-os", "package x\nimport \"os\"\nfunc get(name, def string) string {\nif v, ok := os.LookupEnv(name); ok {\nreturn v\n}\nreturn def\n}\nfunc f() string { return get(\"SOME_KEY\", \"x\") }\n", true},
		{"E16-local-variable-of-os-lookupenv", "package x\nimport \"os\"\nfunc f() { lookup := os.LookupEnv; _, _ = lookup(\"SOME_KEY\") }\n", true},
		{"E17-local-copy-of-a-lookup-field", "package x\ntype Env struct{ Lookup func(string) (string, bool) }\nfunc f(env Env) { lookup := env.Lookup; _, _ = lookup(\"SOME_KEY\") }\n", true},
		{"E18-redactor-of-the-lookup-shape", "package x\ntype R struct{ Scrub func(string) string }\nfunc f(r R) string { return r.Scrub(\"SOME_KEY\") }\n", false},
		{"E19-two-arg-method-named-like-a-lookup-field", "package x\ntype H struct{ Getenv func(string) string }\ntype M struct{}\nfunc (M) Getenv(ctx, key string) string { return key }\nfunc f(m M) string { return m.Getenv(\"ctx\", \"SOME_KEY\") }\n", false},
		{"E20-parameter-shadows-an-import", "package x\nimport \"context\"\nvar _ context.Context\ntype H struct{ Getenv func(string) string }\nfunc g(context H) string { return context.Getenv(\"SOME_KEY\") }\n", true},
		{"E21-neutral-names-hold-a-lookup", "package x\nfunc f(fn func(string) string, get func(string) (string, bool), g func(string) string) {\n_ = fn(\"SOME_KEY\")\n}\n", true},
		{"E22-env-word-beats-an-exempt-word", "package x\ntype R struct{ redactEnv func(string) string }\nfunc f(r R) string { return r.redactEnv(\"SOME_KEY\") }\n", true},
		{"E23-exempt-name-in-another-package", "package x\nfunc f(read func(string) string, value func(string) (string, bool)) string {\n_, _ = value(\"OTHER\")\nreturn read(\"SOME_KEY\")\n}\n", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := len(scanEnvReaders(map[string]string{"internal/x/x.go": c.src})["SOME_KEY"]) > 0
			if got != c.want {
				t.Errorf("%s: SOME_KEY read = %v, want %v", c.name, got, c.want)
			}
		})
	}
}

// The pushcli form: a helper takes cli.Env and calls its Lookup field, whose type secrets.LookupEnv (a named string
// lookup) is declared in two other packages. The helper's callers in the same file read their names.
func TestWorkerSecretKeyReaderScanInjectedFieldOfAnotherPackage(t *testing.T) {
	sources := map[string]string{
		secretsDir + "/source.go": "package secrets\ntype LookupEnv func(string) (string, bool)\n",
		"internal/cli/cli.go":     "package cli\nimport \"" + secretsImportPath + "\"\ntype Env struct{ Lookup secrets.LookupEnv }\n",
		"internal/pushcli/batch.go": "package pushcli\nimport \"github.com/full-chaos/dev-health-ops/internal/cli\"\n" +
			"func lookup(env cli.Env, key string) string {\nif env.Lookup == nil {\nreturn \"\"\n}\nvalue, _ := env.Lookup(key)\nreturn value\n}\n" +
			"func f(env cli.Env) string { return lookup(env, \"SOME_KEY\") }\n",
	}
	if got := scanEnvReaders(sources)["SOME_KEY"]; len(got) != 1 || got[0] != "internal/pushcli/batch.go" {
		t.Errorf("SOME_KEY readers = %v, want [internal/pushcli/batch.go]", got)
	}
}

// Two limits of the DSN checks are accepted and pinned here, so a change to either is seen and decided:
//   - A DSN held under a name the chart does not know, in a ConfigMap or an operator-created Secret, is not seen by the
//     chart. The process start guard still refuses a worker process whose environment carries a PostgreSQL URL under any name.
//   - A libpq key=value connection string (not a URL) under an unknown name is not recognised by the start guard. Closing
//     this needs a change to dsnclass (production code), which is out of scope of this test-only leaf.
func dsnAliasLimits(g *guard, check func(class string, environ []string) error) {
	g.expect(check(dsnclass.River, []string{"UNKNOWN_ALIAS=postgresql://u:p@h:5432/db"}) != nil,
		"the start guard no longer refuses a DSN URL under an unknown name: the accepted limit text is stale")
	g.expect(check(dsnclass.River, []string{"UNKNOWN_ALIAS=host=h port=5432 dbname=d user=u"}) == nil,
		"the start guard now refuses a libpq key=value string under an unknown name: close the accepted limit and update its text")
}
