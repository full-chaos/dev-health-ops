package chartgates_test

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/tools/go/packages"

	"github.com/full-chaos/dev-health-ops/internal/platform/dsnclass"
)

// A name is read by a Go source when a non-test file holds it as a string constant or literal in code (a comment is not
// code) and the same file calls an environment lookup. A key read only by a test is not a product read. The scan
// type-checks the packages (go/types, with their test files and the CI build tags) and decides by the callee object:
//
//   - a function in envLookupFuncs, matched by package path: os.Getenv, os.LookupEnv, syscall.Getenv and the env readers
//     of internal/platform/secrets, under any import alias, a dot import, or unqualified inside the secrets package;
//
//   - a variable, parameter or struct field (embedded too) whose type is, or is a named type or alias of,
//     func(string) string or func(string) (string, bool), unless it is a non-env holder (nonEnvLookupHolders by package
//     and identifier, or a redactor word of nonEnvLookupWords). Copies count by their own type, however they are made.
//
//   - a function or method that passes one of its string parameters straight to such a lookup (envWrappers).
//
// Any other method never counts, whatever its name or signature.
// A name that appears only in a comment, or in a file that never looks anything up, is not read.
const (
	modulePath        = "github.com/full-chaos/dev-health-ops"
	secretsImportPath = modulePath + "/internal/platform/secrets"
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
	{"internal/api/syncadmin", "component"}:                    "reads a PagerDuty config object key",
	{"internal/apiservice/admin", "text"}:                      "reads a response detail key",
	{"internal/auth/contracts/tokencorpus", "q"}:               "quotes a string",
	{"internal/cigates/affected", "table"}:                     "maps a file to its table path",
	{"internal/cigates/docstools", "cell"}:                     "reads a table row cell",
	{"internal/cigates/docstools", "get"}:                      "reads a CSV row field",
	{"internal/cigates/docstools", "invTxThemeOf"}:             "maps a subcategory to its theme",
	{"internal/fixturescli", "flag"}:                           "formats a target flag",
	{"internal/gqlgenguard", "rel"}:                            "maps a path to the module root",
	{"internal/platform/credshape", "segment"}:                 "encodes a probe segment",
	{"internal/providersync", "canonical"}:                     "canonicalises a work item id",
	{"internal/providersync", "label"}:                         "labels a value",
	{"internal/providersync", "writerOf"}:                      "names a relation writer",
	{"internal/queryapi/digest", "Document"}:                   "digests a document",
	{"internal/queryapi/hotspots", "repoLabel"}:                "labels a repository id",
	{"internal/scheduler/sync", "derive"}:                      "derives a materialization id",
	{"internal/scheduler/sync", "field"}:                       "reads a project JSON field",
	{"internal/storage/postgres/authschema", "qualified"}:      "qualifies a table name",
	{"internal/storage/roleacl", "on"}:                         "renders an SQL class condition",
	{"internal/storage/roleacl", "own"}:                        "renders an SQL ownership case",
	{"internal/syncdispatchruntime", "get"}:                    "reads a budget bucket key",
	{"internal/syncdispatchruntime", "text"}:                   "reads an observation key",
	{"internal/testsupport/chlogin", "with"}:                   "builds a DSN with a password",
	{"internal/testsupport/idpstub", "esc"}:                    "escapes a string",
	{"internal/testsupport/sessionscenario", "aliceAccess"}:    "reads a scenario token",
	{"internal/testsupport/sessionscenario", "aliceRefresh"}:   "reads a scenario token",
	{"internal/testsupport/sessionscenario", "latest"}:         "reads a scenario token",
	{"internal/testsupport/sessionscenario", "successor"}:      "reads a scenario token",
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

// envUnit is one type-checked package: its files by path relative to the module root, test files included.
type envUnit struct {
	files map[string]*ast.File
	info  *types.Info
}

func scanEnvReaders(sources map[string]string) map[string][]string {
	return scanEnvUnits(checkSources(sources))
}

func scanEnvUnits(units []envUnit) map[string][]string {
	wrappers := envWrappers(units)
	found := map[string]map[string]bool{}
	for _, unit := range units {
		for path, file := range unit.files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			lookups := false
			names := map[string]bool{}
			ast.Inspect(file, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CallExpr:
					lookups = lookups || isEnvLookupCall(unit.info, typed, wrappers)
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
				if found[name] == nil {
					found[name] = map[string]bool{}
				}
				found[name][path] = true
			}
		}
	}
	readers := map[string][]string{}
	for name, paths := range found {
		for path := range paths {
			readers[name] = append(readers[name], path)
		}
		sort.Strings(readers[name])
	}
	return readers
}

// isEnvLookupCall reports whether a call's callee is an environment lookup by its object. wrappers holds the functions
// and methods (by types.Func.FullName) that pass one of their string parameters straight to a lookup.
func isEnvLookupCall(info *types.Info, call *ast.CallExpr, wrappers map[string]bool) bool {
	var obj types.Object
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		obj = info.Uses[fun]
	case *ast.SelectorExpr:
		if selection, ok := info.Selections[fun]; ok {
			if selection.Kind() != types.FieldVal {
				method, isFunc := selection.Obj().(*types.Func)
				return isFunc && wrappers[method.FullName()]
			}
			obj = selection.Obj()
		} else {
			obj = info.Uses[fun.Sel]
		}
	}
	switch typed := obj.(type) {
	case *types.Func:
		if typed.Pkg() == nil {
			return false
		}
		if wrappers[typed.FullName()] {
			return true
		}
		if typed.Signature().Recv() != nil {
			return false
		}
		return envLookupFuncs[typed.Pkg().Path()][typed.Name()]
	case *types.Var:
		return isStringLookup(typed.Type()) && !nonEnvLookupHolder(typed)
	}
	return false
}

// envWrappers finds the functions and methods whose body passes one of their string parameters, as the argument itself,
// to an environment lookup (or to another wrapper): loader.getenv(name) { return loader.Getenv(name) }. A call of a
// wrapper is a lookup wherever it is, so a file that reads its names through a wrapper method or a helper of another
// package is a reader. Keys are types.Func.FullName, the same for a package loaded from source and from export data.
func envWrappers(units []envUnit) map[string]bool {
	wrappers := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for _, unit := range units {
			for _, file := range unit.files {
				for _, decl := range file.Decls {
					fn, ok := decl.(*ast.FuncDecl)
					if !ok || fn.Body == nil {
						continue
					}
					obj, ok := unit.info.Defs[fn.Name].(*types.Func)
					if !ok || wrappers[obj.FullName()] {
						continue
					}
					params := map[types.Object]bool{}
					for i := 0; i < obj.Signature().Params().Len(); i++ {
						if param := obj.Signature().Params().At(i); types.Identical(param.Type().Underlying(), types.Typ[types.String]) {
							params[param] = true
						}
					}
					if len(params) == 0 {
						continue
					}
					ast.Inspect(fn.Body, func(node ast.Node) bool {
						call, ok := node.(*ast.CallExpr)
						if !ok || wrappers[obj.FullName()] || !isEnvLookupCall(unit.info, call, wrappers) {
							return true
						}
						for _, arg := range call.Args {
							if id, ok := ast.Unparen(arg).(*ast.Ident); ok && params[unit.info.Uses[id]] {
								wrappers[obj.FullName()], changed = true, true
							}
						}
						return true
					})
				}
			}
		}
	}
	return wrappers
}

// isStringLookup reports whether a type is, or is a named type or alias of, func(string) string or
// func(string) (string, bool).
func isStringLookup(t types.Type) bool {
	sig, ok := t.Underlying().(*types.Signature)
	if !ok || sig.Variadic() || sig.Params().Len() != 1 || !types.Identical(sig.Params().At(0).Type(), types.Typ[types.String]) {
		return false
	}
	results := sig.Results()
	switch results.Len() {
	case 1:
		return types.Identical(results.At(0).Type(), types.Typ[types.String])
	case 2:
		return types.Identical(results.At(0).Type(), types.Typ[types.String]) && types.Identical(results.At(1).Type(), types.Typ[types.Bool])
	}
	return false
}

// nonEnvLookupHolder reports whether a lookup-shaped holder reads no environment: the holder, or the named type it has,
// is in nonEnvLookupHolders by package directory and identifier, or its name has a word of nonEnvLookupWords and no
// word that names the environment (env, getenv, environment, lookup): redactEnv still counts.
func nonEnvLookupHolder(holder *types.Var) bool {
	if _, ok := nonEnvLookupHolders[lookupHolder{packageDir(holder.Pkg()), holder.Name()}]; ok {
		return true
	}
	if named, ok := types.Unalias(holder.Type()).(*types.Named); ok {
		if _, ok := nonEnvLookupHolders[lookupHolder{packageDir(named.Obj().Pkg()), named.Obj().Name()}]; ok {
			return true
		}
	}
	exempt := false
	for _, word := range nameWords.FindAllString(holder.Name(), -1) {
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

// packageDir is a package's directory relative to the module root ("internal/x"), or its path outside the module.
func packageDir(pkg *types.Package) string {
	if pkg == nil {
		return ""
	}
	return strings.TrimSuffix(strings.TrimPrefix(pkg.Path(), modulePath+"/"), "_test")
}

// checkSources type-checks in-memory sources by package directory and package clause. An import of a module package the
// sources hold is checked from them; any other import comes from the installed export data, and one that resolves
// nowhere stays unresolved (its uses are no lookup).
func checkSources(sources map[string]string) []envUnit {
	fset := token.NewFileSet()
	type pkgKey struct{ dir, name string }
	byPkg := map[pkgKey]map[string]*ast.File{}
	for path, src := range sources {
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			continue
		}
		key := pkgKey{filepath.ToSlash(filepath.Dir(path)), file.Name.Name}
		if byPkg[key] == nil {
			byPkg[key] = map[string]*ast.File{}
		}
		byPkg[key][path] = file
	}
	im := &sourceImporter{fset: fset, std: importer.Default(), done: map[string]*types.Package{}, pkgs: map[string]map[string]*ast.File{}}
	for key, files := range byPkg {
		if !strings.HasSuffix(key.name, "_test") {
			im.pkgs[key.dir] = files
		}
	}
	var units []envUnit
	for key, files := range byPkg {
		path := modulePath + "/" + key.dir
		if strings.HasSuffix(key.name, "_test") {
			path += "_test"
		}
		units = append(units, envUnit{files: files, info: im.check(path, files)})
	}
	return units
}

type sourceImporter struct {
	fset *token.FileSet
	std  types.Importer
	done map[string]*types.Package
	pkgs map[string]map[string]*ast.File
}

func (im *sourceImporter) Import(path string) (*types.Package, error) {
	if pkg, ok := im.done[path]; ok {
		return pkg, nil
	}
	if dir, ok := strings.CutPrefix(path, modulePath+"/"); ok {
		if files, held := im.pkgs[dir]; held {
			im.check(path, files)
			return im.done[path], nil
		}
	}
	return im.std.Import(path)
}

func (im *sourceImporter) check(path string, files map[string]*ast.File) *types.Info {
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	list := make([]*ast.File, 0, len(files))
	for _, file := range files {
		list = append(list, file)
	}
	config := types.Config{Importer: im, Error: func(error) {}}
	pkg, _ := config.Check(path, im.fset, list, info)
	im.done[path] = pkg
	return info
}

// realEnvTree is the type-checked tree, loaded once per test binary: the packages under internal and cmd, with their
// test files, once without build tags and once with -tags=integration (the two CI legs).
var realEnvTree struct {
	once    sync.Once
	units   []envUnit
	errs    []string
	elapsed time.Duration
}

func loadRealEnvTree(root string) {
	start := time.Now()
	for _, flags := range [][]string{nil, {"-tags=integration"}} {
		config := &packages.Config{
			Mode:       packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
			Dir:        root,
			Tests:      true,
			BuildFlags: flags,
		}
		pkgs, err := packages.Load(config, "./internal/...", "./cmd/...")
		if err != nil {
			realEnvTree.errs = append(realEnvTree.errs, err.Error())
			continue
		}
		for _, pkg := range pkgs {
			for _, perr := range pkg.Errors {
				realEnvTree.errs = append(realEnvTree.errs, pkg.ID+": "+perr.Error())
			}
			files := map[string]*ast.File{}
			for _, file := range pkg.Syntax {
				rel, rerr := filepath.Rel(root, pkg.Fset.Position(file.Pos()).Filename)
				if rerr != nil || strings.HasPrefix(rel, "..") {
					continue
				}
				files[filepath.ToSlash(rel)] = file
			}
			realEnvTree.units = append(realEnvTree.units, envUnit{files: files, info: pkg.TypesInfo})
		}
	}
	realEnvTree.elapsed = time.Since(start)
}

// realEnvReaders scans the non-test Go sources of the tree under g.root.
func realEnvReaders(g *guard) map[string][]string {
	realEnvTree.once.Do(func() { loadRealEnvTree(g.root) })
	g.expect(len(realEnvTree.errs) == 0, "the reader scan could not type-check the tree: %s", strings.Join(firstN(realEnvTree.errs, 5), "; "))
	g.expect(len(realEnvTree.units) > 0, "no Go package was found under %s, so the reader scan measured nothing", g.root)
	return scanEnvUnits(realEnvTree.units)
}

func firstN(in []string, n int) []string {
	if len(in) > n {
		return in[:n]
	}
	return in
}

// secretsStub stands in for the secrets package in the single-file cells, so they type-check without the real tree.
const secretsStub = "package secrets\nimport \"os\"\ntype LookupEnv func(string) (string, bool)\ntype Value struct{}\n" +
	"func GetenvSecret(name string) string { return os.Getenv(name) }\n" +
	"func ResolveSecret(key string, lookup LookupEnv) (Value, bool, error) {\n_, ok := lookup(key)\nreturn Value{}, ok, nil\n}\n"

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
			got := len(scanEnvReaders(map[string]string{"internal/x/x.go": c.src, secretsDir + "/stub.go": secretsStub})["SOME_KEY"]) > 0
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

// Multi-file cells, each a small module tree: the forms the r1 review of CHAOS-12688 executed against the name-keyed
// scan, and the ones the typed scan adds. want is whether wantFile is a reader of SOME_KEY.
func TestWorkerSecretKeyReaderScanPackages(t *testing.T) {
	mod := strings.TrimSuffix(secretsImportPath, "/"+secretsDir)
	envStruct := "type Env struct{ Lookup func(string) (string, bool) }\n"
	cases := []struct {
		name     string
		files    map[string]string
		wantFile string
		want     bool
	}{
		{"V01-var-copy-of-a-lookup-field", map[string]string{
			"internal/x/x.go": "package x\n" + envStruct + "func f(env Env) {\nvar lookup = env.Lookup\n_, _ = lookup(\"SOME_KEY\")\n}\n",
		}, "internal/x/x.go", true},
		{"V02-package-var-chain-from-os-lookupenv", map[string]string{
			"internal/x/x.go": "package x\nimport \"os\"\nvar lookupEnv = os.LookupEnv\nvar second = lookupEnv\nfunc f() { _, _ = second(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", true},
		{"V03-embedded-named-lookup-field", map[string]string{
			"internal/x/x.go": "package x\ntype Lookup func(string) (string, bool)\ntype Env struct{ Lookup }\nfunc f(e Env) { _, _ = e.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", true},
		{"V04-dot-imported-os-getenv", map[string]string{
			"internal/x/x.go": "package x\nimport . \"os\"\nfunc f() string { return Getenv(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", true},
		{"V05-chained-copy-of-a-lookup-field", map[string]string{
			"internal/x/x.go": "package x\n" + envStruct + "func f(env Env) {\na := env.Lookup\nb := a\n_, _ = b(\"SOME_KEY\")\n}\n",
		}, "internal/x/x.go", true},
		{"V06-unqualified-secrets-reader", map[string]string{
			secretsDir + "/use.go": "package secrets\nimport \"os\"\nvar _ = os.Args\nfunc GetenvSecret(name string) string { return getenv(name) }\n" +
				"func getenv(name string) string { return name }\nfunc f() string { return GetenvSecret(\"SOME_KEY\") }\n",
		}, secretsDir + "/use.go", true},
		{"V07-same-named-imported-types-with-different-signatures", map[string]string{
			"internal/a/a.go": "package a\ntype Getter func(string) string\n",
			"internal/b/b.go": "package b\ntype Getter func(string) int\n",
			"internal/x/x.go": "package x\nimport (\n\"" + mod + "/internal/a\"\n\"" + mod + "/internal/b\"\n)\nvar _ a.Getter\nfunc f(g b.Getter) int { return g(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", false},
		{"V08-method-named-like-a-lookup-field-of-another-package", map[string]string{
			"internal/localgit/sync.go":               "package localgit\ntype Options struct{ Lookup func(string) (string, bool) }\n",
			"internal/syncdispatchcontract/routes.go": "package syncdispatchcontract\ntype Registry struct{}\nfunc (Registry) Lookup(kind string) (string, bool) { return kind, true }\n",
			"internal/syncreconciler/observer.go": "package syncreconciler\nimport \"" + mod + "/internal/syncdispatchcontract\"\n" +
				"func f(r syncdispatchcontract.Registry) {\n_, _ = r.Lookup(\"SOME_KEY\")\n_, _ = syncdispatchcontract.Registry.Lookup(r, \"SOME_KEY\")\n}\n",
		}, "internal/syncreconciler/observer.go", false},
		{"V09-a-key-read-only-in-a-test-file", map[string]string{
			"internal/x/x.go":      "package x\nconst key = \"SOME_KEY\"\n",
			"internal/x/x_test.go": "package x\nimport \"os\"\nfunc use() string { return os.Getenv(key) + os.Getenv(\"SOME_KEY\") }\n",
		}, "", false},
		{"V10-holder-of-an-exempt-named-type-in-another-package", map[string]string{
			"internal/jobs/metrics/daily/icfinalize/executor.go": "package icfinalize\ntype TeamResolver func(string) string\n",
			"internal/y/y.go": "package y\nimport \"" + mod + "/internal/jobs/metrics/daily/icfinalize\"\nfunc f(r icfinalize.TeamResolver) string { return r(\"SOME_KEY\") }\n",
		}, "internal/y/y.go", false},
		{"V12-env-wrapper-method-in-another-file", map[string]string{
			"internal/syncbudget/loader.go":    "package syncbudget\ntype Loader struct{ Getenv func(string) string }\nfunc (l Loader) getenv(name string) string { return l.Getenv(name) }\n",
			"internal/syncbudget/hydration.go": "package syncbudget\nfunc f(l Loader) string { return l.getenv(\"SOME_KEY\") }\n",
		}, "internal/syncbudget/hydration.go", true},
		{"V13-env-wrapper-function-of-another-package", map[string]string{
			"internal/envutil/envutil.go": "package envutil\nimport \"os\"\nfunc Read(name string) string { return os.Getenv(name) }\nfunc Hash(name string) string { return name }\n",
			"internal/x/x.go":             "package x\nimport \"" + mod + "/internal/envutil\"\nfunc f() string { return envutil.Read(\"SOME_KEY\") }\n",
			"internal/y/y.go":             "package y\nimport \"" + mod + "/internal/envutil\"\nfunc g() string { return envutil.Hash(\"OTHER_KEY\") }\n",
		}, "internal/x/x.go", true},
		{"V11-getenv-of-a-resolved-non-env-package", map[string]string{
			"internal/fakeenv/fakeenv.go": "package fakeenv\nfunc Getenv(name string) string { return name }\n",
			"internal/x/x.go":             "package x\nimport \"" + mod + "/internal/fakeenv\"\nfunc f() string { return fakeenv.Getenv(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			readers := scanEnvReaders(c.files)["SOME_KEY"]
			got := len(readers) > 0
			if c.wantFile != "" {
				got = slices.Contains(readers, c.wantFile)
			}
			if got != c.want {
				t.Errorf("%s: SOME_KEY read by %q = %v (readers %v), want %v", c.name, c.wantFile, got, readers, c.want)
			}
		})
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
