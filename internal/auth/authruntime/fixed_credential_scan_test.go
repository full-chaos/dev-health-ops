package authruntime

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// A test credential is built at run time. No fixed credential-shaped literal (32 or more base64url characters mixing
// upper case, lower case and digits) and no fixed role password (a password name bound to a string literal, or an
// inline SQL role password literal) stands in the package's sources, where a secret scanner would read it as a key.
func TestNoFixedCredentialLiteralInThePackage(t *testing.T) {
	shaped := regexp.MustCompile("[\"`][A-Za-z0-9_+=-]{32,}[\"`]")
	mixed := func(text string) bool {
		return strings.ContainsAny(text, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") && strings.ContainsAny(text, "abcdefghijklmnopqrstuvwxyz") && strings.ContainsAny(text, "0123456789")
	}
	bound := regexp.MustCompile("(?i)\\b[a-z0-9_]*passw(or)?d[a-z0-9_]*\\s*:?=\\s*[\"`]")
	inline := regexp.MustCompile(`(?i)\bpassword\s+'[^'%]`)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	scanned := 0
	sources := map[string][]byte{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		raw, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		sources[entry.Name()] = raw
		for number, line := range strings.Split(string(raw), "\n") {
			for _, found := range shaped.FindAllString(line, -1) {
				if mixed(found) {
					t.Errorf("%s:%d holds a fixed credential-shaped literal of %d characters", entry.Name(), number+1, len(found)-2)
				}
			}
			if bound.MatchString(line) || inline.MatchString(line) {
				t.Errorf("%s:%d holds a fixed password literal", entry.Name(), number+1)
			}
		}
	}
	if scanned == 0 {
		t.Fatal("no Go source scanned: the scan measured nothing")
	}
	aliased, parsed, err := aliasedPasswordFindings(sources)
	if err != nil {
		t.Fatal(err)
	}
	if parsed == 0 || parsed != scanned {
		t.Fatalf("the alias pass parsed %d of %d Go sources: it measured nothing", parsed, scanned)
	}
	for _, finding := range aliased {
		t.Errorf("%s holds a fixed password bound to a literal and formatted into a role password", finding)
	}
}

type emptyImporter struct{}

func (emptyImporter) Import(importPath string) (*types.Package, error) {
	pkg := types.NewPackage(importPath, path.Base(importPath))
	pkg.MarkComplete()
	return pkg, nil
}

type bindingState struct {
	values []string
	other  bool
}

// aliasedPasswordFindings type-checks every source (imports are empty stubs, errors are ignored) and reports, as
// "<line> <file>", each call whose format operand holds PASSWORD '%s' when the argument that fills that verb is a
// constant string or a variable that every assignment binds to a constant string. The format operand is the first
// constant or literal-bound string argument of the call that holds a format verb. Variables are keyed by types.Object, so scope and overwrite
// follow object identity. Residuals: string concatenation in the password position, indexed verbs (%[2]s) and *
// width or precision operands shift the verb count.
func aliasedPasswordFindings(sources map[string][]byte) (findings []string, parsed int, err error) {
	format := regexp.MustCompile(`(?i)password\s+'%s'`)
	fset := token.NewFileSet()
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	groups := map[string][]*ast.File{}
	fileName := map[*ast.File]string{}
	for _, name := range names {
		file, err := parser.ParseFile(fset, name, sources[name], 0)
		if err != nil {
			return nil, 0, err
		}
		groups[file.Name.Name] = append(groups[file.Name.Name], file)
		fileName[file] = name
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}}
	for pkgName, files := range groups {
		conf := types.Config{Importer: emptyImporter{}, Error: func(error) {}, FakeImportC: true}
		_, _ = conf.Check(pkgName, fset, files, info)
	}
	constString := func(expr ast.Expr) (string, bool) {
		tv, ok := info.Types[expr]
		if ok && tv.Value != nil && tv.Value.Kind() == constant.String {
			return constant.StringVal(tv.Value), true
		}
		return "", false
	}
	states := map[types.Object]*bindingState{}
	note := func(ident *ast.Ident, value ast.Expr) {
		obj := info.Defs[ident]
		if obj == nil {
			obj = info.Uses[ident]
		}
		if obj == nil || ident.Name == "_" {
			return
		}
		state := states[obj]
		if state == nil {
			state = &bindingState{}
			states[obj] = state
		}
		if value != nil {
			if text, ok := constString(value); ok {
				state.values = append(state.values, text)
				return
			}
		}
		state.other = true
	}
	for _, file := range fileName2Files(groups) {
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.ValueSpec:
				for i, ident := range n.Names {
					switch {
					case len(n.Values) == 0:
					case len(n.Values) == len(n.Names):
						note(ident, n.Values[i])
					default:
						note(ident, nil)
					}
				}
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					ident, ok := lhs.(*ast.Ident)
					if !ok {
						continue
					}
					if (n.Tok == token.ASSIGN || n.Tok == token.DEFINE) && len(n.Lhs) == len(n.Rhs) {
						note(ident, n.Rhs[i])
					} else {
						note(ident, nil)
					}
				}
			case *ast.RangeStmt:
				for _, expr := range []ast.Expr{n.Key, n.Value} {
					if ident, ok := expr.(*ast.Ident); ok {
						note(ident, nil)
					}
				}
			}
			return true
		})
	}
	valuesOf := func(expr ast.Expr) []string {
		if text, ok := constString(expr); ok {
			return []string{text}
		}
		if ident, ok := expr.(*ast.Ident); ok {
			if state := states[info.Uses[ident]]; state != nil && !state.other {
				return state.values
			}
		}
		return nil
	}
	for _, pkgName := range sortedKeys(groups) {
		for _, file := range groups[pkgName] {
			name := fileName[file]
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				for i, arg := range call.Args {
					texts := valuesOf(arg)
					if len(texts) == 0 {
						continue
					}
					for _, text := range texts {
						at := format.FindStringIndex(text)
						if at == nil {
							continue
						}
						verbs := 0
						for j := 0; j < at[0]; j++ {
							if text[j] != '%' {
								continue
							}
							if text[j+1] == '%' {
								j++
								continue
							}
							verbs++
						}
						if i+1+verbs >= len(call.Args) {
							continue
						}
						target := call.Args[i+1+verbs]
						if len(valuesOf(target)) > 0 {
							findings = append(findings, fmt.Sprintf("%d %s", fset.Position(target.Pos()).Line, name))
							break
						}
					}
					break
				}
				return true
			})
		}
	}
	return findings, len(fileName), nil
}

func unusedHasFormatVerb(text string) bool {
	for j := 0; j+1 < len(text); j++ {
		if text[j] != '%' {
			continue
		}
		if text[j+1] == '%' {
			j++
			continue
		}
		return true
	}
	return false
}

func sortedKeys(groups map[string][]*ast.File) []string {
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func fileName2Files(groups map[string][]*ast.File) []*ast.File {
	var out []*ast.File
	for _, key := range sortedKeys(groups) {
		out = append(out, groups[key]...)
	}
	return out
}

func TestAliasedPasswordConstIsFound(t *testing.T) {
	src, err := os.ReadFile("testdata/pw_alias_fixture.txt")
	if err != nil {
		t.Fatal(err)
	}
	found, parsed, err := aliasedPasswordFindings(map[string][]byte{"pw_alias_fixture.go": src})
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 1 {
		t.Fatalf("parsed %d files, want 1", parsed)
	}
	want := map[int]bool{8: true, 13: true, 17: true, 30: true}
	for _, line := range found {
		var number int
		if _, err := fmt.Sscanf(line, "%d", &number); err != nil {
			t.Fatalf("finding %q has no leading line number", line)
		}
		if !want[number] {
			t.Errorf("unexpected finding %q", line)
		}
		delete(want, number)
	}
	for number := range want {
		t.Errorf("fixture line %d is not reported", number)
	}
}

func TestAliasedPasswordConstInAnotherFileIsFound(t *testing.T) {
	found, parsed, err := aliasedPasswordFindings(map[string][]byte{
		"a.go": []byte("package p\n\nconst pw = \"x1\"\n"),
		"b.go": []byte("package p\n\nimport \"fmt\"\n\nfunc f() string {\n\treturn fmt.Sprintf(`CREATE ROLE r LOGIN PASSWORD '%s'`, pw)\n}\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 2 || len(found) != 1 || found[0] != "6 b.go" {
		t.Fatalf("parsed %d, findings %q", parsed, found)
	}
}

const aliasCaseHead = `package p

import "fmt"

func build() string { return "" }

`

func aliasCaseFindings(t *testing.T, body string) []string {
	t.Helper()
	found, parsed, err := aliasedPasswordFindings(map[string][]byte{"case.go": []byte(aliasCaseHead + body)})
	if err != nil {
		t.Fatal(err)
	}
	if parsed != 1 {
		t.Fatalf("parsed %d files, want 1", parsed)
	}
	return found
}

func TestNamedFormatBindingsAreFound(t *testing.T) {
	cases := map[string]string{
		"const": `const f = "CREATE ROLE r LOGIN PASSWORD '%s'"
const pw = "x1"

func a() string { return fmt.Sprintf(f, pw) }
`,
		"var": `var f = "CREATE ROLE r LOGIN PASSWORD '%s'"
const pw = "x1"

func a() string { return fmt.Sprintf(f, pw) }
`,
		"const-expression": `const f = "CREATE ROLE r LOGIN " + "PASSWORD '%s'"
const pw = "x1"

func a() string { return fmt.Sprintf(f, pw) }
`,
		"short-declaration": `const pw = "x1"

func a() string {
	f := "CREATE ROLE r LOGIN PASSWORD '%s'"
	return fmt.Sprintf(f, pw)
}
`,
		"assignment": `const pw = "x1"

func a() string {
	var f string
	f = "CREATE ROLE r LOGIN PASSWORD '%s'"
	return fmt.Sprintf(f, pw)
}
`,
	}
	for name, body := range cases {
		if found := aliasCaseFindings(t, body); len(found) != 1 {
			t.Errorf("%s: findings %q, want one", name, found)
		}
	}
	if found := aliasCaseFindings(t, `func a() string {
	return fmt.Sprintf("%s%s", "x PASSWORD '%s'", "y")
}
`); len(found) != 0 {
		t.Errorf("a marker in a data argument is flagged: %q", found)
	}
	if found := aliasCaseFindings(t, `const pw = "x1"

func a(t *testing.T) string {
	return helper(t, "label", "CREATE ROLE r LOGIN PASSWORD '%s'", pw)
}

func helper(t *testing.T, label, format string, args ...any) string { return fmt.Sprintf(format, args...) }
`); len(found) != 1 {
		t.Errorf("a leading label constant before the real format: findings %q, want one", found)
	}
	// A run-time format followed by a constant data argument that holds the marker reads that argument as the
	// format and flags the next argument: the scan fails closed on this shape (a named residual).
	if found := aliasCaseFindings(t, `func a() string {
	return fmt.Sprintf(build(), "x PASSWORD '%s'", "y")
}
`); len(found) != 1 {
		t.Errorf("a run-time format with a constant marker data argument: findings %q, want one (fails closed)", found)
	}
}

func TestIdentifierBindingsFollowObjects(t *testing.T) {
	use := `func b(role string) string {
	value := build()
	return fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", role, value)
}
`
	cases := map[string]string{
		"const-other-scope":         "func a() { const value = \"x\"; _ = value }\n\n" + use,
		"var-other-scope":           "func a() { var value = \"x\"; _ = value }\n\n" + use,
		"short-declaration-other":   "func a() { value := \"x\"; _ = value }\n\n" + use,
		"assignment-other-function": "func a() { var value string; value = \"x\"; _ = value }\n\n" + use,
		"overwritten": `func b(role string) string {
	value := "x"
	value = build()
	return fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", role, value)
}
`,
		"nested-shadow": `func b(role string) string {
	value := "x"
	_ = value
	{
		value := build()
		return fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", role, value)
	}
}
`,
	}
	for name, body := range cases {
		if found := aliasCaseFindings(t, body); len(found) != 0 {
			t.Errorf("%s: findings %q, want none", name, found)
		}
	}
	if found := aliasCaseFindings(t, `func b(role string) string {
	value := "x"
	return fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", role, value)
}
`); len(found) != 1 {
		t.Errorf("a literal-bound value in the same function: findings %q, want one", found)
	}
	if found := aliasCaseFindings(t, `func a() string { value := build(); return value }

func b(role string) string {
	value := "x"
	return fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", role, value)
}
`); len(found) != 1 {
		t.Errorf("a literal-bound value beside a run-time value of the same name elsewhere: findings %q, want one", found)
	}
}

func TestAliasPassFailsWhenNoStringConstantWasEvaluated(t *testing.T) {
	_, parsed, err := aliasedPasswordFindings(map[string][]byte{"empty.go": []byte("package p\n\nfunc f() {}\n")})
	if parsed != 1 {
		t.Fatalf("parsed %d files, want 1", parsed)
	}
	if err == nil {
		t.Fatal("a package with no string constant passed: the alias pass measured nothing")
	}
}
