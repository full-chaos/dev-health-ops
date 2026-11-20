package inputs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// classCensus (go/ast only, no go/types) reads the non-test source of the
// package: the Class* consts, the Classes slice, and the first argument of
// every refuse / notBuilt call. It returns the problems found.
func classCensus(srcs map[string]string) (declared map[string]string, used map[string]bool, problems []string) {
	declared, used = map[string]string{}, map[string]bool{}
	var inSlice []string
	fset := token.NewFileSet()
	var files []*ast.File
	for name, src := range srcs {
		f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
		if err != nil {
			return nil, nil, []string{err.Error()}
		}
		files = append(files, f)
	}
	for _, f := range files {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, sp := range gd.Specs {
				vs, ok := sp.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, id := range vs.Names {
					switch {
					case gd.Tok == token.CONST && strings.HasPrefix(id.Name, "Class") && i < len(vs.Values):
						if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
							v, _ := strconv.Unquote(lit.Value)
							declared[id.Name] = v
						} else {
							problems = append(problems, id.Name+" is not a string literal const")
						}
					case gd.Tok == token.VAR && id.Name == "Classes" && i < len(vs.Values):
						if cl, ok := vs.Values[i].(*ast.CompositeLit); ok {
							for _, e := range cl.Elts {
								if x, ok := e.(*ast.Ident); ok {
									inSlice = append(inSlice, x.Name)
								} else {
									problems = append(problems, "Classes holds a non-const element")
								}
							}
						}
					}
				}
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || (id.Name != "refuse" && id.Name != "notBuilt") {
				return true
			}
			if len(call.Args) == 0 {
				problems = append(problems, id.Name+" call without a class")
				return true
			}
			a, ok := call.Args[0].(*ast.Ident)
			if !ok || !strings.HasPrefix(a.Name, "Class") {
				problems = append(problems, id.Name+" call whose first argument is not a Class const at "+fset.Position(call.Pos()).String())
				return true
			}
			used[a.Name] = true
			return true
		})
	}
	for name := range declared {
		if !slices.Contains(inSlice, name) {
			problems = append(problems, name+" is not in Classes")
		}
	}
	for _, name := range inSlice {
		if _, ok := declared[name]; !ok {
			problems = append(problems, name+" in Classes is not declared")
		}
	}
	for name := range used {
		if _, ok := declared[name]; !ok {
			problems = append(problems, name+" used but not declared")
		}
	}
	return declared, used, problems
}

// classesUsedOutsidePackage are emitted by deploycli, not by Parse.
var classesUsedOutsidePackage = []string{"ClassStdinUnreadable", "ClassTargetMismatch"}

func TestReasonClassSetIsClosed(t *testing.T) {
	srcs := map[string]string{}
	for _, f := range []string{"inputs.go", "devhealth.go"} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		srcs[f] = string(b)
	}
	declared, used, problems := classCensus(srcs)
	for _, p := range problems {
		t.Error(p)
	}
	token := regexp.MustCompile(`^[a-z][a-z_]*$`)
	seen := map[string]bool{}
	for name, v := range declared {
		if !token.MatchString(v) || seen[v] {
			t.Errorf("%s = %q is not a unique snake_case token", name, v)
		}
		seen[v] = true
		if !used[name] && !slices.Contains(classesUsedOutsidePackage, name) {
			t.Errorf("%s has no refusal site", name)
		}
	}
	if len(declared) != len(Classes) {
		t.Errorf("%d consts, %d in Classes", len(declared), len(Classes))
	}
}

func TestTheClassCensusCatchesEachPlantedForm(t *testing.T) {
	base := `package inputs
const ClassA = "a"
var Classes = []string{ClassA}
func f() error { return refuse(ClassA, "", 0, "x") }
`
	if _, _, p := classCensus(map[string]string{"x.go": base}); len(p) != 0 {
		t.Fatalf("clean file: %v", p)
	}
	for name, src := range map[string]string{
		"string literal class": strings.Replace(base, "refuse(ClassA", `refuse("a"`, 1),
		"variable class":       strings.Replace(base, "refuse(ClassA", "refuse(c", 1),
		"const not in set":     base + "const ClassB = \"b\"\n",
		"set holds unknown":    strings.Replace(base, "{ClassA}", "{ClassA, ClassZ}", 1),
		"no args":              strings.Replace(base, `refuse(ClassA, "", 0, "x")`, "notBuilt()", 1),
		"non-literal const":    base + "const ClassC = \"c\" + \"d\"\n",
	} {
		if _, _, p := classCensus(map[string]string{"x.go": src}); len(p) == 0 {
			t.Errorf("%s: not caught", name)
		}
	}
}

func TestRefusalKeyHoldsOnlyFixedKeyNames(t *testing.T) {
	for buf, want := range map[string]string{
		goodDev + "ZZ_CANARY_KEY=v\n":                    "",
		"ZZ_CANARY_KEY=v\n" + goodDev:                    "",
		goodDev + "STEP=42\n":                            "STEP",
		goodDev + "CMDELTA=1\n":                          "CMDELTA",
		strings.Replace(goodDev, "STEP=41", "STEP=x", 1): "STEP",
	} {
		_, err := Parse([]byte(buf))
		r, ok := err.(*Refusal)
		if !ok || r.Key != want {
			t.Errorf("key %q, want %q (%v)", keyOf(r), want, err)
		}
	}
}

func keyOf(r *Refusal) string {
	if r == nil {
		return "<nil>"
	}
	return r.Key
}
