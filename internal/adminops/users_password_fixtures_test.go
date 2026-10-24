package adminops

import (
	"encoding/base64"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

const minFixtureRunes = 12

// usersGoldenFileSHA256 is the digest of testdata/golden/users.json, the frozen
// Python record. It keeps the legacy short passwords the Python session used;
// it is the only testdata file allowed to hold one, and only while it is
// byte-identical to the file the golden's key pins.
const usersGoldenFileSHA256 = "3e8bad8fd8e602826042b00f4e69eca20a7fce5cb57a86fd7871a1fc3663fbe5"

var passwordArgInTestdata = regexp.MustCompile(`--password\\?",\s*\\?"((?:[^"\\]|\\u[0-9a-fA-F]{4})*)`)

// No password literal in a test, and no password a golden or fixture file
// feeds to the verbs, is shorter than the policy's 12 characters. The one
// exception is the frozen Python golden (by digest) and refusedShortPassword.
func TestUsersPasswordFixturesAreAtLeastTwelve(t *testing.T) {
	short := func(value string) bool { return utf8.RuneCountInString(value) < minFixtureRunes }
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no test files: %v", err)
	}
	checked := 0
	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		literal := func(expr ast.Expr) (string, bool) {
			lit, ok := expr.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return "", false
			}
			value, err := strconv.Unquote(lit.Value)
			return value, err == nil
		}
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		usesPasswordStdin := strings.Contains(string(source), "--password-stdin")
		ast.Inspect(parsed, func(n ast.Node) bool {
			var elems []ast.Expr
			switch node := n.(type) {
			case *ast.CallExpr:
				elems = node.Args
			case *ast.CompositeLit:
				elems = node.Elts
			case *ast.KeyValueExpr:
				if key, ok := node.Key.(*ast.Ident); ok && key.Name == "Stdin" {
					if call, ok := node.Value.(*ast.CallExpr); ok && len(call.Args) == 1 {
						if value, ok := literal(call.Args[0]); ok && usesPasswordStdin {
							checked++
							if first := strings.SplitN(value, "\n", 2)[0]; first != "" && short(strings.TrimRight(first, "\r")) {
								t.Errorf("%s: stdin password literal %q is shorter than %d characters", file, first, minFixtureRunes)
							}
						}
					}
				}
				return true
			default:
				return true
			}
			for i := 0; i+1 < len(elems); i++ {
				if flag, ok := literal(elems[i]); ok && flag == "--password" {
					if value, ok := literal(elems[i+1]); ok {
						checked++
						if short(value) {
							t.Errorf("%s: --password literal %q is shorter than %d characters", file, value, minFixtureRunes)
						}
					}
				}
			}
			return true
		})
	}
	if checked < 2 {
		t.Fatalf("the scan found %d password literals: it measures too little", checked)
	}
	for name, value := range map[string]string{
		"fixturePassword1": fixturePassword1, "fixturePassword2": fixturePassword2, "fixturePassword3": fixturePassword3,
		"fixturePasswordUnicode": fixturePasswordUnicode, "fixturePasswordNew": fixturePasswordNew,
		"fixturePasswordLongCreate": fixturePasswordLongCreate, "fixturePasswordLongUpdate": fixturePasswordLongUpdate,
	} {
		if short(value) {
			t.Errorf("%s = %q is shorter than %d characters", name, value, minFixtureRunes)
		}
	}
	datafiles, err := filepath.Glob("testdata/*")
	if err != nil {
		t.Fatal(err)
	}
	more, _ := filepath.Glob("testdata/golden/*")
	scanned := 0
	for _, file := range append(datafiles, more...) {
		raw, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		scanned++
		if sha256Text(raw) == usersGoldenFileSHA256 && filepath.Base(file) == "users.json" {
			continue
		}
		for _, match := range passwordArgInTestdata.FindAllStringSubmatch(string(raw), -1) {
			if short(match[1]) {
				t.Errorf("%s: password argument %q is shorter than %d characters", file, match[1], minFixtureRunes)
			}
		}
	}
	if scanned < 3 {
		t.Fatalf("the scan read %d testdata files: it measures too little", scanned)
	}
}

// The users golden keeps its Python key: the script with each fixture swapped
// back to its legacy literal is byte-for-byte the body the golden pins. Every
// swap is used, and a short literal that reaches the stricter policy
// unswapped is one of the steps the policy retires (and is pinned).
func TestUsersLegacyPasswordTableReproducesTheGoldenKey(t *testing.T) {
	if got := sha256Text([]byte(base64.StdEncoding.EncodeToString(usersGoldenInput(t)))); got != usersGoldenBodySHA256 {
		t.Fatalf("the swapped script hashes to %s, the golden pins %s", got, usersGoldenBodySHA256)
	}
	raw, err := os.ReadFile("testdata/golden/users.json")
	if err != nil {
		t.Fatal(err)
	}
	if sha256Text(raw) != usersGoldenFileSHA256 || !strings.Contains(string(raw), `"body_sha256": "`+usersGoldenBodySHA256+`"`) {
		t.Fatal("testdata/golden/users.json was edited: it is the frozen Python record and stays byte-identical")
	}
	used := map[string]int{}
	fixtures := map[string]bool{}
	for _, pair := range legacyPasswords {
		if fixtures[pair.fixture] {
			t.Errorf("fixture %q is in the swap table twice", pair.fixture)
		}
		fixtures[pair.fixture] = true
		if utf8.RuneCountInString(pair.fixture) < minFixtureRunes {
			t.Errorf("swapped fixture %q is shorter than %d characters", pair.fixture, minFixtureRunes)
		}
	}
	for index, s := range usersScript {
		for i := 0; i+1 < len(s.args); i++ {
			if s.args[i] != "--password" {
				continue
			}
			value := s.args[i+1]
			switch {
			case fixtures[value]:
				used[value]++
			case value == refusedShortPassword && retiredUsersSteps[index]:
			default:
				t.Errorf("step %d: password %q is neither a swapped fixture nor a retired refusal", index, value)
			}
		}
	}
	for _, pair := range legacyPasswords {
		if used[pair.fixture] == 0 {
			t.Errorf("swapped fixture for %q is never used by the script", pair.legacy)
		}
	}
}
