package membershipreads_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/membershipreads"
)

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func check(t *testing.T, root string) []string {
	t.Helper()
	got, err := membershipreads.Check(root, membershipreads.Scopes)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	return got
}

func wantOne(t *testing.T, got []string, substrs ...string) {
	t.Helper()
	if len(got) != 1 {
		t.Fatalf("want exactly one finding, got %q", got)
	}
	for _, s := range substrs {
		if !strings.Contains(got[0], s) {
			t.Fatalf("finding %q lacks %q", got[0], s)
		}
	}
}

const head = "package p\n\nconst q = "

// CHAOS-10985 (acp OPTION P, D6358): go-api and query-api read public.memberships and public.organizations only; the auth
// schema's copies are written by auth-service in the same transaction and are not a second read authority.

func TestPlantedGoAPIReadOfTheAuthMembershipsIsRefusedByName(t *testing.T) {
	root := writeTree(t, map[string]string{"internal/apiservice/x.go": head + "\"SELECT role FROM auth.memberships WHERE user_id = $1\"\n"})
	wantOne(t, check(t, root), "internal/apiservice/x.go:3", "membership-read", "go-api", "auth.memberships", "D6358")
}

func TestPlantedQueryAPIReadOfTheAuthOrganizationsIsRefusedByName(t *testing.T) {
	root := writeTree(t, map[string]string{"internal/queryapi/server/y.go": head + "\"SELECT o.name FROM public.memberships m JOIN auth.organizations o ON o.id = m.org_id\"\n"})
	wantOne(t, check(t, root), "internal/queryapi/server/y.go:3", "membership-read", "query-api", "auth.organizations")
}

func TestPlantedQueryAPIReadOfTheAuthMembershipsIsRefusedByName(t *testing.T) {
	root := writeTree(t, map[string]string{"internal/queryapiservice/z.go": head + "`SELECT role FROM auth.memberships`\n"})
	wantOne(t, check(t, root), "internal/queryapiservice/z.go:3", "query-api", "auth.memberships")
}

func TestTheSchemaNameHiddenBehindQuotesAFormatVerbOrAConcatenationIsStillARead(t *testing.T) {
	for name, literal := range map[string]string{
		"quoted":          "\"SELECT 1 FROM \\\"auth\\\".\\\"memberships\\\"\"",
		"mixed case":      "\"select 1 from Auth.Memberships\"",
		"format verb":     "fmt.Sprintf(\"SELECT 1 FROM %s.memberships\", schema)",
		"concatenation":   "\"SELECT 1 FROM \" + schema + \".organizations\"",
		"split literal":   "\"SELECT 1 FROM auth.\" + \"memberships\"",
		"another schema":  "\"SELECT 1 FROM acp.memberships\"",
		"spaces around .": "\"SELECT 1 FROM auth . memberships\"",
	} {
		src := "package p\n\nimport \"fmt\"\n\nvar _ = fmt.Sprintf\n\nvar q = " + literal + "\n"
		got := check(t, writeTree(t, map[string]string{"internal/api/billing/b.go": src}))
		if len(got) != 1 || !strings.Contains(got[0], "internal/api/billing/b.go:7") {
			t.Errorf("%s: want one finding at line 7, got %q", name, got)
		}
	}
}

func TestPublicReadsComposeAndCommentsTestsAndOtherCodeAreNotFindings(t *testing.T) {
	root := writeTree(t, map[string]string{
		"internal/apiservice/ok.go":         "package p\n\n// reads auth.memberships? no: this comment is not a read\nconst a = \"SELECT role FROM public.memberships WHERE user_id = $1\"\nconst b = \"SELECT slug FROM \\\"public\\\".\\\"organizations\\\"\"\nconst c = \"select 1 from PUBLIC.memberships\"\nconst d = \"memberships\"\n",
		"internal/apiservice/ok_test.go":    "package p\n\nconst t = \"SELECT 1 FROM auth.memberships\"\n",
		"internal/auth/identity/store.go":   "package p\n\nconst s = \"INSERT INTO auth.memberships DEFAULT VALUES\"\n",
		"internal/apiservice/testdata/x.go": "package p\n\nconst s = \"SELECT 1 FROM auth.memberships\"\n",
	})
	if got := check(t, root); len(got) != 0 {
		t.Fatalf("want no finding, got %q", got)
	}
}

func TestEveryScopeRootHoldsProductionCodeSoTheRealScanIsNotVacuous(t *testing.T) {
	root := repoRoot(t)
	pkgs, err := membershipreads.Packages(root, membershipreads.Scopes)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range membershipreads.Scopes {
		n := 0
		for _, p := range pkgs {
			if p == scope.Root || strings.HasPrefix(p, scope.Root+"/") {
				n++
			}
		}
		if n == 0 {
			t.Errorf("scope root %s (%s) holds no production package", scope.Root, scope.Service)
		}
	}
	for _, want := range []string{"internal/api/policy", "internal/apiservice", "internal/queryapi/server"} {
		found := false
		for _, p := range pkgs {
			found = found || p == want
		}
		if !found {
			t.Errorf("the scan does not cover %s", want)
		}
	}
	t.Logf("scanned %d package(s): %s", len(pkgs), strings.Join(pkgs, " "))
}

func TestTheRepositoryHoldsNoGoAPIOrQueryAPIReadOfTheAuthMembershipTables(t *testing.T) {
	if got := check(t, repoRoot(t)); len(got) != 0 {
		t.Fatalf("%d finding(s):\n%s", len(got), strings.Join(got, "\n"))
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// A constant that holds part of the table name is folded like a literal: the schema, the dot and the table may each be a
// constant, declared in this file or another file of the same package, grouped or alone, typed or not, built from other
// constants, parenthesised or nested.

func TestAConstantSchemaSplitAcrossAConcatenationIsStillARead(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"same file":             {"internal/queryapi/server/q.go": "package p\n\nconst schema = \"auth\"\n\nfunc q() string {\n\treturn \"SELECT role FROM \" + schema + \".\" + \"memberships\"\n}\n"},
		"another file":          {"internal/queryapi/server/a.go": "package p\n\nconst schema = \"auth\"\n", "internal/queryapi/server/b.go": "package p\n\nfunc q() string {\n\treturn \"SELECT role FROM \" + schema + \".memberships\"\n}\n"},
		"grouped block":         {"internal/apiservice/q.go": "package p\n\nconst (\n\tother = \"x\"\n\tschema = \"auth\"\n\ttable = \"organizations\"\n)\n\nfunc q() string {\n\treturn \"SELECT 1 FROM \" + schema + \".\" + table\n}\n"},
		"typed constant":        {"internal/apiservice/q.go": "package p\n\nconst schema string = \"auth\"\n\nfunc q() string {\n\treturn \"SELECT 1 FROM \" + schema + \".memberships\"\n}\n"},
		"constant of constants": {"internal/queryapi/q.go": "package p\n\nconst (\n\ts = \"au\"\n\tschema = s + \"th\"\n\tqualified = schema + \".\" + \"memberships\"\n)\n\nfunc q() string {\n\treturn \"SELECT 1 FROM \" + qualified\n}\n"},
		"parenthesised":         {"internal/api/q.go": "package p\n\nconst schema = \"auth\"\n\nfunc q() string {\n\treturn \"SELECT 1 FROM \" + (schema + (\".\" + \"memberships\"))\n}\n"},
		"string conversion":     {"internal/api/q.go": "package p\n\nconst schema = string(\"auth\")\n\nfunc q() string {\n\treturn \"SELECT 1 FROM \" + schema + \".memberships\"\n}\n"},
		"constant in a func":    {"internal/api/q.go": "package p\n\nfunc q() string {\n\tconst schema = \"auth\"\n\treturn \"SELECT 1 FROM \" + schema + \".\" + \"memberships\"\n}\n"},
	} {
		got := check(t, writeTree(t, files))
		if len(got) == 0 {
			t.Errorf("%s: want a finding naming auth.memberships or auth.organizations, got none", name)
			continue
		}
		joined := strings.Join(got, "\n")
		if !strings.Contains(joined, "auth.memberships") && !strings.Contains(joined, "auth.organizations") {
			t.Errorf("%s: the findings do not name the auth table: %q", name, got)
		}
	}
}

func TestAConstantThatNamesAPublicTableOrADifferentTableIsNotAFinding(t *testing.T) {
	for name, src := range map[string]string{
		"public schema":        "package p\n\nconst schema = \"public\"\n\nfunc q() string {\n\treturn \"SELECT 1 FROM \" + schema + \".\" + \"memberships\"\n}\n",
		"auth, another table":  "package p\n\nconst schema = \"auth\"\n\nfunc q() string {\n\treturn \"SELECT 1 FROM \" + schema + \".sessions\"\n}\n",
		"local shadows global": "package p\n\nconst schema = \"auth\"\n\nfunc q() string {\n\tconst schema = \"public\"\n\treturn \"SELECT 1 FROM \" + schema + \".memberships\"\n}\n",
	} {
		if got := check(t, writeTree(t, map[string]string{"internal/apiservice/q.go": src})); len(got) != 0 {
			t.Errorf("%s: want no finding, got %q", name, got)
		}
	}
}

// A constant from another package is not loaded (stated limit): a concatenation that puts one in front of a table name is
// refused by name, never passed silently.
func TestAQualifiedConstantInFrontOfATableNameIsRefusedAsUnresolved(t *testing.T) {
	src := "package p\n\nimport \"example.test/cfg\"\n\nfunc q() string {\n\treturn \"SELECT 1 FROM \" + cfg.Schema + \".\" + \"memberships\"\n}\n"
	got := check(t, writeTree(t, map[string]string{"internal/queryapi/server/q.go": src}))
	wantOne(t, got, "internal/queryapi/server/q.go:6", "query-api", "another package", "memberships")
}

// A variable is not folded, so a table name right behind a non-constant operand is refused structurally: whatever the variable
// holds, the schema in front of the table is not a constant "public".
func TestATableNameBehindANonConstantQualifierIsARefusal(t *testing.T) {
	for name, body := range map[string]string{
		"variable, dot and table as separate literals": "\"SELECT 1 FROM \" + schema + \".\" + \"memberships\"",
		"variable then a dotted literal":               "\"SELECT 1 FROM \" + schema + \".memberships\"",
		"variable then organizations":                  "\"SELECT 1 FROM \" + schema + \".organizations\"",
		"call result":                                  "\"SELECT 1 FROM \" + pick() + \".memberships\"",
		"field":                                        "\"SELECT 1 FROM \" + cfg.Schema + \".memberships\"",
		"quoted table":                                 "\"SELECT 1 FROM \" + schema + \".\\\"memberships\\\"\"",
		"space and comment before the dot":             "\"SELECT 1 FROM \" + schema + \" /* c */ . memberships\"",
		"format verb":                                  "fmt.Sprintf(\"SELECT 1 FROM %s.memberships\", schema)",
		"format verb with an index":                    "fmt.Sprintf(\"SELECT 1 FROM %[1]s.organizations\", schema)",
	} {
		src := "package p\n\nimport \"fmt\"\n\nvar schema = \"auth\"\nvar cfg struct{ Schema string }\n\nfunc pick() string { return schema }\n\nvar _ = fmt.Sprintf\n\nvar q = " + body + "\n"
		got := check(t, writeTree(t, map[string]string{"internal/api/q.go": src}))
		if len(got) == 0 || !strings.Contains(strings.Join(got, "\n"), "membership-read") {
			t.Errorf("%s: want a finding, got %q", name, got)
		}
	}
}

func TestANonConstantQualifierBehindAnotherTableOrAConstantPublicIsClean(t *testing.T) {
	for name, body := range map[string]string{
		"another table":       "\"SELECT 1 FROM \" + schema + \".sessions\"",
		"constant public":     "\"SELECT 1 FROM \" + pub + \".\" + \"memberships\"",
		"public literal":      "\"SELECT 1 FROM public.\" + \"memberships\"",
		"table without a dot": "\"SELECT 1 FROM \" + schema + \" memberships\"",
	} {
		src := "package p\n\nconst pub = \"public\"\n\nvar schema = \"auth\"\n\nvar q = " + body + "\n"
		if got := check(t, writeTree(t, map[string]string{"internal/api/q.go": src})); len(got) != 0 {
			t.Errorf("%s: want no finding, got %q", name, got)
		}
	}
}

// Every constant expression the compiler evaluates has the same value in the gate: integer and rune conversions, arithmetic,
// shifts, typed constants and len of a constant string.
func TestAConstantEvaluatedLikeTheCompilerIsStillARead(t *testing.T) {
	for name, decl := range map[string]string{
		"string of an integer":     "const schema = string(97) + \"uth\"",
		"string of a rune":         "const schema = string('a') + \"uth\"",
		"string of arithmetic":     "const schema = string(96+1) + \"uth\"",
		"string of a shift":        "const schema = string(1<<6+33) + \"uth\"",
		"string of a typed rune":   "const r rune = 97\nconst schema = string(r) + \"uth\"",
		"string of a byte":         "const b byte = 97\nconst schema = string(rune(b)) + \"uth\"",
		"string of a constant sum": "const (\n\ta = 90\n\tb = 7\n\tschema = string(a+b) + \"uth\"\n)",
		"string of a hex literal":  "const schema = string(0x61) + \"uth\"",
	} {
		src := "package p\n\n" + decl + "\n\nvar q = \"SELECT 1 FROM \" + schema + \".memberships\"\n"
		got := check(t, writeTree(t, map[string]string{"internal/queryapi/q.go": src}))
		if !strings.Contains(strings.Join(got, "\n"), "auth.memberships") {
			t.Errorf("%s: want a finding naming auth.memberships, got %q", name, got)
		}
	}
}

// A SQL comment between the schema, the dot and the table does not hide the read: both comment kinds, nested block comments,
// and a quoted string that looks like the start of a line comment.
func TestASQLCommentInsideTheNameDoesNotHideTheRead(t *testing.T) {
	for name, literal := range map[string]string{
		"block comment after the dot":        "\"SELECT 1 FROM auth./* x */memberships\"",
		"block comment around the dot":       "\"SELECT 1 FROM auth /*x*/ . memberships\"",
		"block comment before the dot":       "\"SELECT 1 FROM auth/*x*/.memberships\"",
		"block comment around the schema":    "\"SELECT 1 FROM /*x*/auth/*y*/./*z*/organizations\"",
		"line comment between the parts":     "\"SELECT 1 FROM auth.-- x\\nmemberships\"",
		"line comment before the dot":        "\"SELECT 1 FROM auth -- x\\n.memberships\"",
		"nested block comment":               "\"SELECT 1 FROM auth./* a /* b */ c */memberships\"",
		"quoted schema with a comment":       "\"SELECT 1 FROM \\\"auth\\\"/*x*/.\\\"memberships\\\"\"",
		"a quoted dash pair before the read": "\"SELECT '--' FROM auth.memberships\"",
	} {
		src := "package p\n\nvar q = " + literal + "\n"
		got := check(t, writeTree(t, map[string]string{"internal/apiservice/q.go": src}))
		if len(got) == 0 || !strings.Contains(strings.Join(got, "\n"), "membership-read") {
			t.Errorf("%s: want a finding, got %q", name, got)
		}
	}
}
