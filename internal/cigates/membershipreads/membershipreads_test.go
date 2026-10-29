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
