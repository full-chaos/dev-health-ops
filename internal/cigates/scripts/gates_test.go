package scripts_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates"
	"github.com/full-chaos/dev-health-ops/internal/cigates/affected"
)

func TestGovernance(t *testing.T) {
	cases := []struct {
		name  string
		files []string
		body  string
		rc    int
		want  string
	}{
		{"no src", []string{"README.md"}, "", 0, "no src/ changes detected"},
		{"src with tests", []string{"src/a.py", "tests/x.py"}, "", 0, "policy satisfied without PR markers"},
		{"src with test_ file", []string{"src/a.py", "pkg/test_x.py"}, "", 0, "policy satisfied without PR markers"},
		{"src with _test.PY file", []string{"src/a.py", "pkg/Foo_test.PY"}, "", 0, "policy satisfied without PR markers"},
		{"inline markers", []string{"src/a.py"}, "TEST-EVIDENCE: ran go test\nRISK-NOTES: low", 0, "PR markers provided"},
		{"heading markers", []string{"src/a.py"}, "## TEST-EVIDENCE\n- ok\n## RISK-NOTES\n- revert\n", 0, "PR markers provided"},
		{"crlf markers", []string{"src/a.py"}, "TEST-EVIDENCE: a\r\nRISK-NOTES: b\r\n", 0, "PR markers provided"},
		{"placeholder", []string{"src/a.py"}, "TEST-EVIDENCE: TBD\nRISK-NOTES: none", 1, "Missing/invalid markers: TEST-EVIDENCE, RISK-NOTES"},
		{"template text", []string{"src/a.py"}, "## TEST-EVIDENCE\n<commands + results>\n## RISK-NOTES\nx", 1, "Missing/invalid markers: TEST-EVIDENCE"},
		{"marker stops at next marker", []string{"src/a.py"}, "## TEST-EVIDENCE\nfoo\nBAR: baz\nRISK-NOTES: r", 0, "PR markers provided"},
		{"empty", []string{"src/a.py"}, "", 1, "Governance gate failed."},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			args := []string{"--pr-body", c.body}
			for _, f := range c.files {
				args = append(args, "--changed-file", f)
			}
			var out bytes.Buffer
			if rc := cigates.Governance(args, &out); rc != c.rc || !strings.Contains(out.String(), c.want) {
				t.Fatalf("rc=%d want %d, output %q lacks %q", rc, c.rc, out.String(), c.want)
			}
		})
	}
}

func TestGovernanceListsTenAndCountsRest(t *testing.T) {
	args := []string{"--pr-body", ""}
	for i := 0; i < 13; i++ {
		args = append(args, "--changed-file", "src/f"+string(rune('a'+i))+".py")
	}
	var out bytes.Buffer
	if rc := cigates.Governance(args, &out); rc != 1 {
		t.Fatalf("rc=%d", rc)
	}
	if !strings.Contains(out.String(), "(13 files)") || !strings.Contains(out.String(), "  ... and 3 more") {
		t.Fatalf("output %q", out.String())
	}
}

func TestGovernanceNeedsInput(t *testing.T) {
	var out bytes.Buffer
	if rc := cigates.Governance(nil, &out); rc != 2 || !strings.Contains(out.String(), "supply --base-sha and --head-sha") {
		t.Fatalf("rc=%d out=%q", rc, out.String())
	}
}

func TestAuthGovulncheck(t *testing.T) {
	cases := []struct {
		name, in string
		rc       int
		stdout   string
		stderr   string
	}{
		{"empty stream", "", 1, "", "no scan configuration"},
		{"clean", `{"config":{}}`, 0, "auth govulncheck: 0 reachable\n", ""},
		{"reachable", `{"config":{}}` + "\n" + `{"finding":{"osv":"GO-1","fixed_version":"v1.2","trace":[{"module":"m/a","version":"v1","function":"F"}]}}`, 1, "auth govulncheck: 1 reachable\n", "reachable GO-1 m/a@v1 fixed in v1.2"},
		{"import-only finding", `{"config":{}}{"finding":{"osv":"GO-2","trace":[{"module":"m/b"}]}}`, 0, "auth govulncheck: 0 reachable\n", ""},
		{"defaults", `{"config":{}}{"finding":{"osv":"GO-3","trace":[{"function":"G"}]}}`, 1, "", "reachable GO-3 ?@? fixed in ?"},
		{"truncated", `{"config":`, 1, "", "unreadable"},
		{"finding without osv", `{"config":{}}{"finding":{"trace":[{"function":"F"}]}}`, 1, "", "without an osv"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var out, errb bytes.Buffer
			rc := cigates.AuthGovulncheck(strings.NewReader(c.in), &out, &errb)
			if rc != c.rc || !strings.Contains(out.String(), c.stdout) || !strings.Contains(errb.String(), c.stderr) {
				t.Fatalf("rc=%d out=%q err=%q", rc, out.String(), errb.String())
			}
		})
	}
}

func TestAuthScopeCandidates(t *testing.T) {
	const mod = "example.com/m"
	in := strings.Join([]string{
		`{"ImportPath":"example.com/m/internal/auth/keys"}`,
		`{"ImportPath":"example.com/m/internal/audit"}`,
		`{"ImportPath":"example.com/m/internal/author"}`,
		`{"ImportPath":"example.com/m/internal/billing","Imports":["example.com/m/internal/storage/postgres/authschema"]}`,
		`{"ImportPath":"example.com/m/internal/x","TestImports":["example.com/m/internal/storage/postgres/authschema"]}`,
		`{"ImportPath":"example.com/m/internal/y","XTestImports":["example.com/m/internal/storage/postgres/authschema"]}`,
		`{"ImportPath":"example.com/m/internal/unrelated","Imports":["fmt"]}`,
		`{"ImportPath":"other.org/auth"}`,
	}, "\n")
	var out, errb bytes.Buffer
	if rc := cigates.AuthScopeCandidates(mod, strings.NewReader(in), &out, &errb); rc != 0 {
		t.Fatalf("rc=%d err=%q", rc, errb.String())
	}
	want := "example.com/m/internal/audit\nexample.com/m/internal/auth/keys\nexample.com/m/internal/author\nexample.com/m/internal/billing\nexample.com/m/internal/x\nexample.com/m/internal/y\n"
	if out.String() != want {
		t.Fatalf("got %q want %q", out.String(), want)
	}
	out.Reset()
	if rc := cigates.AuthScopeCandidates(mod, strings.NewReader(`{"ImportPath":"other.org/auth"}`), &out, &errb); rc != 1 || !strings.Contains(errb.String(), "no package of the module") {
		t.Fatalf("rc=%d err=%q", rc, errb.String())
	}
}

func TestAffectedIntegration(t *testing.T) {
	dir := t.TempDir()
	root := "/repo"
	golist := strings.Join([]string{
		`{"ImportPath":"m/a","Dir":"/repo/a","Module":{"Path":"m"}}`,
		`{"ImportPath":"m/b","Dir":"/repo/b","Module":{"Path":"m"},"EmbedFiles":["tmpl.txt"]}`,
		`{"ImportPath":"m/c","Dir":"/repo/c","Module":{"Path":"m"}}`,
		`{"ImportPath":"m/a.test","Deps":["m/a [m/a.test]","m/b"]}`,
		`{"ImportPath":"m/b.test","Deps":["m/b [m/b.test]"]}`,
		`{"ImportPath":"m/c.test","Deps":["fmt"]}`,
	}, "\n")
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	g := write("golist.json", golist)
	k := write("keys.txt", "a\nb\nc\n")
	cases := []struct{ name, changed, want string }{
		{"go file in dep", "b/x.go\n", "AFFECTED 2\na\nb\n"},
		{"go file in leaf", "c/x.go\n", "AFFECTED 1\nc\n"},
		{"nul separated", "b/x.go\x00", "AFFECTED 2\na\nb\n"},
		{"testdata of b", "b/testdata/f.json\n", "FULL non-Go file: b/testdata/f.json\n"},
		{"embed file", "b/tmpl.txt\n", "FULL non-Go file: b/tmpl.txt\n"},
		{"non embed file", "b/readme.md\n", "FULL non-Go file: b/readme.md\n"},
		{"go.mod", "go.mod\n", "FULL shared change: go.mod\n"},
		{"ci file", "./ci/x.sh\n", "FULL shared change: ci/x.sh\n"},
		{"unlisted go", "zz/x.go\n", "FULL Go file outside any listed package: zz/x.go\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var out, errb bytes.Buffer
			rc := affected.PlanVerdict([]string{"--event", "pull_request", "--changed", write("c.txt", c.changed), "--golist", g, "--keys", k, "--root", root}, &out, &errb)
			if rc != 0 || out.String() != c.want {
				t.Fatalf("rc=%d out=%q err=%q want %q", rc, out.String(), errb.String(), c.want)
			}
		})
	}
	var out, errb bytes.Buffer
	if rc := affected.PlanVerdict([]string{"--event", "pull_request", "--changed", write("e.txt", "a/x.go\n"), "--golist", write("e.json", "{}"), "--keys", k, "--root", root}, &out, &errb); rc != 1 {
		t.Fatalf("empty go list must fail, rc=%d", rc)
	}
}
