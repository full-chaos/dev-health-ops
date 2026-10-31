package bigboyrender_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-12268: with ACR_ACCESS_PROJECT_SCOPE=true, go-api's ClickHouse readiness requires APIPostureFor(db, true):
// APIPosture plus ProjectScopeGrant (SELECT on projects), exact both ways. The bigboy users.d render, the grants
// comparison and the two checks must therefore know the flag go-api runs with.

const projectScopeManifest = syntheticManifest + `
func ProjectScopeGrant(database string) TableGrant {
	return TableGrant{Database: database, Table: "projects", AllowSelect: true}
}
`

// renderWith runs render-ch-users with extra leading options on manifest text.
func renderWith(t *testing.T, manifest, password string, opts ...string) (code int, out, stderr string, dir string) {
	t.Helper()
	dir = t.TempDir()
	t.Setenv("API_CH_PASSWORD", password)
	writeFile(t, filepath.Join(dir, "authorization.go"), manifest)
	args := append([]string{"render-ch-users"}, opts...)
	args = append(args, filepath.Join(dir, "authorization.go"), filepath.Join(dir, "out.xml"))
	code, _, stderr = run(args...)
	if raw, err := os.ReadFile(filepath.Join(dir, "out.xml")); err == nil {
		out = string(raw)
	}
	return code, out, stderr, dir
}

func projectScopeGolden(t *testing.T) []string {
	t.Helper()
	text := readFile(t, filepath.Join(repoRootDir(t), "internal", "storage", "clickhouse", "testdata", "api_grants_project_scope.golden"))
	golden := strings.Split(strings.TrimSpace(text), "\n")
	if len(golden) < 2 {
		t.Fatal("the flag-on golden is empty: this test would prove nothing")
	}
	return golden
}

func TestRenderWithTheProjectScopeEqualsTheFlagOnGolden(t *testing.T) {
	code, out, stderr, _ := renderWith(t, realManifest(t), "Zq7-throwaway-credential", "--project-scope")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if diff := goldenDiff(queries(out), projectScopeGolden(t)); diff != "" {
		t.Errorf("the flag-on render of the real manifest differs from its golden: %s", diff)
	}
	if got, want := len(queries(out)), len(realGolden(t))+1; got != want {
		t.Errorf("%d grants with the project scope, want the %d of the flag-off render plus one", got, want-1)
	}
	if strings.Contains(out, "Zq7-throwaway-credential") {
		t.Error("the password is in the output")
	}
	// Without the option the render is the flag-off one: no projects grant.
	code, out, stderr, _ = renderWith(t, realManifest(t), "Zq7-throwaway-credential")
	if code != 0 || strings.Contains(out, "default.projects") {
		t.Fatalf("the flag-off render: exit %d, projects grant present %v: %s", code, strings.Contains(out, "default.projects"), stderr)
	}
}

func TestRenderWithTheProjectScopeAddsExactlyTheProjectScopeRow(t *testing.T) {
	code, out, stderr, _ := renderWith(t, projectScopeManifest, "pw-1", "--project-scope")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	want := []string{
		"GRANT SELECT, INSERT, ALTER DELETE ON default.teams",
		"GRANT SELECT ON default.team_sync_policies",
		"GRANT INSERT ON default.team_memberships",
		"GRANT SELECT ON default.projects",
	}
	if diff := goldenDiff(queries(out), want); diff != "" {
		t.Errorf("render: %s", diff)
	}
}

// The option never guesses: a manifest whose ProjectScopeGrant is missing, grants nothing, or holds two rows is
// refused, and nothing is written.
func TestRenderWithTheProjectScopeRefusesAManifestWithoutOneProjectScopeRow(t *testing.T) {
	for name, manifest := range map[string]string{
		"no ProjectScopeGrant": syntheticManifest,
		"a row with no privilege": syntheticManifest + "\nfunc ProjectScopeGrant(database string) TableGrant {\n" +
			"\treturn TableGrant{Database: database, Table: \"projects\"}\n}\n",
		"two rows": syntheticManifest + "\nfunc ProjectScopeGrant(database string) TableGrant {\n" +
			"\t_ = TableGrant{Database: database, Table: \"teams\", AllowSelect: true}\n" +
			"\treturn TableGrant{Database: database, Table: \"projects\", AllowSelect: true}\n}\n",
	} {
		t.Run(name, func(t *testing.T) {
			code, out, stderr, _ := renderWith(t, manifest, "pw-1", "--project-scope")
			if code == 0 || out != "" || !strings.Contains(stderr, "ProjectScopeGrant") || !strings.Contains(stderr, "nothing written") {
				t.Fatalf("exit %d, out %q, stderr %q", code, out, stderr)
			}
		})
	}
}

// Codex r1 on #5387 (executed): a comment inside ProjectScopeGrant was read as its row, so the render and the
// comparison accepted a projects grant the function does not return. Comments are not rows: a `//` comment is
// ignored, a block comment is refused, and a trailing comment on the real row is fine.
func TestTheProjectScopeRowIsNeverReadFromAComment(t *testing.T) {
	commented := syntheticManifest + "\nfunc ProjectScopeGrant(database string) TableGrant {\n" +
		"\t// {Database: database, Table: \"projects\", AllowSelect: true}\n" +
		"\treturn TableGrant{Database: database, Table: \"teams\"}\n}\n"
	block := syntheticManifest + "\nfunc ProjectScopeGrant(database string) TableGrant {\n" +
		"\t/*\n\t{Database: database, Table: \"projects\", AllowSelect: true}\n\t*/\n" +
		"\treturn TableGrant{Database: database, Table: \"teams\"}\n}\n"
	for name, manifest := range map[string]string{"line comment": commented, "block comment": block} {
		t.Run(name, func(t *testing.T) {
			code, out, stderr, _ := renderWith(t, manifest, "pw-1", "--project-scope")
			if code == 0 || out != "" || !strings.Contains(stderr, "ProjectScopeGrant") {
				t.Fatalf("render: exit %d, out %q, stderr %q", code, out, stderr)
			}
			code, cmp, _ := run("grants-compare", "--project-scope", "SELECT\tteams\nSELECT\tprojects\n", manifest)
			if code != 1 || !strings.Contains(cmp, "GRANTS_CHECK_FAIL") || !strings.Contains(cmp, "ProjectScopeGrant") {
				t.Fatalf("compare: exit %d, stdout %q", code, cmp)
			}
		})
	}
	trailing := syntheticManifest + "\nfunc ProjectScopeGrant(database string) TableGrant {\n" +
		"\treturn TableGrant{Database: database, Table: \"projects\", AllowSelect: true} // the flag-on row\n}\n"
	code, out, stderr, _ := renderWith(t, trailing, "pw-1", "--project-scope")
	if code != 0 || !strings.Contains(out, "<query>GRANT SELECT ON default.projects</query>") {
		t.Fatalf("a trailing comment on the real row: exit %d, out %q, stderr %q", code, out, stderr)
	}
}

func TestGrantsCompareWithTheProjectScope(t *testing.T) {
	source := realManifest(t)
	off := realExpectation(t)
	on := append(append([]string{}, off...), "SELECT\tprojects")
	tsv := func(rows []string) string { return strings.Join(rows, "\n") + "\n" }
	cases := []struct {
		name   string
		args   []string
		code   int
		output string
	}{
		{"flag on, grant present", []string{"--project-scope", tsv(on), source}, 0, "GRANTS_CHECK_OK"},
		{"flag on, grant missing", []string{"--project-scope", tsv(off), source}, 1, "missing=[('SELECT', 'projects')] extra=[]"},
		{"flag off, grant extra", []string{tsv(on), source}, 1, "missing=[] extra=[('SELECT', 'projects')]"},
		{"flag off, grant absent", []string{tsv(off), source}, 0, "GRANTS_CHECK_OK"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, stderr := run(append([]string{"grants-compare"}, tc.args...)...)
			if code != tc.code || !strings.Contains(out, tc.output) {
				t.Fatalf("exit %d, want %d; stdout %q (want %q); stderr %q", code, tc.code, out, tc.output, stderr)
			}
		})
	}
	// A manifest without ProjectScopeGrant cannot give the flag-on expectation: refused, not compared.
	code, out, _ := run("grants-compare", "--project-scope", tsv(off), syntheticManifest)
	if code != 1 || !strings.Contains(out, "GRANTS_CHECK_FAIL") || !strings.Contains(out, "ProjectScopeGrant") {
		t.Fatalf("no ProjectScopeGrant: exit %d, stdout %q", code, out)
	}
}

// goAPIProjectScope runs `go-api-project-scope` on an env file holding text.
func goAPIProjectScope(t *testing.T, text string) (int, string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "go-api.env")
	writeFile(t, path, text)
	return run("go-api-project-scope", path)
}

func TestGoAPIProjectScopeReadsTheFlagFromTheEnvFile(t *testing.T) {
	for _, tc := range []struct {
		name, text, want string
	}{
		{"absent", "OTHER=1\n", "false"},
		{"empty file", "", "false"},
		{"empty value", "ACR_ACCESS_PROJECT_SCOPE=\n", "false"},
		{"true", "ACR_ACCESS_PROJECT_SCOPE=true\n", "true"},
		{"1", "A=b\nACR_ACCESS_PROJECT_SCOPE=1\n", "true"},
		{"TRUE with CRLF", "ACR_ACCESS_PROJECT_SCOPE=TRUE\r\n", "true"},
		{"false", "ACR_ACCESS_PROJECT_SCOPE=false\n", "false"},
		{"double quoted", "ACR_ACCESS_PROJECT_SCOPE=\"true\"\n", "true"},
		{"single quoted", "ACR_ACCESS_PROJECT_SCOPE='0'\n", "false"},
		{"spaces around the value", "ACR_ACCESS_PROJECT_SCOPE=  true  \n", "true"},
		{"commented out", "# ACR_ACCESS_PROJECT_SCOPE=true\n", "false"},
		{"another key with the prefix", "ACR_ACCESS_PROJECT_SCOPE_X=true\n", "false"},
		{"no newline at the end", "ACR_ACCESS_PROJECT_SCOPE=true", "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out, stderr := goAPIProjectScope(t, tc.text)
			if code != 0 || out != tc.want+"\n" {
				t.Fatalf("exit %d, stdout %q, want %q; stderr %q", code, out, tc.want+"\n", stderr)
			}
		})
	}
}

// Every value go-api would refuse at start, and every line whose meaning could differ between this reader and
// Compose's env-file parser, is refused loudly: the check must never read a different flag than go-api runs with.
// The refusal names the env and the line number, never the line.
func TestGoAPIProjectScopeRefusesAnInvalidOrAmbiguousLine(t *testing.T) {
	for _, tc := range []struct{ name, text string }{
		{"not a boolean", "ACR_ACCESS_PROJECT_SCOPE=yes-secret-ish\n"},
		{"repeated", "ACR_ACCESS_PROJECT_SCOPE=false\nACR_ACCESS_PROJECT_SCOPE=true\n"},
		{"export prefix", "export ACR_ACCESS_PROJECT_SCOPE=true\n"},
		{"space before the equals", "ACR_ACCESS_PROJECT_SCOPE =true\n"},
		{"inline comment", "ACR_ACCESS_PROJECT_SCOPE=true # on\n"},
		{"unbalanced quote", "ACR_ACCESS_PROJECT_SCOPE=\"true\n"},
		{"interpolation", "ACR_ACCESS_PROJECT_SCOPE=${FLAG}\n"},
		{"indented", "  ACR_ACCESS_PROJECT_SCOPE=true\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out, stderr := goAPIProjectScope(t, tc.text)
			if code != 1 || out != "" || !strings.Contains(stderr, "ACR_ACCESS_PROJECT_SCOPE") || !strings.Contains(stderr, "line ") {
				t.Fatalf("exit %d, stdout %q, stderr %q", code, out, stderr)
			}
			if strings.Contains(stderr, "secret-ish") || strings.Contains(stderr, "FLAG") {
				t.Fatalf("the refusal prints the line: %q", stderr)
			}
		})
	}
}

func TestGoAPIProjectScopeRefusesAMissingFileAndBadUsage(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "go-api.env")
	if code, out, stderr := run("go-api-project-scope", missing); code != 1 || out != "" || !strings.Contains(stderr, missing) {
		t.Errorf("missing file: exit %d, stdout %q, stderr %q", code, out, stderr)
	}
	if code, _, stderr := run("go-api-project-scope"); code != 2 || !strings.Contains(stderr, "usage: cigate bigboy go-api-project-scope") {
		t.Errorf("usage: exit %d, stderr %q", code, stderr)
	}
	dir := t.TempDir()
	if code, _, stderr := run("go-api-project-scope", dir); code != 1 || !strings.Contains(stderr, "not a file") {
		t.Errorf("a directory: exit %d, stderr %q", code, stderr)
	}
}

// The checks read go-api's flag from its env file. That is the flag go-api runs with only while the compose file
// gives go-api the flag through that file alone: this pins the env_file path the scripts read and that no
// environment entry of go-api sets the flag.
func TestTheComposeFileGivesGoAPITheProjectScopeFlagOnlyThroughItsEnvFile(t *testing.T) {
	raw := readFile(t, filepath.Join(repoRootDir(t), "ci", "bigboy", "compose.bigboy.yml"))
	var doc struct {
		Services map[string]yaml.Node `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatal(err)
	}
	node, ok := doc.Services["go-api"]
	if !ok {
		t.Fatal("compose.bigboy.yml has no go-api service")
	}
	var api struct {
		EnvFile []struct {
			Path string `yaml:"path"`
		} `yaml:"env_file"`
		Environment []string `yaml:"environment"`
	}
	if err := node.Decode(&api); err != nil {
		t.Fatalf("go-api's env_file or environment changed shape: %v", err)
	}
	if len(api.EnvFile) != 1 || api.EnvFile[0].Path != "./.go-api-dev/go-api.env" {
		t.Fatalf("go-api's env_file is %+v, want the one ./.go-api-dev/go-api.env the checks read", api.EnvFile)
	}
	if len(api.Environment) == 0 {
		t.Fatal("go-api has no environment entries: the compose file was not read")
	}
	for _, e := range api.Environment {
		if strings.HasPrefix(strings.TrimSpace(e), "ACR_ACCESS_PROJECT_SCOPE") {
			t.Fatalf("go-api's environment sets %q: the checks read the flag from the env file only", e)
		}
	}
	for _, script := range []string{"check-dho-api-ch-user.sh", "bigboy-grants-check.sh"} {
		text := readFile(t, filepath.Join(repoRootDir(t), "ci", "bigboy", script))
		if !strings.Contains(text, `"$R/.go-api-dev/go-api.env"`) || !strings.Contains(text, "bigboy go-api-project-scope") {
			t.Errorf("%s does not read go-api's flag from $R/.go-api-dev/go-api.env through cigate", script)
		}
	}
}
