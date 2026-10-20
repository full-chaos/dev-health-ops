package bigboyrender_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/bigboyrender"
)

// The cases below are the in-process form of the guards the Python helpers had in tests/tooling (deleted with the
// helpers): the renderer's own tests, the render of the real manifest against its golden, the posture delta tests and
// the grants comparison. Each guard has a planted-defect case that must make it fire with a message naming the defect.
// The cases that needed the bash stub harness (live user, login, credential on stdin, scratch dir of
// check-dho-api-ch-user.sh) are not here.

const syntheticManifest = `package clickhouse

// func APIPosture is quoted here in comments: password="leaked" clickhouse://u:p@h/db
func APIPosture(database string) Posture {
	return Posture{RequiredTables: []TableGrant{
		{Database: database, Table: "teams", AllowInsert: true, AllowSelect: true, AllowDelete: true},
		// marker-comment: <password>plain</password> Table: "hidden"
		{Database: database, Table: "team_sync_policies", AllowSelect: true},
		{Database: database, Table: "team_memberships", AllowInsert: true},
		{Database: database, Table: "no_privileges"},
	}}
}

func other() {
	_ = TableGrant{Database: database, Table: "outside", AllowSelect: true}
}
`

func realManifest(t *testing.T) string {
	t.Helper()
	return readFile(t, filepath.Join(repoRootDir(t), "internal", "storage", "clickhouse", "authorization.go"))
}

func realGolden(t *testing.T) []string {
	t.Helper()
	text := readFile(t, filepath.Join(repoRootDir(t), "internal", "storage", "clickhouse", "testdata", "api_grants.golden"))
	golden := strings.Split(strings.TrimSpace(text), "\n")
	if len(golden) == 0 || golden[0] == "" {
		t.Fatal("the golden is empty: this test would prove nothing")
	}
	return golden
}

// renderTo runs render-ch-users on manifest text and returns the exit code, the output file ("" when none) and stderr.
func renderTo(t *testing.T, manifest, password string) (code int, out, stderr string, dir string) {
	t.Helper()
	dir = t.TempDir()
	t.Setenv("API_CH_PASSWORD", password)
	writeFile(t, filepath.Join(dir, "authorization.go"), manifest)
	code, _, stderr = run("render-ch-users", filepath.Join(dir, "authorization.go"), filepath.Join(dir, "out.xml"))
	if raw, err := os.ReadFile(filepath.Join(dir, "out.xml")); err == nil {
		out = string(raw)
	}
	return code, out, stderr, dir
}

var queryText = regexp.MustCompile(`<query>([^<]*)</query>`)

func queries(xml string) []string {
	var out []string
	for _, m := range queryText.FindAllStringSubmatch(xml, -1) {
		out = append(out, m[1])
	}
	return out
}

// goldenDiff is the guard: the rendered grants must equal the golden the Go manifest test is pinned to, in order.
func goldenDiff(got, golden []string) string {
	for i := 0; i < len(golden) || i < len(got); i++ {
		switch {
		case i >= len(got):
			return "the render lacks " + golden[i]
		case i >= len(golden):
			return "the render has an extra " + got[i]
		case got[i] != golden[i]:
			return "the render has " + got[i] + " where the golden has " + golden[i]
		}
	}
	return ""
}

func TestRenderOutputIsExactlyTheFixedTable(t *testing.T) {
	password := "the-api-password"
	code, out, stderr, dir := renderTo(t, syntheticManifest, password)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	sum := sha256.Sum256([]byte(password))
	digest := hex.EncodeToString(sum[:])
	want := "<clickhouse><users><dho_api_ch><networks><ip>::/0</ip></networks>" +
		"<profile>default</profile><quota>default</quota>" +
		"<access_management>0</access_management>" +
		"<password_sha256_hex>" + digest + "</password_sha256_hex><grants>" +
		"<query>GRANT SELECT, INSERT, ALTER DELETE ON default.teams</query>" +
		"<query>GRANT SELECT ON default.team_sync_policies</query>" +
		"<query>GRANT INSERT ON default.team_memberships</query>" +
		"</grants></dho_api_ch></users></clickhouse>\n"
	if out != want {
		t.Errorf("render differs\ngot:  %q\nwant: %q", out, want)
	}
	info, err := os.Stat(filepath.Join(dir, "out.xml"))
	if err != nil || info.Mode().Perm() != 0o644 {
		t.Errorf("out.xml mode %v (err %v), want 0644", info.Mode().Perm(), err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 2 {
		t.Errorf("the directory holds %d entries, want the manifest and the output only", len(entries))
	}
}

func TestNoTextOfTheManifestOutsideItsRowsReachesTheOutput(t *testing.T) {
	_, out, _, _ := renderTo(t, syntheticManifest, "pw-1")
	for _, banned := range []string{"leaked", "clickhouse://", "marker", "plain", "hidden", "outside", "<!--", "<?", "="} {
		if strings.Contains(out, banned) {
			t.Errorf("the output holds %q", banned)
		}
	}
	// planted: a defect that lets a table name carry markup is not reachable, a table name is \w+ only
	planted := strings.Replace(syntheticManifest, `Table: "team_memberships"`, `Table: "x</query><query>GRANT ALL ON *.*"`, 1)
	_, out, _, _ = renderTo(t, planted, "pw-1")
	if strings.Contains(out, "GRANT ALL") {
		t.Errorf("a planted table name with markup reached the output: %s", out)
	}
}

func TestRealManifestRendersOneGrantPerTableAndEqualsTheGolden(t *testing.T) {
	source := realManifest(t)
	body := strings.SplitN(strings.SplitN(source, "func APIPosture(", 2)[1], "\n}\n", 2)[0]
	rows := 0
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, "{Database: database, Table:") && (strings.Contains(line, "AllowSelect: true") || strings.Contains(line, "AllowInsert: true") || strings.Contains(line, "AllowDelete: true")) {
			rows++
		}
	}
	if rows == 0 {
		t.Fatal("the manifest could not be read: this test would prove nothing")
	}
	code, out, stderr, _ := renderTo(t, source, "Zq7-throwaway-credential")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if got := strings.Count(out, "<query>"); got != rows {
		t.Errorf("%d grants for %d rows with a privilege", got, rows)
	}
	if !strings.Contains(out, "<query>GRANT SELECT, INSERT, ALTER DELETE ON default.teams</query>") {
		t.Error("the teams grant is missing")
	}
	if diff := goldenDiff(queries(out), realGolden(t)); diff != "" {
		t.Errorf("the render of the real manifest differs from the golden: %s", diff)
	}
	if strings.Contains(out, "Zq7-throwaway-credential") {
		t.Error("the password is in the output")
	}
}

func TestPlantedDefectInTheManifestMakesTheGoldenGuardFire(t *testing.T) {
	source := realManifest(t)
	golden := realGolden(t)
	var table string
	for _, line := range golden {
		if m := regexp.MustCompile(`ON default\.(\w+)$`).FindStringSubmatch(line); m != nil && strings.Contains(source, `Table: "`+m[1]+`",`) {
			table = m[1]
			break
		}
	}
	if table == "" {
		t.Fatal("no golden table found in the manifest")
	}
	// plant: a manifest row grants nothing any more (the privileges are dropped from the table's row)
	rowPattern := regexp.MustCompile(`(?m)^.*\{Database: database, Table: "` + table + `",[^}]*\}.*\n`)
	planted := rowPattern.ReplaceAllString(source, "")
	if planted == source {
		t.Fatal("the defect was not planted")
	}
	code, out, stderr, _ := renderTo(t, planted, "pw")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	diff := goldenDiff(queries(out), golden)
	if diff == "" || !strings.Contains(diff, table) && !strings.Contains(diff, "default.") {
		t.Fatalf("the guard did not fire on the dropped %s row: %q", table, diff)
	}
	t.Logf("guard message: %s", diff)
}

func TestRenderRefusesWhenThePasswordWouldAppearInTheOutput(t *testing.T) {
	// "default" is also the profile name: the file would then hold the credential.
	code, out, stderr, dir := renderTo(t, syntheticManifest, "default")
	if code == 0 || out != "" || !strings.Contains(stderr, "nothing written") || !strings.Contains(stderr, "password would appear") {
		t.Fatalf("exit %d, out %q, stderr %q", code, out, stderr)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("files left behind: %v", entries)
	}
}

func TestRenderRefusesMissingInputsAndAnUnreadableManifest(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "m.go"), syntheticManifest)
	out := filepath.Join(dir, "out.xml")
	t.Setenv("API_CH_PASSWORD", "")
	if code, _, stderr := run("render-ch-users", filepath.Join(dir, "m.go"), out); code != 2 || !strings.Contains(stderr, "API_CH_PASSWORD is not set") {
		t.Errorf("no password: exit %d, %q", code, stderr)
	}
	t.Setenv("API_CH_PASSWORD", "x")
	if code, _, stderr := run("render-ch-users", filepath.Join(dir, "absent.go"), out); code != 2 || !strings.Contains(stderr, "is not a file") {
		t.Errorf("absent manifest: exit %d, %q", code, stderr)
	}
	if code, _, stderr := run("render-ch-users", filepath.Join(dir, "m.go")); code != 2 || !strings.Contains(stderr, "usage: cigate bigboy render-ch-users") {
		t.Errorf("usage: exit %d, %q", code, stderr)
	}
	for name, text := range map[string]string{"no posture": "package clickhouse\n", "no rows": "func APIPosture(d string) {\n}\n"} {
		code, got, stderr, _ := renderTo(t, text, "x")
		if code == 0 || got != "" || !strings.Contains(stderr, "nothing written") {
			t.Errorf("%s: exit %d, out %q, stderr %q", name, code, got, stderr)
		}
	}
}

func TestRenderLibraryRefusesAnEmptyGrantList(t *testing.T) {
	if _, err := bigboyrender.RenderUsersXML(nil, "pw"); err == nil || !strings.Contains(err.Error(), "no grants") {
		t.Errorf("err %v", err)
	}
	if got := bigboyrender.GrantsFromManifest(syntheticManifest); len(got) != 3 || got[0] != "GRANT SELECT, INSERT, ALTER DELETE ON default.teams" {
		t.Errorf("grants %v", got)
	}
}

// ---- grants-compare ----

func realExpectation(t *testing.T) []string {
	t.Helper()
	var rows []string
	for _, line := range realGolden(t) {
		m := regexp.MustCompile(`^GRANT (.+) ON default\.(\w+)$`).FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("unexpected golden line %q", line)
		}
		for _, p := range strings.Split(m[1], ", ") {
			rows = append(rows, p+"\t"+m[2])
		}
	}
	return rows
}

func TestGrantsCompareMatchesTheRealManifestAndFiresOnAPlantedDefect(t *testing.T) {
	source := realManifest(t)
	rows := realExpectation(t)
	actual := strings.Join(rows, "\n") + "\n"
	code, out, _ := run("grants-compare", actual, source)
	if code != 0 || !strings.HasSuffix(out, "GRANTS_CHECK_OK\n") || !strings.Contains(out, "missing=[] extra=[]") {
		t.Fatalf("exit %d: %s", code, out)
	}
	// plant 1: a grant the manifest wants is missing from the live server
	dropped := rows[0]
	code, out, _ = run("grants-compare", strings.Join(rows[1:], "\n")+"\n", source)
	privilege, table, _ := strings.Cut(dropped, "\t")
	if code != 1 || !strings.Contains(out, "missing=[('"+privilege+"', '"+table+"')]") || !strings.HasSuffix(out, "GRANTS_CHECK_FAIL\n") {
		t.Errorf("missing grant: exit %d: %s", code, out)
	}
	// plant 2: the live server holds a grant the manifest does not
	code, out, _ = run("grants-compare", actual+"ALTER DELETE\tusers_planted\n", source)
	if code != 1 || !strings.Contains(out, "extra=[('ALTER DELETE', 'users_planted')]") {
		t.Errorf("extra grant: exit %d: %s", code, out)
	}
}

func TestGrantsCompareFailsLoudlyWhenTheManifestCannotBeParsed(t *testing.T) {
	for name, source := range map[string]string{"no rows": "func APIPosture(d string) {\n}\n", "no function": "package clickhouse\n"} {
		code, out, _ := run("grants-compare", "SELECT\tteams\n", source)
		if code != 1 || out != "GRANTS_CHECK_FAIL manifest not parsed\n" {
			t.Errorf("%s: exit %d: %q", name, code, out)
		}
	}
	if code, _, stderr := run("grants-compare", "only one"); code != 2 || !strings.Contains(stderr, "usage:") {
		t.Errorf("usage: exit %d, %q", code, stderr)
	}
}

// ---- pg-posture ----

const postureV1 = "package postgres\n\nfunc apiPosture() []RequiredTable {\n\treturn []RequiredTable{\n\t\t{\"alpha\", true, false, false},\n\t}\n}\n"

var postureV2 = strings.Replace(postureV1, `{"alpha", true, false, false},`, "{\"alpha\", true, false, false},\n\t\t{\"beta\", true, true, false},", 1)

const apiOld = `package postgres

func apiPosture() RolePosture {
	return RolePosture{
		RequiredTables: []TablePrivilege{
			{"users", true, true, true},
			{"billing_audit_log", true, false, true},
		},
	}
}
`

const apiNew = `package postgres

func apiPosture() RolePosture {
	return RolePosture{
		RequiredTables: []TablePrivilege{
			{"users", true, true, true},
			{"billing_audit_log", true, false, true},
			{"settings", false, true, false},
		},
		ColumnScoped: []ColumnPrivilege{
			{"billing_audit_log", "actor_id", "UPDATE"},
		},
	}
}
`

const domainOld = `package postgres

func domainPosture() RolePosture {
	return RolePosture{
		RequiredTables: []TablePrivilege{
			{"sync_runs", true, true, false},
		},
		ColumnScoped: []ColumnPrivilege{
			{"integration_credentials", "is_active", "UPDATE"},
		},
	}
}
`

const domainNew = `package postgres

func domainPosture() RolePosture {
	return RolePosture{
		RequiredTables: []TablePrivilege{
			{"sync_runs", true, true, false},
		},
		ColumnScoped: []ColumnPrivilege{
			{"integration_credentials", "is_active", "UPDATE"},
			{"integration_credentials", "updated_at", "UPDATE"},
			{"worker_job_completion_fences", "completion_key", "SELECT"},
			{"worker_job_completion_fences", "completion_key", "INSERT"},
		},
	}
}
`

// posture runs `pg-posture <role> old new out` on a repository holding the two texts and returns the exit code, stdout
// and the lines the verb appended to the out file.
func posture(t *testing.T, role, gofile, old, new string) (int, string, []string) {
	t.Helper()
	repo, shas := gitRepo(t, "internal/storage/postgres/"+gofile, old, new)
	t.Setenv("BIGBOY_OPS_REPO", repo)
	out := filepath.Join(t.TempDir(), "pg-expect.txt")
	code, stdout, _ := run("pg-posture", role, shas[0], shas[1], out)
	var lines []string
	if raw, err := os.ReadFile(out); err == nil && len(raw) > 0 {
		lines = strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	}
	return code, stdout, lines
}

func contains(lines []string, want string) bool {
	for _, l := range lines {
		if l == want {
			return true
		}
	}
	return false
}

func TestAPIPostureDeltaExpectsAddedColumnPrivileges(t *testing.T) {
	code, stdout, lines := posture(t, "api", "api_authorization.go", apiOld, apiNew)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stdout)
	}
	if !contains(lines, "devhealth_api settings SELECT,UPDATE") || !contains(lines, "devhealth_api billing_audit_log.actor_id UPDATE") {
		t.Errorf("lines %v", lines)
	}
	for _, l := range lines {
		if strings.HasPrefix(l, "devhealth_api users") {
			t.Errorf("an unchanged table has a line: %s", l)
		}
	}
	if !strings.Contains(stdout, "billing_audit_log.actor_id UPDATE") {
		t.Errorf("stdout %q", stdout)
	}
}

func TestAPIPostureDeltaExpectsNoLineForAnUnchangedColumn(t *testing.T) {
	added := strings.Replace(apiNew, "{\"settings\", false, true, false},\n", "{\"settings\", false, true, false},\n\t\t\t{\"tier_limits\", false, false, false},\n", 1)
	code, _, lines := posture(t, "api", "api_authorization.go", apiNew, added)
	if code != 0 || len(lines) != 1 || lines[0] != "devhealth_api tier_limits SELECT" {
		t.Errorf("exit %d, lines %v", code, lines)
	}
}

func TestDomainPostureDeltaExpectsAddedColumnPrivileges(t *testing.T) {
	code, _, lines := posture(t, "domain", "domain_authorization.go", domainOld, domainNew)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	if !contains(lines, "devhealth_domain integration_credentials.updated_at UPDATE") || !contains(lines, "devhealth_domain worker_job_completion_fences.completion_key SELECT,INSERT") {
		t.Errorf("lines %v", lines)
	}
	for _, l := range lines {
		if strings.Contains(l, "integration_credentials.is_active") {
			t.Errorf("an unchanged column has a line: %s", l)
		}
	}
}

func TestPostureDeltaReadsACompactButValidRow(t *testing.T) {
	code, stdout, _ := posture(t, "api", "api_authorization.go", postureV2, strings.ReplaceAll(postureV2, ", ", ","))
	if code != 0 || strings.Contains(stdout, "unparsable") {
		t.Errorf("exit %d: %s", code, stdout)
	}
}

// A manifest the parser does not recognise fails the step (non-zero, "unparsable"); it never reads as "no privileges".
func TestPostureDeltaFailsOnAManifestTheParserDoesNotRecognise(t *testing.T) {
	broken := map[string]string{
		"string flag":      strings.Replace(postureV2, `{"beta", true, true, false}`, `{"beta", "yes", true, false}`, 1),
		"three fields":     strings.Replace(postureV2, `{"beta", true, true, false}`, `{"beta", true, true}`, 1),
		"no rows":          strings.Replace(postureV1, `{"alpha", true, false, false},`, "", 1),
		"sequences ident":  strings.Replace(postureV2, "return []RequiredTable{\n", "return Posture{\n\t\tRequiredSequences: seqs,\n\t\tRequiredTables: []RequiredTable{\n", 1),
		"sequences call":   strings.Replace(postureV2, "return []RequiredTable{\n", "return Posture{\n\t\tRequiredSequences: seqList(),\n\t\tRequiredTables: []RequiredTable{\n", 1),
		"sequences list":   strings.Replace(postureV2, "return []RequiredTable{\n", "return Posture{\n\t\tRequiredSequences: []string{\"new_seq\"},\n\t\tRequiredTables: []RequiredTable{\n", 1),
		"keyed row":        strings.Replace(postureV2, `{"beta", true, true, false}`, `{TableName: "beta", AllowInsert: true}`, 1),
		"mixed row":        strings.Replace(postureV2, `{"beta", true, true, false}`, `{Table: "beta", Insert: true}, {"gamma", "x", "UPDATE"}`, 1),
		"function missing": "package postgres\n\nfunc somethingElse() {}\n",
	}
	for name, text := range broken {
		code, stdout, lines := posture(t, "api", "api_authorization.go", postureV1, text)
		if code == 0 || !strings.Contains(stdout, "unparsable") || len(lines) != 0 {
			t.Errorf("%s: exit %d, stdout %q, lines %v", name, code, stdout, lines)
		}
	}
}

func TestPostureDeltaFailsWithoutAnOpsCloneOrAtAMissingSha(t *testing.T) {
	out := filepath.Join(t.TempDir(), "o.txt")
	t.Setenv("BIGBOY_OPS_REPO", "")
	if code, stdout, _ := run("pg-posture", "api", "HEAD", "HEAD", out); code != 1 || !strings.Contains(stdout, "unparsable") {
		t.Errorf("no clone: exit %d: %q", code, stdout)
	}
	repo, shas := gitRepo(t, "internal/storage/postgres/api_authorization.go", postureV2)
	t.Setenv("BIGBOY_OPS_REPO", repo)
	if code, stdout, _ := run("pg-posture", "api", strings.Repeat("0", 40), shas[0], out); code != 1 || !strings.Contains(stdout, "unparsable") {
		t.Errorf("missing sha: exit %d: %q", code, stdout)
	}
	if code, stdout, _ := run("pg-posture", "domain", shas[0], shas[0], out); code != 1 || stdout != "PG_DOMAIN_POSTURE_DELTA unparsable\n" {
		t.Errorf("domain file absent: exit %d: %q", code, stdout)
	}
	if _, err := os.Stat(out); err == nil {
		t.Error("an unparsable run wrote the out file")
	}
}

func TestTheRealPostureManifestsStillParseAndAPlantedSequenceListIsCaught(t *testing.T) {
	for _, c := range []struct{ role, file, fn string }{{"api", "api_authorization.go", "apiPosture"}, {"domain", "domain_authorization.go", "domainPosture"}} {
		real := readFile(t, filepath.Join(repoRootDir(t), "internal", "storage", "postgres", c.file))
		code, stdout, _ := posture(t, c.role, c.file, real, real)
		if code != 0 || strings.Contains(stdout, "unparsable") {
			t.Errorf("%s: the shipped manifest does not parse: exit %d: %s", c.role, code, stdout)
		}
		// planted: a grant list the read-back cannot see (RequiredSequences) must fail, not vanish from the delta
		needle := "func " + c.fn + "()"
		start := strings.Index(real, needle)
		at := strings.Index(real[start:], "RequiredTables:")
		if start < 0 || at < 0 {
			t.Fatalf("%s: RequiredTables not found in the shipped manifest", c.role)
		}
		pos := start + at
		planted := real[:pos] + "RequiredSequences: []string{\"planted_seq\"},\n\t\t" + real[pos:]
		code, stdout, _ = posture(t, c.role, c.file, real, planted)
		if code == 0 || !strings.Contains(stdout, "unparsable") {
			t.Errorf("%s: a planted RequiredSequences list was not refused: exit %d: %s", c.role, code, stdout)
		}
	}
}

// ---- bump-swap-values ----

const (
	oldSHA = "0123456789abcdef0123456789abcdef01234567"
	newSHA = "fedcba9876543210fedcba9876543210fedcba98"
)

var (
	oldOp  = "sha256:" + strings.Repeat("a1", 32)
	newOp  = "sha256:" + strings.Repeat("b2", 32)
	oldDho = "sha256:" + strings.Repeat("c3", 32)
	newDho = "sha256:" + strings.Repeat("d4", 32)
	newArm = "sha256:" + strings.Repeat("f6", 32)
)

func bumpFiles(t *testing.T, prod, local string) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "values.prod.yaml"), prod)
	writeFile(t, filepath.Join(dir, "values.local.yaml"), local)
	t.Chdir(dir)
	return dir
}

func bumpArgs() []string {
	return []string{"bump-swap-values", oldSHA, newSHA, oldSHA[:7], newSHA[:7], oldOp, newOp, oldDho, newDho, newArm}
}

func prodValues() string {
	return "image:\n  dho: ghcr.io/full-chaos/dev-health-go-dho:sha-" + oldSHA[:7] + "@" + oldDho + "\n  operator: x@" + oldOp + "\n  tag: " + oldSHA +
		"\nqueryApi:\n  # The arm64 child (prod's own arch) is\n  # sha256:" + strings.Repeat("e5", 32) + "\n"
}

func localValues() string { return "tag: sha-" + oldSHA[:12] + "\ndho: " + oldDho + "\n" }

func TestBumpSwapValuesRewritesBothFiles(t *testing.T) {
	dir := bumpFiles(t, prodValues(), localValues())
	if code, out, stderr := run(bumpArgs()...); code != 0 || out != "" || stderr != "" {
		t.Fatalf("exit %d: %q %q", code, out, stderr)
	}
	prod := readFile(t, filepath.Join(dir, "values.prod.yaml"))
	for _, gone := range []string{oldSHA, oldOp, oldDho, strings.Repeat("e5", 32), "sha-" + oldSHA[:7]} {
		if strings.Contains(prod, gone) {
			t.Errorf("values.prod.yaml still holds %s", gone)
		}
	}
	for _, kept := range []string{newSHA, newOp, newDho, strings.Repeat("f6", 32), "sha-" + newSHA[:7]} {
		if !strings.Contains(prod, kept) {
			t.Errorf("values.prod.yaml lacks %s", kept)
		}
	}
	local := readFile(t, filepath.Join(dir, "values.local.yaml"))
	if local != "tag: sha-"+newSHA[:12]+"\ndho: "+newDho+"\n" {
		t.Errorf("values.local.yaml: %q", local)
	}
}

// Planted defects: each refusal names what was missing, and a refusal before the write leaves the file as it was.
func TestBumpSwapValuesRefusesAPlantedDefect(t *testing.T) {
	cases := []struct {
		name          string
		prod, local   string
		wantErr       string
		prodRewritten bool
	}{
		{"old sha absent from prod", strings.ReplaceAll(prodValues(), oldSHA, strings.Repeat("1", 40)), localValues(), "AssertionError: " + oldSHA, false},
		{"operator digest absent from prod", strings.ReplaceAll(prodValues(), oldOp, "sha256:"+strings.Repeat("9", 64)), localValues(), "AssertionError: " + oldOp, false},
		{"no arm64 comment", strings.ReplaceAll(prodValues(), "# sha256:", "# digest:"), localValues(), "arm64 child comment not found", false},
		{"two arm64 comments", prodValues() + "# The arm64 child (prod's own arch) is\n  # sha256:" + strings.Repeat("11", 64/2) + "\n", localValues(), "arm64 child comment not found", false},
		{"local lacks the old dho digest", prodValues(), "tag: sha-" + oldSHA[:12] + "\n", "AssertionError", true},
		{"local lacks the old sha", prodValues(), "dho: " + oldDho + "\n", "AssertionError", true},
	}
	for _, c := range cases {
		dir := bumpFiles(t, c.prod, c.local)
		code, _, stderr := run(bumpArgs()...)
		if code != 1 || !strings.Contains(stderr, c.wantErr) {
			t.Errorf("%s: exit %d, stderr %q", c.name, code, stderr)
		}
		if got := readFile(t, filepath.Join(dir, "values.prod.yaml")); (got != c.prod) != c.prodRewritten {
			t.Errorf("%s: values.prod.yaml rewritten=%v, want %v", c.name, got != c.prod, c.prodRewritten)
		}
		if got := readFile(t, filepath.Join(dir, "values.local.yaml")); got != c.local {
			t.Errorf("%s: values.local.yaml was rewritten", c.name)
		}
	}
	bumpFiles(t, prodValues(), localValues())
	if code, _, stderr := run("bump-swap-values", "only", "two"); code != 2 || !strings.Contains(stderr, "usage:") {
		t.Errorf("usage: exit %d, %q", code, stderr)
	}
}

func TestRunRefusesAnUnknownVerb(t *testing.T) {
	if code, _, stderr := run(); code != 2 || !strings.Contains(stderr, "usage:") {
		t.Errorf("no verb: exit %d %q", code, stderr)
	}
	if code, _, stderr := run("nope"); code != 2 || !strings.Contains(stderr, `unknown verb "nope"`) {
		t.Errorf("unknown: exit %d %q", code, stderr)
	}
	if code, _, stderr := run("pg-posture", "other", "a", "b", "c"); code != 2 || !strings.Contains(stderr, "unknown role") {
		t.Errorf("role: exit %d %q", code, stderr)
	}
}
