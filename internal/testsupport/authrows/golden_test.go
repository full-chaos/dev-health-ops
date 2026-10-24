package authrows

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

func repoFile(t *testing.T, parts ...string) string {
	t.Helper()
	_, file, _, _ := moduleroot.Caller(0)
	return filepath.Join(append([]string{filepath.Dir(file), "..", "..", ".."}, parts...)...)
}

// goldenRows are the frozen rows of the auth-flow venue golden, by comparison name.
func goldenRows(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(repoFile(t, "internal", "apiservice", "authflowvenue", "testdata", "golden", "auth-flow-venue-oracle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Rows map[string]struct {
			Rows string `json:"rows"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for name, rows := range golden.Rows {
		out[name] = rows.Rows
	}
	return out
}

func lines(rows string) []string { return strings.Split(rows, rowSeparator) }

// acpRows is what the ACP target is expected to hold for the golden's rows: the quirks' effect written out by
// hand, not by the quirk table.
func acpRows(t *testing.T, golden map[string]string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for name, rows := range golden {
		out[name] = rows
	}
	var users []string
	for _, line := range lines(golden[TableUsers]) {
		if strings.HasPrefix(line, "alice@example.com") || strings.HasPrefix(line, "pia@example.com") {
			if !strings.Contains(line, " 1 true true$2b$12$") {
				t.Fatalf("the golden's users row has changed: %q", line)
			}
			line = strings.Replace(line, " 1 true true$2b$12$", " 1 false true$2b$12$", 1)
		}
		users = append(users, line)
	}
	out[TableUsers] = strings.Join(users, rowSeparator)
	var audit []string
	failed := 0
	for _, line := range lines(golden[TableAuditLogs]) {
		if strings.Contains(line, "login_failedsession") {
			failed++
			line = regexp.MustCompile(`\{"email": "[^"]*"\}`).ReplaceAllString(line, "{}")
		}
		audit = append(audit, line)
	}
	if failed != 1 {
		t.Fatalf("the golden holds %d failed sign-in rows, want 1", failed)
	}
	audit = append(audit, `<nil>pia@example.comloginsessionuser:pia@example.comUser logged in{}{"ip_address": "203.0.113.7", "user_agent": "venue-oracle/1"}success<null>`)
	out[TableAuditLogs] = strings.Join(audit, rowSeparator)
	return out
}

func TestQuirksCoverTheGoldenAndTheExpectedTargetRowsExactly(t *testing.T) {
	golden := goldenRows(t)
	acp := acpRows(t, golden)
	l, err := NewLedger(forwarded(t, "sessions,local_login,social"))
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{TableUsers, TableRefreshTokens, TableAuditLogs} {
		if problems := l.Compare(table, golden[table], acp[table]); len(problems) != 0 {
			t.Errorf("%s:\n%s", table, strings.Join(problems, "\n"))
		}
	}
	if dead := l.Dead(); len(dead) != 0 {
		t.Errorf("dead quirks: %v", dead)
	}
	want := "audit-login-failed-attempted-email-not-recorded=1 audit-login-without-organization-recorded=1 users-last-login-not-stamped=2"
	if got := l.Covered(); got != want {
		t.Errorf("Covered = %q, want %q", got, want)
	}
}

func TestQuirksReportARowTheGoldenDoesNotHold(t *testing.T) {
	golden := goldenRows(t)
	acp := acpRows(t, golden)
	l, err := NewLedger(forwarded(t, "sessions,local_login,social"))
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(acp[TableRefreshTokens], "alice@example.comorg-bfalse 1", "alice@example.comorg-bfalse 2", 1)
	if changed == acp[TableRefreshTokens] {
		t.Fatal("the golden's refresh_tokens row has changed")
	}
	if problems := l.Compare(TableRefreshTokens, golden[TableRefreshTokens], changed); len(problems) != 2 {
		t.Fatalf("a changed count was not reported once on each side: %v", problems)
	}
	if problems := l.Compare(TableAuditLogs, golden[TableAuditLogs], strings.Replace(acp[TableAuditLogs], "User logged in", "Signed in", 1)); len(problems) < 2 {
		t.Fatalf("a changed description was classified: %v", problems)
	}
}

func TestQuirkRewritesHitARowOfTheFrozenGolden(t *testing.T) {
	golden := goldenRows(t)
	for _, q := range Quirks {
		if q.Rewrite == nil {
			continue
		}
		hits := 0
		for _, line := range lines(golden[q.Table]) {
			if _, ok := q.Rewrite(line); ok {
				hits++
			}
		}
		if hits == 0 {
			t.Errorf("%s: no row of the golden's %s rows is rewritten", q.ID, q.Table)
		}
	}
}

func TestEveryProjectedTableIsAGoldenComparison(t *testing.T) {
	golden := goldenRows(t)
	for table := range projected {
		if golden[table] == "" {
			t.Errorf("%s has a projection and no frozen golden rows", table)
		}
	}
}

func TestQuirkExtraRowsAreLimitedToTheirCount(t *testing.T) {
	golden := goldenRows(t)
	acp := acpRows(t, golden)
	l, err := NewLedger(forwarded(t, "sessions,local_login,social"))
	if err != nil {
		t.Fatal(err)
	}
	dup := `<nil>pia@example.comloginsessionuser:pia@example.comUser logged in{}{"ip_address": "203.0.113.7", "user_agent": "venue-oracle/1"}success<null>`
	if !strings.Contains(acp[TableAuditLogs], dup) {
		t.Fatal("the expected orgless sign-in row changed")
	}
	if problems := l.Compare(TableAuditLogs, golden[TableAuditLogs], acp[TableAuditLogs]+rowSeparator+dup); len(problems) == 0 {
		t.Fatal("a second orgless sign-in row was accepted")
	}
}

func TestScriptExportsTheForwardedSetTheTestReads(t *testing.T) {
	raw, err := os.ReadFile(repoFile(t, "ci", "auth_corpus_external.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	var names []string
	for _, a := range Aggregates {
		names = append(names, string(a))
	}
	if want := "AGGREGATES=(" + strings.Join(names, " ") + ")"; !strings.Contains(script, want) {
		t.Errorf("ci/auth_corpus_external.sh does not declare %q", want)
	}
	export := strings.Index(script, "export "+EnvForwarded+"\n")
	run := strings.Index(script, "go test -trimpath -tags integration")
	if export < 0 || run < 0 || export > run {
		t.Errorf("the script must export %s before the corpus runs (export at %d, go test at %d)", EnvForwarded, export, run)
	}
}

func TestScriptExportedValueIsTheForwardedSet(t *testing.T) {
	raw, err := os.ReadFile(repoFile(t, "ci", "auth_corpus_external.sh"))
	if err != nil {
		t.Fatal(err)
	}
	var assign string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, EnvForwarded+"=") {
			assign = line
		}
	}
	if assign == "" {
		t.Fatalf("the script does not assign %s", EnvForwarded)
	}
	for _, c := range []struct{ array, want string }{{"", ""}, {"sessions", "sessions"}, {"sessions local_login social", "sessions,local_login,social"}} {
		out, err := exec.Command("bash", "-c", "FORWARDED=("+c.array+"); "+assign+`; printf '%s' "$`+EnvForwarded+`"`).Output()
		if err != nil || string(out) != c.want {
			t.Errorf("FORWARDED=(%s) exports %q (%v), want %q", c.array, out, err, c.want)
		}
	}
}
