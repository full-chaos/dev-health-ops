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
	var linkMails []string
	for name, rows := range golden.Rows {
		out[name] = rows.Rows
		if !strings.HasPrefix(name, "python mails: ") {
			continue
		}
		var mails []struct{ Subject, Token string }
		if err := json.Unmarshal([]byte(rows.Rows), &mails); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, mail := range mails {
			if mail.Token != "" {
				linkMails = append(linkMails, LinkMailRow(mail.Subject, LinkTokenGoAPI))
			}
		}
	}
	out[TableLinkMails] = strings.Join(linkMails, rowSeparator)
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
	var refresh []string
	for _, line := range lines(golden[TableRefreshTokens]) {
		switch line {
		case "alice@example.comorg-bfalse 1":
			line = "alice@example.comorg-afalse 1"
		case "alice@example.comorg-btrue 1":
			line = "alice@example.comorg-atrue 1"
		}
		refresh = append(refresh, line)
	}
	out[TableRefreshTokens] = strings.Join(refresh, rowSeparator)
	var audit []string
	failed := 0
	for _, line := range lines(golden[TableAuditLogs]) {
		if strings.HasPrefix(line, "Org Balice@example.comloginsessionuser:") {
			line = "Org A" + strings.TrimPrefix(line, "Org B")
		}
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
	l, err := NewLedger(forwarded(t, "sessions,local_login"), false)
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
	want := "audit-login-failed-attempted-email-not-recorded=1 audit-login-without-organization-recorded=1 login-organization-chosen-without-activity=2 audit-login-organization-chosen-without-activity=2 users-last-login-not-stamped=2"
	if got := l.Covered(); got != want {
		t.Errorf("Covered = %q, want %q", got, want)
	}
}

func TestQuirksReportARowTheGoldenDoesNotHold(t *testing.T) {
	golden := goldenRows(t)
	acp := acpRows(t, golden)
	l, err := NewLedger(forwarded(t, "sessions,local_login"), false)
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(acp[TableRefreshTokens], "alice@example.comorg-afalse 1", "alice@example.comorg-afalse 2", 1)
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
				continue
			}
			// a quirk may act on the line another quirk's rewrite produced
			for _, other := range Quirks {
				if other.Table != q.Table || other.Rewrite == nil || other.ID == q.ID {
					continue
				}
				if once, ok := other.Rewrite(line); ok {
					if _, ok := q.Rewrite(once); ok {
						hits++
					}
				}
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
	l, err := NewLedger(forwarded(t, "sessions,local_login"), false)
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

// The organization-choice quirks cover alice's sign-in rows and nothing else: every other organization-B row, in
// either table, stays a difference.
func TestOrganizationChoiceQuirksRewriteOnlyAlicesSignInRows(t *testing.T) {
	golden := goldenRows(t)
	for id, want := range map[string]int{"login-organization-chosen-without-activity": 2, "audit-login-organization-chosen-without-activity": 2} {
		var quirk *Quirk
		for i := range Quirks {
			if Quirks[i].ID == id {
				quirk = &Quirks[i]
			}
		}
		if quirk == nil {
			t.Fatalf("no quirk %s", id)
		}
		hits := 0
		for _, line := range lines(golden[quirk.Table]) {
			if rewritten, ok := quirk.Rewrite(line); ok {
				hits++
				if !strings.Contains(line, "alice@example.com") || strings.Contains(rewritten, "Org B") || strings.Contains(rewritten, "org-b") {
					t.Errorf("%s rewrote %q to %q", id, line, rewritten)
				}
			}
		}
		if hits != want {
			t.Errorf("%s rewrites %d golden rows, want %d", id, hits, want)
		}
		for _, line := range []string{
			"ivan@example.comorg-bfalse 1", "alice@example.comorg-bfalse 2", "alice@example.comorg-dfalse 1",
			"Org Bivan@example.comloginsessionuser:ivan@example.comUser logged in{}{}success<null>",
			"Org Balice@example.comlogin_failedsessionuser:alice@example.comLogin failed: invalid credentials{}{}failureInvalid credentials",
			"Org Balice@example.comloginsessionuser:alice@example.comUser logged in{}{}success<null> | Org B",
		} {
			if rewritten, ok := quirk.Rewrite(line); ok {
				t.Errorf("%s rewrote %q to %q", id, line, rewritten)
			}
		}
	}
	acp := acpRows(t, golden)
	l, err := NewLedger(forwarded(t, "sessions,local_login"), false)
	if err != nil {
		t.Fatal(err)
	}
	// The revoked count is pinned: a second revoked credential is a difference.
	moreRevoked := strings.Replace(acp[TableRefreshTokens], "alice@example.comorg-atrue 1", "alice@example.comorg-atrue 2", 1)
	if moreRevoked == acp[TableRefreshTokens] {
		t.Fatal("the expected organization-A revoked row changed")
	}
	if problems := l.Compare(TableRefreshTokens, golden[TableRefreshTokens], moreRevoked); len(problems) != 2 {
		t.Errorf("a changed revoked count was classified: %v", problems)
	}
	l, err = NewLedger(forwarded(t, "sessions,local_login"), false)
	if err != nil {
		t.Fatal(err)
	}
	extra := acp[TableRefreshTokens] + rowSeparator + "ivan@example.comorg-bfalse 1"
	if problems := l.Compare(TableRefreshTokens, golden[TableRefreshTokens], extra); len(problems) != 1 {
		t.Errorf("a non-alice organization-B row was classified: %v", problems)
	}
}

// A chain of quirks covers one target row per golden row, and every quirk it used is counted: a row the golden holds
// twice and the target once leaves one difference, and so does a target row beyond the golden's. (A rewrite quirk is
// bounded by the golden's own rows, so it has no Max; an extra-row quirk keeps its Max, see
// TestQuirkExtraRowsAreLimitedToTheirCount.)
func TestChainedRewritesCoverOneTargetRowPerGoldenRow(t *testing.T) {
	golden := goldenRows(t)
	acp := acpRows(t, golden)
	run := func(goldenRefresh, acpRefresh string) (problems []string, covered string) {
		l, err := NewLedger(forwarded(t, "sessions,local_login"), false)
		if err != nil {
			t.Fatal(err)
		}
		return l.Compare(TableRefreshTokens, goldenRefresh, acpRefresh), l.Covered()
	}
	if problems, covered := run(golden[TableRefreshTokens], acp[TableRefreshTokens]); len(problems) != 0 ||
		!strings.Contains(covered, "login-organization-chosen-without-activity=2") {
		t.Fatalf("the expected rows were not covered by the chain: %v %q", problems, covered)
	}
	twice := golden[TableRefreshTokens] + rowSeparator + "alice@example.comorg-btrue 1"
	if problems, _ := run(twice, acp[TableRefreshTokens]); len(problems) != 1 || !strings.Contains(problems[0], "golden only") {
		t.Errorf("a golden row held twice and covered once left %v, want one golden-only difference", problems)
	}
	more := acp[TableRefreshTokens] + rowSeparator + "alice@example.comorg-atrue 2"
	if problems, _ := run(golden[TableRefreshTokens], more); len(problems) != 1 || !strings.Contains(problems[0], "target only") {
		t.Errorf("a target row beyond the golden's left %v, want one target-only difference", problems)
	}
}

func TestOwnedLegExpectsTheGoldensUsersRowsWithNoQuirk(t *testing.T) {
	golden := goldenRows(t)
	l, err := NewLedger(forwarded(t, "sessions,local_login"), true)
	if err != nil {
		t.Fatal(err)
	}
	if problems := l.Compare(TableUsers, golden[TableUsers], golden[TableUsers]); len(problems) != 0 {
		t.Fatalf("users rows equal to the golden's: %v", problems)
	}
	if got := l.Covered(); strings.Contains(got, "users-last-login-not-stamped") {
		t.Fatalf("the last-login quirk is live on the owned leg: %q", got)
	}
	notStamped := acpRows(t, golden)[TableUsers]
	if problems := l.Compare(TableUsers, golden[TableUsers], notStamped); len(problems) == 0 {
		t.Fatal("never-stamped users rows passed on the owned leg")
	}
}
