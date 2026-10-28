package authrows

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestQueryCredentialsProjectsTheLinkTablesFromLinkCredentials(t *testing.T) {
	f := forwarded(t, "sessions,local_login,credentials")
	for table, purpose := range map[string]string{TableEmailVerificationTokens: "email_verification", TablePasswordResetTokens: "password_reset"} {
		query, projected, err := Query(table, "PUBLIC", f)
		if err != nil || !projected {
			t.Fatalf("%s: projected %v err %v", table, projected, err)
		}
		for _, want := range []string{"auth.link_credentials", "lc.purpose = '" + purpose + "'", "lc.consumed_at IS NULL", "octet_length(lc.token_hash) * 2", "FROM " + table + " t"} {
			if !strings.Contains(query, want) {
				t.Errorf("%s: the projection lacks %q:\n%s", table, want, query)
			}
		}
	}
}

func TestQueryCredentialsLeavesUsersAndRefreshOnTheirExistingProjections(t *testing.T) {
	f := forwarded(t, "sessions,local_login,credentials")
	if got, projected, err := Query(TableRefreshTokens, publicRefresh, f); err != nil || !projected || got != refreshTokensACP {
		t.Fatalf("refresh_tokens: projected %v err %v", projected, err)
	}
	if got, projected, err := Query(TableUsers, publicUsers, f); err != nil || !projected || !strings.Contains(got, "auth.users al") {
		t.Fatalf("users: projected %v err %v", projected, err)
	}
}

func TestQueryCredentialsAuditArmOnlyWhenCredentialsIsForwarded(t *testing.T) {
	for _, text := range []string{"sessions", "sessions,local_login", "sessions,local_login,social"} {
		got, projected, err := Query(TableAuditLogs, publicAudit, forwarded(t, text))
		if err != nil || !projected || got != auditLogsACP {
			t.Errorf("forwarded %s: the audit query is not byte-identical to the sessions projection (err %v)", text, err)
		}
	}
	got, _, err := Query(TableAuditLogs, publicAudit, forwarded(t, "sessions,local_login,credentials"))
	if err != nil {
		t.Fatal(err)
	}
	if got == auditLogsACP || !strings.Contains(got, "auth.principal.provisioned") {
		t.Errorf("the credentials audit query holds no credentials arm:\n%s", got)
	}
}

func TestQueryOIDCAndSAMLStayRefusedByNameWithCredentials(t *testing.T) {
	for _, a := range []Aggregate{OIDC, SAML} {
		f := forwarded(t, "sessions,credentials,"+string(a))
		for _, table := range []string{TableUsers, TableRefreshTokens, TableAuditLogs} {
			_, _, err := Query(table, "PUBLIC", f)
			if err == nil || !strings.Contains(err.Error(), "aggregate "+string(a)) {
				t.Errorf("%s with %s forwarded: err %v, want a refusal naming %s", table, a, err, a)
			}
		}
	}
}

func TestCredentialAuditEventMapping(t *testing.T) {
	want := map[string][3]string{
		"auth.principal.provisioned":    {"create", "user", "User registered"},
		"auth.email.verified":           {"email_verified", "user", "Email verified"},
		"auth.user.email_verified":      {"email_verified", "user", "Email verified"},
		"auth.password.reset":           {"password_reset", "user", "User reset password"},
		"auth.user.password_changed":    {"password_changed", "user", "User changed own password"},
		"auth.password.reset_requested": {"password_reset_requested", "user", "Password reset requested"},
	}
	got := CredentialAuditEvents()
	if len(got) != len(want) {
		t.Fatalf("the mapping holds %d events, want %d: %v", len(got), len(want), got)
	}
	for eventType, w := range want {
		m, ok := got[eventType]
		if !ok || m.Action != w[0] || m.ResourceType != w[1] || m.Description != w[2] {
			t.Errorf("%s: got %+v ok %v, want %v", eventType, m, ok, w)
		}
	}
}

func TestCredentialAuditArmNamesAnUnmappedEventByErroring(t *testing.T) {
	got, _, err := Query(TableAuditLogs, publicAudit, forwarded(t, "credentials"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range range3(`e.event_type ~ '^auth\.(user|password|email)\.'`, `ELSE e.event_type::int::text`, `'auth.principal.projected'`) {
		if !strings.Contains(got, want) {
			t.Errorf("the credentials arm lacks %q:\n%s", want, got)
		}
	}
}

func range3(s ...string) []string { return s }

// go-api deletes every link token of a user once it redeems one (verify.go, reset); it never marks a row
// consumed, so the auth arm's consumed_at IS NULL filter matches the golden's rows.
func TestGoAPIRedeemDeletesLinkTokensAndNeverMarksThemConsumed(t *testing.T) {
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join("..", "..", "api", "session", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	verify, tokens := read("verify.go"), read("linktokens.go")
	if strings.Contains(tokens, "consumed") || strings.Contains(verify, "consumed_at") {
		t.Fatal("go-api marks a redeemed link token consumed; the auth arm must keep consumed rows")
	}
	for _, call := range []string{"deleteLinkTokens(ctx, tx, verificationTokens, user.ID)", "deleteLinkTokens(ctx, tx, resetTokens, user.ID)"} {
		if strings.Count(verify, call) != 1 {
			t.Errorf("verify.go: %q appears %d times, want once", call, strings.Count(verify, call))
		}
	}
	if !regexp.MustCompile(`deleteByUser: +` + "`" + `DELETE FROM email_verification_tokens WHERE user_id`).MatchString(tokens) {
		t.Error("linktokens.go: the replace-all delete is gone")
	}
}

func TestCredentialQuirksAreWellFormedAndNeedCredentials(t *testing.T) {
	ids := map[string]bool{}
	for _, q := range Quirks {
		ids[q.ID] = true
	}
	for _, id := range []string{
		"audit-register-changes-not-recorded",
		"audit-register-without-organization-recorded",
		"audit-email-verified-recorded",
		"audit-password-reset-recorded-without-membership",
		"audit-password-reset-requested-without-membership",
		"audit-password-reset-requested-unknown-address",
	} {
		if !ids[id] {
			t.Errorf("no quirk %s", id)
		}
	}
	for _, id := range []string{
		"audit-register-organization-and-changes-not-recorded",
		"audit-password-reset-organization-not-recorded",
		"audit-password-reset-requested-not-recorded",
	} {
		if ids[id] {
			t.Errorf("quirk %s is closed and must be gone", id)
		}
	}
	for _, q := range Quirks {
		if strings.HasPrefix(q.ID, "audit-register") || strings.HasPrefix(q.ID, "audit-email-verified") || strings.HasPrefix(q.ID, "audit-password") {
			if len(q.Needs) != 1 || q.Needs[0] != Credentials {
				t.Errorf("%s: needs %v, want credentials only", q.ID, q.Needs)
			}
		}
	}
}

// the golden's audit lines of the scenario's credential steps, and the lines auth-service's events project to
const (
	nl       = `{"ip_address": "203.0.113.7", "user_agent": "venue-oracle/1"}`
	goldenN  = `Nina's Team  Ünïnina.new@example.comcreateuseruser:nina.new@example.comUser registered{"email": "nina.new@example.com", "organization_id": "<uuid>"}` + nl + `success<null>`
	acpN     = `Nina's Team  Ünïnina.new@example.comcreateuseruser:nina.new@example.comUser registered{}` + nl + `success<null>`
	acpSolo  = `<nil>solo@example.comcreateuseruser:solo@example.comUser registered{}` + nl + `success<null>`
	goldenR  = `Org Aalice@example.compassword_resetuseruser:alice@example.comUser reset password{}` + nl + `success<null>`
	acpPiaR  = `<nil>pia@example.compassword_resetuseruser:pia@example.comUser reset password{}` + nl + `success<null>`
	goldenRQ = `Org Aalice@example.compassword_reset_requesteduseruser:alice@example.comPassword reset requested{}` + nl + `success<null>`
	acpPiaRQ = `<nil>pia@example.compassword_reset_requesteduseruser:pia@example.comPassword reset requested{}` + nl + `success<null>`
	acpNoRQ  = `<nil><null>password_reset_requesteduser<nil>Password reset requested{}` + nl + `success<null>`
	acpNinaV = `Nina's Team  Ünïnina.new@example.comemail_verifieduseruser:nina.new@example.comEmail verified{}` + nl + `success<null>`
	acpUmaV  = `<nil>uma@example.comemail_verifieduseruser:uma@example.comEmail verified{}` + nl + `success<null>`
)

func TestCredentialQuirksCoverTheScenarioRowsExactly(t *testing.T) {
	l, err := NewLedger(forwarded(t, "credentials"), false)
	if err != nil {
		t.Fatal(err)
	}
	goldenRows := []string{goldenN, goldenR, goldenRQ}
	acpRows := []string{acpN, goldenR, goldenRQ, acpSolo, acpPiaR, acpPiaRQ, acpNoRQ, acpNinaV, acpUmaV}
	for _, name := range []string{"emma", "ned", "maya"} {
		golden := strings.Replace(strings.Replace(goldenN, "nina.new", name, 2), "Nina's Team  Ünï", "My Organization", 1)
		goldenRows = append(goldenRows, golden)
		acpRows = append(acpRows, strings.Replace(golden, `{"email": "`+name+`@example.com", "organization_id": "<uuid>"}`, "{}", 1))
	}
	golden, acp := strings.Join(goldenRows, rowSeparator), strings.Join(acpRows, rowSeparator)
	if problems := l.Compare(TableAuditLogs, golden, acp); len(problems) != 0 {
		t.Fatalf("unclassified: %v", problems)
	}
	for _, p := range l.Dead() {
		if !strings.Contains(p, "covered no row") {
			t.Errorf("unexpected: %s", p)
		}
	}
	covered := l.Covered()
	for _, want := range []string{"audit-register-changes-not-recorded=4", "audit-email-verified-recorded=2",
		"audit-password-reset-requested-without-membership=1", "audit-password-reset-requested-unknown-address=1"} {
		if !strings.Contains(covered, want) {
			t.Errorf("covered lacks %s: %s", want, covered)
		}
	}
	for _, closed := range []string{"audit-password-reset-requested-not-recorded", "audit-password-reset-organization-not-recorded"} {
		if strings.Contains(covered, closed) {
			t.Errorf("closed quirk %s still covers a row: %s", closed, covered)
		}
	}
}

func TestCredentialQuirkMaxIsExactBelowAndAbove(t *testing.T) {
	verified := acpNinaV
	for _, n := range []int{1, 3} {
		l, err := NewLedger(forwarded(t, "credentials"), false)
		if err != nil {
			t.Fatal(err)
		}
		var rows []string
		for i := 0; i < n; i++ {
			rows = append(rows, verified)
		}
		problems := strings.Join(append(l.Compare(TableAuditLogs, "x", strings.Join(rows, rowSeparator)), l.Dead()...), "\n")
		if !strings.Contains(problems, "audit-email-verified-recorded") && !strings.Contains(problems, "target only: <nil>nina.new") {
			t.Errorf("%d email-verified rows were accepted; the scenario holds exactly 2:\n%s", n, problems)
		}
	}
}
