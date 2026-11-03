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

func TestTheAuditQueryIsThePublicTableForEveryForwarding(t *testing.T) {
	for _, text := range []string{"sessions", "sessions,local_login", "sessions,local_login,social", "sessions,local_login,credentials", "credentials"} {
		got, projected, err := Query(TableAuditLogs, publicAudit, forwarded(t, text))
		if err != nil || !projected || got != auditLogsACP {
			t.Errorf("forwarded %s: the audit query is not the public-table query (err %v)", text, err)
		}
	}
	// auth-service writes go-api's rows itself (CHAOS-11400): an auth.* event projected into the comparison would
	// double them.
	if strings.Contains(auditLogsACP, "security_audit_events") || !strings.Contains(auditLogsACP, "- 'request_id'") {
		t.Errorf("the audit query reads auth events or keeps request_id:\n%s", auditLogsACP)
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

func TestTheSyntheticAuditQuirksAreClosed(t *testing.T) {
	ids := map[string]bool{}
	for _, q := range Quirks {
		ids[q.ID] = true
	}
	for _, id := range []string{
		"audit-login-failed-attempted-email-not-recorded", "audit-login-without-organization-recorded",
		"audit-register-organization-and-changes-not-recorded", "audit-register-without-organization-recorded",
		"audit-password-reset-recorded-without-membership", "audit-password-reset-requested-without-membership",
		"audit-password-reset-requested-unknown-address", "audit-password-reset-organization-not-projected",
		"audit-password-reset-requested-organization-not-projected", "audit-password-reset-organization-not-recorded",
		"audit-password-reset-requested-not-recorded", "audit-email-verified-recorded",
	} {
		if ids[id] {
			t.Errorf("quirk %s excused a projected auth event; auth-service writes the real row now, so it must be gone", id)
		}
	}
	if !ids["audit-login-organization-chosen-without-activity"] {
		t.Error("the organization choice difference still holds for the real login rows")
	}
}

const nl = `{"ip_address": "203.0.113.7", "user_agent": "venue-oracle/1"}`

// The credential steps' rows auth-service writes are go-api's: the golden's lines compare equal with no quirk.
func TestCredentialRowsCompareEqualWithNoQuirk(t *testing.T) {
	l, err := NewLedger(forwarded(t, "credentials"), false)
	if err != nil {
		t.Fatal(err)
	}
	rows := []string{
		`Nina's Team  Ünïnina.new@example.comcreateuseruser:nina.new@example.comUser registered{"email": "nina.new@example.com", "organization_id": "<uuid>"}` + nl + `success<null>`,
		`Org Aalice@example.compassword_resetuseruser:alice@example.comUser reset password{}` + nl + `success<null>`,
		`Org Aalice@example.compassword_reset_requesteduseruser:alice@example.comPassword reset requested{}` + nl + `success<null>`,
	}
	golden := strings.Join(rows, rowSeparator)
	if problems := l.Compare(TableAuditLogs, golden, golden); len(problems) != 0 {
		t.Fatalf("problems: %v", problems)
	}
	if covered := l.Covered(); strings.Contains(covered, "audit-") {
		t.Errorf("an audit quirk covered an equal row: %s", covered)
	}
}
