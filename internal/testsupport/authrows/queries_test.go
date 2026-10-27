package authrows

import (
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proofprincipals"
)

// the corpus's public queries as the test holds them on main (CHAOS-10160), reduced to what Query reads.
var publicUsers = `SELECT email, token_version, last_login_at IS NOT NULL, updated_at >= created_at FROM users WHERE ` + proofprincipals.ComparedSQL("users") + ` ORDER BY email`

const (
	publicRefresh = `SELECT u.email FROM refresh_tokens rt JOIN users u ON u.id = rt.user_id`
	publicAudit   = `SELECT 1 FROM audit_logs a`
)

func forwarded(t *testing.T, text string) Forwarded {
	t.Helper()
	f, err := ParseForwarded(text)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestProjectionRoutingEmptyForwardedKeepsThePublicQueriesByteForByte(t *testing.T) {
	for _, table := range []string{TableUsers, TableRefreshTokens, TableAuditLogs, TableOrganizations, TableEmailVerificationTokens, "anything"} {
		got, projected, err := Query(table, "PUBLIC "+table, forwarded(t, ""))
		if err != nil || projected || got != "PUBLIC "+table {
			t.Errorf("%s: got %q projected %v err %v, want the public query unchanged", table, got, projected, err)
		}
	}
}

func TestProjectionRoutingSessionsForwardedReadsAuthRefreshAndAudit(t *testing.T) {
	f := forwarded(t, "sessions,local_login,social")
	refresh, projected, err := Query(TableRefreshTokens, publicRefresh, f)
	if err != nil || !projected || !strings.Contains(refresh, "auth.refresh_credentials") || !strings.Contains(refresh, "FROM refresh_tokens rt") {
		t.Fatalf("refresh_tokens: projected %v err %v\n%s", projected, err, refresh)
	}
	audit, projected, err := Query(TableAuditLogs, publicAudit, f)
	if err != nil || !projected || !strings.Contains(audit, "auth.security_audit_events") || !strings.Contains(audit, "FROM audit_logs a") {
		t.Fatalf("audit_logs: projected %v err %v\n%s", projected, err, audit)
	}
	users, projected, err := Query(TableUsers, publicUsers, f)
	if err != nil || !projected || !strings.Contains(users, "auth.users al") || strings.Count(users, "last_login_at IS NOT NULL") != 2 {
		t.Fatalf("users: projected %v err %v\n%s", projected, err, users)
	}
	for _, table := range []string{TableOrganizations, TableMemberships, TableOrgInvites, TableEmailVerificationTokens, TablePasswordResetTokens} {
		got, projected, err := Query(table, "PUBLIC", f)
		if err != nil || projected || got != "PUBLIC" {
			t.Errorf("%s: got %q projected %v err %v, want the public query", table, got, projected, err)
		}
	}
}

func TestAFlowedAggregateWithoutAProjectionFails(t *testing.T) {
	for _, text := range []string{"oidc", "sessions,oidc", "saml", "sessions,local_login,credentials,social,oidc,saml"} {
		f := forwarded(t, text)
		for _, table := range []string{TableUsers, TableRefreshTokens, TableAuditLogs} {
			if _, _, err := Query(table, "PUBLIC", f); err == nil {
				t.Errorf("forwarded %s: %s was read without a projection", text, table)
			}
		}
	}
	if _, _, err := Query("tokens_of_a_new_table", "PUBLIC", forwarded(t, "sessions")); err == nil {
		t.Error("a table with no named comparison was read")
	}
	if _, _, err := Query(TableUsers, "SELECT 1 FROM users", forwarded(t, "sessions")); err == nil {
		t.Error("a users query without the last-login column was rewritten")
	}
	if _, _, err := Query(TableUsers, publicUsers+" -- last_login_at IS NOT NULL", forwarded(t, "sessions")); err == nil {
		t.Error("a users query holding the last-login column twice was rewritten")
	}
}

var selectStar = regexp.MustCompile(`(?i)select\s+(distinct\s+)?(\w+\.)?\*|\w+\.\*`)

func TestProjectionQueriesNameColumnsNoSelectStar(t *testing.T) {
	f := forwarded(t, "sessions,local_login,social")
	for _, table := range []string{TableUsers, TableRefreshTokens, TableAuditLogs} {
		query, _, err := Query(table, publicUsers, f)
		if err != nil {
			t.Fatal(err)
		}
		if selectStar.MatchString(query) {
			t.Errorf("%s: the projection selects a column by star, so a column added to an auth table changes its rows:\n%s", table, query)
		}
	}
}
