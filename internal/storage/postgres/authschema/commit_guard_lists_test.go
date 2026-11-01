package authschema

import (
	"regexp"
	"sort"
	"testing"
)

// guardedTables carry the audit_commit_guard trigger (migration 0006): a write
// is refused unless it runs inside an internal/audit.Commit transaction.
var guardedTables = []string{
	"auth_outbox_events", "security_audit_events", "resource_grants", "memberships",
	"platform_role_assignments", "policy_revisions", "entitlement_snapshots",
	"credential_registry", "service_accounts", "workloads", "workload_bindings", "workload_assertion_keys", "external_identities",
	"organizations", "aggregate_revisions", "signing_keys", "invites", "restore_fence_state", "oauth_grants",
}

// unguardedTables are deliberately outside the guard, each with its reason.
var unguardedTables = map[string]string{
	"sessions":                     "high-rate session lifecycle rows; a per-write outbox event and audit row would be noise; a new row inside audit.Commit is counted by the 0014 create guard",
	"refresh_credentials":          "high-rate rotation lifecycle rows, same reason as sessions",
	"users":                        "profile and lifecycle columns written at high rate; identity linking is guarded on external_identities; a new row inside audit.Commit is counted by the 0013 create guard",
	"principals":                   "identity root row, created with its user or service account; carries no grant; a new row inside audit.Commit is counted by the 0013 create guard",
	"actions":                      "closed vocabulary; the runtime role has no write privilege, so only the migration role writes it",
	"roles":                        "closed vocabulary; the runtime role has no write privilege",
	"role_actions":                 "closed vocabulary; the runtime role has no write privilege",
	"link_credentials":             "single-use link tokens swept on expiry and removed with their principal; consuming or retargeting one is guarded by column (security columns below)",
	"login_attempts":               "high-rate lockout counters keyed by an identifier hash; rate limiting, not authority",
	"sso_flow_states":              "short-lived SSO flow state, consumed once even when the callback is rejected, so its consume cannot require an audit transaction; consumed_at is write-once and every other column is immutable by trigger",
	"saml_assertion_uses":          "insert-only replay ledger swept after expiry; a row exists to be refused a second time",
	"oauth_authorization_requests": "short-lived authorization state written by an unauthenticated GET, swept by traffic; the grant and the session a decision writes commit inside audit.Commit",
	versionTable:                   "auth-migrate bookkeeping, unreachable by the runtime role",
}

// The two lists, checked against the migrations without a database: a table the
// lineage creates must be in exactly one list, and a table with a commit-guard
// trigger must be in the guarded list. (TestEveryAuthTableIsClassified checks the
// same against a real schema; this one fails on the first push.)
func TestEveryTableTheLineageCreatesIsInExactlyOneCommitGuardList(t *testing.T) {
	guarded := map[string]bool{}
	for _, name := range guardedTables {
		guarded[name] = true
	}
	var unclassified, both []string
	for table := range createdTables(t) {
		_, unguarded := unguardedTables[table]
		switch {
		case guarded[table] && unguarded:
			both = append(both, table)
		case !guarded[table] && !unguarded:
			unclassified = append(unclassified, table)
		}
	}
	sort.Strings(unclassified)
	sort.Strings(both)
	if len(unclassified) > 0 || len(both) > 0 {
		t.Fatalf("tables must be in exactly one list: unclassified=%v in-both=%v", unclassified, both)
	}
}

var commitGuardTrigger = regexp.MustCompile(`(?i)CREATE TRIGGER (\w+)_commit_guard\s+BEFORE INSERT OR UPDATE OR DELETE ON (\w+)`)

func TestEveryCommitGuardTriggerOfTheLineageNamesAGuardedTable(t *testing.T) {
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	guarded := map[string]bool{}
	for _, name := range guardedTables {
		guarded[name] = true
	}
	found := 0
	for _, m := range migrations {
		for _, match := range commitGuardTrigger.FindAllStringSubmatch(stripComments(m.SQL), -1) {
			found++
			if match[1] != match[2] {
				t.Errorf("migration %04d: trigger %s_commit_guard is on table %s", m.Version, match[1], match[2])
			}
			if !guarded[match[2]] {
				t.Errorf("migration %04d: table %s has a commit guard but is not in guardedTables", m.Version, match[2])
			}
		}
	}
	if found == 0 {
		t.Fatal("no commit-guard trigger found; the extraction is broken")
	}
}
