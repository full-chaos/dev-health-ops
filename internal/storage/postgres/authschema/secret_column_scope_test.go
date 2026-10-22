package authschema

import "testing"

// A table-scoped approval admits its own column of its own type and nothing
// else: the same name on another table, or another type on the same table,
// is still flagged by the secret-name rule.
func TestTableScopedSecretApprovalCoversOnlyItsColumn(t *testing.T) {
	if !secretColumnApproved("sessions", "legacy_token_version", "bigint") {
		t.Fatal("sessions.legacy_token_version (bigint) is refused")
	}
	for name, c := range map[string][3]string{
		"another table":       {"credential_store", "legacy_token_version", "bigint"},
		"another type":        {"sessions", "legacy_token_version", "text"},
		"another table, text": {"credential_store", "legacy_token_version", "text"},
	} {
		if secretColumnApproved(c[0], c[1], c[2]) {
			t.Errorf("%s: %s.%s (%s) is approved", name, c[0], c[1], c[2])
		}
	}
	if !secretColumnApproved("refresh_credentials", "token_hash", "bytea") {
		t.Fatal("a name-approved column is refused")
	}
}
