package proofprincipals

import (
	"strings"
	"testing"
)

func TestComparedSQLExcludesTheProofPrincipalsByIdentityNotByShape(t *testing.T) {
	sql := ComparedSQL("users")
	for _, id := range IDs {
		if !strings.Contains(sql, id) {
			t.Errorf("the predicate does not name the proof principal %s: %s", id, sql)
		}
	}
	if len(IDs) != 2 {
		t.Errorf("%d proof principals, want the prove and the admin proof principal", len(IDs))
	}
	if strings.Contains(sql, "'service'") || strings.Contains(sql, "auth_provider") || strings.Contains(sql, "password_hash") {
		t.Errorf("the predicate reads the shape of a row: %s", sql)
	}
	if !strings.HasPrefix(sql, "users.") {
		t.Errorf("the predicate does not use its alias: %s", sql)
	}
}
