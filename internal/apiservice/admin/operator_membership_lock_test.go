package admin

import (
	"context"
	"slices"
	"strings"
	"testing"
)

const operatorUserLockSQL = "tx:SELECT 1 FROM users WHERE id = $1 FOR UPDATE"

func TestUpdateUserMembershipLocksTheUserBeforeTheRead(t *testing.T) {
	db := &provisionDB{rows: operatorRows()}
	_, _ = Operator{db: db}.UpdateUser(context.Background(), operatorMembershipUpdates["add or role"])
	lock := slices.Index(db.log, operatorUserLockSQL)
	read := slices.IndexFunc(db.log, func(sql string) bool { return strings.Contains(sql, "FROM memberships WHERE org_id") })
	if lock < 0 || read < 0 || lock > read {
		t.Fatalf("lock at %d, membership read at %d, want the lock first: %q", lock, read, db.log)
	}
}

func TestUpdateUserTakesNoLockWithoutMembershipOrg(t *testing.T) {
	updates := map[string]UpdateUserInput{
		"remove":   operatorMembershipUpdates["remove"],
		"profile":  {Email: strp("a@example.test"), FullName: strp("A")},
		"password": {Email: strp("a@example.test"), Password: strp("a-long-enough-pass-phrase-41")},
	}
	for name, in := range updates {
		db := &provisionDB{rows: operatorRows()}
		_, _ = Operator{db: db}.UpdateUser(context.Background(), in)
		if len(db.log) < 2 || slices.Contains(db.log, operatorUserLockSQL) {
			t.Fatalf("%s: SQL %q, want the user found and no lock", name, db.log)
		}
	}
}
