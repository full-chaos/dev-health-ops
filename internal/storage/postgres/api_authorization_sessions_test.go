package postgres

import "testing"

func refreshTokensOf(t *testing.T, posture RolePosture) (TablePrivilege, []ColumnPrivilege) {
	t.Helper()
	var table *TablePrivilege
	for i := range posture.RequiredTables {
		if posture.RequiredTables[i].TableName == "refresh_tokens" {
			if table != nil {
				t.Fatal("refresh_tokens is declared twice")
			}
			table = &posture.RequiredTables[i]
		}
	}
	if table == nil {
		t.Fatal("refresh_tokens is not declared")
	}
	var columns []ColumnPrivilege
	for _, column := range posture.ColumnScoped {
		if column.TableName == "refresh_tokens" {
			columns = append(columns, column)
		}
	}
	return *table, columns
}

// With go-api's sessions owned by auth-service the api role inserts and
// rotates no refresh token: INSERT and table UPDATE go, revoked_at stays
// writable for the revoke-all paths, DELETE stays for purge and cleanup.
func TestAPIPostureRefreshTokensFollowTheSessionsFlag(t *testing.T) {
	off, offColumns := refreshTokensOf(t, APIPosture(false))
	if off != (TablePrivilege{"refresh_tokens", true, true, true}) || len(offColumns) != 0 {
		t.Fatalf("flag off: %+v %+v, want insert, update and delete", off, offColumns)
	}
	on, onColumns := refreshTokensOf(t, APIPosture(true))
	if on != (TablePrivilege{"refresh_tokens", false, false, true}) {
		t.Fatalf("flag on: %+v, want select and delete only at table level", on)
	}
	if len(onColumns) != 1 || onColumns[0] != (ColumnPrivilege{"refresh_tokens", "revoked_at", "UPDATE"}) {
		t.Fatalf("flag on: columns %+v, want UPDATE (revoked_at) only", onColumns)
	}
	if len(APIPosture(true).RequiredTables) != len(APIPosture(false).RequiredTables) {
		t.Fatal("the flag changes more than refresh_tokens")
	}
}
