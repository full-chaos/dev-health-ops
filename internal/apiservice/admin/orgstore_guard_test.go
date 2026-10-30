package admin

import (
	"context"
	"reflect"
	"testing"
)

// The guarded delete of an ownerless organization takes the org row lock,
// then checks for a membership in a statement of its own (it sees any insert
// that committed while it waited), then deletes, in one transaction.
func TestDeleteOrganizationWithoutMembersLocksTheOrgBeforeItsCheck(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rows    map[string][]any
		deleted bool
		wantErr bool
		steps   []string
	}{
		{"no member", map[string][]any{orgCreateGuardLock: {1}, orgCreateGuardMembers: {false}}, true, false, orgCreateGuardDeletes},
		{"a member", map[string][]any{orgCreateGuardLock: {1}, orgCreateGuardMembers: {true}}, false, false, orgCreateGuardKeeps},
		{"no org row", map[string][]any{}, false, false, []string{"BEGIN", "tx:" + orgCreateGuardLock, "ROLLBACK"}},
		{"the lock read fails", map[string][]any{orgCreateGuardLock: {failRead{}}}, false, true, []string{"BEGIN", "tx:" + orgCreateGuardLock, "ROLLBACK"}},
		{"the check fails", map[string][]any{orgCreateGuardLock: {1}, orgCreateGuardMembers: {failRead{}}}, false, true,
			[]string{"BEGIN", "tx:" + orgCreateGuardLock, "tx:" + orgCreateGuardMembers, "ROLLBACK"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &provisionDB{rows: tc.rows}
			deleted, err := pgStore{Pool: db}.deleteOrganizationWithoutMembers(context.Background(), provOrg)
			if deleted != tc.deleted || (err != nil) != tc.wantErr {
				t.Fatalf("deleted %v err %v, want %v (error %v)", deleted, err, tc.deleted, tc.wantErr)
			}
			if !reflect.DeepEqual(db.log, tc.steps) {
				t.Fatalf("steps:\n got %q\nwant %q", db.log, tc.steps)
			}
		})
	}
}
