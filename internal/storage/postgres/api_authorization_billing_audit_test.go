package postgres

import (
	"reflect"
	"testing"
)

// A permanent user delete (CHAOS-10019) sets billing_audit_log.actor_id to null through the api role. The grant is that one
// column, never the table: the audit text and state stay out of reach of an UPDATE.
func TestAPIPostureLetsTheAPIRoleUpdateOnlyTheBillingAuditActor(t *testing.T) {
	t.Parallel()
	posture := APIPosture(false)
	var table *TablePrivilege
	for i := range posture.RequiredTables {
		if posture.RequiredTables[i].TableName == "billing_audit_log" {
			table = &posture.RequiredTables[i]
		}
	}
	if table == nil || table.AllowUpdate {
		t.Fatalf("billing_audit_log table entry = %+v, want present with no table-wide UPDATE", table)
	}
	var columns []ColumnPrivilege
	for _, column := range posture.ColumnScoped {
		if column.TableName == "billing_audit_log" {
			columns = append(columns, column)
		}
	}
	want := []ColumnPrivilege{{"billing_audit_log", "actor_id", "UPDATE"}}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("billing_audit_log column privileges = %+v, want %+v", columns, want)
	}
	if len(posture.ColumnScoped) != 1 {
		t.Fatalf("the api posture declares column privileges beyond the billing audit actor: %+v", posture.ColumnScoped)
	}
}
