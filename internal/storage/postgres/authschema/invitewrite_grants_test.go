package authschema

import (
	"reflect"
	"testing"
)

// The invite aggregate writes go-api's org_invites row and go-api's audit_logs
// row (one writer per audit row, acp ruling round1062 Q-D1); nothing else, no
// DELETE, and no token column beyond its digest.
func TestInviteWriteGrantsAreTheReviewedColumns(t *testing.T) {
	want := []InviteWriteGrant{
		{
			Table:  "org_invites",
			Select: []string{"id", "org_id", "email", "role", "token_hash", "invited_by_id", "status", "expires_at"},
			Insert: []string{"id", "org_id", "email", "role", "token_hash", "invited_by_id", "status", "expires_at", "created_at", "updated_at"},
			Update: []string{"status", "accepted_at", "updated_at"},
		},
		{
			Table:  "audit_logs",
			Insert: []string{"id", "org_id", "user_id", "action", "resource_type", "resource_id", "description", "changes", "request_metadata", "status", "created_at"},
		},
	}
	if got := InviteWriteGrants(); !reflect.DeepEqual(got, want) {
		t.Fatalf("InviteWriteGrants = %+v, want %+v", got, want)
	}
	for _, g := range want {
		for _, name := range append(append(append([]string{g.Table}, g.Select...), g.Insert...), g.Update...) {
			if err := ValidateIdentifier(name); err != nil {
				t.Errorf("identifier %q: %v", name, err)
			}
		}
	}
}

// auth.invites is in the runtime manifest: read, insert and update, never
// delete (a revoked invite keeps its row).
func TestRuntimePostureHoldsInvitesWithoutDelete(t *testing.T) {
	for _, p := range RuntimePosture() {
		if p.Table == "invites" {
			if !p.Select || !p.Insert || !p.Update || p.Delete {
				t.Fatalf("invites privileges = %+v, want SELECT, INSERT, UPDATE and no DELETE", p)
			}
			return
		}
	}
	t.Fatal("RuntimePosture has no invites row")
}
