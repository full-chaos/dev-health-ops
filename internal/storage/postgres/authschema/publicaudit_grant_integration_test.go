//go:build integration

package authschema

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestPublicAuditWriteGrantIsColumnInsertWhateverTheSwitch: with the users-owned
// switch off and on, the runtime role holds INSERT on exactly the
// PublicAuditWriteColumns of public.audit_logs and nothing else there (no
// SELECT, UPDATE, DELETE, TRUNCATE, no table-level privilege), the posture is
// clean, and an UPDATE, DELETE or SELECT by the runtime role is refused.
func TestPublicAuditWriteGrantIsColumnInsertWhateverTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	want := slices.Clone(PublicAuditWriteColumns())
	slices.Sort(want)
	for _, owned := range []bool{false, true} {
		options := env.options(&bytes.Buffer{})
		options.UsersOwned = owned
		if _, err := Apply(ctx, env.migration, options); err != nil {
			t.Fatalf("Apply (users owned %v): %v", owned, err)
		}
		conn, err := env.migration.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		found, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
		conn.Release()
		if err != nil {
			t.Fatalf("VerifyRuntimePosture: %v", err)
		}
		if len(found) > 0 {
			t.Fatalf("posture (users owned %v) is not clean:\n%s", owned, describe(found))
		}
		if got := env.heldColumns(t, ctx, "audit_logs", "INSERT"); !slices.Equal(got, want) {
			t.Errorf("users owned %v: INSERT columns %v, want %v", owned, got, want)
		}
		for _, privilege := range []string{"SELECT", "UPDATE", "REFERENCES"} {
			if got := env.heldColumns(t, ctx, "audit_logs", privilege); len(got) != 0 {
				t.Errorf("users owned %v: %s held on %v", owned, privilege, got)
			}
		}
		for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"} {
			if env.tablePrivilege(t, ctx, "audit_logs", privilege) {
				t.Errorf("users owned %v: table-level %s held", owned, privilege)
			}
		}
		for name, statement := range map[string]string{
			"update": `UPDATE public.audit_logs SET status = 'success' WHERE false`,
			"delete": `DELETE FROM public.audit_logs WHERE false`,
			"select": `SELECT count(*) FROM public.audit_logs`,
		} {
			if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("users owned %v: %s by the runtime role: err = %v, want permission denied", owned, name, err)
			}
		}
	}
}
