//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestPublicRowGrants: with the switch off the runtime role keeps the end of a
// row and cannot start one; on, it holds the start's columns. No other column,
// no DELETE, whatever the switch.
func TestPublicRowGrants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	const table = "impersonation_sessions"
	sorted := func(columns ...string) []string {
		out := slices.Clone(columns)
		slices.Sort(out)
		return out
	}
	offReads := sorted("admin_user_id", "ended_at", "expires_at", "id")
	onReads := sorted("admin_user_id", "ended_at", "expires_at", "id", "target_org_id", "target_user_id")
	inserts := sorted("id", "admin_user_id", "target_user_id", "target_org_id", "target_role", "created_at", "expires_at")
	check := func(owned bool, reads, wantInsert []string) {
		t.Helper()
		options := env.options(&bytes.Buffer{})
		options.UsersOwned = owned
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
		if got := env.heldColumns(t, ctx, table, "SELECT"); !slices.Equal(got, reads) {
			t.Errorf("users owned %v: SELECT columns %v, want %v", owned, got, reads)
		}
		if got := env.heldColumns(t, ctx, table, "INSERT"); !slices.Equal(got, wantInsert) {
			t.Errorf("users owned %v: INSERT columns %v, want %v", owned, got, wantInsert)
		}
		if got := env.heldColumns(t, ctx, table, "UPDATE"); !slices.Equal(got, []string{"ended_at"}) {
			t.Errorf("users owned %v: UPDATE columns %v, want ended_at", owned, got)
		}
		if got := env.heldColumns(t, ctx, table, "REFERENCES"); len(got) != 0 {
			t.Errorf("users owned %v: REFERENCES held on %v", owned, got)
		}
		for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"} {
			if env.tablePrivilege(t, ctx, table, privilege) {
				t.Errorf("users owned %v: table-level %s held", owned, privilege)
			}
		}
		for name, statement := range map[string]string{
			"delete":     `DELETE FROM public.impersonation_sessions WHERE false`,
			"update ip":  `UPDATE public.impersonation_sessions SET ip_address = NULL WHERE false`,
			"select ip":  `SELECT ip_address FROM public.impersonation_sessions`,
			"insert ip":  `INSERT INTO public.impersonation_sessions (ip_address) VALUES (NULL)`,
			"update end": `UPDATE public.impersonation_sessions SET ended_at = now() WHERE id = '00000000-0000-0000-0000-000000000000'`,
		} {
			err := env.runtimeStatement(ctx, statement)
			if name == "update end" {
				if err != nil {
					t.Errorf("users owned %v: %s: %v", owned, name, err)
				}
				continue
			}
			if err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("users owned %v: %s: err = %v, want permission denied", owned, name, err)
			}
		}
	}
	off := env.options(&bytes.Buffer{})
	if _, err := Apply(ctx, env.migration, off); err != nil {
		t.Fatalf("Apply off: %v", err)
	}
	check(false, offReads, nil)
	on := env.options(&bytes.Buffer{})
	on.UsersOwned = true
	if _, err := Apply(ctx, env.migration, on); err != nil {
		t.Fatalf("Apply on: %v", err)
	}
	check(true, onReads, inserts)
	_, err := Apply(ctx, env.migration, off)
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("turning the switch off: err = %v, want the narrowing refusal", err)
	}
	off.AcknowledgeNarrowing = narrowed.Digest
	if _, err := Apply(ctx, env.migration, off); err != nil {
		t.Fatalf("Apply off with the acknowledgement: %v", err)
	}
	check(false, offReads, nil)
}
