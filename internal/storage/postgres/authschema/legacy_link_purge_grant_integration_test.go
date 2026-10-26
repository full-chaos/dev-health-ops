//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestLegacyLinkPurgeGrantFollowsTheSwitch: with the switch on the runtime role
// holds DELETE and SELECT(id) on the two legacy link-token tables and nothing
// else there; with it off it holds neither (turning it off is a narrowing that
// needs its acknowledgement); a widened grant is a posture violation.
func TestLegacyLinkPurgeGrantFollowsTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	posture := func(owned bool) []Violation {
		t.Helper()
		conn, err := env.migration.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		options := env.options(&bytes.Buffer{})
		options.UsersOwned = owned
		found, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
		if err != nil {
			t.Fatalf("VerifyRuntimePosture: %v", err)
		}
		return found
	}
	purge := func(table string) string {
		return fmt.Sprintf(`WITH d AS (DELETE FROM public.%s WHERE id = gen_random_uuid() RETURNING id) SELECT count(*) FROM d`, Quote(mustValidatedIdentifier(table)))
	}
	denied := func(name, statement string) {
		t.Helper()
		if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: err = %v, want permission denied", name, err)
		}
	}

	if found := posture(false); len(found) > 0 {
		t.Fatalf("posture with the switch off is not clean:\n%s", describe(found))
	}
	for _, grant := range LegacyLinkPurgeGrants() {
		denied("purge with the switch off: "+grant.Table, purge(grant.Table))
	}

	owned := env.options(&bytes.Buffer{})
	owned.UsersOwned = true
	if _, err := Apply(ctx, env.migration, owned); err != nil {
		t.Fatalf("Apply with the switch on: %v", err)
	}
	if found := posture(true); len(found) > 0 {
		t.Fatalf("posture with the switch on is not clean:\n%s", describe(found))
	}
	for _, grant := range LegacyLinkPurgeGrants() {
		if err := env.runtimeStatement(ctx, purge(grant.Table)); err != nil {
			t.Errorf("purge with the switch on: %s: %v", grant.Table, err)
		}
		if got := env.heldColumns(t, ctx, grant.Table, "SELECT"); len(got) != 1 || got[0] != "id" {
			t.Errorf("%s: SELECT columns = %v, want [id]", grant.Table, got)
		}
		for _, privilege := range []string{"INSERT", "UPDATE", "REFERENCES"} {
			if got := env.heldColumns(t, ctx, grant.Table, privilege); len(got) != 0 {
				t.Errorf("%s: %s held on %v", grant.Table, privilege, got)
			}
		}
		for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "TRUNCATE", "REFERENCES", "TRIGGER"} {
			if env.tablePrivilege(t, ctx, grant.Table, privilege) {
				t.Errorf("%s: table-level %s held", grant.Table, privilege)
			}
		}
		if !env.tablePrivilege(t, ctx, grant.Table, "DELETE") {
			t.Errorf("%s: table-level DELETE not held with the switch on", grant.Table)
		}
		denied("read the hash: "+grant.Table, fmt.Sprintf(`SELECT token_hash FROM public.%s`, grant.Table))
		denied("update: "+grant.Table, fmt.Sprintf(`UPDATE public.%s SET expires_at = now()`, grant.Table))
		denied("truncate: "+grant.Table, fmt.Sprintf(`TRUNCATE public.%s`, grant.Table))
	}

	if _, err := env.admin.Exec(ctx, fmt.Sprintf(`GRANT UPDATE ON public.password_reset_tokens TO %s`, Quote(mustValidatedIdentifier(env.runtimeRole)))); err != nil {
		t.Fatal(err)
	}
	if found := describe(posture(true)); !strings.Contains(found, "password_reset_tokens") || !strings.Contains(found, "UPDATE") {
		t.Errorf("a widened grant (UPDATE) was not reported:\n%s", found)
	}
	_, err := Apply(ctx, env.migration, owned)
	var widened *NarrowingError
	if !errors.As(err, &widened) {
		t.Fatalf("re-Apply over a widened grant: err = %v, want the narrowing refusal", err)
	}
	owned.AcknowledgeNarrowing = widened.Digest
	if _, err := Apply(ctx, env.migration, owned); err != nil {
		t.Fatalf("re-Apply with the acknowledgement: %v", err)
	}
	if found := posture(true); len(found) > 0 {
		t.Fatalf("re-Apply did not restore the exact grant:\n%s", describe(found))
	}

	off := env.options(&bytes.Buffer{})
	_, err = Apply(ctx, env.migration, off)
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("turning the switch off: err = %v, want the narrowing refusal", err)
	}
	off.AcknowledgeNarrowing = narrowed.Digest
	if _, err := Apply(ctx, env.migration, off); err != nil {
		t.Fatalf("turning the switch off with the acknowledgement: %v", err)
	}
	if found := posture(false); len(found) > 0 {
		t.Fatalf("posture after the revoke is not clean:\n%s", describe(found))
	}
	for _, grant := range LegacyLinkPurgeGrants() {
		denied("purge after the revoke: "+grant.Table, purge(grant.Table))
	}
}
