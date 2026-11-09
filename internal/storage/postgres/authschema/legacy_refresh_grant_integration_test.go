//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestRuntimeRoleHoldsNoRefreshTokenPrivilege: over the real ops schema the
// runtime role can neither read nor write public.refresh_tokens; the old
// legacy-exchange grant is a posture violation, and the next Apply revokes it.
func TestRuntimeRoleHoldsNoRefreshTokenPrivilege(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	denied := func(when string) {
		t.Helper()
		for name, statement := range map[string]string{
			"read the old columns": `SELECT token_hash, user_id, org_id, family_id, expires_at, revoked_at FROM public.refresh_tokens`,
			"revoke":               `UPDATE public.refresh_tokens SET revoked_at = now() WHERE family_id = gen_random_uuid() AND revoked_at IS NULL`,
			"insert":               `INSERT INTO public.refresh_tokens DEFAULT VALUES`,
			"delete":               `DELETE FROM public.refresh_tokens`,
		} {
			if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("%s, %s: err = %v, want permission denied", when, name, err)
			}
		}
	}

	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("posture after Apply is not clean:\n%s", describe(found))
	}
	denied("after Apply")

	role := Quote(mustValidatedIdentifier(env.runtimeRole))
	for _, statement := range []string{
		`GRANT SELECT (token_hash, user_id, org_id, family_id, expires_at, revoked_at) ON public.refresh_tokens TO ` + role,
		`GRANT UPDATE (revoked_at) ON public.refresh_tokens TO ` + role,
	} {
		if _, err := env.migration.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	found := describe(env.violations(t, ctx))
	for _, want := range []string{"refresh_tokens.token_hash", "refresh_tokens.revoked_at"} {
		if !strings.Contains(found, want) {
			t.Errorf("the old legacy-exchange grant (%s) was not reported:\n%s", want, found)
		}
	}

	// Revoking the held grant is a narrowing: refused with the pinned digest (the
	// value a roll acknowledges), then applied with it.
	_, err := Apply(ctx, env.migration, env.options(&bytes.Buffer{}))
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("Apply over the old grant without the acknowledgement: err = %v, want the narrowing refusal", err)
	}
	if narrowed.Digest != retiredRefreshTokensAckDigest || len(narrowed.Lost) != len(retiredRefreshTokensLost()) {
		t.Fatalf("narrowing = %s, %d lost; want %s, %d lost:\n%v", narrowed.Digest, len(narrowed.Lost), retiredRefreshTokensAckDigest, len(retiredRefreshTokensLost()), narrowed)
	}
	acknowledged := env.options(&bytes.Buffer{})
	acknowledged.AcknowledgeNarrowing = retiredRefreshTokensAckDigest
	if _, err := Apply(ctx, env.migration, acknowledged); err != nil {
		t.Fatalf("Apply over the old grant with the acknowledgement: %v", err)
	}
	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("posture after the acknowledged Apply is not clean:\n%s", describe(found))
	}
	denied("after the acknowledged Apply")
	if _, err := Apply(ctx, env.migration, env.options(&bytes.Buffer{})); err != nil {
		t.Fatalf("Apply after the revoke, without the acknowledgement: %v", err)
	}
	denied("after the next Apply")
}

// A table-level privilege on public.refresh_tokens is held too: the grants pass
// revokes it (with its own acknowledgement) instead of leaving it to the posture.
func TestATableLevelRefreshTokensPrivilegeIsRevoked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	role := Quote(mustValidatedIdentifier(env.runtimeRole))
	if _, err := env.migration.Exec(ctx, `GRANT SELECT ON public.refresh_tokens TO `+role); err != nil {
		t.Fatal(err)
	}
	_, err := Apply(ctx, env.migration, env.options(&bytes.Buffer{}))
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("Apply over a table-level grant: err = %v, want the narrowing refusal", err)
	}
	acknowledged := env.options(&bytes.Buffer{})
	acknowledged.AcknowledgeNarrowing = narrowed.Digest
	if _, err := Apply(ctx, env.migration, acknowledged); err != nil {
		t.Fatalf("Apply with the acknowledgement: %v", err)
	}
	var held bool
	if err := env.admin.QueryRow(ctx, `SELECT has_table_privilege($1, 'public.refresh_tokens', 'SELECT')`, env.runtimeRole).Scan(&held); err != nil || held {
		t.Fatalf("the runtime role still holds SELECT on public.refresh_tokens (%v)", err)
	}
}
