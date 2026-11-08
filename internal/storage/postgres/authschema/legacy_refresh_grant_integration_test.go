//go:build integration

package authschema

import (
	"bytes"
	"context"
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

	if _, err := Apply(ctx, env.migration, env.options(&bytes.Buffer{})); err != nil {
		t.Fatalf("Apply over the old grant: %v", err)
	}
	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("posture after the second Apply is not clean:\n%s", describe(found))
	}
	denied("after the second Apply")
}
