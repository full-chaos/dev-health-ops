//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestLegacyRefreshTokenGrantReadsAndRevokesOnly: over the real ops schema the
// runtime role reads the declared public.refresh_tokens columns and writes
// revoked_at, and nothing else; a wider or a missing privilege is a posture
// violation.
func TestLegacyRefreshTokenGrantReadsAndRevokesOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	grant := LegacyRefreshTokenGrant()
	if grant.RemovedBy != "CHAOS-10408" {
		t.Fatalf("the temporary grant names %q as its removal, want CHAOS-10408", grant.RemovedBy)
	}

	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("posture after Apply is not clean:\n%s", describe(found))
	}
	for name, statement := range map[string]string{
		"read the declared columns": `SELECT token_hash, user_id, org_id, family_id, expires_at, revoked_at FROM public.refresh_tokens FOR UPDATE`,
		"revoke":                    `UPDATE public.refresh_tokens SET revoked_at = now() WHERE family_id = gen_random_uuid() AND revoked_at IS NULL`,
	} {
		if err := env.runtimeStatement(ctx, statement); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, statement := range map[string]string{
		"read successor_jti":  `SELECT successor_jti FROM public.refresh_tokens`,
		"read every column":   `SELECT * FROM public.refresh_tokens`,
		"write successor_jti": `UPDATE public.refresh_tokens SET successor_jti = NULL`,
		"write expires_at":    `UPDATE public.refresh_tokens SET expires_at = now()`,
		"insert":              `INSERT INTO public.refresh_tokens DEFAULT VALUES`,
		"delete":              `DELETE FROM public.refresh_tokens`,
		"write users":         `UPDATE public.users SET token_version = 0`,
	} {
		if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: err = %v, want permission denied", name, err)
		}
	}

	role := Quote(mustValidatedIdentifier(env.runtimeRole))
	if _, err := env.migration.Exec(ctx, `GRANT UPDATE (successor_jti) ON public.refresh_tokens TO `+role); err != nil {
		t.Fatal(err)
	}
	if found := describe(env.violations(t, ctx)); !strings.Contains(found, "refresh_tokens.successor_jti") {
		t.Errorf("a wider refresh_tokens grant was not reported:\n%s", found)
	}
	if _, err := env.migration.Exec(ctx, `REVOKE UPDATE (successor_jti, revoked_at) ON public.refresh_tokens FROM `+role); err != nil {
		t.Fatal(err)
	}
	if found := describe(env.violations(t, ctx)); !strings.Contains(found, "refresh_tokens.revoked_at") || !strings.Contains(found, "CHAOS-10408") {
		t.Errorf("a missing revoked_at grant was not reported with its removal ticket:\n%s", found)
	}
}
