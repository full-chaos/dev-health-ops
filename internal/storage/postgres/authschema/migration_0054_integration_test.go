//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"
)

// TestMigration0054RevokedReasonAdmitsOrgDeprovisioned: the column CHECK takes
// org_deprovisioned and still refuses a near miss and prose (CHAOS-12941).
func TestMigration0054RevokedReasonAdmitsOrgDeprovisioned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	if _, err := env.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'm54')`, m11Principal); err != nil {
		t.Fatal(err)
	}
	exec := func(ctx context.Context, sql string, args ...any) error {
		return guardedExec(ctx, env.admin, sql, args...)
	}
	for _, reason := range []string{"org_deprovisioned", "compromised", "domain_revoked", "owner_request"} {
		reason := reason
		if err := m11Insert(ctx, exec, &reason); err != nil {
			t.Errorf("listed revoked_reason %q was refused: %v", reason, err)
		}
	}
	for _, reason := range []string{"Org_deprovisioned", "org_deprovisioned_", "org deprovisioned", "organization_deprovisioned"} {
		reason := reason
		if err := m11Insert(ctx, exec, &reason); err == nil {
			t.Errorf("revoked_reason %q is outside the vocabulary but was stored", reason)
		}
	}
	var validated bool
	if err := env.admin.QueryRow(ctx, `SELECT convalidated FROM pg_constraint WHERE conname = 'credential_registry_revoked_reason_code' AND connamespace = 'auth'::regnamespace`).Scan(&validated); err != nil || !validated {
		t.Fatalf("credential_registry_revoked_reason_code validated = %v (%v)", validated, err)
	}
}
