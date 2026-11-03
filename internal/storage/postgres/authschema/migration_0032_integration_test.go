//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"
)

// TestMigration0032AdmitsTheWorkloadBootstrapCredentialClass: credential_class
// admits workload_bootstrap_credential, keeps the 0030 values, and still
// refuses a class nobody defined.
func TestMigration0032AdmitsTheWorkloadBootstrapCredentialClass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	var def string
	if err := env.admin.QueryRow(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conname = 'credential_registry_credential_class_closed'
		  AND conrelid = 'auth.credential_registry'::regclass`).Scan(&def); err != nil {
		t.Fatalf("credential_class CHECK: %v", err)
	}
	for _, class := range []string{"workload_bootstrap_credential", "kubernetes_sa_token_exchange", "workload_token"} {
		if !contains(def, "'"+class+"'") {
			t.Errorf("credential_class CHECK does not list %s: %s", class, def)
		}
	}
	if contains(def, "'workload_bootstrap'") || contains(def, "'bootstrap_credential'") {
		t.Errorf("credential_class CHECK lists a class nobody defined: %s", def)
	}
}
