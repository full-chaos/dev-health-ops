//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"
)

// TestMigration0021AdmitsTheLinkCredentialClass: credential_class
// admits link_credential and still refuses a class nobody defined.
func TestMigration0021AdmitsTheLinkCredentialClass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	var def string
	if err := env.admin.QueryRow(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conname = 'credential_registry_credential_class_closed'
		  AND conrelid = 'auth.credential_registry'::regclass`).Scan(&def); err != nil {
		t.Fatalf("credential_class CHECK: %v", err)
	}
	for _, class := range []string{"link_credential", "internal_svc_authsess_token", "refresh_credential"} {
		if !contains(def, "'"+class+"'") {
			t.Errorf("credential_class CHECK does not list %s: %s", class, def)
		}
	}
	if contains(def, "'link_credentials'") {
		t.Errorf("credential_class CHECK lists a class nobody defined: %s", def)
	}
}
