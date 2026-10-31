//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"
)

// TestMigration0037AdmitsTheACRBrowserRefreshTokenClass: credential_class
// admits acr_browser_refresh_token, keeps the 0036 values, and still refuses a
// class nobody defined.
func TestMigration0037AdmitsTheACRBrowserRefreshTokenClass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	var def string
	if err := env.admin.QueryRow(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conname = 'credential_registry_credential_class_closed'
		  AND conrelid = 'auth.credential_registry'::regclass`).Scan(&def); err != nil {
		t.Fatalf("credential_class CHECK: %v", err)
	}
	for _, class := range []string{"acr_browser_refresh_token", "acr_client_credential", "acr_device_flow_code", "workload_bootstrap_credential", "refresh_credential"} {
		if !contains(def, "'"+class+"'") {
			t.Errorf("credential_class CHECK does not list %s: %s", class, def)
		}
	}
	for _, retired := range []string{"legacy_ingest_api_key", "legacy_ingest_hmac_signature"} {
		if contains(def, "'"+retired+"'") {
			t.Errorf("credential_class CHECK lists %s, which 0036 retired: %s", retired, def)
		}
	}
	if contains(def, "'acr_refresh_token'") || contains(def, "'browser_refresh_token'") {
		t.Errorf("credential_class CHECK lists a class nobody defined: %s", def)
	}
}
