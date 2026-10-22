package authschema

import (
	"reflect"
	"strings"
	"testing"
)

// The reviewed oracle of the integration test and the production manifest must
// say the same thing; a column added to one and not the other fails here.
func TestPublicReadColumnsMatchTheReviewedList(t *testing.T) {
	want := map[string][]string{
		"users": {"id", "email", "password_hash", "full_name", "username", "auth_provider", "auth_provider_id",
			"is_active", "is_verified", "is_superuser", "token_version"},
		"memberships": {"user_id", "org_id", "role", "joined_at", "created_at"},
		"sso_providers": {"id", "org_id", "protocol", "status", "allow_idp_initiated", "auto_provision_users",
			"config", "allowed_domains", "encrypted_secrets", "is_default", "created_at"},
		"organizations":                {"id", "slug", "name", "is_active", "tier"},
		"saml_assertion_replays":       {"provider_id", "assertion_id", "expires_at"},
		"org_licenses":                 {"org_id", "tier", "features_override"},
		"internal_service_credentials": {"id", "service_name", "token_hash", "scopes", "expires_at", "revoked_at"},
	}
	got := map[string][]string{}
	for _, entry := range PublicReadColumns() {
		if _, dup := got[entry.Table]; dup {
			t.Errorf("table %s listed twice", entry.Table)
		}
		got[entry.Table] = entry.Columns
		for _, name := range append([]string{entry.Table}, entry.Columns...) {
			if err := ValidateIdentifier(name); err != nil {
				t.Errorf("manifest name %q is not a valid identifier: %v", name, err)
			}
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PublicReadColumns = %v, want %v", got, want)
	}
}

// The bound-value form of the system-schema filter used by publicreads.go must
// say what the shared text form says.
func TestSystemSchemaFilterFormsAgree(t *testing.T) {
	quoted := make([]string, len(systemSchemaNames))
	for i, name := range systemSchemaNames {
		quoted[i] = "'" + name + "'"
	}
	want := "n.nspname NOT IN (" + strings.Join(quoted, ", ") + ") AND n.nspname NOT LIKE '" + systemSchemaPattern + "'"
	if systemSchemaFilter != want {
		t.Errorf("systemSchemaFilter = %q, bound form says %q", systemSchemaFilter, want)
	}
}

// The write grant on go-api's refresh tokens is temporary and revoke-only:
// SELECT on what the legacy exchange reads, UPDATE on revoked_at alone, and
// CHAOS-10408 as its removal. This test goes with the grant.
func TestLegacyRefreshTokenGrantIsRevokeOnlyAndNamesItsRemoval(t *testing.T) {
	grant := LegacyRefreshTokenGrant()
	if grant.Table != "refresh_tokens" || grant.RemovedBy != "CHAOS-10408" {
		t.Fatalf("grant on %q removed by %q, want refresh_tokens removed by CHAOS-10408", grant.Table, grant.RemovedBy)
	}
	if strings.Join(grant.Update, ",") != "revoked_at" {
		t.Fatalf("UPDATE on %v, want revoked_at only", grant.Update)
	}
	if got := strings.Join(grant.Select, ","); got != "token_hash,user_id,org_id,family_id,expires_at,revoked_at" {
		t.Fatalf("SELECT on %s", got)
	}
	for _, entry := range PublicReadColumns() {
		if entry.Table == grant.Table {
			t.Fatal("refresh_tokens is in PublicReadColumns as well; the read-only manifest would hide the write grant")
		}
	}
}

// The switch-gated write access to public.users is the reviewed column list
// and nothing wider: no token_version update, no email or is_active update.
func TestUsersWriteGrantIsTheReviewedColumns(t *testing.T) {
	insert, update := UsersWriteGrant()
	wantInsert := []string{"id", "email", "username", "password_hash", "full_name", "auth_provider", "auth_provider_id",
		"is_active", "is_verified", "is_superuser", "token_version", "created_at", "updated_at"}
	wantUpdate := []string{"password_hash", "is_verified", "updated_at"}
	if !reflect.DeepEqual(insert, wantInsert) || !reflect.DeepEqual(update, wantUpdate) {
		t.Fatalf("UsersWriteGrant = %v / %v, want %v / %v", insert, update, wantInsert, wantUpdate)
	}
	for _, name := range append(append([]string{}, insert...), update...) {
		if err := ValidateIdentifier(name); err != nil {
			t.Errorf("column %q is not a valid identifier: %v", name, err)
		}
	}
	for _, column := range update {
		if column == "token_version" || column == "email" || column == "is_active" {
			t.Errorf("UPDATE must not include %s", column)
		}
	}
}
