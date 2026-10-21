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
		"memberships": {"user_id", "org_id", "role"},
		"sso_providers": {"id", "org_id", "protocol", "status", "allow_idp_initiated", "auto_provision_users",
			"config", "allowed_domains", "encrypted_secrets", "is_default", "created_at"},
		"organizations":          {"id", "slug", "name", "is_active", "tier"},
		"saml_assertion_replays": {"provider_id", "assertion_id", "expires_at"},
		"org_licenses":           {"org_id", "tier", "features_override"},
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
