package authschema

import (
	"reflect"
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
			"config", "allowed_domains", "encrypted_secrets"},
		"organizations":          {"id", "slug", "name", "is_active"},
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
