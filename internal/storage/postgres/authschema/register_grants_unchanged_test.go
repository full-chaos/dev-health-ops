package authschema

import (
	"reflect"
	"testing"
)

func TestUsersOwnedGrantsAreUnchangedByRegister(t *testing.T) {
	insert, update := UsersWriteGrant()
	if !reflect.DeepEqual(insert, []string{"id", "email", "username", "password_hash", "full_name", "auth_provider", "auth_provider_id",
		"is_active", "is_verified", "is_superuser", "token_version", "created_at", "updated_at"}) ||
		!reflect.DeepEqual(update, []string{"password_hash", "is_verified", "token_version", "updated_at"}) {
		t.Fatalf("the public.users write grant changed: %v / %v", insert, update)
	}
	reads := map[string][]string{}
	for _, grant := range PublicReadColumns() {
		reads[grant.Table] = grant.Columns
	}
	if !reflect.DeepEqual(reads["memberships"], []string{"user_id", "org_id", "role", "joined_at", "created_at"}) ||
		!reflect.DeepEqual(reads["organizations"], []string{"id", "slug", "name", "is_active", "tier"}) {
		t.Fatalf("the memberships or organizations read grant changed: %v", reads)
	}
	if got := LegacyRefreshTokenGrant(); got.RemovedBy != "CHAOS-10408" || !reflect.DeepEqual(got.Update, []string{"revoked_at"}) {
		t.Fatalf("the refresh token grant changed: %+v", got)
	}
}
