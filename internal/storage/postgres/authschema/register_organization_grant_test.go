package authschema

import (
	"reflect"
	"testing"
)

func TestRegisterOrganizationGrantIsTheReviewedColumns(t *testing.T) {
	got := map[string][]string{}
	for _, grant := range RegisterOrganizationGrants() {
		got[grant.Table] = grant.Columns
	}
	want := map[string][]string{
		"organizations": {"id", "slug", "name", "settings", "tier", "managed_by", "is_active", "created_at", "updated_at"},
		"memberships":   {"id", "user_id", "org_id", "role", "joined_at", "created_at", "updated_at"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RegisterOrganizationGrants = %v, want %v", got, want)
	}
	insert, update := UsersWriteGrant()
	if len(insert) != 14 || len(update) != 5 {
		t.Fatalf("the public.users write grant changed: %v / %v", insert, update)
	}
}
