package authschema

import (
	"reflect"
	"testing"
)

func TestMembershipWriteGrantsAreTheReviewedColumns(t *testing.T) {
	want := MembershipWriteGrant{
		Table:  "memberships",
		Delete: true,
		Select: []string{"id"},
		Update: []string{"role", "updated_at"},
		Insert: []string{"invited_by_id"},
	}
	if got := MembershipWriteGrants(); !reflect.DeepEqual(got, want) {
		t.Fatalf("MembershipWriteGrants = %+v, want %+v", got, want)
	}
	for _, column := range append(append([]string{want.Table}, want.Select...), want.Update...) {
		if err := ValidateIdentifier(column); err != nil {
			t.Errorf("identifier %q: %v", column, err)
		}
	}
}

// The INSERT the projection needs is RegisterOrganizationGrants' and nothing
// here repeats it: one grant set per privilege.
func TestMembershipWriteGrantsDoNotRepeatTheInsertGrant(t *testing.T) {
	g := MembershipWriteGrants()
	for _, insert := range RegisterOrganizationGrants() {
		if insert.Table != g.Table {
			continue
		}
		for _, column := range g.Update {
			if column == "id" || column == "user_id" || column == "org_id" {
				t.Errorf("UPDATE of key column %q", column)
			}
		}
		return
	}
	t.Fatal("RegisterOrganizationGrants has no memberships row")
}
