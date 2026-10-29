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

// No column is both in RegisterOrganizationGrants' memberships INSERT and in
// MembershipWriteGrants' INSERT (one grant set per privilege and column), and
// no key column is updatable.
func TestMembershipWriteGrantsDoNotRepeatTheInsertGrant(t *testing.T) {
	g := MembershipWriteGrants()
	for _, column := range g.Update {
		if column == "id" || column == "user_id" || column == "org_id" {
			t.Errorf("UPDATE of key column %q", column)
		}
	}
	for _, insert := range RegisterOrganizationGrants() {
		if insert.Table != g.Table {
			continue
		}
		for _, column := range g.Insert {
			for _, registered := range insert.Columns {
				if column == registered {
					t.Errorf("INSERT column %q is in both RegisterOrganizationGrants and MembershipWriteGrants", column)
				}
			}
		}
		return
	}
	t.Fatal("RegisterOrganizationGrants has no memberships row")
}
