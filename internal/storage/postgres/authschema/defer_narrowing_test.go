package authschema

import (
	"reflect"
	"testing"
)

func TestUsersOwnedPublicGrantIsExactlyWhatTheSwitchGives(t *testing.T) {
	for _, tc := range []struct {
		g    GrantRecord
		want bool
	}{
		{GrantRecord{Kind: "column", Object: "users", Column: "password_hash", Privilege: "UPDATE"}, true},
		{GrantRecord{Kind: "column", Object: "users", Column: "last_login_at", Privilege: "UPDATE"}, true},
		{GrantRecord{Kind: "column", Object: "users", Column: "email", Privilege: "INSERT"}, true},
		{GrantRecord{Kind: "column", Object: "organizations", Column: "slug", Privilege: "INSERT"}, true},
		{GrantRecord{Kind: "column", Object: "memberships", Column: "role", Privilege: "INSERT"}, true},
		{GrantRecord{Kind: "relation", Object: "password_reset_tokens", Privilege: "DELETE"}, true},
		{GrantRecord{Kind: "column", Object: "email_verification_tokens", Column: "id", Privilege: "SELECT"}, true},
		{GrantRecord{Kind: "column", Object: "users", Column: "full_name", Privilege: "UPDATE"}, false},
		{GrantRecord{Kind: "column", Object: "users", Column: "last_login_at", Privilege: "INSERT"}, false},
		{GrantRecord{Kind: "column", Object: "users", Column: "password_hash", Privilege: "UPDATE", Grantable: true}, false},
		{GrantRecord{Kind: "relation", Object: "users", Privilege: "UPDATE"}, false},
		{GrantRecord{Kind: "relation", Object: "memberships", Privilege: "DELETE"}, true},
		{GrantRecord{Kind: "column", Object: "memberships", Column: "role", Privilege: "UPDATE"}, true},
		{GrantRecord{Kind: "column", Object: "memberships", Column: "invited_by_id", Privilege: "INSERT"}, true},
		{GrantRecord{Kind: "relation", Object: "memberships", Privilege: "TRUNCATE"}, false},
		{GrantRecord{Kind: "column", Object: "memberships", Column: "user_id", Privilege: "UPDATE"}, false},
		{GrantRecord{Kind: "relation", Object: "organizations", Privilege: "DELETE"}, false},
		{GrantRecord{Kind: "column", Object: "password_reset_tokens", Column: "token_hash", Privilege: "SELECT"}, false},
	} {
		if got := usersOwnedPublicGrant(tc.g); got != tc.want {
			t.Errorf("usersOwnedPublicGrant(%s) = %v, want %v", tc.g, got, tc.want)
		}
	}
}

func TestExcuseRetainedDropsOnlyTheRetainedRecords(t *testing.T) {
	retained := []GrantRecord{
		{Kind: "column", Object: "users", Column: "password_hash", Privilege: "UPDATE"},
		{Kind: "relation", Object: "password_reset_tokens", Privilege: "DELETE"},
	}
	excused := []Violation{
		{Kind: "column", Schema: publicSchema, Object: "users.password_hash", Privilege: "UPDATE"},
		{Kind: "relation", Schema: publicSchema, Object: "password_reset_tokens", Privilege: "DELETE"},
	}
	kept := []Violation{
		{Kind: "column", Schema: publicSchema, Object: "users.full_name", Privilege: "UPDATE"},
		{Kind: "column", Schema: publicSchema, Object: "users.password_hash", Privilege: "INSERT"},
		{Kind: "relation", Schema: publicSchema, Object: "users", Privilege: "UPDATE"},
		{Kind: "relation", Schema: "auth", Object: "password_reset_tokens", Privilege: "DELETE"},
		{Kind: "relation", Schema: publicSchema, Object: "email_verification_tokens", Privilege: "DELETE"},
		{Kind: "role_membership", Object: "owner"},
	}
	if got := excuseRetained(append(append([]Violation{}, excused...), kept...), retained); !reflect.DeepEqual(got, kept) {
		t.Errorf("excuseRetained = %v, want %v", got, kept)
	}
	if got := excuseRetained(kept, nil); !reflect.DeepEqual(got, kept) {
		t.Errorf("excuseRetained without retained records = %v, want %v", got, kept)
	}
}
