package authschema

import (
	"reflect"
	"strings"
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

// A session reader's lost platform role read is a record of its own: named by
// the reader, with a digest apart from the runtime role's, retained only for
// the platform reader columns, and excused only for that reader.
func TestReaderPlatformReadNarrowingIsTheReaders(t *testing.T) {
	record := GrantRecord{Kind: "column", Object: "principals", Column: "kind", Privilege: "SELECT", Role: "devhealth_api"}
	runtime := record
	runtime.Role = ""
	if narrowingDigest([]GrantRecord{record}) == narrowingDigest([]GrantRecord{runtime}) {
		t.Error("a reader's loss has the runtime role's digest")
	}
	if !strings.Contains(record.String(), `session reader "devhealth_api"`) || strings.Contains(runtime.String(), "session reader") {
		t.Errorf("records: %s / %s", record, runtime)
	}
	if !runtime.less(record) || record.less(runtime) {
		t.Error("runtime role records sort before reader records")
	}
	for _, grant := range PlatformRoleReaderColumns() {
		for _, column := range grant.Columns {
			if !isPlatformReaderColumn(grant.Table, column) {
				t.Errorf("%s.%s is not a platform reader column", grant.Table, column)
			}
		}
	}
	for table, column := range map[string]string{"principals": "display_name", "platform_role_assignments": "granted_by", "sessions": "id"} {
		if isPlatformReaderColumn(table, column) {
			t.Errorf("%s.%s is a platform reader column", table, column)
		}
	}
	excused := Violation{Kind: "session_reader", Schema: "auth", Object: "principals.kind", Privilege: "SELECT",
		Detail: "session reader devhealth_api holds an undeclared column privilege"}
	kept := []Violation{
		{Kind: "session_reader", Schema: "auth", Object: "principals.kind", Privilege: "SELECT",
			Detail: "session reader devhealth_query holds an undeclared column privilege"},
		{Kind: "session_reader", Schema: "auth", Object: "principals.display_name", Privilege: "SELECT",
			Detail: "session reader devhealth_api holds an undeclared column privilege"},
		{Kind: "column", Schema: publicSchema, Object: "principals.kind", Privilege: "SELECT"},
	}
	if got := excuseRetained(append([]Violation{excused}, kept...), []GrantRecord{record}); !reflect.DeepEqual(got, kept) {
		t.Errorf("excuseRetained = %v, want %v", got, kept)
	}
}
