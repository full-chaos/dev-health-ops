package authschema

import (
	"reflect"
	"testing"
)

// The session readers' grant is the reviewed one, written out here
// independently of SessionReaderColumns.
func TestSessionReaderColumnsAreTheReviewedOnes(t *testing.T) {
	want := ColumnGrant{Table: "sessions", Columns: []string{"id", "principal_id", "revoked_at", "legacy_token_version"}}
	if got := SessionReaderColumns(); !reflect.DeepEqual(got, want) {
		t.Fatalf("SessionReaderColumns() = %+v, want %+v", got, want)
	}
}

func TestSessionReaderRolesAreValidated(t *testing.T) {
	base := Options{Schema: "auth", RuntimeRole: "auth_runtime"}
	for name, readers := range map[string][]string{
		"runtime role":   {"auth_runtime"},
		"migration role": {"auth_migrator"},
		"duplicate":      {"devhealth_api", "devhealth_api"},
		"bad name":       {"Bad-Name"},
	} {
		options := base
		options.SessionReaderRoles = readers
		if err := options.Validate("auth_migrator"); err == nil {
			t.Errorf("%s: %v accepted", name, readers)
		}
	}
	options := base
	options.SessionReaderRoles = []string{"devhealth_api", "devhealth_query"}
	if err := options.Validate("auth_migrator"); err != nil {
		t.Errorf("two distinct readers: %v", err)
	}
}

// The platform role read is the reviewed one (the columns of
// platformrole.HoldsSQL), held only while the switch is on.
func TestPlatformRoleReaderColumnsAreTheReviewedOnes(t *testing.T) {
	platform := []ColumnGrant{
		{Table: "platform_role_assignments", Columns: []string{"principal_id", "role_key", "expires_at"}},
		{Table: "principals", Columns: []string{"id", "kind", "disabled_at"}},
	}
	if got := PlatformRoleReaderColumns(); !reflect.DeepEqual(got, platform) {
		t.Fatalf("PlatformRoleReaderColumns() = %+v, want %+v", got, platform)
	}
	session := ColumnGrant{Table: "sessions", Columns: []string{"id", "principal_id", "revoked_at", "legacy_token_version"}}
	if got := SessionReaderReads(false); !reflect.DeepEqual(got, []ColumnGrant{session}) {
		t.Errorf("SessionReaderReads(false) = %+v, want the session read alone", got)
	}
	if got, want := SessionReaderReads(true), append([]ColumnGrant{session}, platform...); !reflect.DeepEqual(got, want) {
		t.Errorf("SessionReaderReads(true) = %+v, want %+v", got, want)
	}
}
