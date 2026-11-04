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

func TestReaderAttributeViolationsNameEachRiskyAttribute(t *testing.T) {
	if got := readerAttributeViolations("session_reader", "r", readerRoleAttributes{}); len(got) != 0 {
		t.Fatalf("plain reader: %v", got)
	}
	for name, attrs := range map[string]readerRoleAttributes{
		"SUPERUSER":   {Superuser: true},
		"CREATEROLE":  {CreateRole: true},
		"CREATEDB":    {CreateDB: true},
		"BYPASSRLS":   {BypassRLS: true},
		"REPLICATION": {Replication: true},
	} {
		got := readerAttributeViolations("session_reader", "r", attrs)
		if len(got) != 1 || got[0].Privilege != name || got[0].Object != "r" || got[0].Kind != "session_reader" {
			t.Errorf("%s: got %+v", name, got)
		}
	}
}

func TestReaderMembershipViolationsKeepOnlyReachableOnlyRoles(t *testing.T) {
	memberships := []Violation{
		{Kind: "role_membership", Object: "auth_runtime", SystemRole: true},
		{Kind: "role_membership", Object: "inherited_ctx", SystemRole: false},
	}
	got := readerMembershipViolations("session_reader", "r", memberships)
	if len(got) != 1 || got[0].Object != "auth_runtime" || got[0].Privilege != "MEMBER" || got[0].Kind != "session_reader" {
		t.Fatalf("got %+v", got)
	}
	if got := readerMembershipViolations("session_reader", "r", nil); len(got) != 0 {
		t.Fatalf("no memberships: %v", got)
	}
}
