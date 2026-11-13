package authschema

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestGrantOptionGapRefusalNamesVerbAndBothRoles(t *testing.T) {
	err := describeGrantOptionGaps("om_migrator", []string{"om_old"}, []grantOptionGap{{Table: "users", Privilege: "UPDATE", Column: "email"}})
	if !errors.Is(err, ErrMigratorGrantOptionMissing) || !errors.Is(err, ErrMigrationFailed) {
		t.Fatalf("error %v does not wrap the sentinels", err)
	}
	for _, want := range []string{"auth-migrate owner-move -grants-only", "dry run first, then -apply", "om_migrator", "om_old", "nothing was granted"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not contain %q:\n%v", want, err)
		}
	}
}

func TestGrantOptionGapListIsSortedCompleteAndFormatted(t *testing.T) {
	gaps := []grantOptionGap{
		{Table: "users", Privilege: "UPDATE", Column: "username"},
		{Table: "memberships", Privilege: "DELETE"},
		{Table: "users", Privilege: "INSERT", Column: "id"},
		{Table: "users", Privilege: "UPDATE", Column: "avatar_url"},
		{Table: "audit_logs", Privilege: "INSERT", Column: "id"},
	}
	text := describeGrantOptionGaps("m", []string{"b", "a"}, gaps).Error()
	var got []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "  - ") {
			got = append(got, strings.TrimPrefix(line, "  - "))
		}
	}
	want := []string{
		"INSERT (id) ON public.audit_logs",
		"DELETE ON public.memberships",
		"INSERT (id) ON public.users",
		"UPDATE (avatar_url) ON public.users",
		"UPDATE (username) ON public.users",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("gap lines %q, want %q", got, want)
	}
	if !strings.Contains(text, "(a, b)") {
		t.Errorf("owners are not listed sorted: %s", text)
	}
	if gaps[0].Table != "users" || gaps[0].Column != "username" {
		t.Error("the caller's slice was reordered")
	}
}

func TestPublicPrivilegeSetForSwitchOffDropsOnlyTheOwnedGrants(t *testing.T) {
	if !slices.EqualFunc(PublicPrivilegeSet(), publicPrivilegeSetFor(true, true), func(a, b PublicPrivilege) bool {
		return a.Table == b.Table && a.Privilege == b.Privilege && slices.Equal(a.Columns, b.Columns)
	}) {
		t.Fatal("PublicPrivilegeSet is not the switch-on set")
	}
	for _, entry := range publicPrivilegeSetFor(false, false) {
		if entry.Table == "users" && entry.Privilege != "SELECT" {
			t.Errorf("switch off still needs %s on users", entry.Privilege)
		}
		if entry.Privilege == "DELETE" {
			t.Errorf("switch off still needs table-level DELETE on %s", entry.Table)
		}
	}
	var sawAudit bool
	for _, entry := range publicPrivilegeSetFor(false, false) {
		if entry.Table == publicAuditTable && entry.Privilege == "INSERT" {
			sawAudit = true
		}
	}
	if !sawAudit {
		t.Error("switch off dropped the ungated audit INSERT")
	}
}
