package aclguard_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The columns the user erase sets to NULL (and so may UPDATE): a widened or narrowed list fails here first.
func TestTheUserEraseNullColumnsArePinned(t *testing.T) {
	want := []authschema.UserEraseNullColumn{
		{Table: "billing_audit_log", Column: "actor_id"},
		{Table: "impersonation_sessions", Column: "admin_user_id"},
		{Table: "impersonation_sessions", Column: "target_user_id"},
		{Table: "refunds", Column: "initiated_by"},
	}
	if got := authschema.UserEraseNullColumns(); !slices.Equal(got, want) {
		t.Errorf("UserEraseNullColumns = %v, want %v", got, want)
	}
}

// The user erase pass is least privilege: the table-level DELETE (public.users), UPDATE and SELECT by column list, and
// REVOKE ALL only to clear its own erase-only tables; no GRANT OPTION, no TRUNCATE, no REFERENCES, no ALL.
func TestTheUserEraseGrantPassIsLeastPrivilege(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal", "storage", "postgres", "authschema", "usererase_grant.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	grant := regexp.MustCompile("(?i)GRANT\\s+(\\w+)(\\s*\\(%s\\))?\\s+ON\\s+TABLE")
	var seen []string
	for _, m := range grant.FindAllStringSubmatch(text, -1) {
		seen = append(seen, strings.ToUpper(m[1])+strings.ReplaceAll(m[2], " ", ""))
	}
	if want := []string{"DELETE", "UPDATE(%s)", "SELECT(%s)"}; !slices.Equal(seen, want) {
		t.Fatalf("GRANT statements in usererase_grant.go: %v, want %v", seen, want)
	}
	revoke := regexp.MustCompile("(?i)REVOKE\\s+[^`]*")
	for _, m := range revoke.FindAllString(text, -1) {
		if m != "REVOKE ALL ON TABLE %s FROM %s" {
			t.Errorf("REVOKE form %q, want only REVOKE ALL ON TABLE %%s FROM %%s", m)
		}
	}
	for _, banned := range []string{`WITH GRANT OPTION`, `TRUNCATE`, `REFERENCES`, `ALL PRIVILEGES`, `GRANT ALL`, `CASCADE`} {
		if strings.Contains(strings.ToUpper(text), banned) {
			t.Errorf("usererase_grant.go contains %s", banned)
		}
	}
}
