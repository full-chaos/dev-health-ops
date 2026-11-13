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

// The grants the SSO providers switch (AUTH_SSO_PROVIDERS_OWNED, default off) gives the auth runtime role on
// public.sso_providers: one row per GRANT. A widened, narrowed or reordered list fails here first.
func TestTheSSOProviderOwnGrantIsPinnedToItsColumns(t *testing.T) {
	g := authschema.SSOProviderOwn()
	rows := map[string][]string{
		"INSERT": {"id", "org_id", "name", "protocol", "status", "default_role", "is_default", "allow_idp_initiated",
			"auto_provision_users", "config", "encrypted_secrets", "allowed_domains", "created_at", "updated_at"},
		"UPDATE": {"name", "status", "default_role", "is_default", "allow_idp_initiated", "auto_provision_users", "config",
			"encrypted_secrets", "allowed_domains", "updated_at"},
		"SELECT": {"name", "last_metadata_sync_at", "last_login_at", "last_error", "last_error_at", "updated_at"},
	}
	for privilege, got := range map[string][]string{"INSERT": g.Insert, "UPDATE": g.Update, "SELECT": g.Select} {
		if !slices.Equal(got, rows[privilege]) {
			t.Errorf("SSOProviderOwn %s = %v, want %v", privilege, got, rows[privilege])
		}
	}
	if !g.Delete {
		t.Error("SSOProviderOwn has no DELETE: the delete route needs the table-level DELETE")
	}
	for _, column := range []string{"id", "org_id", "protocol", "created_at"} {
		if slices.Contains(g.Update, column) {
			t.Errorf("UPDATE %s is granted: the row identity is set by the one INSERT", column)
		}
	}
}

// The own-grant pass is the one file that grants these privileges: INSERT, UPDATE and SELECT by column list, DELETE on the
// table, no ALL, no TRUNCATE, no REFERENCES, no GRANT OPTION, and no REVOKE (the public read pass owns the table's REVOKE ALL).
func TestTheSSOProviderOwnGrantPassIsLeastPrivilege(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal", "storage", "postgres", "authschema", "ssoproviderown_grants.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	grant := regexp.MustCompile("(?i)GRANT\\s+(\\w+)(\\s*\\(%s\\))?")
	var seen []string
	for _, m := range grant.FindAllStringSubmatch(text, -1) {
		seen = append(seen, strings.ToUpper(m[1])+strings.ReplaceAll(m[2], " ", ""))
	}
	want := []string{"INSERT(%s)", "UPDATE(%s)", "SELECT(%s)", "DELETE"}
	if !slices.Equal(seen, want) {
		t.Fatalf("GRANT statements in ssoproviderown_grants.go: %v, want %v", seen, want)
	}
	for _, banned := range []string{`WITH GRANT OPTION`, `TRUNCATE`, `REFERENCES`, `REVOKE`, `ALL PRIVILEGES`, `GRANT ALL`} {
		if strings.Contains(strings.ToUpper(text), banned) {
			t.Errorf("ssoproviderown_grants.go contains %s", banned)
		}
	}
}
