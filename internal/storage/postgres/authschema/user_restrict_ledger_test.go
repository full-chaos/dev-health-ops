package authschema

import (
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

// Keys into auth.principals with ON DELETE RESTRICT that do not name a user as a referencing party: the row of the
// principal itself (deleted with it, or the principal is of another kind).
var principalOwnRowKeys = []string{
	"credential_projection.principal_id", "memberships.principal_id", "platform_role_assignments.principal_id",
	"push_sources.principal_id", "service_accounts.principal_id", "users.principal_id", "workloads.principal_id",
}

// The admin force delete is refused (409 reference_blocked) by exactly these keys; the accepted known-stricter row of
// ci/auth_exception_ledger.tsv names them. A new RESTRICT key into principals fails here until it is decided.
func TestForceDeleteBlockingKeysAreTheLedgerRowKeys(t *testing.T) {
	chain, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	table := regexp.MustCompile(`(?is)(?:CREATE TABLE|ALTER TABLE)\s+(?:ONLY\s+)?(?:\w+\.)?(\w+)`)
	column := regexp.MustCompile(`(?is)(\w+)\s+uuid[^,]*?REFERENCES\s+principals\s*\(id\)\s+ON DELETE RESTRICT`)
	foreign := regexp.MustCompile(`(?is)FOREIGN KEY\s*\((\w+)\)\s+REFERENCES\s+principals\s*\(id\)\s+ON DELETE RESTRICT`)
	found := map[string]bool{}
	for _, file := range chain {
		for _, statement := range strings.Split(file.SQL, ";") {
			t := table.FindStringSubmatch(statement)
			if t == nil {
				continue
			}
			for _, re := range []*regexp.Regexp{column, foreign} {
				for _, m := range re.FindAllStringSubmatch(statement, -1) {
					found[t[1]+"."+m[1]] = true
				}
			}
		}
	}
	var got []string
	for key := range found {
		if !slices.Contains(principalOwnRowKeys, key) {
			got = append(got, key)
		}
	}
	sort.Strings(got)
	want := []string{"platform_role_assignments.granted_by", "resource_grants.principal_id", "resource_grants.revoked_by", "service_accounts.owner_principal_id"}
	if !slices.Equal(got, want) {
		t.Fatalf("RESTRICT keys into auth.principals that block a user's erase = %v, want %v", got, want)
	}
	raw, err := os.ReadFile("../../../../ci/auth_exception_ledger.tsv")
	if err != nil {
		t.Fatal(err)
	}
	var row string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "known-stricter-force-delete-reference-blocked\t") {
			row = line
		}
	}
	if row == "" {
		t.Fatal("the known-stricter-force-delete-reference-blocked row is gone from the ledger")
	}
	for _, key := range want {
		if !strings.Contains(row, key) {
			t.Errorf("ledger row does not name the blocking key %s", key)
		}
	}
	if strings.Contains(row, "credential projection") {
		t.Error("ledger row names a credential projection: that key names an external_source principal, never a user")
	}
	if !strings.Contains(row, "\tpermanent\t") || !strings.Contains(row, "closed: accepted difference") {
		t.Error("ledger row is not a permanent row that states closed: accepted difference")
	}
}
