package authschema

import (
	"regexp"
	"strings"
	"testing"
)

const domainOwnedCheck = "credential_registry_domain_owned_fingerprint_only"

func domainOwnedMigration(t *testing.T) Migration {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var found []Migration
	for _, m := range migrations {
		if strings.Contains(m.SQL, "ADD CONSTRAINT "+domainOwnedCheck) {
			found = append(found, m)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d migrations add %s, want exactly 1", len(found), domainOwnedCheck)
	}
	return found[0]
}

func sqlCode(sql string) string {
	return regexp.MustCompile(`(?m)^--.*$`).ReplaceAllString(sql, "")
}

// TestDomainOwnedRegistryRowsStoreAFingerprintAndNoHash: the CHECK allows a
// non-platform row only in the fingerprint-only form (algorithm 'fingerprint',
// stored bytes = the public fingerprint) and keeps the 'fingerprint' algorithm
// out of platform rows.
func TestDomainOwnedRegistryRowsStoreAFingerprintAndNoHash(t *testing.T) {
	code := sqlCode(domainOwnedMigration(t).SQL)
	for _, want := range []string{
		"domain_owner = 'platform'",
		"hash_algorithm <> 'fingerprint'",
		"hash_algorithm = 'fingerprint'",
		"fingerprint IS NOT NULL",
		"secret_hash = decode(fingerprint, 'hex')",
		"VALIDATE CONSTRAINT " + domainOwnedCheck,
	} {
		if !strings.Contains(code, want) {
			t.Errorf("the migration does not contain %q", want)
		}
	}
	if got := strings.Count(code, "secret_hash = decode(fingerprint, 'hex')"); got != 2 {
		t.Errorf("the pre-check and the CHECK must state the same rule: found %d copies, want 2", got)
	}
	for _, forbidden := range []string{"DROP ", "UPDATE ", "DELETE ", "INSERT ", "ADD COLUMN", "digest(", "sha256(", "md5("} {
		if strings.Contains(code, forbidden) {
			t.Errorf("the migration contains %q", forbidden)
		}
	}
}

// TestDomainOwnedMigrationRefusesWithCountOnly: the pre-check names the
// constraint and a count and never selects row data.
func TestDomainOwnedMigrationRefusesWithCountOnly(t *testing.T) {
	code := sqlCode(domainOwnedMigration(t).SQL)
	if !strings.Contains(code, "SELECT count(*) INTO violating FROM credential_registry") {
		t.Error("the pre-check does not count violating rows")
	}
	if !strings.Contains(code, "credential_registry_domain_owned_fingerprint_only has % violating row(s)") {
		t.Error("the refusal does not name the constraint and the count")
	}
	for _, leak := range []string{"secret_hash::", "encode(secret_hash", "fingerprint::", "%L", "string_agg"} {
		if strings.Contains(code, leak) {
			t.Errorf("the refusal path can print row data: %q", leak)
		}
	}
}
