package contracts

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// credential-entry.v1 is a projection of credential_registry, so its member set
// is DERIVED from the producer's migrations (0003 creates the table, 0007 adds
// the TRD columns), never hand-picked, and compared in both directions.

const (
	credentialTableMigration = "internal/storage/postgres/authschema/migrations/0003_sessions_and_credentials.sql"
	credentialColumnsAdded   = "internal/storage/postgres/authschema/migrations/0007_credential_registry_trd_columns.sql"
)

// credentialNotOnTheWire: columns that never cross the boundary, with the reason.
var credentialNotOnTheWire = map[string]string{
	"secret_hash":    "the stored verifier of the secret: secret material, unrepresentable on the wire (TRD section 12: secret values are returned once)",
	"hash_algorithm": "describes how secret_hash was made; meaningless without it and not wire metadata",
}

// credentialDerived: wire members with no column, each with its derivation rule.
var credentialDerived = map[string]string{
	"status": "revoked_at set -> revoked; else expires_at <= now -> expired; else active",
}

var addColumnLine = regexp.MustCompile(`^\s*ADD COLUMN\s+([a-z_][a-z0-9_]*)\b`)

func registryColumns(t *testing.T) []string {
	t.Helper()
	read := func(rel string) string {
		raw, err := os.ReadFile(filepath.Join(testRoot(t), rel))
		if err != nil {
			t.Fatalf("reading %s: %v", rel, err)
		}
		return string(raw)
	}
	columns := tableBodyColumns(t, read(credentialTableMigration), "credential_registry")
	for _, line := range strings.Split(read(credentialColumnsAdded), "\n") {
		if match := addColumnLine.FindStringSubmatch(line); match != nil {
			columns = append(columns, match[1])
		}
	}
	sort.Strings(columns)
	return columns
}

func TestCredentialEntryMatchesTheRegistryColumnsInBothDirections(t *testing.T) {
	columns := registryColumns(t)
	if len(columns) < 15 {
		t.Fatalf("parsed only %d columns (%v): the parser, not the table, is probably wrong", len(columns), columns)
	}
	members := map[string]bool{}
	for _, m := range schemaPropertyNames(t, CredentialEntrySurface) {
		if m != "schema_version" {
			members[m] = true
		}
	}
	columnSet := map[string]bool{}
	for _, c := range columns {
		columnSet[c] = true
		_, excluded := credentialNotOnTheWire[c]
		switch {
		case members[c] && excluded:
			t.Errorf("column %s is both an entry member and listed as not on the wire", c)
		case !members[c] && !excluded:
			t.Errorf("column %s of credential_registry is neither a credential-entry.v1 member nor excluded with a reason", c)
		}
	}
	for m := range members {
		if _, derived := credentialDerived[m]; derived {
			if columnSet[m] {
				t.Errorf("%q is declared derived but is also a column", m)
			}
			continue
		}
		if !columnSet[m] {
			t.Errorf("entry member %q has no column and is not declared derived: the contract invented it", m)
		}
	}
	for c := range credentialNotOnTheWire {
		if !columnSet[c] {
			t.Errorf("exclusion %q names a column the table no longer has", c)
		}
	}
	for d := range credentialDerived {
		if !members[d] {
			t.Errorf("derived member %q is declared but absent from the schema", d)
		}
	}
}
