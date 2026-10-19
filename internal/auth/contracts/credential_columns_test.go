package contracts

import (
	"encoding/json"
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
	credentialTableMigration      = "internal/storage/postgres/authschema/migrations/0003_sessions_and_credentials.sql"
	credentialColumnsAdded        = "internal/storage/postgres/authschema/migrations/0007_credential_registry_trd_columns.sql"
	credentialWireShapesMigration = "internal/storage/postgres/authschema/migrations/0008_credential_registry_wire_shapes.sql"
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

// producerChecksForEntryConditionals names, for every conditional the entry
// schema states over columns, the producer constraint that enforces the same
// rule (or why none is needed). A conditional added to the schema without a
// decision here fails the test: a rule the wire states and the table does not
// is a row nobody can project (codex executed two of them against 0008).
var producerChecksForEntryConditionals = map[string]struct {
	constraint string // name that must appear in the migrations; empty = derived
	reason     string
}{
	"domain_owner=platform": {constraint: "credential_registry_external_reference_matches_owner"},
	"status=revoked":        {constraint: "credential_registry_revoked_pair"},
	"status=expired":        {reason: "status is derived from expires_at, so the pairing cannot be stored inconsistently"},
}

func TestEveryEntryConditionalHasAProducerCheck(t *testing.T) {
	raw, err := os.ReadFile(SchemaPath(testRoot(t), CredentialEntrySurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		AllOf []struct {
			If struct {
				Properties map[string]struct {
					Const string `json:"const"`
				} `json:"properties"`
			} `json:"if"`
		} `json:"allOf"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if len(schema.AllOf) == 0 {
		t.Fatal("the entry schema has no conditionals; the sweep would be vacuous")
	}
	var sql strings.Builder
	for _, rel := range []string{credentialTableMigration, credentialColumnsAdded, credentialWireShapesMigration} {
		raw, err := os.ReadFile(filepath.Join(testRoot(t), rel))
		if err != nil {
			t.Fatal(err)
		}
		sql.Write(raw)
	}
	seen := map[string]bool{}
	for _, c := range schema.AllOf {
		for property, value := range c.If.Properties {
			key := property + "=" + value.Const
			seen[key] = true
			decision, ok := producerChecksForEntryConditionals[key]
			if !ok {
				t.Errorf("entry conditional %q has no producer decision: add a constraint and name it here", key)
				continue
			}
			if decision.constraint != "" && !strings.Contains(sql.String(), decision.constraint) {
				t.Errorf("entry conditional %q expects the producer constraint %s, which no migration defines", key, decision.constraint)
			}
		}
	}
	for key := range producerChecksForEntryConditionals {
		if !seen[key] {
			t.Errorf("a producer decision names the conditional %q, which the schema no longer has", key)
		}
	}
}
