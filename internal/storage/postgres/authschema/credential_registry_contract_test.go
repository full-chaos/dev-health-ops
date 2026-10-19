package authschema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCredentialScopeShapeMatchesThePrincipalContract: the registry stores
// scopes that principal.v1 documents later carry, so the database's scope shape
// must be the contract's. A registry that accepts `read` projects a scope no
// principal document may carry.
func TestCredentialScopeShapeMatchesThePrincipalContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contracts", "auth", "v1", "jsonschema", "principal.v1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Credential struct {
				Properties struct {
					Scopes struct {
						Items struct {
							Pattern string `json:"pattern"`
						} `json:"items"`
						UniqueItems bool `json:"uniqueItems"`
					} `json:"scopes"`
				} `json:"properties"`
			} `json:"credential"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	scopes := schema.Properties.Credential.Properties.Scopes
	if scopes.Items.Pattern == "" {
		t.Fatal("principal.v1 declares no scope pattern; the comparison would be vacuous")
	}
	if !scopes.UniqueItems {
		t.Fatal("principal.v1 scopes are no longer a set; revisit credential_array_is_set")
	}
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var sql string
	for _, m := range migrations {
		if m.Version == 7 {
			sql = m.SQL
		}
	}
	if sql == "" {
		t.Fatal("migration 0007 is not embedded")
	}
	if !strings.Contains(sql, "VALUE ~ '"+scopes.Items.Pattern+"'") {
		t.Fatalf("migration 0007's credential_scope domain does not use principal.v1's scope pattern %q", scopes.Items.Pattern)
	}
	if !strings.Contains(sql, "credential_array_is_set(scopes)") {
		t.Fatal("scopes are not required to be a set")
	}
}
