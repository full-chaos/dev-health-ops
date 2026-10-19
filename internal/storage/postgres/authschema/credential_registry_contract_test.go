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

// TestCredentialClassCheckEqualsThePrincipalContractsVocabulary: migration 0008
// closes credential_class to the vocabulary principal.v1 carries, so a stored
// class always has a wire name. The list is copied into SQL, so it is compared
// here, both directions.
func TestCredentialClassCheckEqualsThePrincipalContractsVocabulary(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contracts", "auth", "v1", "jsonschema", "principal.v1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Credential struct {
				Properties struct {
					Class struct {
						Enum []string `json:"enum"`
					} `json:"class"`
				} `json:"properties"`
			} `json:"credential"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	want := schema.Properties.Credential.Properties.Class.Enum
	if len(want) == 0 {
		t.Fatal("principal.v1 declares no credential classes; the comparison would be vacuous")
	}
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var sql string
	for _, m := range migrations {
		if m.Version == 8 {
			sql = m.SQL
		}
	}
	start := strings.Index(sql, "credential_registry_credential_class_closed")
	if start < 0 {
		t.Fatal("migration 0008 does not close credential_class")
	}
	body := sql[start:]
	body = body[strings.Index(body, "IN (")+len("IN ("):]
	body = body[:strings.Index(body, ")")]
	got := map[string]bool{}
	for _, part := range strings.Split(body, ",") {
		got[strings.Trim(strings.TrimSpace(part), "'")] = true
	}
	for _, class := range want {
		if !got[class] {
			t.Errorf("principal.v1 class %q is missing from the 0008 CHECK", class)
		}
		delete(got, class)
	}
	for extra := range got {
		t.Errorf("the 0008 CHECK admits %q, which principal.v1 does not carry", extra)
	}
}
