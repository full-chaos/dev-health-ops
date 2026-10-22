package authschema

import (
	"encoding/json"
	"fmt"
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
// closes credential_class (the last migration that redefines the CHECK, 0017,
// is the one compared) to the vocabulary principal.v1 carries, so a stored
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
		if strings.Contains(m.SQL, "ADD CONSTRAINT credential_registry_credential_class_closed") {
			sql = m.SQL
		}
	}
	start := strings.Index(sql, "ADD CONSTRAINT credential_registry_credential_class_closed")
	if start < 0 {
		t.Fatal("no migration closes credential_class")
	}
	body := sql[start:]
	body = body[strings.Index(body, "IN (")+len("IN ("):]
	body = body[:strings.Index(body, ")")]
	got := map[string]bool{}
	for _, part := range strings.Split(body, ",") {
		got[strings.Trim(strings.TrimSpace(part), "'")] = true
	}
	for _, problem := range classCheckDifferences(want, got) {
		t.Error(problem)
	}
}

// classCheckDifferences returns, in both directions, the principal.v1 classes
// the CHECK lacks and the CHECK classes principal.v1 does not carry.
func classCheckDifferences(want []string, got map[string]bool) []string {
	var problems []string
	remaining := map[string]bool{}
	for class := range got {
		remaining[class] = true
	}
	for _, class := range want {
		if !got[class] {
			problems = append(problems, fmt.Sprintf("principal.v1 class %q is missing from the credential_class CHECK", class))
		}
		delete(remaining, class)
	}
	for extra := range remaining {
		problems = append(problems, fmt.Sprintf("the credential_class CHECK admits %q, which principal.v1 does not carry", extra))
	}
	return problems
}

// TestTheClassCheckComparisonFailsWhenTheyDifferByOneClassEitherWay plants one
// class of difference in each direction.
func TestTheClassCheckComparisonFailsWhenTheyDifferByOneClassEitherWay(t *testing.T) {
	want := []string{"a_class", "b_class", "c_class"}
	same := map[string]bool{"a_class": true, "b_class": true, "c_class": true}
	if problems := classCheckDifferences(want, same); len(problems) != 0 {
		t.Fatalf("equal sets reported %v", problems)
	}
	checkLacks := map[string]bool{"a_class": true, "b_class": true}
	if problems := classCheckDifferences(want, checkLacks); len(problems) != 1 {
		t.Errorf("a class missing from the CHECK reported %v, want one problem", problems)
	}
	checkExtra := map[string]bool{"a_class": true, "b_class": true, "c_class": true, "d_class": true}
	if problems := classCheckDifferences(want, checkExtra); len(problems) != 1 {
		t.Errorf("a class the CHECK adds reported %v, want one problem", problems)
	}
}
