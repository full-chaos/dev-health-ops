package contractsv1

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The OpenAPI document must reference the SAME schema files the validators
// read, never restate them. These tests pin that: a `$ref` that stops
// resolving, or an inline schema that quietly replaces a `$ref`, is the
// second-copy drift the cross-language corpus exists to catch -- and it is
// invisible to every runtime test, because nothing at runtime reads the
// OpenAPI document at all.

func openAPIDocument(t *testing.T) (map[string]any, string) {
	t.Helper()
	dir := filepath.Join(ContractsDir(testRoot(t)), "openapi")
	path := filepath.Join(dir, "auth-v1.openapi.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	return document, dir
}

func TestTheOpenAPIDocumentDeclares31SoItSharesTheSchemaDialect(t *testing.T) {
	// 3.0 uses a modified draft-04 subset and could not $ref these schemas;
	// a silent downgrade would force a translated second copy of each.
	document, _ := openAPIDocument(t)
	version, _ := document["openapi"].(string)
	if !strings.HasPrefix(version, "3.1") {
		t.Fatalf("openapi = %q, want 3.1.x -- only 3.1 uses JSON Schema draft 2020-12 "+
			"as its dialect, which is what lets $ref point at the validated schema files",
			version)
	}
}

func TestEveryOpenAPISchemaComponentIsARefToAValidatedSchemaFile(t *testing.T) {
	document, dir := openAPIDocument(t)
	components, ok := document["components"].(map[string]any)
	if !ok {
		t.Fatal("no components object")
	}
	schemas, ok := components["schemas"].(map[string]any)
	if !ok || len(schemas) == 0 {
		t.Fatal("components.schemas is missing or empty -- with nothing to check, " +
			"every assertion below would pass vacuously")
	}
	for name, entry := range schemas {
		t.Run(name, func(t *testing.T) {
			object, ok := entry.(map[string]any)
			if !ok {
				t.Fatalf("component %s is not an object", name)
			}
			ref, ok := object["$ref"].(string)
			if !ok {
				t.Fatalf("component %s does not use $ref. Inline schemas are a SECOND "+
					"COPY of a contract the three validators read from file; they drift "+
					"silently because nothing at runtime reads this document.", name)
			}
			if len(object) != 1 {
				t.Errorf("component %s carries keys beside $ref (%d total); sibling keys "+
					"next to a $ref are ignored under draft 2020-12's predecessors and "+
					"are an easy way to believe a constraint applies when it does not",
					name, len(object))
			}
			target := filepath.Join(dir, filepath.FromSlash(ref))
			if _, err := os.Stat(target); err != nil {
				t.Fatalf("component %s references %q which does not resolve to a file (%v)",
					name, ref, err)
			}
			if !strings.HasSuffix(ref, ".schema.json") {
				t.Errorf("component %s references %q, which is not a *.schema.json file "+
					"-- components must point into contracts/auth/v1/jsonschema/", name, ref)
			}
		})
	}
}

func TestTheOpenAPIDocumentReferencesEveryWireSchemaOnDisk(t *testing.T) {
	// The direction that actually rots: a surface lands with its schema,
	// fixtures and validators, and nobody adds it here. The OpenAPI document
	// then silently describes less than the service speaks, and every other
	// test in this package still passes.
	document, dir := openAPIDocument(t)
	components, _ := document["components"].(map[string]any)
	schemas, _ := components["schemas"].(map[string]any)
	referenced := map[string]bool{}
	for _, entry := range schemas {
		if object, ok := entry.(map[string]any); ok {
			if ref, ok := object["$ref"].(string); ok {
				referenced[filepath.Base(ref)] = true
			}
		}
	}
	for _, path := range wireSchemaFiles(t) {
		base := filepath.Base(path)
		if !referenced[base] {
			t.Errorf("contracts/auth/v1/jsonschema/%s has no component in "+
				"%s/auth-v1.openapi.json -- add one as a $ref so the OpenAPI document "+
				"describes the whole wire surface, not the part someone remembered",
				base, filepath.Base(dir))
		}
	}
}
