package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCredentialReferenceIsCurrent(t *testing.T) {
	root := filepath.Join("..", "..")
	raw, err := os.ReadFile(filepath.Join(root, registryPath))
	if err != nil {
		t.Fatal(err)
	}
	referenceRaw, err := os.ReadFile(filepath.Join(root, referencePath))
	if err != nil {
		t.Fatal(err)
	}
	want, err := render(raw, referenceRaw)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, pagePath))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("credential reference is stale; run go run ./cmd/credrefgen from the repository root")
	}
}

func TestCredentialReferenceIDsMatch(t *testing.T) {
	root := filepath.Join("..", "..")
	raw, err := os.ReadFile(filepath.Join(root, registryPath))
	if err != nil {
		t.Fatal(err)
	}
	var inventory registry
	if err := json.Unmarshal(raw, &inventory); err != nil {
		t.Fatal(err)
	}
	referenceRaw, err := os.ReadFile(filepath.Join(root, referencePath))
	if err != nil {
		t.Fatal(err)
	}
	var references map[string]publicReference
	if err := json.Unmarshal(referenceRaw, &references); err != nil {
		t.Fatal(err)
	}
	ids := make(map[string]bool)
	for _, class := range inventory.Classes {
		ids[class.ID] = true
		if _, ok := references[class.ID]; !ok {
			t.Errorf("contract class %q has no docs row", class.ID)
		}
	}
	for id := range references {
		if !ids[id] {
			t.Errorf("docs row %q names no contract class", id)
		}
	}
}

func fixture() (credentialClass, publicReference) {
	return credentialClass{ID: "example", Status: "active"}, publicReference{
		Description: "Example", Issuer: "Issuer", Acceptors: "Verifier",
		Format: "opaque <redacted>", Storage: "Hash only", Lifecycle: "Single use",
	}
}

func encode(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRenderRejectsIncompleteOrNonPublicInventory(t *testing.T) {
	for _, name := range []string{"empty", "duplicate", "status", "summary", "private", "missing docs row", "extra docs row"} {
		t.Run(name, func(t *testing.T) {
			c, p := fixture()
			classes := []credentialClass{c}
			references := map[string]publicReference{c.ID: p}
			wantError := ""
			switch name {
			case "empty":
				classes = nil
			case "duplicate":
				classes = append(classes, c)
			case "status":
				classes[0].Status = "unknown"
			case "summary":
				p.Issuer = ""
				references[c.ID] = p
			case "private":
				p.Description = "D1234"
				references[c.ID] = p
			case "missing docs row":
				delete(references, c.ID)
				wantError = `contract class "example" has no docs row`
			case "extra docs row":
				references["unknown"] = p
				wantError = `docs row "unknown" in ` + referencePath + ` names no contract class`
			}
			_, err := render(encode(t, registry{Classes: classes}), encode(t, references))
			if err == nil {
				t.Fatal("invalid inventory rendered successfully")
			}
			if !strings.Contains(err.Error(), wantError) {
				t.Fatalf("got error %q, want %q", err, wantError)
			}
		})
	}
	if _, err := render([]byte("{"), []byte("{}")); err == nil {
		t.Fatal("malformed registry JSON rendered successfully")
	}
	c, _ := fixture()
	if _, err := render(encode(t, registry{Classes: []credentialClass{c}}), []byte("{")); err == nil {
		t.Fatal("malformed docs JSON rendered successfully")
	}
}

func TestRenderUsesRegistryFieldsAndEscapesCells(t *testing.T) {
	a, p := fixture()
	b, q := fixture()
	a.ID, b.ID, b.Status = "a", "b", "retired"
	p.Issuer = "First | issuer\nwith & punctuation"
	references := map[string]publicReference{a.ID: p, b.ID: q}
	got, err := render(encode(t, registry{Classes: []credentialClass{b, a}}), encode(t, references))
	if err != nil {
		t.Fatal(err)
	}
	text := string(got)
	for _, want := range []string{"First &#124; issuer with &amp; punctuation", "opaque &lt;redacted&gt;", "| retired |", "DO NOT EDIT"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q", want)
		}
	}
	if strings.Index(text, "| `a` |") > strings.Index(text, "| `b` |") {
		t.Fatal("rows are not sorted by class id")
	}
	p.Issuer = "Replacement issuer"
	references[a.ID] = p
	changed, err := render(encode(t, registry{Classes: []credentialClass{a, b}}), encode(t, references))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(got, changed) || !bytes.Contains(changed, []byte("Replacement issuer")) {
		t.Fatal("a changed docs field did not change the generated page")
	}
}
