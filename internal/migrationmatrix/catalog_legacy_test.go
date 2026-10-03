package migrationmatrix

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// CHAOS-8000 dual accept: a legacy catalog entry is a document the catalog still names, so a live row at it is
// NOT drift (R14), while a row at a digest the catalog does not name still is.
func TestLoadCatalog_LegacyEntryIsAnotherNamedDocumentOfTheOperation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go_api_operations.json")
	body := `[
  {"operation": "foo", "digest": "d-new"},
  {"operation": "foo", "digest": "d-old", "legacy": true},
  {"operation": "bar", "digest": "d-bar"}
]`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	catalog, err := LoadCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := catalog.Documents("foo"); !reflect.DeepEqual(got, []string{"d-new", "d-old"}) {
		t.Fatalf("Documents(foo) = %v, want both texts", got)
	}
	row := func(digest string) OperationRow {
		return OperationRow{Operation: "foo", Mode: "canary", Live: true, DocumentDigest: digest}
	}
	if DocumentDrift(row("d-old"), catalog) {
		t.Error("a live row at the legacy text is DOCUMENT_DRIFT; the catalog names it")
	}
	if DocumentDrift(row("d-new"), catalog) {
		t.Error("a live row at the current text is DOCUMENT_DRIFT")
	}
	if !DocumentDrift(row("d-unknown"), catalog) {
		t.Error("a live row at a digest the catalog does not name must still be DOCUMENT_DRIFT")
	}
}

func TestLoadCatalog_LegacyMustBeAbsentOrTrue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go_api_operations.json")
	if err := os.WriteFile(path, []byte(`[{"operation":"foo","digest":"d1"},{"operation":"foo","digest":"d0","legacy":false}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCatalog(path); err == nil || !strings.Contains(err.Error(), "legacy") {
		t.Fatalf("err = %v, want a refusal naming legacy", err)
	}
}
