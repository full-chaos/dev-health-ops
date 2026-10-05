package migrationmatrix

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-8000 dual accept: a legacy entry is another document of the same operation, so it counts toward the operation
// once, not twice.
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
	if got := catalog.OperationCount(); got != 2 {
		t.Fatalf("OperationCount = %d, want 2: a legacy text is another document of foo, not another operation", got)
	}
	if got := len(catalog.documents["foo"]); got != 2 {
		t.Fatalf("foo has %d documents, want both texts", got)
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
