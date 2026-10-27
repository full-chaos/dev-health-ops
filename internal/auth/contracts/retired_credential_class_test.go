package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// retiredCredentialClasses are classes with no minter, verifier or reader
// left in code (CHAOS-6761). None may reappear in the registry, a wire
// schema or the credential-classes docs table.
var retiredCredentialClasses = []string{"worker_operational_bridge_token"}

func TestRetiredCredentialClassesAreGoneFromEveryContractSurface(t *testing.T) {
	root := testRoot(t)
	dir := ContractsDir(root)
	files := []string{
		filepath.Join(dir, "credential-classes.json"),
		filepath.Join(dir, "jsonschema", "credential-entry.v1.schema.json"),
		filepath.Join(dir, "jsonschema", "credential-list.v1.schema.json"),
		filepath.Join(dir, "jsonschema", "credential-receipt.v1.schema.json"),
		filepath.Join(dir, "jsonschema", "principal.v1.schema.json"),
		filepath.Join(root, "docs", "reference", "auth", "credential-classes.md"),
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		for _, class := range retiredCredentialClasses {
			if strings.Contains(string(raw), class) {
				t.Errorf("%s still names retired credential class %q", file, class)
			}
		}
	}
}
