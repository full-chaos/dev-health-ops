package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// retiredCredentialClasses are classes with no minter, verifier or reader
// left in code (worker_operational_bridge_token: CHAOS-6761; the legacy ingest
// API key and HMAC signature: their routes were retired by CHAOS-11813 and the
// classes deleted by CHAOS-11820). None may reappear in the registry, a wire
// schema, an endpoint profile or example, or the credential-classes docs table.
var retiredCredentialClasses = []string{"worker_operational_bridge_token", "legacy_ingest_api_key", "legacy_ingest_hmac_signature"}

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
	for _, pattern := range []string{
		filepath.Join(dir, "endpoint-profiles.*.json"),
		filepath.Join(dir, "examples", "endpoint-auth-profile", "*.json"),
	} {
		matches, err := filepath.Glob(pattern)
		if err != nil || len(matches) == 0 {
			t.Fatalf("no files match %s (%v): the guard would read nothing", pattern, err)
		}
		files = append(files, matches...)
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
