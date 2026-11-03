package contracts

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// The route's parser is the disk parser's twin: the same bytes, and the same
// verdict on every example.
func TestEmbeddedAcrSchemaEqualsTheContract(t *testing.T) {
	root := repoRootForAcrEmbed(t)
	raw, err := os.ReadFile(SchemaPath(root, AcrCredentialLifecycleEventSurface))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, authv1.AcrCredentialLifecycleEventSchema) {
		t.Fatal("the embedded schema differs from the contract")
	}
	names, err := filepath.Glob(filepath.Join(root, "contracts/auth/v1/examples/acr-credential-lifecycle-event/*.json"))
	if err != nil || len(names) < 10 {
		t.Fatalf("examples: %d, %v", len(names), err)
	}
	for _, name := range names {
		base := filepath.Base(name)
		if base == "manifest.json" {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		_, diskErr := ParseAcrCredentialLifecycleEvent(root, body)
		_, embeddedErr := ParseEmbeddedAcrCredentialLifecycleEvent(body)
		if (diskErr == nil) != (embeddedErr == nil) {
			t.Errorf("%s: disk %v, embedded %v", base, diskErr, embeddedErr)
		}
		if strings.HasPrefix(base, "valid-") != (embeddedErr == nil) {
			t.Errorf("%s: embedded parser: %v", base, embeddedErr)
		}
	}
}

func repoRootForAcrEmbed(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	return root
}
