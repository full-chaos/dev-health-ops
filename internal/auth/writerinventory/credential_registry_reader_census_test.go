package writerinventory

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// credentialRegistryReaders are the sources that read auth.credential_registry.
// A domain-owned row (acr, push) holds a public fingerprint and no verifier, so no
// path may authenticate against one. A new reader needs a ruling and a place here.
var credentialRegistryReaders = map[string]string{
	"internal/auth/workload/bootstrap.go":           "ResolveBootstrap: by a 32-byte digest, a domain row holds 8 bytes; the caller checks BootstrapClass",
	"internal/auth/workload/enable.go":              "counts liveBootstrap rows of a workload principal; no authentication",
	"internal/auth/workload/bootstrap_lifecycle.go": "bootstrap lifecycle verbs, by class or liveBootstrap; no authentication",
	"internal/auth/serviceaccount/keys.go":          "key verbs by class or liveKey, and resolve by a 32-byte digest with the class checked",
	"internal/auth/outboxprojection/store.go":       "the projection's own rows, never a credential check",
	"internal/auth/decisionstate/decisionstate.go":  "ServiceAccountKey: one row by id for a service-account reference; the PDP counts it live only with the service-account key class and domain_owner platform (authapi/authorize.go DecisionKey.liveFor), so a domain-owned row never decides an allow",
}

var credentialRegistryRead = regexp.MustCompile("(?i)\\b(FROM|JOIN)\\s+(`\\s*\\+\\s*\\w+\\.q\\s*\\+\\s*`)?\\S*credential_registry")

func TestNoAuthenticatePathReadsADomainOwnedRow(t *testing.T) {
	root := repoRoot(t)
	var found []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if credentialRegistryRead.Match(raw) {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found = append(found, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(found)
	var want []string
	for file := range credentialRegistryReaders {
		want = append(want, file)
	}
	sort.Strings(want)
	if strings.Join(found, "\n") != strings.Join(want, "\n") {
		t.Fatalf("auth.credential_registry readers:\n%s\nwant:\n%s", strings.Join(found, "\n"), strings.Join(want, "\n"))
	}
}
