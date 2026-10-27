package identity

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// placeholderCopy matches a hand-written copy of the provider literal in Go source or embedded SQL.
var placeholderCopy = regexp.MustCompile(`["'` + "`" + `]service["'` + "`" + `]`)

// providerDefinition matches a provider const or var defined by the literal "service".
var providerDefinition = regexp.MustCompile(`(?m)^\s*(?:(?:const|var)\s+)?\w*[Pp]rovider\w*\s*(?:\w+\s*)?=\s*"service"\s*$`)

// theProviderDefinition is the one definition of the service provider (edgetokenmint.ServiceAuthProvider).
const theProviderDefinition = "internal/edgetokenmint/edgetokenmint.go"

// TestUsersOwnedGuardAndBackfillCarryNoCopyOfThePlaceholderRule keeps one definition: the guard and the backfill
// name it through ServicePlaceholderSQL / IsServicePlaceholder and carry no literal of their own, so a planted copy
// fails here even when it matches today.
func TestUsersOwnedGuardAndBackfillCarryNoCopyOfThePlaceholderRule(t *testing.T) {
	guard, err := filepath.Glob("../authruntime/*.go")
	if err != nil {
		t.Fatal(err)
	}
	backfill, err := filepath.Glob("../usersbackfill/*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range []struct {
		dir, want string
		files     []string
	}{{"../authruntime", "ServicePlaceholderSQL(", guard}, {"../usersbackfill", "IsServicePlaceholder(", backfill}} {
		dir, want, files := pkg.dir, pkg.want, pkg.files
		if len(files) == 0 {
			t.Fatalf("%s: no Go files", dir)
		}
		used := false
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			src, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if loc := placeholderCopy.FindIndex(src); loc != nil {
				t.Errorf("%s carries its own copy of the placeholder literal at byte %d; use identity.IsServicePlaceholder or identity.ServicePlaceholderSQL", file, loc[0])
			}
			if strings.Contains(string(src), "identity."+want) {
				used = true
			}
		}
		if !used {
			t.Errorf("%s no longer names identity.%s", dir, want)
		}
	}
}

// TestTheServiceProviderHasOneDefinitionInTheModule walks every non-test Go file of the module: exactly one provider
// const or var is defined by the literal "service", edgetokenmint.ServiceAuthProvider.
func TestTheServiceProviderHasOneDefinitionInTheModule(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); path != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if providerDefinition.Match(src) {
			rel, _ := filepath.Rel(root, path)
			found = append(found, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0] != theProviderDefinition {
		t.Fatalf("provider definitions by the literal \"service\": %v; want exactly [%s]", found, theProviderDefinition)
	}
}
