package idpstub_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const idpstubImport = "github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"

// idpstub is test support: only a _test.go file or another internal/testsupport package may import it.
func TestNoProductionPackageImportsIdpstub(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	var offenders []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "node_modules" || d.Name() == "testdata" || strings.HasPrefix(rel, "third_party/") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.HasPrefix(rel, "internal/testsupport/") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return nil
		}
		for _, imp := range file.Imports {
			if strings.Trim(imp.Path.Value, `"`) == idpstubImport || strings.HasPrefix(strings.Trim(imp.Path.Value, `"`), idpstubImport+"/") {
				offenders = append(offenders, rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Fatalf("production files import the test-support identity provider: %v", offenders)
	}
}

// No image except docker/idpstub.Dockerfile names the binary, and that one holds no key or certificate file.
func TestNoProductionImageHoldsIdpstubOrAKey(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	dockerfiles, err := filepath.Glob(filepath.Join(root, "docker", "*Dockerfile*"))
	if err != nil || len(dockerfiles) == 0 {
		t.Fatalf("no Dockerfiles found: %v", err)
	}
	for _, path := range dockerfiles {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text, name := string(raw), filepath.Base(path)
		if name == "idpstub.Dockerfile" {
			for _, line := range strings.Split(text, "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "COPY") && (strings.Contains(trimmed, ".pem") || strings.Contains(trimmed, ".key") || strings.Contains(trimmed, ".crt")) {
					t.Errorf("%s copies a key or certificate file: %s", name, trimmed)
				}
			}
			if !strings.Contains(text, "Not a release image") {
				t.Errorf("%s must say it is not a release image", name)
			}
			continue
		}
		if strings.Contains(text, "idpstub") {
			t.Errorf("%s names idpstub: no other image may build or copy it", name)
		}
	}
}

// No key or certificate file belonging to the stub is checked in under its directory.
func TestNoKeyOrCertificateFileIsCheckedIn(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		switch strings.ToLower(filepath.Ext(path)) {
		case ".pem", ".key", ".crt", ".cer", ".p12", ".pfx":
			t.Errorf("%s: a key or certificate file is checked in", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
