package docstools

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The real mkdocs.yml and every page of docs/ go through the port of safe_load and the verb: the port must accept what
// Python accepted on the tree this repository publishes.
func TestValidatePublicationPassesTheRealRepositoryDocs(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mkdocs.yml", "docs", "docs-data"} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Skip("the repository docs are not here:", err)
		}
	}
	work, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mkdocs.yml", "docs", "docs-data"} {
		if err := os.Symlink(filepath.Join(root, name), filepath.Join(work, name)); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(work)
	var stdout, stderr bytes.Buffer
	if code := Run([]string{"validate-publication"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\nstdout: %q\nstderr: %q", code, stdout.String(), stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "Validated ") {
		t.Fatalf("stdout %q", stdout.String())
	}
	for _, name := range []string{"docs-v2-publication-inventory.tsv", "docs-v2-ia-coverage.tsv", "docs-v2-publication-summary.md"} {
		if _, err := os.Stat(filepath.Join(work, ".build", name)); err != nil {
			t.Errorf("the verb did not write %s: %v", name, err)
		}
	}
	// every front matter block of the real pages loads
	pages := 0
	_ = filepath.Walk(filepath.Join(root, "docs"), func(path string, info os.FileInfo, err error) error {
		if err != nil || !info.Mode().IsRegular() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Errorf("%s: %v", path, rerr)
			return nil
		}
		if body, ok := dcPubFrontMatter(strings.ReplaceAll(string(raw), "\r\n", "\n")); ok {
			pages++
			if _, yerr := dcPubLoadYAML(body); yerr != nil {
				t.Errorf("%s: front matter refused: %v", path, yerr)
			}
		}
		return nil
	})
	t.Logf("%d pages with front matter loaded", pages)
}

// The tags safe_load accepts, one row each (the recorded corpus has every form of them); the tags it refuses.
func TestYAMLLoaderAcceptsTheStandardTagsAndRefusesTheOthers(t *testing.T) {
	accepted := []string{
		"!!str 5", "!!int '7'", "!!float 1", "!!bool yes", "!!null ''", "!!seq [a]", "!!map {a: b}", "!!timestamp 2001-01-01",
		"!!binary YQ==", "!!set {a}", "!!omap [a: 1]", "!!pairs [a: 1, a: 2]", "!<tag:yaml.org,2002:str> x", "! x", "!!str",
	}
	for _, text := range accepted {
		if _, err := dcYAMLSafeLoad("a: " + text + "\n"); err != nil {
			t.Errorf("%q refused: %v", text, err)
		}
	}
	refused := []string{"!!python/name:os.system x", "!!python/object:x {}", "!foo x", "!e!x x", "!!merge x", "!!value x", "!!yaml x", "!<x> y"}
	for _, text := range refused {
		_, err := dcYAMLSafeLoad("a: " + text + "\n")
		if err == nil {
			t.Errorf("%q accepted", text)
		}
	}
}

func TestValidatePublicationParityCasesArePinned(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join("testdata", "parity"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "pub-") {
			n++
		}
	}
	if n != pubParityCases {
		t.Fatalf("%d recorded pub-* pairs, pinned %d", n, pubParityCases)
	}
}

const pubParityCases = 163
