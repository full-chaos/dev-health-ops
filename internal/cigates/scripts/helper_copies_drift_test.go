package scripts_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The packages under internal/cigates keep their own copies of shared test helpers, because a _test.go file cannot be
// imported. This guard derives the copies from the tree and fails when one drifts from its source, so a fix made in one
// copy cannot silently diverge, and a new package that copies a helper file is covered without editing a list.

var (
	copiedWholeFile = []string{"ci_script_helpers_test.go"}
	// Files whose functions are copies of functions defined in a source file of the same tree; the source is found by scanning.
	copiedFuncFiles = []string{"support_test.go", "affected_helpers_test.go"}
	funcDecl        = regexp.MustCompile(`(?ms)^func (\w+)\(.*?^}\n`)
)

func withoutPackageClause(s string) string {
	_, rest, _ := strings.Cut(s, "\n")
	return rest
}

func readTree(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, "_test.go") {
			return err
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		files[filepath.ToSlash(rel)] = string(raw)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func copyDefects(files map[string]string) []string {
	var out []string
	var rels []string
	for rel := range files {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	for _, name := range copiedWholeFile {
		votes := map[string]int{}
		for _, rel := range rels {
			if filepath.Base(rel) == name {
				votes[withoutPackageClause(files[rel])]++
			}
		}
		var want string
		for body, n := range votes {
			if n > votes[want] || (n == votes[want] && body < want) {
				want = body
			}
		}
		for _, rel := range rels {
			if filepath.Base(rel) == name && withoutPackageClause(files[rel]) != want {
				out = append(out, rel+" differs from the majority copy beyond the package clause")
			}
		}
	}
	for _, name := range copiedFuncFiles {
		for _, rel := range rels {
			if filepath.Base(rel) != name {
				continue
			}
			for _, m := range funcDecl.FindAllStringSubmatch(files[rel], -1) {
				found, same := false, false
				for _, other := range rels {
					if other == rel || filepath.Base(other) == name {
						continue
					}
					for _, om := range funcDecl.FindAllStringSubmatch(files[other], -1) {
						if om[1] == m[1] {
							found = true
							same = same || om[0] == m[0]
						}
					}
				}
				if !found {
					out = append(out, rel+": func "+m[1]+" has no source in another file of the tree")
				} else if !same {
					out = append(out, rel+": func "+m[1]+" differs from its source")
				}
			}
		}
	}
	return out
}

func TestHelperCopiesAreByteIdenticalAcrossPackages(t *testing.T) {
	files := readTree(t, filepath.Join(repoRoot(t), "internal", "cigates"))
	if len(files) == 0 {
		t.Fatal("no test files read under internal/cigates")
	}
	for _, d := range copyDefects(files) {
		t.Error(d)
	}
}

func TestHelperCopyGuardCatchesPlantedDrift(t *testing.T) {
	files := readTree(t, filepath.Join(repoRoot(t), "internal", "cigates"))
	if d := copyDefects(files); len(d) != 0 {
		t.Fatalf("the real tree already has drift: %v", d)
	}
	mut := func(f func(map[string]string)) map[string]string {
		c := map[string]string{}
		for k, v := range files {
			c[k] = v
		}
		f(c)
		return c
	}
	cases := []struct {
		name, want string
		tree       map[string]string
	}{
		{"one byte planted in a whole-file copy", "affected/ci_script_helpers_test.go differs",
			mut(func(c map[string]string) { c["affected/ci_script_helpers_test.go"] += "// x\n" })},
		{"a new package copies a helper file with a change", "newpkg/ci_script_helpers_test.go differs",
			mut(func(c map[string]string) {
				c["newpkg/ci_script_helpers_test.go"] = strings.Replace(c["scripts/ci_script_helpers_test.go"], "subprocessSlots", "slots", 1)
			})},
		{"a copied function drifts from its source", "func repoRoot differs from its source",
			mut(func(c map[string]string) {
				c["affected/support_test.go"] = strings.Replace(c["affected/support_test.go"], "go.mod", "go.sum", 1)
			})},
		{"a copied function has no source", "has no source",
			mut(func(c map[string]string) {
				c["newpkg/support_test.go"] = "package newpkg_test\n\nfunc orphanCopy() {\n}\n"
			})},
	}
	for _, c := range cases {
		got := strings.Join(copyDefects(c.tree), "\n")
		if !strings.Contains(got, c.want) {
			t.Errorf("%s: want a defect containing %q, got %q", c.name, c.want, got)
		}
	}
}
