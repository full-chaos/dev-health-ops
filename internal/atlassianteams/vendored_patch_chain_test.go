package atlassianteams

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The vendored atlassian tree, the patch records and the PROVENANCE table must agree in BOTH directions
// (CHAOS-7989): reversing every patch, newest first, from the vendored tree must give back exactly the pinned
// upstream tree (UPSTREAM.sha256: one digest per upstream file, taken from a clone of the pinned commit), and the
// PROVENANCE table must name every patch file once and nothing else. A deleted patch file, a deleted PROVENANCE
// row, an edit with no record, or a record with no edit each fail here.

const vendoredAtlassianDir = "third_party/vendor/atlassian"

var provenancePatchRow = regexp.MustCompile("^\\| `patches/([^`]+\\.patch)`")

type patchHunk struct {
	newStart int
	oldSide  []string
	newSide  []string
}

type filePatch struct {
	path  string
	isNew bool
	hunks []patchHunk
}

func parseUnifiedPatch(text string) ([]filePatch, error) {
	var files []filePatch
	inHunk := false
	for _, line := range strings.Split(text, "\n") {
		switch {
		case strings.HasPrefix(line, "diff --git "):
			fields := strings.Fields(line)
			if len(fields) != 4 || !strings.HasPrefix(fields[3], "b/") {
				return nil, fmt.Errorf("unreadable diff header %q", line)
			}
			files = append(files, filePatch{path: strings.TrimPrefix(fields[3], "b/")})
			inHunk = false
		case len(files) == 0:
			continue
		case !inHunk && strings.HasPrefix(line, "new file mode"):
			files[len(files)-1].isNew = true
		case strings.HasPrefix(line, "@@ "):
			m := regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@`).FindStringSubmatch(line)
			if m == nil {
				return nil, fmt.Errorf("unreadable hunk header %q", line)
			}
			start, _ := strconv.Atoi(m[1])
			f := &files[len(files)-1]
			f.hunks = append(f.hunks, patchHunk{newStart: start})
			inHunk = true
		case inHunk && line != "":
			f := &files[len(files)-1]
			h := &f.hunks[len(f.hunks)-1]
			body := line[1:]
			switch line[0] {
			case ' ':
				h.oldSide = append(h.oldSide, body)
				h.newSide = append(h.newSide, body)
			case '-':
				h.oldSide = append(h.oldSide, body)
			case '+':
				h.newSide = append(h.newSide, body)
			case '\\':
				return nil, fmt.Errorf("no-newline marker is not supported: %q", line)
			default:
				return nil, fmt.Errorf("unreadable hunk line %q", line)
			}
		}
	}
	return files, nil
}

// reversePatch turns the tree that carries a patch back into the tree before it. Every hunk's new side must sit
// exactly where the hunk header says; a file the patch created must be exactly what the patch added.
func reversePatch(tree map[string][]string, patch string) error {
	files, err := parseUnifiedPatch(patch)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("the patch names no file")
	}
	for _, f := range files {
		current, ok := tree[f.path]
		if !ok {
			return fmt.Errorf("%s is not in the tree", f.path)
		}
		if len(f.hunks) == 0 {
			return fmt.Errorf("%s: no hunk", f.path)
		}
		var out []string
		pos := 0
		for _, h := range f.hunks {
			start := h.newStart - 1
			if len(h.newSide) == 0 {
				start = h.newStart
			}
			if start < pos || start+len(h.newSide) > len(current) {
				return fmt.Errorf("%s: a hunk at line %d is outside the file", f.path, h.newStart)
			}
			for i, want := range h.newSide {
				if current[start+i] != want {
					return fmt.Errorf("%s: line %d is %q, the patch says %q: the vendored file does not carry this patch", f.path, start+i+1, current[start+i], want)
				}
			}
			out = append(out, current[pos:start]...)
			out = append(out, h.oldSide...)
			pos = start + len(h.newSide)
		}
		out = append(out, current[pos:]...)
		if f.isNew {
			if len(out) != 0 {
				return fmt.Errorf("%s: the patch creates the file but the vendored file has more than the patch added", f.path)
			}
			delete(tree, f.path)
			continue
		}
		tree[f.path] = out
	}
	return nil
}

// readVendoredTree reads EVERY file of the vendored directory except the records themselves (patches/, PROVENANCE.md,
// UPSTREAM.sha256), so a file added anywhere in it is part of the tree the patches must explain. A file with no final
// newline is reported (the digests are over newline-terminated files).
func readVendoredTree(t *testing.T, root string) (map[string][]string, []string) {
	t.Helper()
	tree := map[string][]string{}
	var problems []string
	base := filepath.Join(root, vendoredAtlassianDir)
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(base, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel == "patches" {
				return filepath.SkipDir
			}
			return nil
		}
		if rel == "PROVENANCE.md" || rel == "UPSTREAM.sha256" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(raw)
		if !strings.HasSuffix(text, "\n") {
			problems = append(problems, fmt.Sprintf("%s does not end with a newline", rel))
		}
		tree[vendoredAtlassianDir+"/"+rel] = strings.Split(strings.TrimSuffix(text, "\n"), "\n")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return tree, problems
}

func provenancePatchNames(t *testing.T, root string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, vendoredAtlassianDir, "PROVENANCE.md"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, line := range strings.Split(string(raw), "\n") {
		if m := provenancePatchRow.FindStringSubmatch(line); m != nil {
			names = append(names, m[1])
		}
	}
	sort.Strings(names)
	return names
}

// checkPatchChain is the whole two-way check, returning every disagreement.
func checkPatchChain(t *testing.T, root string) []string {
	t.Helper()
	var problems []string
	patchPaths, err := filepath.Glob(filepath.Join(root, vendoredAtlassianDir, "patches", "*.patch"))
	if err != nil || len(patchPaths) == 0 {
		return []string{fmt.Sprintf("no patch record found (%v)", err)}
	}
	sort.Strings(patchPaths)
	// patches/ holds patch records and nothing else: any other entry (Go code, a directory) is vendored content no digest and no
	// patch explains.
	entries, err := os.ReadDir(filepath.Join(root, vendoredAtlassianDir, "patches"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".patch") || !entry.Type().IsRegular() {
			problems = append(problems, fmt.Sprintf("patches/%s is not a *.patch record file", entry.Name()))
		}
	}
	if len(problems) > 0 {
		return problems // the records cannot be read as patches
	}
	var onDisk []string
	for _, p := range patchPaths {
		onDisk = append(onDisk, filepath.Base(p))
	}
	rows := provenancePatchNames(t, root)
	if len(rows) == 0 {
		problems = append(problems, "PROVENANCE.md has no patch row")
	}
	if strings.Join(rows, "\n") != strings.Join(onDisk, "\n") {
		problems = append(problems, fmt.Sprintf("PROVENANCE rows %v and patch files %v differ: every patch needs exactly one row and every row a patch", rows, onDisk))
	}

	tree, treeProblems := readVendoredTree(t, root)
	problems = append(problems, treeProblems...)
	for i := len(patchPaths) - 1; i >= 0; i-- {
		raw, err := os.ReadFile(patchPaths[i])
		if err != nil {
			t.Fatal(err)
		}
		if err := reversePatch(tree, string(raw)); err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", filepath.Base(patchPaths[i]), err))
			return problems
		}
	}

	manifestRaw, err := os.ReadFile(filepath.Join(root, vendoredAtlassianDir, "UPSTREAM.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for _, line := range strings.Split(string(manifestRaw), "\n") {
		if line == "" {
			continue
		}
		fields := strings.SplitN(line, "  ", 2)
		if len(fields) != 2 {
			t.Fatalf("unreadable UPSTREAM.sha256 line %q", line)
		}
		want[vendoredAtlassianDir+"/"+fields[1]] = fields[0]
	}
	if len(want) == 0 {
		return append(problems, "UPSTREAM.sha256 is empty")
	}
	for path, digest := range want {
		lines, ok := tree[path]
		if !ok {
			problems = append(problems, fmt.Sprintf("upstream file %s is missing once the patches are reversed", path))
			continue
		}
		sum := sha256.Sum256([]byte(strings.Join(lines, "\n") + "\n"))
		if hex.EncodeToString(sum[:]) != digest {
			problems = append(problems, fmt.Sprintf("%s differs from the pinned upstream once the patches are reversed: a change with no patch record", path))
		}
	}
	for path := range tree {
		if _, ok := want[path]; !ok {
			problems = append(problems, fmt.Sprintf("%s is in the vendored tree but not in upstream and no patch creates it", path))
		}
	}
	sort.Strings(problems)
	return problems
}

func TestVendoredTreeIsUpstreamPlusExactlyTheRecordedPatches(t *testing.T) {
	for _, problem := range checkPatchChain(t, filepath.Join("..", "..")) {
		t.Error(problem)
	}
}

// copyRepoSlice copies the vendored directory into a scratch root so a planted defect never touches the tree.
func copyRepoSlice(t *testing.T) string {
	t.Helper()
	src := filepath.Join("..", "..", vendoredAtlassianDir)
	dst := filepath.Join(t.TempDir(), vendoredAtlassianDir)
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, raw, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(filepath.Dir(filepath.Dir(dst)))
}

// addPatchRecord adds a patch file and its PROVENANCE row to a scratch copy.
func addPatchRecord(t *testing.T, base, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(base, "patches", name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(base, "PROVENANCE.md"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString("\n| `patches/" + name + "` | planted |\n"); err != nil {
		t.Fatal(err)
	}
}

// TestPatchChainCheckSeesEveryKindOfDrift plants each defect in a scratch copy: the check must report it.
func TestPatchChainCheckSeesEveryKindOfDrift(t *testing.T) {
	if got := checkPatchChain(t, copyRepoSlice(t)); len(got) != 0 {
		t.Fatalf("the unmodified copy must pass: %v", got)
	}
	plant := map[string]func(t *testing.T, base string){
		"deleted patch file": func(t *testing.T, base string) {
			if err := os.Remove(filepath.Join(base, "patches", "0005-default-http-clients-follow-no-redirect.patch")); err != nil {
				t.Fatal(err)
			}
		},
		"deleted PROVENANCE row": func(t *testing.T, base string) {
			path := filepath.Join(base, "PROVENANCE.md")
			raw, _ := os.ReadFile(path)
			var kept []string
			for _, line := range strings.Split(string(raw), "\n") {
				if !strings.HasPrefix(line, "| `patches/0003-") {
					kept = append(kept, line)
				}
			}
			if err := os.WriteFile(path, []byte(strings.Join(kept, "\n")), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"edit with no record": func(t *testing.T, base string) {
			path := filepath.Join(base, "atlassian", "graph", "schema_fetcher.go")
			raw, _ := os.ReadFile(path)
			if err := os.WriteFile(path, append(raw, []byte("// unrecorded\n")...), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"patch with no edit": func(t *testing.T, base string) {
			path := filepath.Join(base, "patches", "0002-operation-names-match-documents.patch")
			raw, _ := os.ReadFile(path)
			loc := regexp.MustCompile(`(?m)^\+[^+]`).FindIndex(raw)
			if loc == nil {
				t.Fatal("no added line in the patch")
			}
			text := string(raw[:loc[0]+1]) + "// " + string(raw[loc[0]+1:])
			if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"unrecorded new file": func(t *testing.T, base string) {
			if err := os.WriteFile(filepath.Join(base, "atlassian", "extra.go"), []byte("package atlassian\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"unrecorded new file at the module root": func(t *testing.T, base string) {
			if err := os.WriteFile(filepath.Join(base, "extra.go"), []byte("package atlassian\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"unrecorded new package directory": func(t *testing.T, base string) {
			if err := os.MkdirAll(filepath.Join(base, "newpkg"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(base, "newpkg", "x.go"), []byte("package newpkg\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"deleted upstream file": func(t *testing.T, base string) {
			if err := os.Remove(filepath.Join(base, "LICENSE")); err != nil {
				t.Fatal(err)
			}
		},
		"extra line in a patch-created file": func(t *testing.T, base string) {
			path := filepath.Join(base, "atlassian", "default_http_client.go")
			raw, _ := os.ReadFile(path)
			if err := os.WriteFile(path, append(raw, []byte("// more than the patch added\n")...), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"final newline removed": func(t *testing.T, base string) {
			path := filepath.Join(base, "LICENSE")
			raw, _ := os.ReadFile(path)
			if err := os.WriteFile(path, []byte(strings.TrimSuffix(string(raw), "\n")), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"non-patch file in patches/": func(t *testing.T, base string) {
			if err := os.WriteFile(filepath.Join(base, "patches", "extra.go"), []byte("package patches\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"directory in patches/": func(t *testing.T, base string) {
			if err := os.MkdirAll(filepath.Join(base, "patches", "sub"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(base, "patches", "sub", "x.go"), []byte("package sub\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"directory named like a patch in patches/": func(t *testing.T, base string) {
			if err := os.MkdirAll(filepath.Join(base, "patches", "9999-dir.patch"), 0o755); err != nil {
				t.Fatal(err)
			}
		},
		"symlink named like a patch in patches/": func(t *testing.T, base string) {
			if err := os.Symlink("0001-alias-typed-cypher-values.patch", filepath.Join(base, "patches", "9999-link.patch")); err != nil {
				t.Fatal(err)
			}
		},
		"empty patch record with its row": func(t *testing.T, base string) {
			addPatchRecord(t, base, "0006-empty.patch", "")
		},
		"header-only patch record with its row": func(t *testing.T, base string) {
			addPatchRecord(t, base, "0006-header-only.patch", "diff --git a/third_party/vendor/atlassian/go.mod b/third_party/vendor/atlassian/go.mod\n")
		},
	}
	plantedReason := map[string]string{"deleted upstream file": "is missing once the patches are reversed", "non-patch file in patches/": "is not a *.patch record file", "directory in patches/": "is not a *.patch record file", "directory named like a patch in patches/": "9999-dir.patch is not a *.patch record file", "symlink named like a patch in patches/": "9999-link.patch is not a *.patch record file"}
	for name, mutate := range plant {
		t.Run(name, func(t *testing.T) {
			root := copyRepoSlice(t)
			mutate(t, filepath.Join(root, vendoredAtlassianDir))
			got := checkPatchChain(t, root)
			if len(got) == 0 {
				t.Fatalf("%s: the check passed", name)
			}
			// a defect must be reported for its own reason, not only through a later digest mismatch
			if want, ok := plantedReason[name]; ok && !strings.Contains(strings.Join(got, "\n"), want) {
				t.Fatalf("%s: reported %v, want a report containing %q", name, got, want)
			}
		})
	}
}
