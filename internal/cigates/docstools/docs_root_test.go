package docstools_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every Python script behind these verbs took its repo root from its own file path (Path(__file__).resolve().parents[1]),
// so a run from any directory inside the repo, however reached, used the same root.
var docsRootVerbs = []struct {
	verb  string
	files []string
	want  string
}{
	{"gen-taxonomy", []string{"docs/reference/taxonomies/investment.md"}, ""},
	{"check-investment-drift", []string{"docs/reference/taxonomies/investment.md"}, "Investment docs drift check passed"},
	{"gen-queue-mapping", []string{"docs/contribute/architecture/go-worker-runtime.md", "deploy/go-workers/deployment.json", "contracts/jobs/v1/registry.json", "contracts/jobs/v1/migration-state.json"}, ""},
	{"check-queue-mapping-drift", []string{"docs/contribute/architecture/go-worker-runtime.md", "deploy/go-workers/deployment.json", "contracts/jobs/v1/registry.json", "contracts/jobs/v1/migration-state.json"}, "Queue mapping docs drift check passed"},
	{"check-provider-limits", []string{"mkdocs.yml", "docs/reference/limits-and-compatibility/provider-limits.md"}, "Provider limits page check passed"},
}

func docsRootTree(t *testing.T, files []string) string {
	t.Helper()
	root := t.TempDir()
	for _, rel := range files {
		body, err := os.ReadFile(filepath.Join("..", "..", "..", filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func wantDocsVerbPass(t *testing.T, verb, want string) {
	t.Helper()
	code, stdout, stderr := runDocs(verb)
	if code != 0 || !strings.Contains(stdout, want) || stderr != "" {
		t.Errorf("%s: exit %d, stdout %q, stderr %q; want a pass", verb, code, stdout, stderr)
	}
}

func TestDocsVerbsFindTheRepoRootFromANestedDirectory(t *testing.T) {
	for _, v := range docsRootVerbs {
		t.Run(v.verb, func(t *testing.T) {
			root := docsRootTree(t, v.files)
			nested := filepath.Join(root, "ci", "deep")
			if err := os.MkdirAll(nested, 0o755); err != nil {
				t.Fatal(err)
			}
			t.Chdir(nested)
			wantDocsVerbPass(t, v.verb, v.want)
		})
	}
}

func TestDocsVerbsFindTheRepoRootFromASymlinkedDirectory(t *testing.T) {
	for _, v := range docsRootVerbs {
		t.Run(v.verb, func(t *testing.T) {
			root := docsRootTree(t, v.files)
			nested := filepath.Join(root, "ci", "deep")
			if err := os.MkdirAll(nested, 0o755); err != nil {
				t.Fatal(err)
			}
			link := filepath.Join(t.TempDir(), "link")
			if err := os.Symlink(nested, link); err != nil {
				t.Fatal(err)
			}
			t.Chdir(link)
			wantDocsVerbPass(t, v.verb, v.want)
		})
	}
}

// Run from a subdirectory of this repo, the repo's own files are the ones read (the gen verbs rewrite their block in place,
// so they run on a copy of the tree).
func TestDocsVerbsFromARepoSubdirectoryReadTheRepoFiles(t *testing.T) {
	for _, v := range docsRootVerbs {
		t.Run(v.verb, func(t *testing.T) {
			root := docsRootTree(t, v.files)
			sub := filepath.Join(root, "internal", "cigates", "docstools")
			if err := os.MkdirAll(sub, 0o755); err != nil {
				t.Fatal(err)
			}
			t.Chdir(sub)
			wantDocsVerbPass(t, v.verb, v.want)
		})
	}
}

// Known difference from the Python (stricter, named): outside any tree holding the verb's files nothing is found.
func TestDocsVerbsOutsideAnyTreeStillRefuse(t *testing.T) {
	for _, v := range docsRootVerbs {
		t.Run(v.verb, func(t *testing.T) {
			t.Chdir(t.TempDir())
			code, stdout, stderr := runDocs(v.verb)
			if code != 1 || stdout == "" && stderr == "" {
				t.Errorf("%s: exit %d, stdout %q, stderr %q; want a refusal", v.verb, code, stdout, stderr)
			}
		})
	}
}
