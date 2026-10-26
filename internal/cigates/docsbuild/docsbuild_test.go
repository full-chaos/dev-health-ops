package docsbuild

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fake struct {
	calls     []string
	failAt    string
	mkdocsErr error
	skipAsset string
	start     string
	roots     []string
}

func (f *fake) deps(t *testing.T) Deps {
	return Deps{
		Start: f.start,
		Docs: func(root string, args []string) int {
			f.roots = append(f.roots, root)
			f.calls = append(f.calls, "docs "+args[0])
			if f.failAt == args[0] {
				return 1
			}
			return 0
		},
		Prepare: func(root string, args []string) int {
			f.roots = append(f.roots, root)
			f.calls = append(f.calls, "prepare "+strings.Join(args, " "))
			if f.failAt == "docs-prepare" {
				return 1
			}
			for _, name := range requiredAssets {
				if name == f.skipAsset {
					continue
				}
				path := filepath.Join(root, assetDir, name)
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			return 0
		},
		MkDocs: func(root string, announce func(string)) error {
			f.roots = append(f.roots, root)
			f.calls = append(f.calls, "mkdocs")
			announce("python3")
			return f.mkdocsErr
		},
		Revision: func(string) string { return "rev1" },
	}
}

// repoDir makes a temporary repository root (go.mod and mkdocs.yml) and returns it.
func repoDir(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{"go.mod", "mkdocs.yml"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func runWith(t *testing.T, f *fake, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(args, f.deps(t), &out, &errb)
	return code, out.String(), errb.String()
}

func TestFullCheckChainOrder(t *testing.T) {
	f := &fake{}
	f.start = repoDir(t)
	if code, _, stderr := runWith(t, f, "--mode", "preview", "--full-check"); code != 0 {
		t.Fatalf("code %d: %s", code, stderr)
	}
	want := "docs validate-publication|mkdocs|docs check-built-site-links|docs check-docs-search|docs check-docs-accessibility|docs check-candidate-facts"
	got := strings.Join(f.calls[:len(f.calls)-1], "|")
	if got != want {
		t.Errorf("chain\n got %s\nwant %s", got, want)
	}
	if last := f.calls[len(f.calls)-1]; !strings.Contains(last, "--mode preview") || !strings.Contains(last, "--source-revision rev1") {
		t.Errorf("prepare call: %s", last)
	}
}

func TestPreviewWithoutFullCheckBuildsAndPreparesOnly(t *testing.T) {
	f := &fake{}
	f.start = repoDir(t)
	if code, _, _ := runWith(t, f, "--mode=preview"); code != 0 {
		t.Fatal(code)
	}
	if len(f.calls) != 2 || f.calls[0] != "mkdocs" {
		t.Errorf("calls %v", f.calls)
	}
}

func TestProductionForcesFullCheck(t *testing.T) {
	f := &fake{}
	f.start = repoDir(t)
	if code, stdout, _ := runWith(t, f, "--mode", "production"); code != 0 || !strings.Contains(stdout, "full_check=true") {
		t.Fatalf("code %d out %s", code, stdout)
	}
	if f.calls[0] != "docs validate-publication" {
		t.Errorf("calls %v", f.calls)
	}
}

func TestEachStepFailureStopsTheBuild(t *testing.T) {
	for _, step := range []string{"validate-publication", "check-built-site-links", "check-docs-search", "check-docs-accessibility", "check-candidate-facts", "docs-prepare"} {
		f := &fake{failAt: step}
		f.start = repoDir(t)
		code, _, stderr := runWith(t, f, "--mode", "preview", "--full-check")
		if code != 1 || !strings.Contains(stderr, step) {
			t.Errorf("%s: code %d stderr %q", step, code, stderr)
		}
		if strings.Contains(strings.Join(f.calls, "|"), "prepare --source") && step != "docs-prepare" {
			t.Errorf("%s: assets were prepared after the failure", step)
		}
	}
}

func TestMkDocsFailureStopsTheBuild(t *testing.T) {
	f := &fake{mkdocsErr: errors.New("exit status 1")}
	f.start = repoDir(t)
	code, _, stderr := runWith(t, f, "--mode", "preview", "--full-check")
	if code != 1 || !strings.Contains(stderr, "mkdocs build") || len(f.calls) != 2 {
		t.Errorf("code %d stderr %q calls %v", code, stderr, f.calls)
	}
}

func TestIncompleteAssetTreeFails(t *testing.T) {
	f := &fake{skipAsset: "_headers"}
	f.start = repoDir(t)
	code, _, stderr := runWith(t, f, "--mode", "preview")
	if code != 1 || !strings.Contains(stderr, "_headers") {
		t.Errorf("code %d stderr %q", code, stderr)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{{}, {"--mode"}, {"--mode", "dev"}, {"--mode", "preview", "extra"}, {"--mode", "preview", "--full-check=1"}} {
		f := &fake{}
		f.start = repoDir(t)
		if code, _, _ := runWith(t, f, args...); code != 2 || len(f.calls) != 0 {
			t.Errorf("%v: code %d calls %v", args, code, f.calls)
		}
	}
}

func TestBuildRunsFromTheRepositoryRootWhateverTheStartDirectory(t *testing.T) {
	root := repoDir(t)
	sub := filepath.Join(root, "docs", "deep")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	f := &fake{start: sub}
	if code, _, stderr := runWith(t, f, "--mode", "preview", "--full-check"); code != 0 {
		t.Fatalf("code %d: %s", code, stderr)
	}
	for _, got := range f.roots {
		if got != root {
			t.Errorf("a step ran in %s, not the repository root %s", got, root)
		}
	}
	if _, err := os.Stat(filepath.Join(root, assetDir, "index.html")); err != nil {
		t.Errorf("assets are not under the repository root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(sub, ".build")); err == nil {
		t.Errorf(".build was created in the start directory")
	}
}

func TestNoRepositoryRootIsRefused(t *testing.T) {
	f := &fake{start: t.TempDir()}
	code, _, stderr := runWith(t, f, "--mode", "preview")
	if code != 1 || !strings.Contains(stderr, "no repository root") || len(f.calls) != 0 {
		t.Errorf("code %d stderr %q calls %v", code, stderr, f.calls)
	}
}

func TestRootIsFoundThroughASymlinkedStartDirectory(t *testing.T) {
	root := repoDir(t)
	sub := filepath.Join(root, "docs")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "docs-link")
	if err := os.Symlink(sub, link); err != nil {
		t.Fatal(err)
	}
	physicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	f := &fake{start: link}
	if code, _, stderr := runWith(t, f, "--mode", "preview"); code != 0 {
		t.Fatalf("code %d: %s", code, stderr)
	}
	for _, got := range f.roots {
		if got != physicalRoot {
			t.Errorf("a step ran in %s, not the repository root %s", got, physicalRoot)
		}
	}
}

func TestMkDocsBannerIsPrintedBeforeTheCall(t *testing.T) {
	f := &fake{start: repoDir(t), mkdocsErr: errors.New("exit status 1")}
	_, stdout, _ := runWith(t, f, "--mode", "preview")
	if !strings.Contains(stdout, "+ python3 -m mkdocs build --strict --config-file mkdocs.yml") {
		t.Errorf("a failing MkDocs call left no banner: %q", stdout)
	}
}

func TestEmptyStartDirectoryIsRefused(t *testing.T) {
	f := &fake{start: ""}
	code, _, stderr := runWith(t, f, "--mode", "preview")
	if code != 1 || !strings.Contains(stderr, "no working directory") || len(f.calls) != 0 {
		t.Errorf("code %d stderr %q calls %v", code, stderr, f.calls)
	}
}
