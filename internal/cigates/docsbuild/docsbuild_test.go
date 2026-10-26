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
}

func (f *fake) deps(t *testing.T) Deps {
	return Deps{
		Docs: func(args []string) int {
			f.calls = append(f.calls, "docs "+args[0])
			if f.failAt == args[0] {
				return 1
			}
			return 0
		},
		Prepare: func(args []string) int {
			f.calls = append(f.calls, "prepare "+strings.Join(args, " "))
			if f.failAt == "docs-prepare" {
				return 1
			}
			for _, name := range requiredAssets {
				if name == f.skipAsset {
					continue
				}
				path := filepath.Join(assetDir, name)
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			return 0
		},
		MkDocs: func() error {
			f.calls = append(f.calls, "mkdocs")
			return f.mkdocsErr
		},
		Revision: func() string { return "rev1" },
	}
}

func inTempDir(t *testing.T) {
	t.Helper()
	old, _ := os.Getwd()
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
}

func runWith(t *testing.T, f *fake, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(args, f.deps(t), &out, &errb)
	return code, out.String(), errb.String()
}

func TestFullCheckChainOrder(t *testing.T) {
	inTempDir(t)
	f := &fake{}
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
	inTempDir(t)
	f := &fake{}
	if code, _, _ := runWith(t, f, "--mode=preview"); code != 0 {
		t.Fatal(code)
	}
	if len(f.calls) != 2 || f.calls[0] != "mkdocs" {
		t.Errorf("calls %v", f.calls)
	}
}

func TestProductionForcesFullCheck(t *testing.T) {
	inTempDir(t)
	f := &fake{}
	if code, stdout, _ := runWith(t, f, "--mode", "production"); code != 0 || !strings.Contains(stdout, "full_check=true") {
		t.Fatalf("code %d out %s", code, stdout)
	}
	if f.calls[0] != "docs validate-publication" {
		t.Errorf("calls %v", f.calls)
	}
}

func TestEachStepFailureStopsTheBuild(t *testing.T) {
	for _, step := range []string{"validate-publication", "check-built-site-links", "check-docs-search", "check-docs-accessibility", "check-candidate-facts", "docs-prepare"} {
		inTempDir(t)
		f := &fake{failAt: step}
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
	inTempDir(t)
	f := &fake{mkdocsErr: errors.New("exit status 1")}
	code, _, stderr := runWith(t, f, "--mode", "preview", "--full-check")
	if code != 1 || !strings.Contains(stderr, "mkdocs build") || len(f.calls) != 2 {
		t.Errorf("code %d stderr %q calls %v", code, stderr, f.calls)
	}
}

func TestIncompleteAssetTreeFails(t *testing.T) {
	inTempDir(t)
	f := &fake{skipAsset: "_headers"}
	code, _, stderr := runWith(t, f, "--mode", "preview")
	if code != 1 || !strings.Contains(stderr, "_headers") {
		t.Errorf("code %d stderr %q", code, stderr)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{{}, {"--mode"}, {"--mode", "dev"}, {"--mode", "preview", "extra"}, {"--mode", "preview", "--full-check=1"}} {
		inTempDir(t)
		f := &fake{}
		if code, _, _ := runWith(t, f, args...); code != 2 || len(f.calls) != 0 {
			t.Errorf("%v: code %d calls %v", args, code, f.calls)
		}
	}
}
