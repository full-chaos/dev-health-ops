package routing_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const goMissingLine = "FAIL: bigboy-cut: go not found on PATH; needed to build cigate for the DEPLOY_SHA STEPs\n"

// runCut runs the real bigboy-cut.sh on an empty bigboy root with PATH holding only bin: no docker, no seq, so a cut
// that gets past its preflight stops at images-wait at once and touches nothing.
func runCut(t *testing.T, bin string, deploySHA string) result {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal(err)
	}
	repo := routingRepoRoot(t)
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "_records"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "_records"), "images.env", nil)
	script := filepath.Join(repo, "ci", "bigboy", "bigboy-cut.sh")
	// go test keys its cache on the files this process opens; bash reads the script in another process.
	for _, f := range []string{script, filepath.Join(repo, "ci", "bigboy", "arm64-child-digest.sh")} {
		if _, err := os.ReadFile(f); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(bash, script, "aaaaaaaa", strings.Repeat("a", 40))
	cmd.Env = []string{"PATH=" + bin, "BIGBOY_ROOT=" + root, "BIGBOY_TOOLS_DIR=" + filepath.Join(repo, "ci", "bigboy"), "BIGBOY_OPS_REPO=" + repo}
	if deploySHA != "" {
		cmd.Env = append(cmd.Env, "DEPLOY_SHA="+deploySHA)
	}
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	code := 0
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		code = ee.ExitCode()
	}
	return result{code, so.String(), se.String()}
}

func TestTheCutStopsWithOneLineWhenGoIsMissing(t *testing.T) {
	r := runCut(t, t.TempDir(), strings.Repeat("d", 40))
	wantCode(t, r, 1)
	if r.stderr != goMissingLine || r.stdout != "" {
		t.Fatalf("want exactly the go line on stderr and no STEP:\nstdout:\n%s\nstderr:\n%s", r.stdout, r.stderr)
	}
}

func TestTheCutGoesPastTheGoCheckWhenGoIsThereOrNotNeeded(t *testing.T) {
	withGo := t.TempDir()
	writeFile(t, withGo, "go", []byte("#!/bin/sh\nexit 0\n"))
	if err := os.Chmod(filepath.Join(withGo, "go"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, r := range map[string]result{
		"go on PATH":           runCut(t, withGo, strings.Repeat("d", 40)),
		"no DEPLOY_SHA, no go": runCut(t, t.TempDir(), ""),
	} {
		wantNotIn(t, r.stderr, "go not found")
		if !strings.Contains(r.stdout, "cut start") || !strings.Contains(r.stdout, "STEP images-wait rc=1") {
			t.Errorf("%s: the cut did not run past its preflight:\nstdout:\n%s\nstderr:\n%s", name, r.stdout, r.stderr)
		}
	}
}
