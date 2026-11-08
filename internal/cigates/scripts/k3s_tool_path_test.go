package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-13084: an ARC runner has helm only under _work/_tool (azure/setup-helm), outside the fixed base PATH. The skip guard
// finds it there, so the script a cell runs must find the same helm: here a marked helm that only that kind of directory holds.
func TestK3sRunnersUseTheHelmTheSkipGuardFound(t *testing.T) {
	tool := filepath.Join(t.TempDir(), "_work", "_tool", "helm", "3.21.4", "arm64", "linux-arm64")
	if err := os.MkdirAll(tool, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tool, "helm"), []byte("#!/usr/bin/env bash\necho \"tool-helm $*\" >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tool+string(os.PathListSeparator)+os.Getenv("PATH"))
	for name, run := range map[string]func() cmdResult{
		"pdp_load_k3s":          func() cmdResult { return pdpRun(t, nil, "--print-plan") },
		"auth_owner_move_k3s":   func() cmdResult { return k3sRun(t, nil, "--print-plan") },
	} {
		if res := run(); !strings.Contains(res.stderr, "tool-helm ") {
			t.Errorf("%s --print-plan did not run the helm under _work/_tool: exit %d, stderr %q", name, res.code, res.stderr)
		}
	}
}

// CHAOS-13084: a run that fakes kubectl also fakes envsubst, so it needs no gettext on the host (the ARC image has none), and
// the fake renders the templates the scripts fill exactly as envsubst does.
func TestK3sFakeRunsUseAFaithfulFakeEnvsubst(t *testing.T) {
	mark := filepath.Join(t.TempDir(), "envsubst.used")
	if res, _, _ := pdpFakeRun(t, "", "FAKE_ENVSUBST_MARK="+mark); res.code != 0 {
		t.Fatalf("a fake run without a breach: exit %d\n%s", res.code, res.stderr)
	}
	if _, err := os.Stat(mark); err != nil {
		t.Fatal("the fake pdp_load_k3s run did not use the fake envsubst")
	}
	real, err := exec.LookPath("envsubst")
	if err != nil {
		t.Skip("envsubst is not installed: the fake is not compared with it")
	}
	fake := filepath.Join(t.TempDir(), "envsubst")
	if err := os.WriteFile(fake, []byte(fakeEnvsubst), 0o755); err != nil {
		t.Fatal(err)
	}
	root := rootOf(t)
	env := []string{shimBasePath(), "IMG_MIG=registry.example/m@sha256:" + strings.Repeat("b", 64), "RUNNER_IMAGE=registry.example/r:1", "LOCAL_PG=pg-local:17"}
	for _, c := range []struct{ tpl, format string }{
		{"ci/pdp_load_k3s/runner-pod.tpl", "${IMG_MIG} ${RUNNER_IMAGE}"},
		{"ci/auth_k8s/postgres.yaml", "${LOCAL_PG}"},
		{"ci/auth_k8s/postgres.yaml", ""},
	} {
		args := []string{}
		if c.format != "" {
			args = append(args, c.format)
		}
		e := append(append([]string{}, env...), "IN="+filepath.Join(root, c.tpl))
		want := cmdSpec{dir: root, env: e}.run("bash", append([]string{"-c", `exec "$0" "$@" <"$IN"`, real}, args...)...)
		got := cmdSpec{dir: root, env: e}.run("bash", append([]string{"-c", `exec "$0" "$@" <"$IN"`, fake}, args...)...)
		if want.code != 0 || got.code != 0 || got.stdout != want.stdout {
			t.Errorf("%s %q: fake envsubst differs from envsubst (exit %d/%d)\nwant %q\ngot  %q", c.tpl, c.format, want.code, got.code, want.stdout, got.stdout)
		}
	}
}
