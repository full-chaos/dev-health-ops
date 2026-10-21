package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// ci/auth-contracts-ajv/scope.sh must fail closed: a failed `git diff` read as "nothing changed" would skip the whole
// ajv check and leave a green job.

func runScope(t *testing.T, repo, script string, env ...string) (cmdResult, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "github_output")
	if err := os.WriteFile(out, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	res := cmdSpec{dir: repo, env: append([]string{shimBasePath(), "GITHUB_OUTPUT=" + out}, env...)}.run("bash", script)
	return res, readText(t, out)
}

func TestAjvScopeStepFailsClosed(t *testing.T) {
	root := rootOf(t)
	script := filepath.Join(root, "ci", "auth-contracts-ajv", "scope.sh")
	change := func(rel string) (repo, base, head string) {
		r := newGitRepo(t.TempDir())
		base = r.commit("a.txt", "a")
		head = r.commit(rel, "x")
		if r.err != nil {
			t.Fatal(r.err)
		}
		return r.dir, base, head
	}
	t.Run("a contract change requires the check", func(t *testing.T) {
		repo, base, head := change("contracts/auth/v1/jsonschema/x.json")
		res, out := runScope(t, repo, script, "EVENT=pull_request", "BASE_SHA="+base, "HEAD_SHA="+head)
		if res.code != 0 || strings.TrimSpace(out) != "run=true" {
			t.Fatalf("exit %d output %q: %s", res.code, out, res.stderr)
		}
	})
	t.Run("an unrelated change skips the check", func(t *testing.T) {
		repo, base, head := change("docs/other.md")
		res, out := runScope(t, repo, script, "EVENT=pull_request", "BASE_SHA="+base, "HEAD_SHA="+head)
		if res.code != 0 || strings.TrimSpace(out) != "run=false" {
			t.Fatalf("exit %d output %q: %s", res.code, out, res.stderr)
		}
	})
	t.Run("a push always runs the check", func(t *testing.T) {
		repo, _, _ := change("docs/other.md")
		res, out := runScope(t, repo, script, "EVENT=push")
		if res.code != 0 || strings.TrimSpace(out) != "run=true" {
			t.Fatalf("exit %d output %q: %s", res.code, out, res.stderr)
		}
	})
	for name, env := range map[string][]string{
		"unknown shas": {"EVENT=pull_request", "BASE_SHA=" + strings.Repeat("0", 40), "HEAD_SHA=" + strings.Repeat("1", 40)},
		"empty shas":   {"EVENT=pull_request", "BASE_SHA=", "HEAD_SHA="},
		"missing shas": {"EVENT=pull_request"},
	} {
		t.Run("a failed diff fails the step: "+name, func(t *testing.T) {
			repo, _, _ := change("docs/other.md")
			res, out := runScope(t, repo, script, env...)
			if res.code == 0 || strings.Contains(out, "run=false") {
				t.Fatalf("the scope step swallowed the failure (exit %d) and reported %q", res.code, out)
			}
		})
	}
}

// Secret handling in the kind proof: no credential in argv, manifests or describe output.

const k8sHarness = `set -euo pipefail
SCRIPT_DIR="$1"; LOG="$2"; LOCAL_PG=img; LOCAL_SVC=svc
K() { printf 'ARGV %s\n' "$*" >>"$LOG"; if [ ! -t 0 ]; then printf 'STDIN-BEGIN\n' >>"$LOG"; cat >>"$LOG" || true; fi; }
K_RENDER() { envsubst '${LOCAL_PG} ${PG_PASSWORD}' <"$1"; }
`

func TestKindProofPutsNoCredentialInArgv(t *testing.T) {
	for _, tool := range []string{"envsubst", "openssl"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not installed", tool)
		}
	}
	root := rootOf(t)
	text := readText(t, filepath.Join(root, "ci", "check_auth_k8s.sh"))
	m := regexp.MustCompile(`(?ms)^PG_PASSWORD=.*?^# --- the release`).FindString(text)
	if m == "" {
		t.Fatal("step 2 of ci/check_auth_k8s.sh moved")
	}
	step := strings.TrimSuffix(strings.Replace(m, "openssl genpkey", "true genpkey", 1), "# --- the release")
	dir := t.TempDir()
	log := filepath.Join(dir, "log")
	script := filepath.Join(dir, "step2.sh")
	if err := os.WriteFile(script, []byte(k8sHarness+"WORK="+dir+"\n"+step), 0o644); err != nil {
		t.Fatal(err)
	}
	res := cmdSpec{dir: root, env: inheritedEnv(nil), stdin: []byte{}}.run("bash", script, filepath.Join(root, "ci"), log)
	if res.code != 0 {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	secret := regexp.MustCompile(`[0-9a-f]{24}`)
	for _, ln := range strings.Split(readText(t, log), "\n") {
		if !strings.HasPrefix(ln, "ARGV ") {
			continue
		}
		if secret.MatchString(ln) {
			t.Errorf("credential in argv (visible to ps and audit tooling): %s", ln)
		}
		if strings.Contains(ln, "--from-literal") {
			t.Errorf("--from-literal in argv: %s", ln)
		}
	}
}

func postgresContainerOf(t *testing.T, text string) map[string]any {
	t.Helper()
	dec := yaml.NewDecoder(strings.NewReader(text))
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		t.Fatal(err)
	}
	spec := doc["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	return spec["containers"].([]any)[0].(map[string]any)
}

// postgresDefects: the password is a secret reference and not a literal, postgres runs as non-root, and the capabilities map
// is exactly {drop: [ALL]}: an `add` entry (or any other key) is a defect, as in the python guard's whole-map comparison.
func postgresDefects(c map[string]any) []string {
	var out []string
	var password map[string]any
	for _, e := range c["env"].([]any) {
		if m := e.(map[string]any); m["name"] == "POSTGRES_PASSWORD" {
			password = m
		}
	}
	if password == nil {
		return []string{"no POSTGRES_PASSWORD env"}
	}
	if _, literal := password["value"]; literal {
		out = append(out, "POSTGRES_PASSWORD is a literal value: kubectl describe prints it into CI logs")
	}
	if vf, _ := password["valueFrom"].(map[string]any); vf["secretKeyRef"] == nil {
		out = append(out, "POSTGRES_PASSWORD has no secretKeyRef")
	}
	sc, _ := c["securityContext"].(map[string]any)
	if sc["runAsNonRoot"] != true {
		out = append(out, "postgres does not run as a non-root user")
	}
	caps, _ := sc["capabilities"].(map[string]any)
	drop, _ := caps["drop"].([]any)
	if len(caps) != 1 || len(drop) != 1 || drop[0] != "ALL" {
		out = append(out, fmt.Sprintf("capabilities is %v, want exactly {drop: [ALL]}", caps))
	}
	return out
}

func TestKindPostgresPasswordIsASecretReferenceAndRunsNonRoot(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	text := readText(t, filepath.Join(root, "ci", "auth_k8s", "postgres.yaml"))
	if d := postgresDefects(postgresContainerOf(t, text)); len(d) > 0 {
		t.Fatal(strings.Join(d, "\n"))
	}
	for _, m := range []struct{ name, old, new string }{
		{"a capability added", "{drop: [ALL]}", "{drop: [ALL], add: [SYS_ADMIN]}"},
		{"a literal password", "              valueFrom:", "              value: hunter2\n              valueFrom:"},
		{"root allowed", "            runAsNonRoot: true", "            runAsNonRoot: false"},
	} {
		if !strings.Contains(text, m.old) {
			t.Fatalf("planted defect %q did not land: %q not in postgres.yaml", m.name, m.old)
		}
		if d := postgresDefects(postgresContainerOf(t, strings.Replace(text, m.old, m.new, 1))); len(d) == 0 {
			t.Errorf("planted defect not caught: %s", m.name)
		}
	}
}

// A shim-first PATH reaches every child these guards start: a script that calls python3 fails with 127 through the same
// runner the scope guard uses.
func TestAScriptCallingPythonFailsThroughTheGuardRunners(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	planted := filepath.Join(dir, "scope.sh")
	if err := os.WriteFile(planted, []byte("#!/usr/bin/env bash\nset -euo pipefail\npython3 -c 'print(1)'\necho run=true >> \"${GITHUB_OUTPUT}\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	res, out := runScope(t, dir, planted, "EVENT=push")
	if res.code != 127 || strings.Contains(out, "run=true") {
		t.Fatalf("a python3 call ran through the scope runner: exit %d output %q", res.code, out)
	}
	if r := (freeSpec{dir: dir, env: []string{shimBasePath()}}).run("bash", "-c", "python -c 1"); r.code != 127 {
		t.Errorf("python resolved to a real interpreter under the shim-first base PATH: exit %d", r.code)
	}
}
