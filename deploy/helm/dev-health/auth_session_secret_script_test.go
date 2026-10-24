package devhealth_test

import (
	"bytes"
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const sessionSecretScript = "../../scripts/create-auth-session-secret.sh"

const fakeKubectl = `#!/usr/bin/env bash
printf '%s\n' "$@" >>"${FAKE_REC}/argv"
printf -- '--call--\n' >>"${FAKE_REC}/argv"
for a in "$@"; do
  case "$a" in
  --from-file=*) cp "${a##*=}" "${FAKE_REC}/value" ;;
  esac
done
case "${FAKE_MODE:-ok}" in
exists) echo 'Error from server (AlreadyExists): secrets "auth-session" already exists' >&2; exit 1 ;;
fail) echo 'boom' >&2; exit 1 ;;
esac
exit 0
`

type scriptRun struct {
	stdout, stderr string
	argv           string
	value          string
	err            error
	tmp            string
}

func runSessionSecretScript(t *testing.T, script, mode string, args ...string) scriptRun {
	t.Helper()
	return runSessionSecretScriptWith(t, script, mode, nil, args...)
}

func runSessionSecretScriptWith(t *testing.T, script, mode string, shims map[string]string, args ...string) scriptRun {
	t.Helper()
	for _, tool := range []string{"bash", "openssl"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("%s is required: %v", tool, err)
		}
	}
	bin, rec, tmp := t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(fakeKubectl), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range shims {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("bash", append([]string{script}, args...)...)
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_REC="+rec, "FAKE_MODE="+mode, "TMPDIR="+tmp)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	argv, _ := os.ReadFile(filepath.Join(rec, "argv"))
	value, _ := os.ReadFile(filepath.Join(rec, "value"))
	return scriptRun{out.String(), errb.String(), string(argv), string(value), err, tmp}
}

func leaks(r scriptRun) []string {
	var found []string
	for name, text := range map[string]string{"argv": r.argv, "stdout": r.stdout, "stderr": r.stderr} {
		if r.value != "" && strings.Contains(text, r.value) {
			found = append(found, name)
		}
	}
	return found
}

func requireReachedKubectl(t *testing.T, r scriptRun) {
	t.Helper()
	if r.err != nil || r.value == "" || strings.Count(r.argv, "--call--") != 1 {
		t.Fatalf("the run did not reach kubectl once: err=%v value-empty=%v\n%s%s", r.err, r.value == "", r.stdout, r.stderr)
	}
}

func TestSessionSecretScriptPlanRunsNothing(t *testing.T) {
	r := runSessionSecretScript(t, sessionSecretScript, "ok", "--plan")
	if r.err != nil {
		t.Fatalf("--plan: %v\n%s%s", r.err, r.stdout, r.stderr)
	}
	if r.argv != "" || r.value != "" {
		t.Fatalf("--plan called kubectl: %q", r.argv)
	}
	for _, want := range []string{"namespace=dev-health", "secret=auth-session", "key=AUTH_SESSION_SUCCESSOR_KEY", "32 bytes base64url"} {
		if !strings.Contains(r.stdout, want) {
			t.Errorf("--plan output lacks %q:\n%s", want, r.stdout)
		}
	}
}

func TestSessionSecretScriptCreatesOnceAndLeaksNothing(t *testing.T) {
	r := runSessionSecretScript(t, sessionSecretScript, "ok")
	if r.err != nil {
		t.Fatalf("run: %v\n%s%s", r.err, r.stdout, r.stderr)
	}
	requireReachedKubectl(t, r)
	if got := leaks(r); len(got) != 0 {
		t.Fatalf("value leaked into %v", got)
	}
	if n := strings.Count(r.argv, "--call--"); n != 1 {
		t.Fatalf("kubectl called %d times, want 1:\n%s", n, r.argv)
	}
	args := strings.Fields(r.argv)
	want := []string{"-n", "dev-health", "create", "secret", "generic", "auth-session"}
	if len(args) < len(want) || strings.Join(args[:len(want)], " ") != strings.Join(want, " ") {
		t.Fatalf("argv = %v, want prefix %v", args, want)
	}
	if !regexp.MustCompile(`--from-file=AUTH_SESSION_SUCCESSOR_KEY=\S+`).MatchString(r.argv) {
		t.Fatalf("argv has no --from-file for the key:\n%s", r.argv)
	}
	if regexp.MustCompile(`\b(get|describe|exec|apply|patch|replace|delete)\b`).MatchString(r.argv) {
		t.Fatalf("argv has a forbidden verb:\n%s", r.argv)
	}
	if strings.ContainsAny(r.value, "=+/\n") {
		t.Fatalf("value is not unpadded base64url (len %d)", len(r.value))
	}
	raw, err := base64.RawURLEncoding.DecodeString(r.value)
	if err != nil || len(raw) != 32 {
		t.Fatalf("value does not decode to 32 bytes: len=%d err=%v", len(raw), err)
	}
	if left, _ := os.ReadDir(r.tmp); len(left) != 0 {
		t.Fatalf("temp dir not removed: %d entries", len(left))
	}
	if !strings.Contains(r.stdout, "created: secret auth-session") {
		t.Fatalf("no created line:\n%s", r.stdout)
	}
}

func TestSessionSecretScriptTwoRunsDiffer(t *testing.T) {
	a := runSessionSecretScript(t, sessionSecretScript, "ok")
	b := runSessionSecretScript(t, sessionSecretScript, "ok")
	if a.value == "" || a.value == b.value {
		t.Fatal("two runs made the same key")
	}
}

func TestSessionSecretScriptRefusesWhenSecretExists(t *testing.T) {
	r := runSessionSecretScript(t, sessionSecretScript, "exists")
	if r.err == nil {
		t.Fatal("exit 0 although the Secret exists")
	}
	if !strings.Contains(r.stdout, "STOPPED: the Secret already exists; nothing was changed") {
		t.Fatalf("no STOPPED line:\n%s", r.stdout)
	}
	if r.value == "" || strings.Count(r.argv, "--call--") != 1 {
		t.Fatalf("the exists path did not reach kubectl once:\n%s", r.argv)
	}
	if got := leaks(r); len(got) != 0 {
		t.Fatalf("value leaked on the exists path into %v", got)
	}
	if strings.Count(r.argv, "--call--") != 1 {
		t.Fatalf("expected one create call, got:\n%s", r.argv)
	}
}

func TestSessionSecretScriptOtherFailureIsQuiet(t *testing.T) {
	r := runSessionSecretScript(t, sessionSecretScript, "fail")
	if r.err == nil || !strings.Contains(r.stdout, "FAILED: the Secret was not created") {
		t.Fatalf("want FAILED and non-zero, got err=%v\n%s", r.err, r.stdout)
	}
	if strings.Contains(r.stdout+r.stderr, "boom") {
		t.Fatal("kubectl error text reached the output")
	}
}

func TestSessionSecretScriptNamesMatchTheChart(t *testing.T) {
	values, err := os.ReadFile("values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?s)successor:\s*\n\s*secretName: ""\s*\n\s*secretKey: AUTH_SESSION_SUCCESSOR_KEY\n`).Match(values) {
		t.Fatal("chart default for authService.sessionKeys.successor changed; update the script")
	}
	script, err := os.ReadFile(sessionSecretScript)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`KEY_NAME="AUTH_SESSION_SUCCESSOR_KEY"`, `SECRET_NAME="${SECRET_NAME:-auth-session}"`, `NAMESPACE="${NAMESPACE:-dev-health}"`} {
		if !strings.Contains(string(script), want) {
			t.Errorf("script lacks %s", want)
		}
	}
}

func mutatedSessionSecretScript(t *testing.T, from, to string) string {
	t.Helper()
	src, err := os.ReadFile(sessionSecretScript)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), from) {
		t.Fatalf("mutation anchor missing in the script: %s", from)
	}
	path := filepath.Join(t.TempDir(), "mutated.sh")
	if err := os.WriteFile(path, []byte(strings.Replace(string(src), from, to, 1)), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSessionSecretLeakCheckCatchesStdoutLeak(t *testing.T) {
	path := mutatedSessionSecretScript(t, "# MUTATION-ANCHOR-STDOUT", `cat "${WORK}/key"; echo`)
	r := runSessionSecretScript(t, path, "ok")
	requireReachedKubectl(t, r)
	if got := leaks(r); len(got) == 0 {
		t.Fatal("a value printed to stdout was not caught")
	}
}

func TestSessionSecretLeakCheckCatchesArgvLeak(t *testing.T) {
	path := mutatedSessionSecretScript(t, `--from-file="${KEY_NAME}=${WORK}/key"`, `--from-file="${KEY_NAME}=${WORK}/key" --from-literal=x="$(cat "${WORK}/key")"`)
	r := runSessionSecretScript(t, path, "ok")
	requireReachedKubectl(t, r)
	if got := leaks(r); len(got) == 0 {
		t.Fatal("a value in argv was not caught")
	}
}

func TestSessionSecretLeakCheckRejectsARunThatNeverReachedKubectl(t *testing.T) {
	path := mutatedSessionSecretScript(t, "# MUTATION-ANCHOR-STDOUT", "exit 3")
	r := runSessionSecretScript(t, path, "ok")
	if r.err == nil || r.value != "" {
		t.Fatalf("the broken script should fail before kubectl: err=%v", r.err)
	}
	if got := leaks(r); len(got) != 0 {
		t.Fatalf("a run with no value reported leaks %v", got)
	}
}

const bsdBase64 = `#!/usr/bin/env bash
case "$1" in -d|--decode) echo "base64: invalid option -- d" >&2; exit 1 ;; esac
exec /usr/bin/base64 "$@"
`

func TestSessionSecretScriptNeedsNoGNUBase64Decode(t *testing.T) {
	r := runSessionSecretScriptWith(t, sessionSecretScript, "ok", map[string]string{"base64": bsdBase64})
	requireReachedKubectl(t, r)
	if got := leaks(r); len(got) != 0 {
		t.Fatalf("value leaked into %v", got)
	}
}

const shortKeyOpenssl = `#!/usr/bin/env bash
printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n'
`

func TestSessionSecretScriptRefusesAShortKeyBeforeKubectl(t *testing.T) {
	r := runSessionSecretScriptWith(t, sessionSecretScript, "ok", map[string]string{"openssl": shortKeyOpenssl})
	if r.err == nil || r.argv != "" || !strings.Contains(r.stdout, "FAILED: the generated key is not 32 bytes") {
		t.Fatalf("a short key must stop before kubectl: err=%v argv=%q\n%s", r.err, r.argv, r.stdout)
	}
}
