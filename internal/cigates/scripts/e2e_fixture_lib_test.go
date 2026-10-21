package scripts_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// ci/lib/e2e_fixture.sh is sourced by the live backend e2e and the metrics executed-proof runners. The tests below run
// its functions for real, with stub psql, dho and cigate executables (shell scripts; no Python anywhere).

func writeStub(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
}

func stubbedSeedEnv(t *testing.T) (bin, state string) {
	t.Helper()
	bin = t.TempDir()
	state = filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	// psql answers the existing-user SELECT with the stored id, and swallows every other statement.
	writeStub(t, bin, "psql", `
case "$*" in
  *"SELECT id FROM users WHERE email"*) [ -f "$STUB_STATE/user" ] && cat "$STUB_STATE/user"; exit 0 ;;
esac
echo "psql $*" >> "$STUB_STATE/psql.log"
exit 0
`)
	// dho refuses a second `users create` the way the verb does, and logs every verb it runs.
	writeStub(t, bin, "dho", `
echo "dho $*" >> "$STUB_STATE/dho.log"
cat > /dev/null
case "$*" in
  *"users create"*)
    if [ -f "$STUB_STATE/user" ]; then echo "Error: User with email e2e@test.local already exists"; exit 1; fi
    echo "bbbbbbbb-1111-4222-8333-444444444444" > "$STUB_STATE/user"
    echo "Created user: e2e@test.local (id: bbbbbbbb-1111-4222-8333-444444444444)"
    exit 0 ;;
esac
echo "Updated user: e2e@test.local (id: bbbbbbbb-1111-4222-8333-444444444444)"
`)
	writeStub(t, bin, "cigate", `echo "token-for-$E2E_USER_ID"`)
	return bin, state
}

func runSeed(t *testing.T, bin, state string) cmdResult {
	t.Helper()
	root := rootOf(t)
	script := `set -eu
source "` + filepath.Join(root, "ci/lib/e2e_fixture.sh") + `"
generate_auth_token
`
	env := append(inheritedEnv([]string{"PATH"}),
		"PATH="+bin+":/usr/bin:/bin",
		"STUB_STATE="+state, "BIN_DIR="+bin,
		"POSTGRES_HOST=h", "POSTGRES_PORT=1", "POSTGRES_SUPERUSER=u", "POSTGRES_SUPERUSER_PASSWORD=p", "POSTGRES_DB=d",
		"POSTGRES_URI=postgresql+asyncpg://u:p@h:1/d", "E2E_ORG_ID=11111111-2222-4333-8444-555555555555",
		"SETTINGS_ENCRYPTION_KEY=k")
	return cmdSpec{dir: root, env: env}.run("bash", "-c", script)
}

func TestSeedIdentityIsIdempotentAndUsesTheVerbs(t *testing.T) {
	bin, state := stubbedSeedEnv(t)
	first := runSeed(t, bin, state)
	if first.code != 0 || strings.TrimSpace(first.stdout) != "token-for-bbbbbbbb-1111-4222-8333-444444444444" {
		t.Fatalf("first seed: rc=%d stdout=%q stderr=%q", first.code, first.stdout, tail(first.stderr, 400))
	}
	second := runSeed(t, bin, state)
	if second.code != 0 || strings.TrimSpace(second.stdout) != "token-for-bbbbbbbb-1111-4222-8333-444444444444" {
		t.Fatalf("a rerun against the same database must converge: rc=%d stdout=%q stderr=%q", second.code, second.stdout, tail(second.stderr, 400))
	}
	log, _ := os.ReadFile(filepath.Join(state, "dho.log"))
	if n := strings.Count(string(log), "users create"); n != 1 {
		t.Errorf("users create ran %d times, want 1 (the second run must reuse the existing user):\n%s", n, log)
	}
	if n := strings.Count(string(log), "users update --id bbbbbbbb-1111-4222-8333-444444444444 --org 11111111-2222-4333-8444-555555555555 --role admin"); n != 2 {
		t.Errorf("the membership update ran %d times, want 2:\n%s", n, log)
	}
	psqlLog, _ := os.ReadFile(filepath.Join(state, "psql.log"))
	if !strings.Contains(string(psqlLog), "INSERT INTO organizations") || strings.Contains(string(psqlLog), "INSERT INTO users") || strings.Contains(string(psqlLog), "INSERT INTO memberships") {
		t.Errorf("only the organization row may be a raw write:\n%s", psqlLog)
	}
}

var stampRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{6})?\+00:00$`)

func TestRunStartStampIsAUTCISO8601Stamp(t *testing.T) {
	root := rootOf(t)
	before := time.Now().UTC().Add(-time.Second)
	res := cmdSpec{dir: root, env: inheritedEnv(nil)}.run("bash", "-c", `set -eu
source "`+filepath.Join(root, "ci/lib/e2e_fixture.sh")+`"
run_start_stamp
`)
	if res.code != 0 {
		t.Fatalf("rc=%d stderr=%q", res.code, res.stderr)
	}
	stamp := strings.TrimSpace(res.stdout)
	if !stampRe.MatchString(stamp) {
		t.Fatalf("stamp %q is not a UTC ISO8601 stamp", stamp)
	}
	got, err := time.Parse("2006-01-02T15:04:05.999999999Z07:00", stamp)
	if err != nil {
		t.Fatal(err)
	}
	if got.Before(before) || got.After(time.Now().UTC().Add(time.Second)) {
		t.Errorf("stamp %s is not the current time", stamp)
	}
}

func TestRunnersTakeTheirRunStartFromTheStampFunction(t *testing.T) {
	for _, rel := range []string{"ci/run_live_backend_e2e.sh", "ci/run_metrics_executed_proof.sh"} {
		text := readText(t, scriptPath(t, rel))
		if !strings.Contains(text, `RUN_START="$(run_start_stamp)"`) {
			t.Errorf("%s must take RUN_START from run_start_stamp", rel)
		}
		if !strings.Contains(text, `source "${ROOT_DIR}/ci/lib/e2e_fixture.sh"`) {
			t.Errorf("%s must source ci/lib/e2e_fixture.sh", rel)
		}
	}
}

func TestRunStartStampGuardCatchesAMutatedStamp(t *testing.T) {
	root := rootOf(t)
	lib := mutatedCopy(t, filepath.Join(root, "ci/lib/e2e_fixture.sh"), `printf '%s\n' "${stamp}"`, `printf 'mutated\n'`)
	res := cmdSpec{dir: root, env: inheritedEnv(nil)}.run("bash", "-c", `set -eu
source "`+lib+`"
run_start_stamp
`)
	if stampRe.MatchString(strings.TrimSpace(res.stdout)) {
		t.Fatalf("a mutated stamp %q passed the stamp format check", res.stdout)
	}
}
