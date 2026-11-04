package checkgo_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// authRaceWeight reads the one row of a package from the real race weights.
func authRaceWeight(t *testing.T, file, pkg string) int {
	t.Helper()
	raw, err := os.ReadFile(scriptPath(t, "ci/go_race_weights.d/"+file))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == pkg {
			n, err := strconv.Atoi(f[1])
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatalf("no row for %s in %s", pkg, file)
	return 0
}

// authRaceTimeouts runs `check_go.sh auth race` against a fake go and returns the -timeout of each test call.
func authRaceTimeouts(t *testing.T, env ...string) ([]string, int, string) {
	t.Helper()
	code, errText, testArgs := runCheckGoWithFakeGo(t, []string{"auth", "race"}, env, jobDeadlineFakeGo)
	return timeoutArgs(testArgs), code, errText
}

func TestAuthRaceTimeoutFollowsTheWeightsOfTheAuthPackages(t *testing.T) {
	t.Parallel()
	w := authRaceWeight(t, "internal__auth__authapi.tsv", "internal/auth/authapi")
	// hosted runs of CHAOS-12938: 459 s, 564 s, 598.5 s (timed out at 10 min), 600.5 s (timed out)
	if w < 600 {
		t.Errorf("internal/auth/authapi race weight is %d s, want >= 600 (the slowest hosted run of CHAOS-12938)", w)
	}
	got, code, errText := authRaceTimeouts(t, "CHECK_GO_JOB_CAP_MIN=30")
	if code != 0 || len(got) != 1 {
		t.Fatalf("rc %d, -timeout of the calls %v, want one call; stderr: %s", code, got, tail(errText, 800))
	}
	secs, err := strconv.Atoi(strings.TrimSuffix(got[0], "s"))
	if err != nil {
		t.Fatalf("-timeout %q is not whole seconds", got[0])
	}
	if secs < w*3/2 {
		t.Errorf("auth race -timeout is %d s, want at least the authapi weight %d s plus the 50%% margin (%d s)", secs, w, w*3/2)
	}
	if secs > 27*60 {
		t.Errorf("auth race -timeout is %d s, want at most the job budget 1620 s", secs)
	}
}

func TestAuthRaceTimeoutCells(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name    string
		rows    []string
		env     []string
		want    string
		wantErr string
	}{
		{"the heaviest auth package sets it, with a 50% margin", []string{"internal/auth/authapi\t800", "internal/auth/session\t100"}, []string{"CHECK_GO_JOB_CAP_MIN=40"}, "1200s", ""},
		{"a light weight keeps the 10 min floor", []string{"internal/auth/authapi\t100"}, []string{"CHECK_GO_JOB_CAP_MIN=30"}, "600s", ""},
		{"the job budget bounds it", []string{"internal/auth/authapi\t1500"}, []string{"CHECK_GO_JOB_CAP_MIN=30"}, "1620s", ""},
		{"a package outside AUTH_PACKAGES does not count", []string{"internal/auth/authapi\t600", "internal/unrelated\t3000"}, []string{"CHECK_GO_JOB_CAP_MIN=30"}, "900s", ""},
		{"no row for the auth packages refuses", []string{"internal/unrelated\t3000"}, []string{"CHECK_GO_JOB_CAP_MIN=30"}, "", "auth race"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "rows.tsv"), []byte(strings.Join(c.rows, "\n")+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			got, code, errText := authRaceTimeouts(t, append(c.env, "GO_RACE_WEIGHTS_DIR="+dir)...)
			if c.wantErr != "" {
				if code == 0 || len(got) != 0 || !strings.Contains(errText, c.wantErr) {
					t.Errorf("rc %d, calls %v, stderr %s; want a refusal naming %q and no go test call", code, got, tail(errText, 800), c.wantErr)
				}
				return
			}
			if code != 0 || len(got) != 1 || got[0] != c.want {
				t.Errorf("rc %d, -timeout of the calls %v, want [%s]; stderr: %s", code, got, c.want, tail(errText, 800))
			}
		})
	}
}

func TestAuthRaceRefusesWithoutTheJobCapInCI(t *testing.T) {
	t.Parallel()
	got, code, errText := authRaceTimeouts(t, "GITHUB_ACTIONS=true")
	if code != 2 || len(got) != 0 || !strings.Contains(errText, "CHECK_GO_JOB_CAP_MIN is not set") {
		t.Errorf("rc %d, calls %v, stderr %s; want rc 2, no go test call, the missing-cap line", code, got, tail(errText, 800))
	}
}
