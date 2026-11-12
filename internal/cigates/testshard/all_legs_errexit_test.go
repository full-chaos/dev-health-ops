package testshard_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The all-legs form must fail the same way `VERB K COUNT` fails (CHAOS-13520): a leg command that fails only through
// errexit (a failing command with no explicit return) makes `VERB all COUNT` exit non-zero and stops at that leg. bash
// ignores set -e in every command of an || / && list or an if condition, functions included, so test_legs_all must not
// run its legs inside one.

// runLegsAll runs test_legs_all from ci/check_go.sh, text as written, with the given leg function, as `v all 2`.
func runLegsAll(t *testing.T, leg string) (string, error) {
	t.Helper()
	fn := functionBody(t, scriptText(t), "test_legs_all") + "\n}\n"
	driver := "set -euo pipefail\ndie() { echo \"$*\" >&2; exit 2; }\n" + fn + leg + "\ntest_legs_all v 2 leg\n"
	cmd := exec.Command("bash", "-c", driver)
	cmd.Env = append(os.Environ(), "TMPDIR="+t.TempDir())
	out, err := cmd.Output()
	return string(out), err
}

func TestTheAllLegsFormFailsOnALegThatOnlyErrexitStops(t *testing.T) {
	t.Parallel()
	// Control: the same leg run as `VERB K COUNT` runs it (errexit on, not in a list) stops at the failing command.
	single := exec.Command("bash", "-c", "set -euo pipefail\nleg() { false; echo continued; }\nleg 1 2\n")
	if out, err := single.Output(); err == nil || strings.Contains(string(out), "continued") {
		t.Fatalf("control: the single-leg form of a leg that fails only through errexit must stop non-zero, got err=%v out=%q", err, out)
	}
	out, err := runLegsAll(t, "leg() { false; echo \"continued $1\"; }")
	if err == nil {
		t.Errorf("v all 2 with a leg that fails only through errexit exits 0; want non-zero (out=%q)", out)
	}
	if strings.Contains(out, "continued") {
		t.Errorf("v all 2 ran past the failing command of a leg (errexit was off inside the leg): %q", out)
	}
	if strings.Contains(out, "==> v 2 2") {
		t.Errorf("v all 2 went on to leg 2 after leg 1 failed: %q", out)
	}
	ok, err := runLegsAll(t, "leg() { echo \"leg $1 of $2\"; }")
	if err != nil {
		t.Fatalf("v all 2 with a passing leg: %v", err)
	}
	if want := "==> v 1 2\nleg 1 of 2\n==> v 2 2\nleg 2 of 2\n"; ok != want {
		t.Errorf("v all 2 with a passing leg printed %q, want %q", ok, want)
	}
}
