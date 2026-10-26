package devhealth_test

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const flipBackRunbook = "../../../docs/operate/runbooks/auth-users-flip-back-grants.md"

// The fake answers `kubectl get rs` with FAKE_RS and `kubectl get pods` with
// FAKE_PODS.
const flipBackKubectl = `#!/usr/bin/env bash
printf '%s\n' "$*" >>"${FAKE_REC}"
case "$2" in
rs) mode="${FAKE_RS}" ;;
pods) mode="${FAKE_PODS}" ;;
*) echo "unexpected kubectl $*" >&2; exit 2 ;;
esac
case "${mode}" in
fail) echo 'Error from server (Forbidden): forbidden' >&2; exit 1 ;;
zerofail) printf '0'; exit 1 ;;
empty) exit 0 ;;
zero) printf '0' ;;
one) printf '1' ;;
two) printf '2' ;;
esac
exit 0
`

// flipBackPodChecks returns the runbook's bash blocks that count pods, with
// the placeholders filled in.
func flipBackPodChecks(t *testing.T) []string {
	t.Helper()
	f, err := os.Open(flipBackRunbook)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var blocks []string
	var cur []string
	in := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case !in && line == "```bash":
			in, cur = true, nil
		case in && line == "```":
			in = false
			if block := strings.Join(cur, "\n"); strings.Contains(block, "kubectl get pods") {
				blocks = append(blocks, strings.NewReplacer(
					"<namespace>", "ns1", "<release>", "rel1", "<owned hash>", "abc123").Replace(block))
			}
		case in:
			cur = append(cur, sc.Text())
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 {
		t.Fatalf("pod-count checks in %s = %d, want 2", flipBackRunbook, len(blocks))
	}
	return blocks
}

// Each pod-count check fails closed. It first proves its selector finds the
// owned ReplicaSet (check 0: exactly 1 by pod-template-hash; check 1: at least
// 1 by the users-owned label), then passes only when the pod count is 0. A
// kubectl failure (also one that prints a count), no count, a missing or
// ambiguous ReplicaSet, or a non-zero pod count stops.
func TestTheFlipBackRunbookPodCheckFailsClosed(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(flipBackKubectl), 0o755); err != nil {
		t.Fatal(err)
	}
	selectors := []string{"pod-template-hash=abc123", "dev-health.io/auth-users-owned=true"}
	rsFound := []map[string]bool{{"one": true}, {"one": true, "two": true}}
	modes := []string{"fail", "zerofail", "empty", "zero", "one", "two"}
	for i, block := range flipBackPodChecks(t) {
		for _, rsMode := range modes {
			for _, podMode := range []string{"fail", "zerofail", "empty", "two", "zero"} {
				rec := filepath.Join(t.TempDir(), "argv")
				cmd := exec.Command("bash", "-c", block)
				cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
					"FAKE_REC="+rec, "FAKE_RS="+rsMode, "FAKE_PODS="+podMode)
				out, err := cmd.CombinedOutput()
				ok := rsFound[i][rsMode] && podMode == "zero"
				stopped := err != nil && strings.Contains(string(out), "STOP")
				if stopped == ok || (ok && (err != nil || !strings.Contains(string(out), "OK: 0 old owned pods"))) {
					t.Errorf("check %d, rs %s, pods %s: err %v, want ok = %v\n%s", i, rsMode, podMode, err, ok, out)
				}
				argv, _ := os.ReadFile(rec)
				if !strings.Contains(string(argv), "get rs -n ns1") || !strings.Contains(string(argv), selectors[i]) {
					t.Errorf("check %d, rs %s, pods %s: kubectl argv = %q, want get rs -n ns1 with %s", i, rsMode, podMode, argv, selectors[i])
				}
				if rsFound[i][rsMode] && !strings.Contains(string(argv), "get pods -n ns1") {
					t.Errorf("check %d, rs %s, pods %s: kubectl argv = %q, want get pods -n ns1", i, rsMode, podMode, argv)
				}
			}
		}
	}
}
