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

const flipBackKubectl = `#!/usr/bin/env bash
printf '%s\n' "$*" >>"${FAKE_REC}"
case "${FAKE_MODE}" in
fail) echo 'Error from server (Forbidden): pods is forbidden' >&2; exit 1 ;;
empty) exit 0 ;;
some) printf '2' ;;
none) printf '0' ;;
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

// Each pod-count check fails closed: it passes only when kubectl succeeds and
// reads a count of 0; a kubectl failure, no count or a non-zero count stops.
func TestTheFlipBackRunbookPodCheckFailsClosed(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(flipBackKubectl), 0o755); err != nil {
		t.Fatal(err)
	}
	selectors := []string{"pod-template-hash=abc123", "dev-health.io/auth-users-owned=true"}
	for i, block := range flipBackPodChecks(t) {
		for _, mode := range []string{"fail", "empty", "some", "none"} {
			rec := filepath.Join(t.TempDir(), "argv")
			cmd := exec.Command("bash", "-c", block)
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_REC="+rec, "FAKE_MODE="+mode)
			out, err := cmd.CombinedOutput()
			stopped := err != nil && strings.Contains(string(out), "STOP")
			if want := mode != "none"; stopped != want {
				t.Errorf("check %d, kubectl %s: stopped = %v, want %v (err %v)\n%s", i, mode, stopped, want, err, out)
			}
			if mode == "none" && err != nil {
				t.Errorf("check %d, kubectl answers 0 pods: err %v\n%s", i, err, out)
			}
			argv, _ := os.ReadFile(rec)
			if !strings.Contains(string(argv), "get pods -n ns1") || !strings.Contains(string(argv), selectors[i]) {
				t.Errorf("check %d, kubectl %s: kubectl argv = %q, want get pods -n ns1 with %s", i, mode, argv, selectors[i])
			}
		}
	}
}
