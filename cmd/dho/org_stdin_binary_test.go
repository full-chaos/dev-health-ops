package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-8892: the PRODUCTION binary wires the real stdin to --org-stdin of
// the two recompute verbs. The build must succeed or the test fails (no skip).
// The environment is empty: no database settings, so a request that passes
// validation stops at the configuration step with a different error.
func TestDhoOrgStdinFromTheRealProcessStdin(t *testing.T) {
	const org = "5d8e2c1a-7b3f-4a90-b6d4-0e9f1a2b3c4d"
	binary := filepath.Join(t.TempDir(), "dho")
	build := exec.Command("go", "build", "-o", binary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build dho: %v\n%s", err, out)
	}
	verbs := map[string][]string{
		"daily-start": {"workers", "metrics", "daily-start", "--org-stdin", "--day", "2026-08-01",
			"--reason", "operator_test", "--correlation-id", "corr-1"},
		"partition-recompute": {"workers", "metrics", "partition-recompute", "--org-stdin", "--from", "2026-08-01",
			"--to", "2026-08-01", "--family", "repo_user_commit", "--review-evidence", "testing",
			"--reason", "operator_test", "--correlation-id", "corr-1"},
	}
	cases := []struct {
		name, stdin string
		code        int
		want        string
	}{
		{"200 byte line", strings.Repeat("a", 200) + "\n", 2, "org_stdin_too_long"},
		{"two lines", org + "\n" + org + "\n", 2, "org_stdin_not_one_line"},
		{"empty", "", 2, "org_stdin_empty"},
		{"malformed", "plainwordorg\n", 2, "org_stdin_malformed"},
		{"valid id passes validation", org + "\n", 1, "configuration_error"},
	}
	for verb, args := range verbs {
		for _, tc := range cases {
			t.Run(verb+"/"+tc.name, func(t *testing.T) {
				cmd := exec.Command(binary, args...)
				cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
				cmd.Stdin = strings.NewReader(tc.stdin)
				var stdout, stderr bytes.Buffer
				cmd.Stdout, cmd.Stderr = &stdout, &stderr
				err := cmd.Run()
				code := 0
				if exitErr, ok := err.(*exec.ExitError); ok {
					code = exitErr.ExitCode()
				} else if err != nil {
					t.Fatal(err)
				}
				if code != tc.code || !strings.Contains(stderr.String(), tc.want) {
					t.Fatalf("code=%d stdout=%q stderr=%q, want code %d and %q", code, stdout.String(), stderr.String(), tc.code, tc.want)
				}
				if tc.want == "configuration_error" && strings.Contains(stderr.String(), "org_stdin_") {
					t.Fatalf("a valid id was refused by the stdin validation: %q", stderr.String())
				}
				for _, secret := range []string{org, "plainwordorg", "aaaaaaaa"} {
					if strings.Contains(stdout.String()+stderr.String(), secret) {
						t.Fatalf("output repeats the stdin value: %q", stdout.String()+stderr.String())
					}
				}
			})
		}
	}
}
