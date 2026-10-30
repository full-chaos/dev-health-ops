package checkgo_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A failed script run reports its stderr: a long stdout (the script echoes its whole command line) must not push the
// reason out of the message.
func TestFailureTailKeepsStderrWhenStdoutIsLong(t *testing.T) {
	t.Parallel()
	res := cmdResult{code: 1, stderr: "reap: docker ps failed for session abcdefgh12\n", stdout: strings.Repeat("-run Test_x ", 400)}
	got := failureTail(res)
	if !strings.Contains(got, "reap: docker ps failed for session abcdefgh12") {
		t.Fatalf("the failure text lost the stderr line: %.200s", got)
	}
}

// The stand-in go test runs do not depend on the docker of the host they run on: with a host docker that fails every
// call first on PATH, a run whose stand-in go test succeeds still succeeds.
func TestRecordedGoTestIgnoresAFailingHostDocker(t *testing.T) {
	script := scriptPath(t, "ci/check_go.sh")
	host := t.TempDir()
	if err := os.WriteFile(filepath.Join(host, "docker"), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", host+string(os.PathListSeparator)+os.Getenv("PATH"))
	lines, err := recordedGoTest(t.TempDir(), script, []string{"CI=true"}, "integration")
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) == 0 {
		t.Fatal("the stand-in go recorded no `go test` call")
	}
}
