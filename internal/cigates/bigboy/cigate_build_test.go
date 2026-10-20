package bigboy_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// prebuiltCigate is the cigate binary the shell callers run through BIGBOY_CIGATE. It is built once per test
// binary with the real environment, so the recorded tool stubs (git, HOME) never see go build's VCS stamping or module downloads.
var prebuiltCigate string

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	dir, err := os.MkdirTemp("", "bigboy-cigate-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "bigboy tests: no scratch directory:", err)
		return 1
	}
	defer os.RemoveAll(dir)
	root, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "bigboy tests: no working directory:", err)
		return 1
	}
	root = filepath.Join(root, "..", "..", "..")
	prebuiltCigate = filepath.Join(dir, "cigate")
	cmd := exec.Command("go", "build", "-C", root, "-buildvcs=false", "-trimpath", "-mod=readonly", "-o", prebuiltCigate, "./cmd/cigate")
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "bigboy tests: cigate build failed: %v\n%s", err, out)
		return 1
	}
	return m.Run()
}
