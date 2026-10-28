package bigboy_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// prebuiltCigate is the cigate binary the shell callers run through BIGBOY_CIGATE. It is built once per test
// binary with the real environment, so the recorded tool stubs (git, HOME) never see go build's VCS stamping or module downloads.
var prebuiltCigate string

func TestMain(m *testing.M) {
	if os.Getenv(multicallEnv) == "1" {
		os.Exit(multicallMain(os.Args))
	}
	os.Exit(runMain(m))
}

// multicallEnv makes this test binary a stand-in for a multi-call coreutils (uutils, ubuntu-26.04): it picks its applet
// from the base name of argv[0] and knows only sleep. multicallBinEnv carries its path to a script that copies it.
const (
	multicallEnv    = "BIGBOY_MULTICALL_STANDIN"
	multicallBinEnv = "BIGBOY_MULTICALL_BIN"
)

func multicallMain(args []string) int {
	applet := filepath.Base(args[0])
	if applet != "sleep" || len(args) != 2 {
		fmt.Fprintf(os.Stderr, "%s: function/utility not found\n", applet)
		return 1
	}
	seconds, err := strconv.Atoi(args[1])
	if err != nil || seconds < 0 {
		fmt.Fprintf(os.Stderr, "sleep: invalid time interval\n")
		return 1
	}
	time.Sleep(time.Duration(seconds) * time.Second)
	return 0
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
