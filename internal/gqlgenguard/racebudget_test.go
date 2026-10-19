//go:build race

package gqlgenguard

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// raceBudget is half the 600 s go test timeout of the race leg (CHAOS-10201). The package took 342-600 s there before the
// real-module tests moved to realmodule_test.go (//go:build !race); a test that grows it back must fail here, loudly and
// with the slow tests named, not as a 10-minute panic.
const raceBudget = 300 * time.Second

func TestMain(m *testing.M) {
	start := time.Now()
	code := m.Run()
	if elapsed := time.Since(start); elapsed > raceBudget {
		fmt.Fprintf(os.Stderr, "FAIL: internal/gqlgenguard took %.0f s under -race, over the %.0f s budget (CHAOS-10201). "+
			"Move the slow test behind //go:build !race and list it in ci/race_excluded_tests.tsv. Slowest tests that run the real generator: %s\n",
			elapsed.Seconds(), raceBudget.Seconds(), slowestTests(5))
		code = 1
	}
	os.Exit(code)
}
