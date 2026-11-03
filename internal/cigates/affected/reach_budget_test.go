package affected_test

import (
	"strings"
	"testing"
)

// CHAOS-12407: a reach computation whose own peak RSS is over the memory budget fails, naming the budget and the peak,
// in the --reach check and in --write. The fixture is tiny, so a 1 MiB budget is over and the default is not.
func TestReachOverTheMemoryBudgetFails(t *testing.T) {
	f, _ := reachFixture(t, reachCacheHead())
	if res := writeTables(t, f.root); res.code != 0 {
		t.Fatalf("plan-reads --write exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	if res := reachRun(t, f.root, reachEnv(), "--reach"); res.code != 0 || !strings.Contains(res.stdout, "peak RSS") {
		t.Fatalf("under the default budget --reach must pass: exit %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	for _, mode := range []string{"--reach", "--write"} {
		res := reachRun(t, f.root, reachEnv(), mode, "--reach-budget-mib", "1")
		if res.code != 1 || !strings.Contains(res.stderr, "over the reach memory budget of 1 MiB") || !strings.Contains(res.stderr, "plan-reads: reach: peak RSS ") {
			t.Fatalf("%s with a 1 MiB budget must fail naming the budget and the peak: exit %d:\n%s%s", mode, res.code, res.stdout, res.stderr)
		}
	}
}
