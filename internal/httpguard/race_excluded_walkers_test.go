//go:build !race

package httpguard

import (
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-8135: these three tests load the whole module with go/packages (static analysis, 26-35 s each under -race when idle;
// the package took 324-495 s under -race on a shared runner and shared a leg with goapiproof). They run no concurrent code of
// their own, so the race detector adds cost and no signal. They are compiled out of the -race leg ONLY. The non-race unit leg
// runs them, and ci/check_go.sh (check_race_excluded_ran) fails unless each runs by name, uncached and without -race, and
// prints its own PASS line (ci/race_excluded_tests.tsv).

func TestEveryHTTPClientSiteIsClassified(t *testing.T) {
	problems, found, rows := realWalkProblems(t, pinnedOutOfScope, true)
	for _, problem := range problems {
		t.Error(problem)
	}
	t.Logf("%d sites, %d rows, %d problems", len(found), len(rows), len(problems))
}

// The pin is applied to the REAL walk: the same function with a pinned list that differs from the walked scope yields the
// SCOPE LIMIT problem (so the comparison cannot be dropped from the real test unseen).
func TestTheScopePinIsAppliedToTheRealWalk(t *testing.T) {
	problems, _, _ := realWalkProblems(t, append(append([]string(nil), pinnedOutOfScope...), "not/a/real/file.go"), false)
	if len(problems) != 1 || !strings.Contains(problems[0], "SCOPE LIMIT changed") {
		t.Fatalf("want exactly the SCOPE LIMIT problem, got %v", problems)
	}
}

// Every CONCRETE type that enters providerfoundation.HTTPDoer anywhere in production code (an argument, an assignment, a
// field of a literal, a return value, a var initialiser) is derived by type, and must be one of:
//
//   - *net/http.Client: the provider constructors guard it once (providerfoundation.NewHTTPClient);
//
//   - a type that implements httpguard.Wrapper: the guard is rebuilt around the doer it wraps;
//
//   - a route decorator (a struct of the same package with a Do method and an HTTPDoer field) assigned to the Doer field of a
//     constructed provider client: it wraps client.Doer, below which the guard already sits.
//
//   - providerfoundation.refusedDoer, the doer that sends nothing (what a PagerDuty entry point uses in place of a refused one).
//
// So a decorator that is not a Wrapper can never be SUPPLIED into a provider client constructor: it cannot become an HTTPDoer
// at all, except by being assigned onto a constructed client. A new production type that implements Do and is used as an
// HTTPDoer fails here until it is made a Wrapper or built from client.Doer. An empty derived set FAILS.
func TestEveryConcreteTypeThatBecomesAnHTTPDoerIsGuardedOrWrapsTheGuardedDoer(t *testing.T) {
	root, err := filepath.Abs(moduleRootRel)
	if err != nil {
		t.Fatal(err)
	}
	problems, entering := doerProblems(t, root, []string{"./..."})
	for _, problem := range problems {
		t.Error(problem)
	}
	if entering < 5 {
		t.Errorf("only %d concrete types derived as entering HTTPDoer: the derivation is vacuous or broken", entering)
	}
	t.Logf("%d concrete types enter HTTPDoer", entering)
}
