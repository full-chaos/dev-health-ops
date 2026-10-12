package goapiproof

import "testing"

// This file pins the corpus-level fix for the investment family's own
// missing repo-scope coverage: GET/POST /api/v1/investment, GET
// /api/v1/investment/sunburst each declare a "repo_scoped" entry
// (restcorpus.go): the sunburst entry is resolved by bounded candidate
// iteration over filters/options' own repo_id list and exposed as
// investment_repo_id, the other two consuming that single winner: a plain
// first-repo id can easily hold zero investment rows for the requested
// window, which these routes' own declared BaselineDefects would
// otherwise refuse as vacuous.

// investmentRepoScopedOperations lists, in restRunOrder's own order, every
// operation this fix touches -- the first entry is the one that resolves
// investment_repo_id, the rest consume it.
var investmentRepoScopedOperations = []string{
	"REST:GET:/api/v1/investment/sunburst",
	"REST:GET:/api/v1/investment",
	"REST:POST:/api/v1/investment",
}

// findRESTRequest returns the named request from operation's own corpus,
// failing the test if either the operation or the request is missing.
func findRESTRequest(t *testing.T, operation, name string) RESTRequest {
	t.Helper()
	spec, err := SpecForREST(operation)
	if err != nil {
		t.Fatalf("SpecForREST(%s): %v", operation, err)
	}
	for _, req := range spec.Requests {
		if req.Name == name {
			return req
		}
	}
	t.Fatalf("%s has no %q entry", operation, name)
	return RESTRequest{}
}

// TestInvestmentFamilyRepoScopedSiblings_BindToTheExposedWinner pins that
// every OTHER repo_scoped entry on the investment family binds to
// investment_repo_id -- the winning candidate the sunburst entry's own
// iteration selected -- never back to the raw repo_id producer a sibling
// bound to would independently, and possibly differently (or vacuously),
// resolve. A regression that reverts any sibling to the raw producer
// fails here.
func TestInvestmentFamilyRepoScopedSiblings_BindToTheExposedWinner(t *testing.T) {
	for _, tc := range []struct {
		operation  string
		queryParam string
		bodyPath   string
	}{
		{"REST:GET:/api/v1/investment", "scope_id", ""},
		{"REST:POST:/api/v1/investment", "", "filters.scope.ids"},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			req := findRESTRequest(t, tc.operation, "repo_scoped")
			// Every operation in this table is a deleted-Python-body
			// route too (CHAOS-6241, restdeletedbody.go).
			if req.WantCandidateStatus != 200 || req.WantBaselineStatus != 500 {
				t.Fatalf("repo_scoped WantCandidateStatus/WantBaselineStatus = %d/%d, want 200/500", req.WantCandidateStatus, req.WantBaselineStatus)
			}
			if len(req.IDBindings) != 1 || req.IDBindings[0].Producer != "investment_repo_id" {
				t.Fatalf("repo_scoped IDBindings = %+v, want exactly one binding on producer investment_repo_id", req.IDBindings)
			}
			binding := req.IDBindings[0]
			if binding.QueryParam != tc.queryParam || binding.BodyPath != tc.bodyPath {
				t.Fatalf("repo_scoped binding = %+v, want QueryParam=%q BodyPath=%q", binding, tc.queryParam, tc.bodyPath)
			}
			if binding.Candidates != 0 || binding.ExposeAs != "" {
				t.Fatalf("repo_scoped binding = %+v, want a plain (non-iterating) binding -- only the winning entry iterates", binding)
			}
		})
	}
}

// TestInvestmentRepoScopedOperations_RunOrderResolvesTheProducerFirst pins
// that restRunOrder actually runs GET /api/v1/investment/sunburst's own
// producer request before every sibling that consumes investment_repo_id
// -- a regression that reordered restRunOrder would leave every sibling
// refused by RESTRefusalIDBindingUnresolved at run time even though the
// corpus itself validates (ValidateRESTIDBindingOrder only requires SOME
// earlier producer, not this specific one).
func TestInvestmentRepoScopedOperations_RunOrderResolvesTheProducerFirst(t *testing.T) {
	order := RESTRunOrder()
	position := make(map[string]int, len(order))
	for i, op := range order {
		position[op] = i
	}
	producerPos, ok := position[investmentRepoScopedOperations[0]]
	if !ok {
		t.Fatalf("%s missing from restRunOrder", investmentRepoScopedOperations[0])
	}
	for _, op := range investmentRepoScopedOperations[1:] {
		pos, ok := position[op]
		if !ok {
			t.Fatalf("%s missing from restRunOrder", op)
		}
		if pos <= producerPos {
			t.Fatalf("%s runs at position %d, want strictly after %s (position %d)", op, pos, investmentRepoScopedOperations[0], producerPos)
		}
	}
}

// TestInvestmentSunburstRepoScoped_IteratesAndExposesTheWinner pins that the
// sunburst repo_scoped entry is the one that searches candidates and
// exposes the winning repository to its siblings.
func TestInvestmentSunburstRepoScoped_IteratesAndExposesTheWinner(t *testing.T) {
	req := findRESTRequest(t, "REST:GET:/api/v1/investment/sunburst", "repo_scoped")
	if len(req.IDBindings) != 1 {
		t.Fatalf("repo_scoped IDBindings = %+v, want exactly one binding", req.IDBindings)
	}
	binding := req.IDBindings[0]
	if binding.Producer != "repo_id" || binding.QueryParam != "scope_id" || binding.Candidates != 10 || binding.ExposeAs != "investment_repo_id" {
		t.Fatalf("repo_scoped binding = %+v, want a 10-candidate search on repo_id exposed as investment_repo_id", binding)
	}
	if len(req.Produces) != 1 || req.Produces[0].Name != "investment_repo_sunburst_slice" {
		t.Fatalf("repo_scoped Produces = %+v, want the declared slice id that makes an empty repository lose", req.Produces)
	}
}
