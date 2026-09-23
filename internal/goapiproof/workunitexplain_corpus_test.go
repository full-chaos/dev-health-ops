package goapiproof

import (
	"testing"
	"time"
)

const workUnitExplainOperation = "REST:POST:/api/v1/work-units/{work_unit_id}/explain"

// nonCallingLLMProviders are the only provider names a corpus request for
// an LLM route may carry. "mock" answers from a canned payload and "none"
// answers without constructing a provider at all; every other name --
// including the endpoint's own "auto" default, which resolves through the
// DEPLOYMENT's configured provider -- can reach a real API.
var nonCallingLLMProviders = map[string]bool{"mock": true, "none": true}

// TestWorkUnitExplainRequestsNameANonCallingProvider is a safety guard, not
// a parity check. A proof run drives BOTH planes against production, and
// this route completes an LLM prompt: a request that left llm_provider
// unset would resolve through the deployment's own configured provider,
// bill a real completion, and write a real llm_token_usage row on the
// baseline leg -- once per request, per run, forever. The two accepted
// names are the only ones that cannot do either, so this asserts the
// parameter is PRESENT and one of them, rather than asserting the absence
// of any particular unsafe value.
//
// Derived from the corpus on every run rather than from a count, so a
// request ADDED later is covered without this test being touched.
func TestWorkUnitExplainRequestsNameANonCallingProvider(t *testing.T) {
	spec, registered := restEndpointSpecs[workUnitExplainOperation]
	if !registered {
		t.Fatalf("%s has no corpus entry -- this guard's premise no longer holds", workUnitExplainOperation)
	}
	if len(spec.Requests) == 0 {
		t.Fatalf("%s carries zero requests -- a vacuous corpus proves nothing", workUnitExplainOperation)
	}

	for _, request := range spec.Requests {
		provider := request.Query.Get("llm_provider")
		if provider == "" {
			t.Errorf("request %q names no llm_provider: it would resolve through the deployment's own configured provider and bill a real completion on the baseline leg",
				request.Name)
			continue
		}
		if !nonCallingLLMProviders[provider] {
			t.Errorf("request %q names llm_provider=%q, which can reach a real provider API; only mock and none cannot",
				request.Name, provider)
		}
	}
}

// workUnitIDProducerNames derives the allowed-producer set for a
// work_unit_id PathParam binding straight from the corpus, rather than a
// literal copy that could drift: every RESTIDProducer GET /api/v1/
// work-units itself declares (its org-scope default_window producer, plus
// its repo_scoped/team_scoped producers workunits_corpus.go declares) is a
// legitimate source of a live work_unit_id -- a request ADDED there later
// is covered without this test being touched, the same "derived, not
// counted" convention TestWorkUnitExplainRequestsNameANonCallingProvider's
// own doc comment states.
func workUnitIDProducerNames(t *testing.T) map[string]bool {
	t.Helper()
	workUnitsSpec, err := SpecForREST("REST:GET:/api/v1/work-units")
	if err != nil {
		t.Fatalf("SpecForREST(REST:GET:/api/v1/work-units): %v", err)
	}
	allowed := map[string]bool{}
	for _, request := range workUnitsSpec.Requests {
		for _, producer := range request.Produces {
			allowed[producer.Name] = true
		}
	}
	if len(allowed) == 0 {
		t.Fatal("GET /api/v1/work-units declares no Produces entries -- this guard's premise no longer holds")
	}
	return allowed
}

// TestWorkUnitExplainBoundEntriesConsumeTheWorkUnitsProducer pins the other
// half of that arrangement: an entry expecting a 200 has to be looking at a
// work unit that exists, and no literal id is one in any org, so it MUST
// bind the path segment from a live producer -- ONE OF the allowed set GET
// /api/v1/work-units itself declares (workUnitIDProducerNames above), never
// an invented name. An entry that lost its binding would send the literal
// placeholder instead and answer 404 -- a refused entry rather than a
// failing one, which is exactly the shape that sits unnoticed in a report.
//
// POST /api/v1/work-units/{work_unit_id}/explain is a deleted-Python-body
// route (CHAOS-6241, restdeletedbody.go): WantBaselineStatus is 500 for
// every request now, including the ones that reach a real explanation, so
// "does this request want an explanation" is read from WantCandidateStatus
// instead -- the only leg that still computes anything.
func TestWorkUnitExplainBoundEntriesConsumeTheWorkUnitsProducer(t *testing.T) {
	spec := restEndpointSpecs[workUnitExplainOperation]
	allowedProducers := workUnitIDProducerNames(t)
	bound := 0
	for _, request := range spec.Requests {
		wantsAnExplanation := request.WantCandidateStatus == 200
		hasPathBinding := false
		for _, binding := range request.IDBindings {
			if binding.PathParam == "work_unit_id" {
				hasPathBinding = true
				if !allowedProducers[binding.Producer] {
					t.Errorf("request %q binds work_unit_id from producer %q, want one of the work-units producers %v", request.Name, binding.Producer, allowedProducers)
				}
			}
		}
		switch {
		case wantsAnExplanation && !hasPathBinding:
			t.Errorf("request %q expects candidate 200 but binds no work_unit_id: the literal path placeholder can never name a real work unit", request.Name)
		case !wantsAnExplanation && hasPathBinding:
			t.Errorf("request %q binds a live work_unit_id but expects candidate %d: a bound entry reaches a real unit, so it cannot also be the negative path",
				request.Name, request.WantCandidateStatus)
		}
		if hasPathBinding {
			bound++
		}
	}
	if bound == 0 {
		t.Fatal("no request binds a live work_unit_id -- nothing here reaches an explanation at all")
	}
}

// TestWorkUnitExplainScopedLiveEntries_BindScopeIDAndTheScopedWorkUnit
// pins the corpus-level fix for this route's own vacuous scope coverage:
// repo_scoped_live_work_unit/team_scoped_live_work_unit each bind
// scope_id AND work_unit_id from the SAME GET /api/v1/work-units scoped
// entry (work_units_repo_id/work_unit_id_repo_scoped, or team_id/
// work_unit_id_team_scoped) -- unlike the pre-existing repo_scoped_absent_work_unit/
// team_scoped_absent_work_unit entries, which bind scope_id alone and so
// can never bind a unit actually inside that scope. A regression that
// drops either binding, or points work_unit_id back at the org-wide
// "work_unit_id" producer, fails here.
func TestWorkUnitExplainScopedLiveEntries_BindScopeIDAndTheScopedWorkUnit(t *testing.T) {
	spec := restEndpointSpecs[workUnitExplainOperation]
	for _, tc := range []struct {
		request       string
		scopeProducer string
		unitProducer  string
		wantTimeout   time.Duration
	}{
		{"repo_scoped_live_work_unit", "work_units_repo_id", "work_unit_id_repo_scoped", 0},
		{"team_scoped_live_work_unit", "team_id", "work_unit_id_team_scoped", 180 * time.Second},
	} {
		t.Run(tc.request, func(t *testing.T) {
			var req *RESTRequest
			for i := range spec.Requests {
				if spec.Requests[i].Name == tc.request {
					req = &spec.Requests[i]
				}
			}
			if req == nil {
				t.Fatalf("%s has no %q entry", workUnitExplainOperation, tc.request)
			}
			// POST /api/v1/work-units/{work_unit_id}/explain is a
			// deleted-Python-body route (CHAOS-6241): the baseline now
			// always answers the fixed sentinel, not a real 200.
			if req.WantCandidateStatus != 200 || req.WantBaselineStatus != 500 {
				t.Fatalf("%s WantCandidateStatus/WantBaselineStatus = %d/%d, want 200/500", tc.request, req.WantCandidateStatus, req.WantBaselineStatus)
			}
			if req.Timeout != tc.wantTimeout {
				t.Fatalf("%s Timeout = %v, want %v", tc.request, req.Timeout, tc.wantTimeout)
			}
			if len(req.IDBindings) != 2 {
				t.Fatalf("%s IDBindings = %+v, want exactly two bindings", tc.request, req.IDBindings)
			}
			var sawScope, sawUnit bool
			for _, binding := range req.IDBindings {
				switch {
				case binding.QueryParam == "scope_id" && binding.Producer == tc.scopeProducer:
					sawScope = true
				case binding.PathParam == "work_unit_id" && binding.Producer == tc.unitProducer:
					sawUnit = true
				}
			}
			if !sawScope {
				t.Errorf("%s IDBindings = %+v, want a QueryParam=scope_id binding on producer %q", tc.request, req.IDBindings, tc.scopeProducer)
			}
			if !sawUnit {
				t.Errorf("%s IDBindings = %+v, want a PathParam=work_unit_id binding on producer %q", tc.request, req.IDBindings, tc.unitProducer)
			}

			produced := map[string]string{tc.scopeProducer: "live-scope-42", tc.unitProducer: "live-unit-7"}
			resolvedPath, resolvedQuery, _, unresolved := ResolveRESTIDBindings("/api/v1/work-units/{work_unit_id}/explain", *req, produced)
			if len(unresolved) != 0 {
				t.Fatalf("%s unresolved = %v, want none", tc.request, unresolved)
			}
			if resolvedPath != "/api/v1/work-units/live-unit-7/explain" {
				t.Fatalf("%s resolvedPath = %q, want /api/v1/work-units/live-unit-7/explain", tc.request, resolvedPath)
			}
			if got := resolvedQuery.Get("scope_id"); got != "live-scope-42" {
				t.Fatalf("%s resolved scope_id = %q, want live-scope-42", tc.request, got)
			}

			// Missing the SCOPED unit producer (the scoped GET request
			// never admitted, e.g. an intermittent baseline timeout) must
			// refuse by name, never silently fall through to a stale or
			// invented id.
			partial := map[string]string{tc.scopeProducer: "live-scope-42"}
			_, _, _, unresolvedPartial := ResolveRESTIDBindings("/api/v1/work-units/{work_unit_id}/explain", *req, partial)
			if len(unresolvedPartial) != 1 || unresolvedPartial[0] != tc.unitProducer {
				t.Fatalf("%s unresolved (unit producer missing) = %v, want [%s]", tc.request, unresolvedPartial, tc.unitProducer)
			}
		})
	}
}

// TestWorkUnitExplainRepoScopedLiveUnit_ScopeIsTheRepositoryThatProducedTheUnit
// pins that repo_scoped_live_work_unit's scope_id is the repository GET
// /api/v1/work-units' own repo_scoped entry SELECTED (its bounded
// candidate binding's ExposeAs), the same entry that produces
// work_unit_id_repo_scoped. Binding scope_id to the raw repo_id producer
// instead would pair a unit from the winning repository with the first
// listed repository, which need not contain it.
func TestWorkUnitExplainRepoScopedLiveUnit_ScopeIsTheRepositoryThatProducedTheUnit(t *testing.T) {
	var exposeAs string
	for _, req := range restEndpointSpecs["REST:GET:/api/v1/work-units"].Requests {
		if req.Name != "repo_scoped" {
			continue
		}
		for _, binding := range req.IDBindings {
			if binding.Candidates > 0 {
				exposeAs = binding.ExposeAs
			}
		}
		producesUnit := false
		for _, producer := range req.Produces {
			if producer.Name == "work_unit_id_repo_scoped" {
				producesUnit = true
			}
		}
		if !producesUnit {
			t.Fatalf("GET /api/v1/work-units repo_scoped Produces = %+v, want work_unit_id_repo_scoped", req.Produces)
		}
	}
	if exposeAs == "" {
		t.Fatal("GET /api/v1/work-units repo_scoped declares no bounded-candidate binding with an ExposeAs")
	}
	var scopeProducer string
	for _, binding := range workUnitExplainRepoScopedLiveUnit {
		if binding.QueryParam == "scope_id" {
			scopeProducer = binding.Producer
		}
	}
	if scopeProducer != exposeAs {
		t.Fatalf("repo_scoped_live_work_unit scope_id producer = %q, want %q (the repository that produced the unit)", scopeProducer, exposeAs)
	}
}
