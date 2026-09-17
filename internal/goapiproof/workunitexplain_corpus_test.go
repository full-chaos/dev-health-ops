package goapiproof

import "testing"

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

// TestWorkUnitExplainBoundEntriesConsumeTheWorkUnitsProducer pins the other
// half of that arrangement: an entry expecting a 200 has to be looking at a
// work unit that exists, and no literal id is one in any org, so it MUST
// bind the path segment from a live producer. An entry that lost its
// binding would send the literal placeholder instead and answer 404 -- a
// refused entry rather than a failing one, which is exactly the shape that
// sits unnoticed in a report.
func TestWorkUnitExplainBoundEntriesConsumeTheWorkUnitsProducer(t *testing.T) {
	spec := restEndpointSpecs[workUnitExplainOperation]
	bound := 0
	for _, request := range spec.Requests {
		wantsAnExplanation := request.WantBaselineStatus == 200
		hasPathBinding := false
		for _, binding := range request.IDBindings {
			if binding.PathParam == "work_unit_id" {
				hasPathBinding = true
				if binding.Producer != "work_unit_id" {
					t.Errorf("request %q binds work_unit_id from producer %q, want the work-units producer", request.Name, binding.Producer)
				}
			}
		}
		switch {
		case wantsAnExplanation && !hasPathBinding:
			t.Errorf("request %q expects 200 but binds no work_unit_id: the literal path placeholder can never name a real work unit", request.Name)
		case !wantsAnExplanation && hasPathBinding:
			t.Errorf("request %q binds a live work_unit_id but expects %d: a bound entry reaches a real unit, so it cannot also be the negative path",
				request.Name, request.WantBaselineStatus)
		}
		if hasPathBinding {
			bound++
		}
	}
	if bound == 0 {
		t.Fatal("no request binds a live work_unit_id -- nothing here reaches an explanation at all")
	}
}
