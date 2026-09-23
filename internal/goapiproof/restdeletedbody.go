package goapiproof

import "fmt"

// This file is the ONE place a Python route-body deletion registers
// itself against the REST corpus. api/main.py's GoServedRouteUnavailableError
// (raised by _raise_served_by_go_api) replaces a deleted handler's whole
// body with a single, fixed diagnostic status that fires UNCONDITIONALLY
// for any request that reaches the handler, regardless of what that
// handler used to compute. A corpus entry written before its route's
// deletion still declares whatever the OLD handler answered -- a real
// 200 with real data, or an earlier production-observed failure like a
// 503 -- which is simply false once the body is gone. Rather than
// hand-editing every affected RESTRequest literal across restcorpus.go
// (many operations, several Requests each), this file's init() rewrites
// them all from the one list below -- a future deletion wave adds its
// operations to DeletedPythonBodyOperations and nothing else.
//
// What is left untouched: a Request whose declared WantCandidateStatus
// and WantBaselineStatus already agree at one of
// frameworkValidatableEqualStatuses (422/401/403/429) AND is
// isFrameworkValidatedEqualStatus (see below) -- FastAPI/Pydantic
// request-shape validation or an auth/rate-limit gate, enforced BEFORE
// the handler body ever runs, so it holds identically whether or not the
// body has been deleted. The framework/handler distinction is DERIVED
// from what the entry already declares, never a bare status-code list
// and never a hand-set per-entry flag: a status like 422 is used BOTH for
// framework-level validation AND for a handler's own data-dependent
// refusal (flame's deployment_gap_entity_id_bound_422 answers 422 from
// validateFlameFrames' gap check, not from Pydantic), so the status code
// alone cannot tell them apart. See isFrameworkValidatedEqualStatus's own
// doc comment for the derivation. Everything else -- WantBaselineStatus
// == 200 (the real happy path), an equal pair the HANDLER used to
// compute itself (a 404 not-found, a 503 availability gate, or a
// data-dependent 422/etc -- exactly as dead as a 200 once the body is
// gone), or an already-divergent pair (an earlier production-observed
// handler failure) -- described what the now-deleted body used to
// compute, so it is overridden to the fixed sentinel.

// DeletedPythonBodyOperations names every REST corpus operation (the
// same "REST:<METHOD>:<path>" key restEndpointSpecs is keyed by) whose
// Python handler body has been deleted. Exported so both
// internal/goapicli/restprove (a local proof run) and this package's own
// tests can enumerate exactly the set this file's init() has rewritten.
var DeletedPythonBodyOperations = map[string]bool{
	"REST:GET:/api/v1/meta":                                true,
	"REST:POST:/api/v1/home":                               true,
	"REST:GET:/api/v1/home":                                true,
	"REST:POST:/api/v1/explain":                            true,
	"REST:GET:/api/v1/explain":                             true,
	"REST:GET:/api/v1/heatmap":                             true,
	"REST:POST:/api/v1/work-units":                         true,
	"REST:GET:/api/v1/work-units":                          true,
	"REST:POST:/api/v1/work-units/{work_unit_id}/explain":  true,
	"REST:GET:/api/v1/flame":                               true,
	"REST:GET:/api/v1/flame/aggregated":                    true,
	"REST:GET:/api/v1/quadrant":                            true,
	"REST:POST:/api/v1/drilldown/prs":                      true,
	"REST:GET:/api/v1/drilldown/prs":                       true,
	"REST:POST:/api/v1/drilldown/issues":                   true,
	"REST:GET:/api/v1/drilldown/issues":                    true,
	"REST:GET:/api/v1/people":                              true,
	"REST:GET:/api/v1/people/{person_id}/summary":          true,
	"REST:GET:/api/v1/people/{person_id}/metric":           true,
	"REST:GET:/api/v1/people/{person_id}/drilldown/prs":    true,
	"REST:GET:/api/v1/people/{person_id}/drilldown/issues": true,
	"REST:GET:/api/v1/opportunities":                       true,
	"REST:POST:/api/v1/opportunities":                      true,
	"REST:GET:/api/v1/investment":                          true,
	"REST:POST:/api/v1/investment":                         true,
	"REST:GET:/api/v1/investment/sunburst":                 true,
	"REST:POST:/api/v1/investment/explain":                 true,
	"REST:POST:/api/v1/investment/flow":                    true,
	"REST:POST:/api/v1/investment/flow/repo-team":          true,
	"REST:GET:/api/v1/sankey":                              true,
	"REST:POST:/api/v1/sankey":                             true,
	"REST:GET:/api/v1/filters/options":                     true,
}

// deletedBodyArrayShapedOperations is the subset of
// DeletedPythonBodyOperations whose Python response_model is a
// list[...] (a JSON-array root) rather than a BaseModel (a JSON-object
// root) -- read directly from the SAME route decorators
// DeletedPythonBodyOperations was built from (src/dev_health_ops/api/
// main.py). Everything in DeletedPythonBodyOperations but not here is
// object-shaped, the overwhelming majority.
var deletedBodyArrayShapedOperations = map[string]bool{
	"REST:POST:/api/v1/work-units":         true,
	"REST:GET:/api/v1/work-units":          true,
	"REST:GET:/api/v1/people":              true,
	"REST:GET:/api/v1/investment/sunburst": true,
}

// pythonBodyDeletedSentinelStatus is the fixed HTTP status every deleted
// Python route body answers, unconditionally, for any request that
// reaches it (GoServedRouteUnavailableError, api/main.py).
const pythonBodyDeletedSentinelStatus = 500

// PythonBodyDeletedReason is the StatusDivergenceReason every request
// this file overrides carries -- behaviour only (what the planes DO),
// never a ticket id: this file's own DeletedPythonBodyOperations list is
// where a route is cited into the deletion, not a string embedded in a
// corpus reason.
const PythonBodyDeletedReason = "the Python handler's body has been deleted and replaced with a fixed diagnostic error; it answers this status unconditionally for any request that reaches it, regardless of what it used to compute, so its real answer is never compared"

// frameworkValidatableEqualStatuses are the ONLY statuses an equal
// (WantCandidateStatus == WantBaselineStatus) pair may declare and still
// be a CANDIDATE for isFrameworkValidatedEqualStatus below: every one of
// these CAN be enforced by FastAPI itself -- Pydantic/type-coercion
// (422), an auth Depends() (401/403), or the rate limiter (429) -- but,
// for 422 specifically, is not always: see isFrameworkValidatedEqualStatus.
var frameworkValidatableEqualStatuses = map[int]bool{422: true, 401: true, 403: true, 429: true}

// isFrameworkValidatedEqualStatus derives, from fields the entry ALREADY
// declares, whether an equal-status pair is enforced by FastAPI/Pydantic
// itself (strictly BEFORE the handler body runs) rather than computed BY
// the handler body -- so this file never hand-sets a per-entry flag, and
// a future entry is classified correctly without anyone touching this
// file.
//
// The derivation: req.IDBindings is empty. IDBindings exists so a LATER
// request can consume a SPECIFIC entity id an earlier request Produced,
// or an operator supplied -- it exists to let the corpus reach a
// particular resolved entity's data. FastAPI's own request-shape
// validation (a malformed/missing query or body field, an out-of-range
// Literal) runs on the raw incoming request, before any handler code --
// let alone any entity resolution -- ever executes; a corpus entry
// proving it needs no resolved entity at all, and every one of today's
// committed framework-validated 422/401/403/429 entries indeed declares
// no IDBindings. A handler-computed refusal is the opposite by
// construction: it can only fire once the handler has resolved and
// inspected a SPECIFIC entity's data (flame's
// deployment_gap_entity_id_bound_422 needs a real, bound deployment id to
// reach validateFlameFrames' gap check at all), so it declares an
// IDBindings entry pointing at that resolution -- which is exactly what
// distinguishes it here, and exactly why it correctly derives to false.
func isFrameworkValidatedEqualStatus(req RESTRequest) bool {
	return frameworkValidatableEqualStatuses[req.WantBaselineStatus] && len(req.IDBindings) == 0
}

// init rewrites every Request under an operation named in
// DeletedPythonBodyOperations, IN PLACE (RESTEndpointSpec.Requests is a
// slice; restEndpointSpecs[op] returns a copy of the struct but that
// copy's Requests header still points at the SAME backing array the map
// value holds, so writing through &spec.Requests[i] mutates what every
// later reader of restEndpointSpecs sees). Go guarantees every
// package-level var initializer -- including restEndpointSpecs' own
// giant literal -- completes before any init() in the package runs, so
// the table is fully built by the time this runs.
// equalStatusIDBindingJudgments records every "<operation>/<name>" whose
// request init() found EQUAL at a frameworkValidatableEqualStatuses code
// AND declaring IDBindings -- i.e. every entry the risky, one-way half of
// isFrameworkValidatedEqualStatus's derivation was actually consulted
// against, regardless of which way it ruled. Populated
// once, during init()'s own single pass over the PRE-override corpus --
// the only point at which "was this entry equal" survives for an entry
// init() goes on to override (an overridden entry's WantBaselineStatus no
// longer equals WantCandidateStatus afterward, so this cannot be
// reconstructed from the committed corpus post-init). Read only by this
// package's own tests (restdeletedbody_test.go's pin, below).
var equalStatusIDBindingJudgments []string

func init() {
	for operation := range DeletedPythonBodyOperations {
		spec, ok := restEndpointSpecs[operation]
		if !ok {
			panic(fmt.Sprintf("goapiproof: DeletedPythonBodyOperations names %q, which restEndpointSpecs does not declare", operation))
		}
		arrayShaped := deletedBodyArrayShapedOperations[operation]
		for i := range spec.Requests {
			req := &spec.Requests[i]
			if req.WantCandidateStatus == req.WantBaselineStatus && frameworkValidatableEqualStatuses[req.WantBaselineStatus] && len(req.IDBindings) != 0 {
				equalStatusIDBindingJudgments = append(equalStatusIDBindingJudgments, operation+"/"+req.Name)
			}
			if req.WantCandidateStatus == req.WantBaselineStatus && isFrameworkValidatedEqualStatus(*req) {
				// Framework-level parity: left exactly as declared,
				// comparison and all.
				continue
			}
			if req.WantCandidateStatus == pythonBodyDeletedSentinelStatus {
				// The candidate itself already declares the sentinel
				// status too (not expected in today's corpus): no
				// divergence to state, and nothing to shape-assert.
				req.WantBaselineStatus = pythonBodyDeletedSentinelStatus
				req.StatusDivergenceReason = ""
				req.BodyMode = RESTBodyModeStatusOnly
				continue
			}
			req.WantBaselineStatus = pythonBodyDeletedSentinelStatus
			req.StatusDivergenceReason = PythonBodyDeletedReason
			// BaselineTimeoutDeclared is left AS DECLARED, not cleared:
			// its NonEmptyPaths is read independently of the run/timeout
			// mechanism it also backs (cmd/query-api's own
			// team_scope_routes_integration_test.go cross-checks it
			// against the real candidate handler's response, regardless
			// of this route's BodyMode) -- clearing it here silently
			// dropped that declaration and broke that test. The timeout
			// MECHANISM itself (RESTAdmitCandidateAlone, proveUnderBaselineTimeout)
			// stays live too: a deleted-body baseline answers the fixed
			// sentinel immediately in the overwhelming case, but a real
			// transport timeout is not impossible, and this declaration
			// still lets that rare case admit on the candidate alone
			// rather than going unproven. ValidateRESTCorpus's own
			// BaselineTimeoutDeclaration.Validate accepts
			// RESTBodyModeCandidateShape for exactly this reason
			// (restbaselinetimeout.go).
			if req.WantCandidateStatus == 200 {
				req.BodyMode = RESTBodyModeCandidateShape
				req.CandidateShapeArray = arrayShaped
			} else {
				// The candidate itself does not reach 200 either (a
				// Go-side validation refusal, for example): no real
				// body worth a shape assertion on that leg.
				req.BodyMode = RESTBodyModeStatusOnly
			}
		}
	}
}

// RequestBreaksDeletedBodyInvariant reports whether req -- a Request
// under an operation named in DeletedPythonBodyOperations -- carries a
// shape init() above should have overridden and did not: WantBaselineStatus
// == 200 (a live happy path against a baseline that no longer computes
// one, ever), or BodyMode == RESTBodyModeJSON on a request that does not
// derive isFrameworkValidatedEqualStatus (a full baseline-equality
// comparison against the fixed sentinel, which would refuse every live
// run). A pure predicate over one Request, not a corpus walk, so a test
// can assert it fires on a constructed bad Request -- proof the guard can
// fail, not just that today's committed corpus happens to pass it (see
// restdeletedbody_test.go).
func RequestBreaksDeletedBodyInvariant(req RESTRequest) bool {
	if req.WantBaselineStatus == 200 {
		return true
	}
	if req.BodyMode == RESTBodyModeJSON {
		return req.WantCandidateStatus != req.WantBaselineStatus || !isFrameworkValidatedEqualStatus(req)
	}
	return false
}
