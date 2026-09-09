package goapiproof

import (
	"fmt"
	"net/http"
)

// This file is the whole answer to a defect class that survived two review
// rounds and ten separate instances.
//
// Round 1 found five ways a pair of responses could produce a
// `deployed_executed`/`match` receipt while the claim that receipt makes was
// never established: a shadow candidate exempt from the plane check, a
// baseline whose plane was checked only when a header happened to be
// present, two identical GraphQL error envelopes, an unrecorded content
// type, an unbound build. Each was fixed. The confirmation pass then found
// FOUR MORE in the same seam: a missing build header on the proof route,
// two identical HTTP 500s, trailing bytes after the JSON value, an empty
// data object.
//
// The four were not bad luck. Every fix up to that point added one more
// entry to what had become a BLACKLIST -- a growing list of ways a response
// could be disqualified -- and a blacklist's default is ADMIT. So every
// shape nobody had thought of still produced a proof, and the seam kept
// yielding new instances precisely because it had just been edited.
//
// R57 inverts it. `Admit` below is the ONLY door to `executed=true`, and it
// admits nothing that does not satisfy every precondition by name. The
// default is REFUSED. A response shape nobody has imagined yet is refused
// on arrival rather than certified, which is the property that makes the
// next unknown shape a non-event instead of the eleventh instance.
//
// Consequence worth stating: refusals are now common and expected. A
// refusal is a working instrument reporting that it could not measure --
// the failure this design will not permit is the other one.

// Admission refusal reasons. Each names one precondition.
const (
	RefusalNonSuccessStatus  = "response_status_was_not_success"
	RefusalTrailingBytes     = "response_body_carried_bytes_after_the_json_value"
	RefusalErroredResponse   = "response_carried_graphql_errors"
	RefusalEmptyResponseRoot = "response_root_field_absent_or_empty"
	RefusalBuildUnbound      = "serving_build_not_bound_on_a_route_that_can_bind_it"
)

// Admission is Admit's verdict. Admitted is false unless every precondition
// held; Reason is one of the refusal constants and is never empty when
// Admitted is false.
type Admission struct {
	Admitted bool
	Reason   string
	Detail   string
	// EdgeBuildBinding records whether the MEASURED response carried the
	// serving build on itself: EdgeBuildPresent or EdgeBuildAbsent.
	//
	// It is recorded rather than merely checked because absence is a known
	// deployment gap (CHAOS-5479: the Python edge rebuilds the response
	// with only content, status and media_type and drops every header
	// query-api sets), and a receipt that did not say whether it had that
	// binding cannot be told apart later from one that did. Empty on a
	// refusal.
	EdgeBuildBinding string
}

// Whether a measured response carried the serving build on itself.
//
// EdgeBuildAbsent is not a failure -- it is the normal state of the edge
// route until #2365 deletes the Python edge -- but it IS the reason the
// routing-row cross-check has to stay a refusal: with no per-request
// binding, nothing else distinguishes one replica from another during a
// rolling deploy.
const (
	EdgeBuildPresent = "per_request"
	EdgeBuildAbsent  = "absent"
)

func refused(reason, detail string) Admission {
	return Admission{Reason: reason, Detail: detail}
}

// AdmissionInput is everything Admit is allowed to consider. It is a struct
// rather than a parameter list so that adding a precondition input is a
// compile-time event at every call site, not a silently-defaulted argument.
type AdmissionInput struct {
	// Route is RouteEdge or RouteProof. It decides whether per-request
	// build binding is REQUIRED or merely unavailable.
	Route string
	// NamedBuild is the build the receipt will name (from /buildinfo).
	NamedBuild string
	// ResponseRoot is the GraphQL field this operation's registered
	// document selects at the top of `data`.
	ResponseRoot string
	// RootNullable is whether the SDL declares that field NULLABLE.
	//
	// Round 2's F4: refusing every null root made the two operations whose
	// root IS nullable unprovable. `capacityForecast` and
	// `throughputForecast` are declared without `!`, and their resolver
	// says so in as many words -- "A null result is a TOLERATED empty (no
	// history, or no positive item ...)". Both planes returning null there
	// is a correct answer and real parity; refusing it would mean this
	// command could never prove those two operations against an org with
	// no history. For the other thirteen the root is non-null in the SDL,
	// so a null is the operation failing to produce its own result.
	RootNullable bool

	Candidate, Baseline         Observation
	CandidateSnap, BaselineSnap Snapshot
}

// Admit reports whether this pair of observations may back a proof receipt.
//
// Ordered so the most fundamental failure is named first: a response nobody
// can attribute to a plane is a worse problem than one whose body is empty,
// and an operator reading the refusal should see the cause closest to the
// root.
func Admit(in AdmissionInput) Admission {
	// 1. Both planes positively identified. Silence is not evidence.
	if a := admitPlanes(in); !a.Admitted {
		return a
	}
	// 2. The build that SERVED the request, where the route can say. The
	//    binding it established is carried to the final verdict so the
	//    receipt records what was checked, not what was assumed.
	build := admitBuild(in)
	if !build.Admitted {
		return build
	}
	// 3. HTTP success on both legs. A proof is a record of the operation
	//    WORKING; a non-2xx is the operation not working, however
	//    symmetrically both planes agree about it.
	for _, leg := range []struct {
		name string
		obs  Observation
	}{{"candidate", in.Candidate}, {"baseline", in.Baseline}} {
		if leg.obs.StatusCode < http.StatusOK || leg.obs.StatusCode >= http.StatusMultipleChoices {
			return refused(RefusalNonSuccessStatus,
				fmt.Sprintf("%s leg answered HTTP %d: a proof records the operation working, and two planes failing the same way is agreement about a failure, not parity", leg.name, leg.obs.StatusCode))
		}
	}
	// 4. Exactly one JSON value, consuming the whole body. Trailing bytes
	//    are dropped by the decoder, so two materially different bodies can
	//    decode to the same value and compare equal.
	for _, leg := range []struct {
		name string
		snap Snapshot
	}{{"candidate", in.CandidateSnap}, {"baseline", in.BaselineSnap}} {
		if leg.snap.TrailingBytes {
			return refused(RefusalTrailingBytes,
				fmt.Sprintf("%s body carried bytes after its first JSON value: the decoder ignores them, so the comparison would not be over the bytes actually served", leg.name))
		}
	}
	// 5. No GraphQL errors on either side.
	for _, leg := range []struct {
		name string
		snap Snapshot
	}{{"candidate", in.CandidateSnap}, {"baseline", in.BaselineSnap}} {
		if len(leg.snap.Errors) > 0 {
			return refused(RefusalErroredResponse,
				fmt.Sprintf("%s returned %d GraphQL error(s): agreement on a failure is not proof the operation works on the candidate plane", leg.name, len(leg.snap.Errors)))
		}
	}
	// 6. The operation's OWN root field, present and non-empty. `data: {}`
	//    and a missing root both decode to something a presence check
	//    accepts while proving that nothing was actually resolved.
	if a := admitResponseRoot(in); !a.Admitted {
		return a
	}
	return Admission{Admitted: true, EdgeBuildBinding: build.EdgeBuildBinding}
}

func admitPlanes(in AdmissionInput) Admission {
	switch {
	case in.Candidate.Plane == "":
		return refused(RefusalPlaneUnidentified,
			fmt.Sprintf("candidate leg carried no %s header (status %d): with no plane evidence this response cannot back a proof. On the edge route GO_API_PLANE_HEADER_ENABLED must be true; on the proof route the deployment predates the header the proof handler stamps", planeHeader, in.Candidate.StatusCode))
	case in.Candidate.Plane != "go":
		return refused(RefusalWrongPlane,
			fmt.Sprintf("candidate leg was served by plane %q (status %d): the edge fell back to Python", in.Candidate.Plane, in.Candidate.StatusCode))
	case in.Baseline.Plane == "":
		return refused(RefusalPlaneUnidentified,
			fmt.Sprintf("baseline leg carried no %s header (status %d): the control cannot be shown to be Python, so the comparison cannot back a proof", planeHeader, in.Baseline.StatusCode))
	case in.Baseline.Plane != "python":
		return refused(RefusalWrongPlane,
			fmt.Sprintf("baseline leg was served by plane %q -- the control must be Python", in.Baseline.Plane))
	}
	return Admission{Admitted: true}
}

// admitBuild requires per-request build binding on the route that CAN
// provide it, and records that the edge route cannot.
//
// The asymmetry is a property of the deployment, not a convenience: the
// proof route stamps the serving build on its own responses, while the
// Python edge rebuilds the response with only content, status and
// media_type and drops every header query-api sets. Requiring the header on
// the edge route would refuse every legitimate canary measurement; NOT
// requiring it on the proof route is how the confirmation pass's C1 got a
// MATCH with no build evidence at all.
func admitBuild(in AdmissionInput) Admission {
	switch in.Route {
	case RouteProof:
		switch {
		case in.Candidate.Build == "":
			return refused(RefusalBuildUnbound,
				fmt.Sprintf("the proof route stamps the serving build on every response and this one carried no %s header: without it the receipt would name a build nothing showed served the request", buildHeader))
		case in.Candidate.Build != in.NamedBuild:
			return refused(RefusalBuildMismatch,
				fmt.Sprintf("the process that served this request reports build %q, but the receipt would name %q", in.Candidate.Build, in.NamedBuild))
		}
	case RouteEdge:
		// When a build header DOES arrive it is required to agree. A
		// present-but-wrong value is evidence that the replica which
		// served this request is not the one the receipt would name --
		// exactly the mixed-replica case, caught here rather than
		// certified.
		if in.Candidate.Build != "" {
			if in.Candidate.Build != in.NamedBuild {
				return refused(RefusalBuildMismatch,
					fmt.Sprintf("the process that served this request reports build %q, but the receipt would name %q", in.Candidate.Build, in.NamedBuild))
			}
			return Admission{Admitted: true, EdgeBuildBinding: EdgeBuildPresent}
		}
		// Absent. Admitted, because requiring it would refuse every
		// legitimate canary measurement until #2365 -- but RECORDED, so a
		// reader of the receipt can see this measurement was not bound to
		// a replica.
		return Admission{Admitted: true, EdgeBuildBinding: EdgeBuildAbsent}
	default:
		return refused(RefusalNotRouted, fmt.Sprintf("route %q is not a measurement route", in.Route))
	}
	return Admission{Admitted: true, EdgeBuildBinding: EdgeBuildPresent}
}

func admitResponseRoot(in AdmissionInput) Admission {
	if in.ResponseRoot == "" {
		return refused(RefusalEmptyResponseRoot,
			"this operation declares no ResponseRoot, so the presence of its own result cannot be checked")
	}
	for _, leg := range []struct {
		name string
		snap Snapshot
	}{{"candidate", in.CandidateSnap}, {"baseline", in.BaselineSnap}} {
		if detail := emptyRootDetail(leg.name, in.ResponseRoot, in.RootNullable, leg.snap); detail != "" {
			return refused(RefusalEmptyResponseRoot, detail)
		}
	}
	return Admission{Admitted: true}
}

// emptyRootDetail returns why this snapshot does not carry a resolved
// result for root, or "" when it does.
func emptyRootDetail(leg, root string, nullable bool, snap Snapshot) string {
	if !snap.DataPresent || snap.Data == nil {
		return fmt.Sprintf("%s returned no data at all", leg)
	}
	data, ok := snap.Data.(map[string]any)
	if !ok {
		return fmt.Sprintf("%s returned a `data` that is not an object (%T)", leg, snap.Data)
	}
	value, present := data[root]
	if !present {
		return fmt.Sprintf("%s returned no %q field: the operation's own result is absent, so nothing was resolved", leg, root)
	}
	switch typed := value.(type) {
	case nil:
		if nullable {
			return ""
		}
		return fmt.Sprintf("%s returned %q = null, and the SDL declares that field non-null: a null root is the operation failing to produce its own result", leg, root)
	case map[string]any:
		if len(typed) == 0 {
			return fmt.Sprintf("%s returned %q as an empty object", leg, root)
		}
		// An object carrying only __typename resolved no actual fields.
		// gqlgen adds __typename to every selection set, so this shape is
		// what a resolver returning nothing looks like on the wire (round
		// 2's F5, reproduced: it was admitted).
		if len(typed) == 1 {
			if _, only := typed["__typename"]; only {
				return fmt.Sprintf("%s returned %q carrying only __typename: no field of the operation's own result resolved", leg, root)
			}
		}
	case string, float64, bool, int, int64:
		// Every registered operation's root is an object or a connection.
		// A scalar there is not the operation's result whatever it says
		// (round 2's F5).
		return fmt.Sprintf("%s returned %q as a scalar (%T): no registered operation has a scalar root", leg, root, typed)
	case []any:
		// An empty LIST is a legitimate result -- "this org has no feature
		// flags" is a real answer, and refusing it would make the
		// instrument unable to prove any operation over empty data. An
		// empty OBJECT is different: it means the resolver produced no
		// fields at all.
		return ""
	}
	return ""
}
