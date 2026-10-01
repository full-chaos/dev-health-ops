package goapiproof

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Go-edge mode.
//
// The default mode of this package measures through an edge that has a
// Python plane behind it: the control document (the registered text plus an
// inert comment) misses the digest and is answered by Python, and that
// answer is the reference. Once the product path /graphql is answered by
// query-api itself there is no Python plane behind the edge, so that mode
// cannot run at all: the control document is refused by query-api, which
// serves registered documents only.
//
// Go-edge mode is the proof for that edge. It is asked for explicitly
// (Config.GoEdge), never detected, because the two modes prove different
// things and a run must say which one it is:
//
//   - WHAT IT PROVES. The edge is query-api and nothing else: every control
//     document is refused there, 404 with the UNREGISTERED_DOCUMENT error,
//     by plane go, from the build the receipt names. And the operation
//     works on that build through that edge: the candidate answered 2xx,
//     from plane go, with the serving build on the response, a JSON body,
//     no GraphQL error, and its own root present and non-empty. The caller's
//     identity is the org the run names, with no impersonation in force
//     (every credential value is checked for its org claim before it is
//     sent, and no leg may carry the impersonation stamp).
//   - WHAT IT DOES NOT PROVE. That the answer equals what the Python
//     implementation answered. No Python answer is read in this mode. That
//     comparison is the frozen two-plane record each go-served ledger entry
//     cites (its two-plane sha) and the frozen venue oracles; a Go-edge
//     PROVEN_GO_ONLY is the candidate standing alone, exactly as the go-only
//     class has always been, and it is admitted only for an operation the
//     go-served ledger names.
//
// A receipt written in this mode is a go-only receipt (the cited-mismatch
// arm, the ledger's own citation) whose provenance says edge_mode=go.

// The edge modes a run can be in. The mode is on every outcome, on the
// summary and in each receipt's provenance, so nobody has to infer it.
const (
	EdgeModePython = "python"
	EdgeModeGo     = "go"
)

// UnregisteredDocumentCode is the error code query-api's /graphql answers an
// unregistered document with (internal/queryapi/server
// refuseGraphQLEdgeUnregistered, pinned against this constant by that
// package's tests): the control document's expected answer.
const UnregisteredDocumentCode = "UNREGISTERED_DOCUMENT"

// goEdgeControlProbe is the control document the run sends once, before any
// case, to refuse a wrong edge with one message instead of one per case. It
// carries the same inert comment every per-case control does.
const goEdgeControlProbe = "query DevHealthProveGoEdgeControl { __typename }" + baselineComment

// The refusals only Go-edge mode can raise. Each names one way the edge
// showed it is not query-api answering alone, or one fact this mode needs
// that the response did not carry.
const (
	// The control document's answer carried no plane header: nothing says
	// which plane refused it.
	RefusalGoEdgeControlPlaneUnidentified = "go_edge_control_plane_unidentified"
	// Another plane (python) answered the control document: a Python plane
	// is behind this edge, and the Python-reference mode is the one to run.
	RefusalGoEdgeControlOtherPlane = "go_edge_control_answered_by_another_plane"
	// The edge answered the control document with a success: something
	// behind it serves, or forwards, a document query-api does not register.
	RefusalGoEdgeControlServed = "go_edge_control_document_was_served"
	// The control document was not refused the way query-api refuses an
	// unregistered document (404, UNREGISTERED_DOCUMENT).
	RefusalGoEdgeControlNotRefused = "go_edge_control_not_refused_as_unregistered"
	// The control document's refusal carried no serving-build header, so it
	// is not tied to the build the receipt names.
	RefusalGoEdgeControlBuildUnbound = "go_edge_control_build_unbound"
	// The candidate carried no serving-build header. query-api's /graphql
	// stamps it on every response, so in this mode its absence is a refusal,
	// not a recorded gap.
	RefusalGoEdgeCandidateBuildUnbound = "go_edge_candidate_build_unbound"
	// The candidate's content type is not JSON. No other leg is compared in
	// this mode, so the type is checked on the candidate itself.
	RefusalGoEdgeCandidateContentType = "go_edge_candidate_content_type"
	// The go-served ledger does not name the operation: with no Python plane
	// to compare against, only an operation whose two-plane record the
	// ledger cites can be proven by the candidate alone.
	RefusalGoEdgeNotGoServed = "go_edge_operation_not_in_the_go_served_ledger"
)

// ErrGoEdge is returned when a Go-edge run cannot start: the edge refused
// the pre-run control probe in a way this mode does not accept, or a
// credential is not bound to the run's org.
var ErrGoEdge = errors.New("goapiproof: go_edge_mode_refused")

// admitGoEdgeControl decides whether control is query-api refusing an
// unregistered document from the named build. It is the whole evidence that
// no Python plane is behind the edge, so each way of being anything else is
// refused by name. The headers and the status are read before the body, so a
// wrong edge is named for what it is even when its body is not JSON at all.
func admitGoEdgeControl(control Observation, namedBuild string) Admission {
	switch {
	case control.Impersonating:
		return refused(RefusalServedUnderImpersonation,
			fmt.Sprintf("the control leg carried the %s header: the edge answered for an impersonation session's target org, not the org this run's credential names", impersonationHeader))
	case control.Plane == "":
		return refused(RefusalGoEdgeControlPlaneUnidentified,
			fmt.Sprintf("the control document's answer carried no %s header (status %d): nothing says which plane answered it, so it cannot show the edge is query-api", planeHeader, control.StatusCode))
	case control.Plane != "go":
		return refused(RefusalGoEdgeControlOtherPlane,
			fmt.Sprintf("the control document was answered by plane %q (status %d): another plane is behind this edge. Go-edge mode proves an edge that is query-api alone; run the Python-reference mode against this one", control.Plane, control.StatusCode))
	case control.StatusCode >= http.StatusOK && control.StatusCode < http.StatusMultipleChoices:
		return refused(RefusalGoEdgeControlServed,
			fmt.Sprintf("the edge answered HTTP %d to the control document: something behind it serves or forwards a document query-api does not register", control.StatusCode))
	case control.StatusCode != http.StatusNotFound || !carriesUnregisteredDocumentError(control.Body):
		return refused(RefusalGoEdgeControlNotRefused,
			fmt.Sprintf("the control document was answered HTTP %d without query-api's %s refusal: this is not the registered-document gate answering", control.StatusCode, UnregisteredDocumentCode))
	case control.Build == "":
		return refused(RefusalGoEdgeControlBuildUnbound,
			fmt.Sprintf("the control document's refusal carried no %s header: it is not tied to the build the receipt would name", buildHeader))
	case control.Build != namedBuild:
		return refused(RefusalBuildMismatch,
			fmt.Sprintf("the process that refused the control document reports build %q, but the receipt would name %q", control.Build, namedBuild))
	}
	return Admission{Admitted: true}
}

// carriesUnregisteredDocumentError reports whether body is query-api's
// refusal of an unregistered document: one JSON value with no data and an
// error whose extensions.code is UnregisteredDocumentCode. A body that does
// not decode is not that refusal.
func carriesUnregisteredDocumentError(body []byte) bool {
	snapshot, err := DecodeSnapshot(body)
	if err != nil || snapshot.TrailingBytes || snapshot.Data != nil {
		return false
	}
	for _, graphQLError := range snapshot.Errors {
		extensions, _ := graphQLError["extensions"].(map[string]any)
		if code, _ := extensions["code"].(string); code == UnregisteredDocumentCode {
			return true
		}
	}
	return false
}

// admitGoEdge is Admit for Go-edge mode. in.Baseline is the control leg. The
// candidate stands alone, so every check a candidate gets in the go-only
// class applies, plus the two facts nothing else would check in this mode:
// the serving build on an edge response, and the content type.
func admitGoEdge(in AdmissionInput) Admission {
	// 1. The edge is query-api alone, answering from the named build.
	if control := admitGoEdgeControl(in.Baseline, in.NamedBuild); !control.Admitted {
		return control
	}
	// 2. The candidate: served for the credential's own org, by plane go.
	switch {
	case in.Candidate.Impersonating:
		return refused(RefusalServedUnderImpersonation,
			fmt.Sprintf("the candidate leg carried the %s header: the edge served it for an impersonation session's target org, not the org this run's credential names", impersonationHeader))
	case in.Candidate.Plane == "":
		return refused(RefusalPlaneUnidentified,
			fmt.Sprintf("candidate leg carried no %s header (status %d): with no plane evidence this response cannot back a proof", planeHeader, in.Candidate.StatusCode))
	case in.Candidate.Plane != "go":
		return refused(RefusalWrongPlane,
			fmt.Sprintf("candidate leg was served by plane %q (status %d): the edge did not answer as query-api", in.Candidate.Plane, in.Candidate.StatusCode))
	}
	// 3. The build that served it. On the edge route the header is required
	//    here: query-api stamps it on every /graphql response.
	if in.Route == RouteEdge && in.Candidate.Build == "" {
		return refused(RefusalGoEdgeCandidateBuildUnbound,
			fmt.Sprintf("the candidate carried no %s header: query-api's own edge stamps the serving build on every response, so without it the receipt would name a build nothing showed served the request", buildHeader))
	}
	build := admitBuild(in)
	if !build.Admitted {
		return build
	}
	// 4. The operation working: a success, one JSON value, a JSON type, no
	//    GraphQL error, its own root present and non-empty.
	if in.Candidate.StatusCode < http.StatusOK || in.Candidate.StatusCode >= http.StatusMultipleChoices {
		return refused(RefusalNonSuccessStatus,
			fmt.Sprintf("candidate leg answered HTTP %d: a proof records the operation working", in.Candidate.StatusCode))
	}
	if contentType := in.Candidate.Headers[contentTypeHeader]; !isJSONContentType(contentType) {
		return refused(RefusalGoEdgeCandidateContentType,
			fmt.Sprintf("candidate leg answered content type %q: a GraphQL answer is JSON, and no other leg is compared in this mode to catch another type", contentType))
	}
	if in.CandidateSnap.TrailingBytes {
		return refused(RefusalTrailingBytes,
			"candidate body carried bytes after its first JSON value: the decoder ignores them, so the check would not be over the bytes actually served")
	}
	if len(in.CandidateSnap.Errors) > 0 {
		return refused(RefusalErroredResponse,
			fmt.Sprintf("candidate returned %d GraphQL error(s): an operation that errors is not working on the candidate plane", len(in.CandidateSnap.Errors)))
	}
	if root := admitResponseRoot(in, true); !root.Admitted {
		return root
	}
	// 5. Only an operation whose two-plane record the ledger cites: the
	//    citation is what stands where a comparison would.
	//    (A run with no ledger names no operation, so it proves none.)
	citation, err := NewGoOnlyCitation(in.GoServed, in.Operation)
	if err != nil {
		return refused(RefusalGoEdgeNotGoServed, err.Error())
	}
	return Admission{Admitted: true, EdgeBuildBinding: build.EdgeBuildBinding, GoOnly: true, GoOnlyCitation: citation}
}

// isJSONContentType reports whether a content type is application/json, with
// or without parameters.
func isJSONContentType(contentType string) bool {
	mediaType, _, _ := strings.Cut(contentType, ";")
	return strings.EqualFold(strings.TrimSpace(mediaType), "application/json")
}

// verifyGoEdge is the pre-run check of Go-edge mode: every edge credential
// is bound to the run's org (so each value sent is checked for its org claim
// and for no impersonation claim), and the edge refuses the control probe as
// query-api does, from the named build. A run against any other edge stops
// here, with one reason, before a case is sent.
func (r *Runner) verifyGoEdge(ctx context.Context) error {
	for _, credential := range []*Credential{r.Config.EdgeCredential, r.Config.AdminEdgeCredential} {
		if credential == nil {
			continue
		}
		if r.Config.OrgID == "" || credential.org != r.Config.OrgID {
			return fmt.Errorf("%w: the %s is not bound to the run's org: in this mode nothing but the credential's own org claim says which org the edge serves it for", ErrGoEdge, credential.kind)
		}
	}
	if r.Config.EdgeCredential == nil {
		return fmt.Errorf("%w: no edge credential", ErrGoEdge)
	}
	control, err := r.post(ctx, r.Config.PythonEdgeURL, goEdgeControlProbe, r.Config.EdgeCredential, map[string]any{})
	if err != nil {
		return fmt.Errorf("%w: control probe: %w", ErrGoEdge, err)
	}
	if admission := admitGoEdgeControl(control, r.Registry.BuildIdentity); !admission.Admitted {
		return fmt.Errorf("%w: %s: %s", ErrGoEdge, admission.Reason, admission.Detail)
	}
	return nil
}
