package goapiproof

import (
	"fmt"
	"strings"
	"time"
)

// The baseline-timeout declaration.
//
// A REST request whose reference (Python) leg cannot answer inside the run's
// budget produces no comparison: the leg is refused as
// rest_baseline_leg_timed_out and no receipt exists for that branch of the
// route. A route branch with no receipt is not proven, however correct the
// candidate is.
//
// BaselineTimeoutDeclaration admits exactly one more shape for a request that
// carries it, and only while every one of these holds in the SAME run:
//
//   - the baseline leg produced no response at all within the request's own
//     timeout (a timeout, not a refused connection, not an HTTP status), and
//     the measured wait was at least the declared MinTimeout;
//   - the candidate leg answered the declared status, carried the build header
//     the receipt names, decoded as JSON, and held a non-empty value at every
//     NonEmptyPaths entry.
//
// The receipt it yields is the existing cited-mismatch arm of the enablement
// predicate: terminal_state=mismatch, differences_outside_baseline_defect=0,
// baseline_defect=[Ticket]. It carries no new citation vocabulary and never the
// go-only prefix, which stays refused on every REST receipt.
//
// The declaration is a claim about the reference plane, so the run can falsify
// it: a baseline that answers inside the timeout leaves the request on the
// ordinary comparison path (nothing here fires), and a candidate that is empty
// or not the declared status is refused, never admitted on the baseline's
// silence.

// BaselineTimeoutFloor is the least MinTimeout a declaration may carry: the
// wait the four production observations that motivated this class all ran to.
const BaselineTimeoutFloor = 180 * time.Second

// RESTRefusalCandidateEmptyUnderBaselineTimeout: the baseline timed out and the
// candidate held nothing at a declared path, so there is no answer for the
// declaration to stand on.
const RESTRefusalCandidateEmptyUnderBaselineTimeout = "rest_baseline_timed_out_and_candidate_is_empty_at_a_declared_path"

// RESTRefusalBaselineTimeoutTooShort: the baseline leg ended without an answer,
// but sooner than the declaration's MinTimeout, so it does not show the plane
// cannot answer inside that budget.
const RESTRefusalBaselineTimeoutTooShort = "rest_baseline_timed_out_before_the_declared_minimum_wait"

// BaselineTimeoutDeclaration is RESTRequest.BaselineTimeoutDeclared.
type BaselineTimeoutDeclaration struct {
	// Ticket is the one citation written to the receipt's baseline_defect.
	Ticket string
	// Reason states why the reference plane cannot answer this request.
	Reason string
	// MinTimeout is the least wait the baseline leg must have run before its
	// silence counts. It is at least BaselineTimeoutFloor and at most the
	// request's own Timeout.
	MinTimeout time.Duration
	// NonEmptyPaths are snapshot paths (the decoded body is "data", a child key
	// is "data.<key>") the candidate must hold a non-empty value at. Never empty.
	NonEmptyPaths []string
}

// Validate refuses a declaration that could not be falsified or that the
// request cannot honour.
func (d BaselineTimeoutDeclaration) Validate(req RESTRequest) error {
	if NamesNothing(d.Ticket) {
		return fmt.Errorf("baseline-timeout declaration has a blank Ticket")
	}
	if HasGoOnlyPrefix(d.Ticket) {
		return fmt.Errorf("baseline-timeout declaration Ticket starts with %q, which no REST citation may carry", GoOnlyCitationPrefix)
	}
	if NamesNothing(d.Reason) {
		return fmt.Errorf("baseline-timeout declaration has a blank Reason")
	}
	if d.MinTimeout < BaselineTimeoutFloor {
		return fmt.Errorf("baseline-timeout declaration MinTimeout %s is below the %s floor", d.MinTimeout, BaselineTimeoutFloor)
	}
	if req.Timeout < d.MinTimeout {
		return fmt.Errorf("baseline-timeout declaration needs the request Timeout (%s) to be at least its MinTimeout (%s): a shorter budget cannot show the wait", req.Timeout, d.MinTimeout)
	}
	if len(d.NonEmptyPaths) == 0 {
		return fmt.Errorf("baseline-timeout declaration names no NonEmptyPaths: an answer that may be empty proves nothing")
	}
	for _, path := range d.NonEmptyPaths {
		segments := strings.Split(path, ".")
		if segments[0] != "data" {
			return fmt.Errorf("baseline-timeout declaration NonEmptyPaths entry %q does not start at \"data\"", path)
		}
		for _, segment := range segments {
			if strings.TrimSpace(segment) == "" {
				return fmt.Errorf("baseline-timeout declaration NonEmptyPaths entry %q has a blank segment", path)
			}
		}
	}
	for _, binding := range req.IDBindings {
		if binding.Candidates > 0 {
			return fmt.Errorf("baseline-timeout declaration cannot ride a bounded-candidate binding (%q): the search needs the baseline's ids to move on", binding.Producer)
		}
	}
	if req.WantCandidateStatus != 200 {
		return fmt.Errorf("baseline-timeout declaration needs WantCandidateStatus 200, got %d", req.WantCandidateStatus)
	}
	if req.BodyMode != RESTBodyModeJSON {
		return fmt.Errorf("baseline-timeout declaration needs BodyMode %q: the candidate body is what is checked", RESTBodyModeJSON)
	}
	return nil
}

// SnapshotHoldsNonEmpty reports whether value, found at path in a decoded snapshot,
// is a present, non-null value that is not an empty array, empty object or empty
// string. A number or boolean is non-empty. path "data" is the body itself.
func SnapshotHoldsNonEmpty(snapshot Snapshot, path string) bool {
	segments := strings.Split(path, ".")
	if len(segments) == 0 || segments[0] != "data" || !snapshot.DataPresent {
		return false
	}
	value := snapshot.Data
	for _, segment := range segments[1:] {
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		value, ok = object[segment]
		if !ok {
			return false
		}
	}
	switch typed := value.(type) {
	case nil:
		return false
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	case string:
		return typed != ""
	default:
		return true
	}
}

// RESTAdmitCandidateAlone is the candidate half of RESTAdmit, plus the
// declaration's non-empty check, for a request whose baseline leg produced no
// response. It refuses everything RESTAdmit refuses on the candidate leg.
func RESTAdmitCandidateAlone(in RESTAdmissionInput, decl BaselineTimeoutDeclaration) RESTAdmission {
	if in.Candidate.Impersonating {
		return restRefused(RESTRefusalServedUnderImpersonation,
			fmt.Sprintf("the candidate leg carried the %s header", impersonationHeader))
	}
	if in.Candidate.StatusCode != in.WantCandidateStatus {
		return restRefused(RESTRefusalUnexpectedStatus,
			fmt.Sprintf("candidate answered HTTP %d, this request declared %d admissible", in.Candidate.StatusCode, in.WantCandidateStatus))
	}
	switch {
	case in.Candidate.Build == "":
		return restRefused(RESTRefusalBuildUnbound,
			"the candidate response carried no x-dev-health-build header: without it the receipt would name a build nothing showed served the request")
	case in.Candidate.Build != in.NamedBuild:
		return restRefused(RESTRefusalBuildUnbound,
			fmt.Sprintf("the process that served this request reports build %q, but the receipt would name %q", in.Candidate.Build, in.NamedBuild))
	}
	snapshot, err := DecodeRESTSnapshot(in.Candidate.Body)
	if err != nil {
		return restRefused(RESTRefusalBodyNotJSON, "candidate: "+err.Error())
	}
	if snapshot.TrailingBytes {
		return restRefused(RESTRefusalTrailingBytes,
			"the candidate body carried bytes after its first JSON value")
	}
	for _, path := range decl.NonEmptyPaths {
		if !SnapshotHoldsNonEmpty(snapshot, path) {
			return restRefused(RESTRefusalCandidateEmptyUnderBaselineTimeout,
				fmt.Sprintf("the baseline timed out and the candidate holds no value at %q", path))
		}
	}
	return RESTAdmission{Admitted: true, CandidateSnap: snapshot}
}
