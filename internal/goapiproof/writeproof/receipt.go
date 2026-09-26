package writeproof

import (
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// ReceiptInput is everything a write_executed receipt names that Execute does not
// know: the registry key of the operation, who ran it, and how it was reached.
type ReceiptInput struct {
	SchemaDigest    string
	DocumentDigest  string
	CandidateBuild  string
	RequestIdentity string
	Org             string
	RecordedBy      string
	ReviewEvidence  string
	// Route is goapiproof.RouteEdge (the deployed product edge) or
	// goapiproof.RouteProof (a direct POST to query-api's /query, the only way a
	// mutation not yet routed to Go can be measured). Primary enablement admits
	// only an edge receipt.
	Route string
	// BuildBinding is the per-response build binding the edge leg observed
	// (goapiproof.EdgeBuildPresent), or goapiproof.EdgeBuildAbsent when nothing
	// tied the build to THIS response. A direct query-api execution reads it from
	// the response's own build header.
	BuildBinding string
	ObservedAt   time.Time
}

// Receipt turns a Result into the write_executed receipt for it, or refuses when
// no honest receipt exists. A Result that carries no digest (the run never got as
// far as reading effects) owes none: the caller reports the failure instead.
func (r Result) Receipt(in ReceiptInput) (goapiproof.Receipt, error) {
	if r.Digest == "" {
		return goapiproof.Receipt{}, fmt.Errorf("writeproof: case %q observed no effects, so there is no digest to record", r.Case)
	}
	evidence := in.ReviewEvidence
	if r.Kept != nil {
		evidence += fmt.Sprintf(" [dataset KEPT: org=%s run=%s]", r.Kept.Org, r.Kept.Run)
	}
	if r.Detail != "" {
		evidence += " [" + r.Detail + "]"
	}
	return goapiproof.Receipt{
		SchemaDigest:         in.SchemaDigest,
		DocumentDigest:       in.DocumentDigest,
		SelectedOperation:    r.Operation,
		CandidateBuild:       in.CandidateBuild,
		RequestIdentity:      in.RequestIdentity,
		Stage:                goapiproof.EnablementWriteProofStage,
		TerminalState:        r.TerminalState,
		OrgID:                in.Org,
		ReviewEvidence:       evidence,
		RecordedBy:           in.RecordedBy,
		ObservedAt:           in.ObservedAt,
		BaselineResponseRef:  "case:" + r.Case + " baseline:" + r.BaselineDigest,
		CandidateResponseRef: "case:" + r.Case + " digest:" + r.Digest,
		MeasurementRoute:     in.Route,
		BuildBinding:         in.BuildBinding,
		SideEffectDigest:     r.Digest,
	}, nil
}
