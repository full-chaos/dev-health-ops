package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// proveUnderBaselineTimeout finishes one request whose baseline leg produced no
// response within its timeout and whose corpus entry carries a
// BaselineTimeoutDeclared (goapiproof.BaselineTimeoutDeclaration states every
// condition). The baseline's own refusal outcome, timedOut, is what is returned
// whenever a condition fails, so a run that cannot use the declaration reports
// exactly what it reported before it existed.
//
// The candidate leg is sent here, after the baseline (the order every request
// uses), and admitted alone by goapiproof.RESTAdmitCandidateAlone. The receipt is
// the existing cited-mismatch arm: mismatch, nothing outside the citation, and
// the declaration's Ticket as the one baseline_defect entry.
func proveUnderBaselineTimeout(
	ctx context.Context,
	client *goapiproof.LegClient,
	f flags,
	operation string,
	spec goapiproof.RESTEndpointSpec,
	request goapiproof.RESTRequest,
	candidateCredential *goapiproof.Credential,
	namedBuild string,
	auth goapiproof.AuthContext,
	observedAt time.Time,
	writer receiptWriter,
	artifacts *goapiproof.ArtifactStore,
	dryRun bool,
	boundIDs map[string]string,
	waited time.Duration,
	timedOut outcome,
) (outcome, error) {
	decl := *request.BaselineTimeoutDeclared
	// From here every outcome carries the baseline's silence: a refusal keeps
	// counting as a leg that never answered, whatever reason names it.
	timedOut.BaselineTimedOut = true
	if waited < decl.MinTimeout {
		timedOut.Refusal = goapiproof.RESTRefusalBaselineTimeoutTooShort
		timedOut.Detail = fmt.Sprintf("baseline leg ended without an answer after %s, less than the declared minimum wait %s: %s", waited.Round(100*time.Millisecond), decl.MinTimeout, timedOut.Detail)
		return timedOut, nil
	}
	timeout := resolveRESTTimeout(request.Timeout, f.timeout)
	candidateLeg, err := doREST(ctx, client, f.queryAPIURL, spec.Method, spec.Path, request.Query, request.Body, candidateCredential, timeout)
	if err != nil {
		if out, ok := legTransportOutcome(ctx, operation, request.Name, "candidate", boundIDs, err); ok {
			return out, nil
		}
		return outcome{}, fmt.Errorf("candidate leg: %w", err)
	}
	candidateObservedAt := time.Now().UTC()
	var candidateRef string
	if artifacts != nil {
		if candidateRef, err = artifacts.Put(candidateLeg.Body); err != nil {
			return outcome{}, fmt.Errorf("store candidate leg artifact: %w", err)
		}
	}
	waitedText := fmt.Sprintf("%.1fs", waited.Seconds())
	admission := goapiproof.RESTAdmitCandidateAlone(goapiproof.RESTAdmissionInput{
		NamedBuild:          namedBuild,
		WantCandidateStatus: request.WantCandidateStatus,
		WantBaselineStatus:  request.WantBaselineStatus,
		Candidate:           candidateLeg,
	}, decl)
	out := outcome{
		Operation: operation, Request: request.Name,
		Admitted: admission.Admitted, Refusal: admission.Reason, Detail: admission.Detail,
		BoundIDs: boundIDs, BaselineTimedOut: true,
		CandidateResponseRef: candidateRef, CandidateObservedAt: candidateObservedAt,
		ObservedAt:            observedAt,
		CandidateWireAttempts: candidateLeg.WireAttempts,
	}
	if !admission.Admitted {
		out.Detail = fmt.Sprintf("baseline timed out after %s; %s", waitedText, out.Detail)
		return out, nil
	}
	out.BaselineTimedOutAfter = waitedText
	out.Detail = "baseline timed out after " + waitedText
	out.TerminalState = goapiproof.EnablementCitedMismatchState
	out.DifferencesOutsideBaselineDefect = 0
	out.BaselineDefectsMatched = []string{decl.Ticket}
	if dryRun || writer == nil {
		return out, nil
	}

	identity, err := goapiproof.RequestIdentity(f.org, auth, restRequestVariables(spec.Method, request.Query, request.Body))
	if err != nil {
		return outcome{}, err
	}
	declared := []string{decl.Ticket}
	for _, defect := range request.Parity.BaselineDefects {
		declared = append(declared, defect.Ticket)
	}
	evidence, err := json.Marshal(restReviewEvidence{Operator: f.reviewEvidence, BaselineTimedOutAfter: waitedText})
	if err != nil {
		return outcome{}, fmt.Errorf("encode review evidence: %w", err)
	}
	id, err := writer.WriteReceipt(ctx, goapiproof.RESTReceipt{
		Method:                           spec.Method,
		Path:                             spec.Path,
		CandidateBuild:                   namedBuild,
		RequestIdentity:                  identity,
		Stage:                            goapiproof.EnablementProofStage,
		TerminalState:                    out.TerminalState,
		OrgID:                            f.org,
		ReviewEvidence:                   string(evidence),
		RecordedBy:                       f.recordedBy,
		ObservedAt:                       observedAt,
		MeasurementRoute:                 goapiproof.RouteProof,
		BaselineDefects:                  out.BaselineDefectsMatched,
		DeclaredDefects:                  declared,
		DifferencesOutsideBaselineDefect: 0,
		BuildBinding:                     goapiproof.EdgeBuildPresent,
		BoundIDs:                         boundIDs,
		CandidateResponseRef:             candidateRef,
	})
	if err != nil {
		return outcome{}, fmt.Errorf("write receipt: %w", err)
	}
	out.ReceiptID = id.String()
	return out, nil
}
