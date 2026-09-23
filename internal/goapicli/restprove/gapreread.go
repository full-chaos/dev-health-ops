package restprove

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// gapRereadMargin is the room the run deadline must leave beyond the
// delay and the two delayed reads.
const gapRereadMargin = 5 * time.Second

// gapRereadRecord is one case's delayed re-read.
type gapRereadRecord struct {
	Outcome goapiproof.GapRereadOutcome `json:"outcome"`
	Detail  string                      `json:"detail,omitempty"`
	// Delay is the wait the stage used (zero when it did not wait).
	Delay string `json:"delay,omitempty"`
	// CandidateReadAt and BaselineReadAt are when the delayed candidate
	// and baseline reads returned (UTC).
	CandidateReadAt          time.Time `json:"candidate_read_at,omitzero"`
	BaselineReadAt           time.Time `json:"baseline_read_at,omitzero"`
	CandidateResponseRef     string    `json:"candidate_response_ref,omitempty"`
	BaselineResponseRef      string    `json:"baseline_response_ref,omitempty"`
	CandidateWireAttempts    int       `json:"candidate_wire_attempts,omitempty"`
	BaselineWireAttempts     int       `json:"baseline_wire_attempts,omitempty"`
	DelayedComparisonOutside int       `json:"delayed_comparison_outside,omitempty"`
}

// gapRereadState is the run's shared delay budget.
type gapRereadState struct {
	delay, budget time.Duration
	mu            sync.Mutex
	spent         time.Duration
	// sleep and until are the clock, replaceable by a test.
	sleep func(ctx context.Context, d time.Duration) error
	until func(t time.Time) time.Duration
}

func newGapRereadState(delay, budget time.Duration) *gapRereadState {
	return &gapRereadState{delay: delay, budget: budget, sleep: sleepContext, until: time.Until}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// reserve takes one delay from the budget, or names why it cannot.
func (g *gapRereadState) reserve(ctx context.Context, timeout time.Duration) (goapiproof.GapRereadOutcome, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if deadline, ok := ctx.Deadline(); ok && g.until(deadline) < g.delay+2*timeout+gapRereadMargin {
		return goapiproof.GapRereadDeadlinePressure, false
	}
	if g.spent+g.delay > g.budget {
		return goapiproof.GapRereadBudgetExhausted, false
	}
	g.spent += g.delay
	return "", true
}

// run is the delayed re-read of one eligible case. It never returns an
// error and never refuses: every failure leaves the case as it was (a
// record whose outcome is not gap_admitted). On gap_admitted the returned
// snapshot is the delayed baseline read the case now stands on.
func (g *gapRereadState) run(
	ctx context.Context,
	client *goapiproof.LegClient,
	f flags,
	spec goapiproof.RESTEndpointSpec,
	request goapiproof.RESTRequest,
	baselineCredential, candidateCredential *goapiproof.Credential,
	timeout time.Duration,
	namedBuild string,
	first goapiproof.RESTAdmission,
	firstResult goapiproof.Result,
	artifacts *goapiproof.ArtifactStore,
) (*gapRereadRecord, goapiproof.Snapshot, goapiproof.GapRereadDecision) {
	var none goapiproof.Snapshot
	var noDecision goapiproof.GapRereadDecision
	record := &gapRereadRecord{}
	if outcome, ok := g.reserve(ctx, timeout); !ok {
		record.Outcome = outcome
		return record, none, noDecision
	}
	record.Delay = g.delay.String()
	if err := g.sleep(ctx, g.delay); err != nil {
		record.Outcome, record.Detail = goapiproof.GapRereadCancelled, "the run context ended during the delay"
		return record, none, noDecision
	}
	fail := func(detail string) (*gapRereadRecord, goapiproof.Snapshot, goapiproof.GapRereadDecision) {
		record.Outcome, record.Detail = goapiproof.GapRereadReadFailed, detail
		if ctx.Err() != nil {
			record.Outcome = goapiproof.GapRereadCancelled
		}
		return record, none, noDecision
	}
	candidateLeg, err := doREST(ctx, client, f.queryAPIURL, spec.Method, spec.Path, request.Query, request.Body, candidateCredential, timeout)
	if err != nil {
		return fail("delayed candidate read failed")
	}
	record.CandidateReadAt = time.Now().UTC()
	baselineLeg, err := doREST(ctx, client, f.pythonAPIURL, spec.Method, spec.Path, request.Query, request.Body, baselineCredential, timeout)
	if err != nil {
		return fail("delayed baseline read failed")
	}
	record.BaselineReadAt = time.Now().UTC()
	record.CandidateWireAttempts, record.BaselineWireAttempts = candidateLeg.WireAttempts, baselineLeg.WireAttempts
	if artifacts != nil {
		if record.CandidateResponseRef, err = artifacts.Put(candidateLeg.Body); err != nil {
			return fail("store delayed candidate artifact failed")
		}
		if record.BaselineResponseRef, err = artifacts.Put(baselineLeg.Body); err != nil {
			return fail("store delayed baseline artifact failed")
		}
	}
	// Both delayed reads are admitted exactly as the first pair was.
	admission := goapiproof.RESTAdmit(goapiproof.RESTAdmissionInput{
		NamedBuild:          namedBuild,
		WantCandidateStatus: request.WantCandidateStatus,
		WantBaselineStatus:  request.WantBaselineStatus,
		PythonForwarder:     spec.PythonForwarder && !f.pythonForwarderOff,
		Candidate:           candidateLeg,
		Baseline:            baselineLeg,
	}, true)
	if !admission.Admitted {
		return fail("delayed reads not admitted: " + admission.Detail)
	}
	candidate2 := admission.CandidateSnap
	candidate2.Data = goapiproof.InjectRESTDedupKeys(candidate2.Data, request.DedupListPath, request.DedupKeyFields)
	baseline3 := admission.BaselineSnap
	baseline3.Data = goapiproof.InjectRESTDedupKeys(baseline3.Data, request.DedupListPath, request.DedupKeyFields)
	decision := goapiproof.ClassifyGapReread(firstResult, first.BaselineSnap, first.CandidateSnap, baseline3, candidate2, request.Parity)
	record.Outcome, record.Detail = decision.Outcome, decision.Detail
	record.DelayedComparisonOutside = decision.Second.DifferencesOutsideBaselineDefect
	return record, baseline3, decision
}

// validateGapRereadFlags bounds -gap-reread-delay (0 disables, at most
// GapRereadMaxDelay) and requires the budget to hold at least one delay.
func validateGapRereadFlags(delay, budget time.Duration) error {
	if delay < 0 || delay > goapiproof.GapRereadMaxDelay {
		return fmt.Errorf("-gap-reread-delay must be between 0 and %s, got %s", goapiproof.GapRereadMaxDelay, delay)
	}
	if delay > 0 && budget < delay {
		return fmt.Errorf("-gap-reread-budget must be at least -gap-reread-delay (%s), got %s", delay, budget)
	}
	return nil
}
