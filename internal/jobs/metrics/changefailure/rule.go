package changefailure

// RuleInputs are the summed stored counts of a view, as the rule reads them.
// ClickHouse readers (WindowRateSQL) encode the same rule.
type RuleInputs struct {
	// StoredRows is the number of stored rows behind the sums. 0 means nothing
	// was collected for the view.
	StoredRows uint64
	// Deployments (D) is every deployment of the view but a cancelled one.
	Deployments uint64
	// StatusKnown (K) is the deployments among them with a final status
	// (succeeded or failed).
	StatusKnown uint64
	// FailedStatusOnly is the deployments whose own status is failed and that
	// no incident link names.
	FailedStatusOnly uint64
	// FailedIncidentLinked is the deployments an incident link names (native
	// plus heuristic), whatever their own status. A deployment that is failed
	// by status AND linked is stored here once, so the union of the two kinds
	// of failure is FailedStatusOnly + FailedIncidentLinked and no third count
	// is needed.
	FailedIncidentLinked uint64
	// IncidentEvidence is true when an incident ties to the view's subject.
	IncidentEvidence bool
}

// RuleResult is the answer of the rule. Deployments (D), StatusKnown (K) and
// Coverage (K / D, nil without a deployment) always go out with it, so a
// deployment without a stored outcome (a status lookup that was skipped) is
// visible as a gap rather than hidden in the rate.
type RuleResult struct {
	Rate          *float64
	Basis         Basis
	UnknownReason UnknownReason
	Deployments   uint64
	StatusKnown   uint64
	Coverage      *float64
}

// ApplyRule is the one change failure rule (CHAOS-9621). In order:
//
//   - no stored row                              -> not collected
//   - no deployment                              -> no deployments in window
//   - no incident evidence and no final status   -> deployments without outcome
//   - else the rate: failed / denominator, where failed = FailedStatusOnly +
//     FailedIncidentLinked, each deployment once, and the denominator and basis
//     are those of denominator.
//
// A rate of 0 is a measured 0.
func ApplyRule(in RuleInputs) RuleResult {
	result := RuleResult{Basis: BasisNone, Deployments: in.Deployments, StatusKnown: in.StatusKnown}
	if in.Deployments > 0 {
		coverage := float64(in.StatusKnown) / float64(in.Deployments)
		result.Coverage = &coverage
	}
	switch {
	case in.StoredRows == 0:
		result.UnknownReason = ReasonNotCollected
		return result
	case in.Deployments == 0:
		result.UnknownReason = ReasonNoDeploymentsInWindow
		return result
	case !in.IncidentEvidence && in.StatusKnown == 0:
		result.UnknownReason = ReasonDeploymentsWithoutOutcome
		return result
	}
	basis, denominator := denominator(in)
	failed := in.FailedStatusOnly + in.FailedIncidentLinked
	if basis == BasisStatus {
		// No incident ties to the view, so no link can count a deployment:
		// the failed deployments are the ones failed by status.
		failed = in.FailedStatusOnly
	}
	rate := float64(failed) / float64(denominator)
	result.Basis, result.Rate = basis, &rate
	return result
}

// denominator is the one place that chooses what a rate is divided by, and the
// evidence basis it stands on:
//
//   - incident evidence, no final status -> incident, every deployment (D)
//   - incident evidence and final status -> both, every deployment (D)
//   - final status only                  -> status, the deployments with a final
//     status (K): a deployment without an outcome is neither failed nor
//     healthy, so it is not in the base of a rate that only statuses can state
//
// The caller has checked that there is evidence of one kind.
func denominator(in RuleInputs) (Basis, uint64) {
	switch {
	case in.IncidentEvidence && in.StatusKnown > 0:
		return BasisBoth, in.Deployments
	case in.IncidentEvidence:
		return BasisIncident, in.Deployments
	default:
		return BasisStatus, in.StatusKnown
	}
}
