package goapiproof

// Venue class 2: "no production data" (CHAOS-6099).
//
// Production holds no saved reports in any org the prover could run as, so
// savedReports / savedReport / reportRuns cannot be proven non-empty there:
// the prover refuses every data-requiring case as vacuous, because two
// empty answers agree without either plane reading anything. The venue
// (bigboy) has the data. This class admits such an operation from TWO
// reports, and the "no data" half is COMPUTED, not attested:
//
//  1. the production prover's own report, at the SAME build, schema and
//     document, in which every case is either proven the normal way or
//     refused as vacuous_empty_legs -- refused AFTER admission, so both
//     legs answered 2xx, error-free, with the root present, and both were
//     empty -- and at least one is vacuous; and
//  2. a venue receipt that proves EVERY declared case at that same build,
//     schema and document, as class 1 does, without needing an admin.
//
// Any production case with another refusal, an error, a transport failure,
// a mismatch, or a missing case is ineligible: nothing but "both planes
// answered, both empty" can stand for "production has no data". The
// operation list below only NARROWS: the computed check is the gate.
//
// Named limits: both files are operator-supplied and not authenticated
// (same as class 1); the production report covers the one org its run was
// made for, so "no data in any org" is the operator's reading of one org's
// answer, recorded by the report's org_id in the evidence digest.

import (
	"errors"
	"fmt"
)

// noProdDataOperations is the narrowing list.
var noProdDataOperations = map[string]bool{
	"savedReports": true,
	"savedReport":  true,
	"reportRuns":   true,
}

// NoProdDataEligible reports whether operation may be admitted by class 2.
func NoProdDataEligible(operation string) bool { return noProdDataOperations[operation] }

// NoProdDataAdmit decides class 2 for operation; nil admits.
func NoProdDataAdmit(production, venue *VenueReceipt, operation, schemaDigest, documentDigest, runningBuild, targetMode string) error {
	switch {
	case !NoProdDataEligible(operation):
		return errors.New("operation is not on the no-production-data list")
	case production == nil:
		return errors.New("no production report")
	case venue == nil:
		return errors.New("no venue receipt")
	case production.Venue != nil:
		return errors.New("the production report carries a venue stamp: it is a venue run, not production's own")
	case documentDigest == "":
		return errors.New("no live document digest for the operation")
	}
	if err := reportIsWholeRunAt(production, schemaDigest, runningBuild); err != nil {
		return fmt.Errorf("production report: %w", err)
	}
	if err := venueReceiptProves(venue, operation, schemaDigest, documentDigest, runningBuild, targetMode); err != nil {
		return fmt.Errorf("venue receipt: %w", err)
	}
	needed, err := declaredVariants(operation)
	if err != nil {
		return err
	}
	vacuous := 0
	for _, outcome := range production.Outcomes {
		if outcome.Operation != operation {
			continue
		}
		if _, known := needed[outcome.Variant]; !known {
			return fmt.Errorf("production report names variant %q the operation does not declare", outcome.Variant)
		}
		switch {
		case outcome.DocumentDigest != documentDigest:
			return fmt.Errorf("production report (variant %q): document digest is not the live one", outcome.Variant)
		case outcome.RefusalReason == RefusalVacuousEmptyLegs && outcome.Admitted && !outcome.Executed:
			vacuous++
		case venueOutcomeRefusal(outcome, documentDigest, targetMode) == "":
			// Proven the normal way: production has data for this case.
		default:
			return fmt.Errorf("production report (variant %q): neither proven nor refused as vacuous on both legs (refusal %q, terminal state %q)", outcome.Variant, outcome.RefusalReason, outcome.TerminalState)
		}
		needed[outcome.Variant] = true
	}
	for name, seen := range needed {
		if !seen {
			return fmt.Errorf("production report has no outcome for variant %q", name)
		}
	}
	if vacuous == 0 {
		return errors.New("production report has no vacuous case: production has data for every case, so the ordinary proof applies")
	}
	return nil
}
