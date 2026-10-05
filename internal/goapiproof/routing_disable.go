package goapiproof

// `disable` turns an MCP class root off: it sets a non-serving mode on the root's go_api_class_decision row and
// never writes the candidate build (-candidate-build is a guard: refuse if the decision was repointed since the
// operator looked). It never contacts query-api, so it works when the deployed process is down, and it never
// inserts: a root with no decision is reported as nothing to disable. A catalog operation has no routing state
// (ErrDocumentOperationNotRouted).

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DisableModes are the only modes `disable` may set. All three keep a class root dark to a real client:
// python (the safe default), disabled (a deliberate decision, readable as "turned off" rather than "never on"),
// and shadow (measurement only). canary/primary are `enable`'s job, and it has preflights this path does not.
var DisableModes = []string{"python", "disabled", "shadow"}

// ErrDisableGuardMismatch reports that a row points at a different
// candidate build than the -candidate-build guard named -- somebody
// repointed it since the operator looked.
var ErrDisableGuardMismatch = errors.New("goapiproof: a routing row points at a different candidate build than the guard named")

// ErrDisableRefusesEnablingMode is returned when a caller asks this verb
// to set a REACHABLE mode.
//
// Enforced at the write and not only at the flag parse: an invariant
// checked only by the caller is an invariant the next caller breaks. The
// Python off-ramp learned this the hard way: a hand-built
// ModeChange(new_mode="primary") reached apply_disable and turned routing
// ON -- the one thing an off-ramp must never be able to do.
var ErrDisableRefusesEnablingMode = errors.New("goapiproof: disable may only set an unreachable mode")

// DisableChange is one row `disable` would change, or did.
type DisableChange struct {
	Operation      string
	DocumentDigest string
	// CurrentMode is empty when no row exists at the live digest --
	// reported as "nothing to disable", never invented.
	CurrentMode string
	NewMode     string
	// CandidateBuild is what the row points at. Reported, NEVER written.
	CandidateBuild string
	Applied        bool
}

// IsNoop is true when there is no row, or the row is already in the
// requested mode.
func (c DisableChange) IsNoop() bool {
	return c.CurrentMode == "" || c.CurrentMode == c.NewMode
}

// DisableRequest is one invocation.
type DisableRequest struct {
	SchemaDigest string
	// Operations is the resolved, catalog-validated list.
	Operations []string
	// DocumentDigest maps operation -> the catalog's document digest.
	// `disable` cannot ask the deployed process for it (it must work when
	// that process is down), so the checked-in catalog is the source --
	// and a row whose document digest has drifted from the catalog is
	// simply not matched, which is correct: the edge could not dispatch
	// to it either.
	DocumentDigest map[string]string
	NewMode        string
	// ExpectedCandidateBuild, when non-empty, is a guard: a row pointing
	// somewhere else is refused. Never written.
	ExpectedCandidateBuild string
	RecordedBy             string
	ReviewEvidence         string

	// Apply writes. Without it nothing is written and the plan is
	// returned for the operator to read.
	Apply bool
}

func (r DisableRequest) validate() error {
	switch {
	case r.SchemaDigest == "":
		return errors.New("goapiproof: schema digest is required")
	case len(r.Operations) == 0:
		return errors.New("goapiproof: no operations selected")
	}
	if !contains(DisableModes, r.NewMode) {
		return fmt.Errorf("%w: %v, got %q", ErrDisableRefusesEnablingMode, DisableModes, r.NewMode)
	}
	if r.Apply {
		if r.RecordedBy == "" {
			return errors.New("goapiproof: recorded-by is required to apply")
		}
		if r.ReviewEvidence == "" {
			return errors.New("goapiproof: review-evidence is required to apply: a mode change is a decision, and a decision with no durable reason is unreadable weeks later")
		}
	}
	return nil
}

// Disable plans and (with Apply) writes the mode changes, in ONE
// transaction.
//
// Returns the FULL plan -- including the rows it will not touch -- so an
// operator sees "three of the five you named have no row" rather than a
// count that quietly omits them.
func Disable(ctx context.Context, pool *pgxpool.Pool, request DisableRequest) ([]DisableChange, error) {
	if pool == nil {
		return nil, errors.New("goapiproof: nil pool")
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	class, document := splitClassOperations(request.Operations)
	if len(document) > 0 {
		if len(class) > 0 {
			return nil, errMixedClassAndDocument
		}
		return nil, refuseDocumentOperations("disable", document)
	}
	return disableClassDecisions(ctx, pool, request)
}

// DisableSummary counts what a plan or an apply did, including the zeros.
type DisableSummary struct {
	Total int
	// Actionable is the number of rows that exist AND are not already in
	// the requested mode.
	Actionable int
	Applied    int
	// NoRow is how many named operations have no row at the live digest.
	// Reported separately from "already in that mode" because they are
	// different facts an operator acts on differently.
	NoRow int
}

// SummarizeDisable counts changes. Every counter is reported even at
// zero, so "nothing needed changing" and "nothing was looked at" cannot
// read alike.
func SummarizeDisable(changes []DisableChange) DisableSummary {
	summary := DisableSummary{Total: len(changes)}
	for _, change := range changes {
		if change.CurrentMode == "" {
			summary.NoRow++
		}
		if !change.IsNoop() {
			summary.Actionable++
		}
		if change.Applied {
			summary.Applied++
		}
	}
	return summary
}
