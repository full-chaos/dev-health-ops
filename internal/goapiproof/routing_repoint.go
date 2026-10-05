package goapiproof

// `repoint` points the MCP class decisions at the running build. It is a provenance correction -- "this decision is
// about the build that is actually running" -- and never writes `mode`: reachability stays with enable/disable.
// The proof harness refuses a run while a decision names another build (VerifyCandidateBuild), so a redeploy is
// followed by a repoint. A decision is digest-agnostic (go_api_class_decision), so a schema move needs no repoint.
// A catalog operation has no routing state (ErrDocumentOperationNotRouted).

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrRepointBuildMismatch is returned when the caller asks to re-point at
// a build that is not the one the deployed process reports.
//
// The check lives here as well as in the command because the whole point
// of the receipt chain is that a build identity is READ from the running
// process, never typed: a re-point to a hand-supplied sha would recreate
// exactly the "somebody typed a sha" provenance the proof runner refuses.
var ErrRepointBuildMismatch = errors.New("goapiproof: re-point build does not match the running build")

// ErrRepointUnknownOperation reports a named class operation with no decision: a filter like `chosen,typo` must not
// re-point `chosen`, drop `typo` without a word, and report success.
var ErrRepointUnknownOperation = errors.New("goapiproof: no class decision for a named operation")

// RepointOutcome is what happened to one operation's row.
type RepointOutcome struct {
	Operation string
	// DocumentDigest is the class digest the decision is recorded under.
	DocumentDigest string
	// Mode is read before the write and re-read after it. The two are
	// compared, so "mode unchanged" is asserted by the code rather than
	// promised by the SQL.
	ModeBefore string
	ModeAfter  string
	BuildFrom  string
	BuildTo    string
	// Changed is false when the row already named the running build.
	// Re-running a re-point is a no-op in effect, and must be able to say
	// so instead of reporting fifteen writes that changed nothing.
	Changed bool
}

// RepointRequest is one invocation.
type RepointRequest struct {
	SchemaDigest string
	// RunningBuild is the build identity read from the deployed
	// process's /buildinfo. It is the value written.
	RunningBuild string
	// ExpectBuild, when non-empty, must equal RunningBuild. It is an
	// operator cross-check, never the source of what is written.
	ExpectBuild string
	// Operations restricts the write; empty means every row at the digest.
	Operations []string
	RecordedBy string
	// ReviewEvidence is why, recorded durably on every row touched.
	ReviewEvidence string
	// PrincipalID is the effective-principal envelope's `sub` -- WHO THE
	// CREDENTIAL SAYS is acting, recorded on the CHAOS-5505 audit row.
	// The build this verb writes is READ from the authenticated
	// /buildinfo, which VERIFIES that envelope, so the row can and must
	// name the subject the verified credential carried.
	PrincipalID string
	// DryRun evaluates and reports without writing.
	DryRun bool
}

func (r RepointRequest) validate() error {
	switch {
	case r.SchemaDigest == "":
		return errors.New("goapiproof: schema digest is required")
	case r.RunningBuild == "":
		return errors.New("goapiproof: running build is required and must come from /buildinfo")
	case r.RecordedBy == "":
		return errors.New("goapiproof: recorded-by is required")
	case r.ReviewEvidence == "":
		return errors.New("goapiproof: review-evidence is required: a provenance write is still a decision")
	case r.ExpectBuild != "" && r.ExpectBuild != r.RunningBuild:
		// Checked BEFORE the principal id: a cross-check mismatch is the
		// more specific fact, and an operator who typed the wrong sha
		// should hear that rather than a message about an audit column.
		return fmt.Errorf("%w: cross-check %q, running %q", ErrRepointBuildMismatch, r.ExpectBuild, r.RunningBuild)
	case r.PrincipalID == "" && !r.DryRun:
		return errors.New("goapiproof: principal id is required: this verb reads the authenticated /buildinfo, so the envelope it presented was verified and the audit row must name the subject that credential carried")
	}
	return nil
}

// Repoint points every (or each named) class decision at the running build, in ONE transaction.
func Repoint(ctx context.Context, pool *pgxpool.Pool, request RepointRequest) ([]RepointOutcome, error) {
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
		return nil, refuseDocumentOperations("repoint", document)
	}
	return repointClassDecisions(ctx, pool, request, class)
}

// RepointSummary counts what a run did, including the zeros.
type RepointSummary struct {
	Total     int
	Changed   int
	Unchanged int
}

// Summarize counts outcomes. Every counter is reported even at zero, so
// "nothing needed changing" and "nothing was looked at" cannot read alike.
func Summarize(outcomes []RepointOutcome) RepointSummary {
	summary := RepointSummary{Total: len(outcomes)}
	for _, outcome := range outcomes {
		if outcome.Changed {
			summary.Changed++
			continue
		}
		summary.Unchanged++
	}
	return summary
}
