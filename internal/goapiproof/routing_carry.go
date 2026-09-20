package goapiproof

// CHAOS-6107: the pre-roll digest carry.
//
// WHY THIS EXISTS. Every go_api_routing_state row is keyed by
// schema_digest, and both planes compute that digest from the SDL THEIR
// OWN image carries. So an SDL change does not merely invalidate the
// rows -- at the instant the first new pod starts, every enabled
// operation is un-routed: requests fall back to Python, and an operation
// whose Python execution path was deleted (goserved_ledger.json) answers
// users the deletion error instead. That is the 2026-09-01 failure's
// shape, and the existing verbs cannot prevent it: `enable` writes only
// the digest a LIVE process reports, so no row can exist at the new
// digest until the new image is already serving -- which is after the
// harm.
//
// `carry` is the missing half. It runs BEFORE the roll, from a tools
// image built at the sha about to roll, and copies every reachable row
// from the live schema digest to the digest of the SDL compiled into
// THIS binary. The rows it writes are inert until a process that
// computes the target digest starts, and the rows it copies FROM are
// never touched -- so a rolling update finds rows at whichever digest
// each pod computes, and a rollback finds its own rows exactly where it
// left them.
//
// WHAT IT DELIBERATELY DOES NOT DO.
//
//   - It does not decide reachability. mode, rollout_percentage,
//     eligible_orgs and owner are copied verbatim; a carry that
//     normalised any of them would be an enablement wearing a
//     preservation's name.
//   - It does not re-point. current_candidate_build is copied, not
//     re-read, so the carried row makes the same provenance claim the
//     source row makes. `repoint` remains the AFTER-the-roll verb that
//     corrects that claim once the new process runs, and this verb
//     refuses outright when a row being carried names a build the
//     deployed process is not running: copying a claim that is already
//     stale is worse than not carrying it.
//   - It claims NO proof. A receipt is keyed by schema_digest
//     (receipt.go), so nothing proven at the live digest transfers, and
//     `status` reports every carried row UNPROVEN from the moment it
//     exists -- correctly: that proof ran against the old SDL on an older
//     build. Re-proving is go-api-prove's job, after the roll. Proof is
//     not a dispatch input on either plane (go_api_dispatcher.py's
//     dispatch reads mode alone; routeswitch's PostgresSwitch selects
//     mode alone), so an UNPROVEN carried row serves Go immediately on a
//     new pod -- which is the whole point, and why the verb records what
//     it did so plainly.
//   - It does not delete, disable or rewrite anything at the live
//     digest, and it never overwrites a row that already exists at the
//     target digest.
//
// WHAT AN AUDIT ROW SAYS. alembic 0130's CHECK admits exactly
// enable|disable|repoint for `action`, so a carried row is audited as an
// `enable` -- which is what it is: an enablement at a new key, with the
// mode and build the operator already decided. The
// CarriedEvidencePrefix on review_evidence is what distinguishes the two
// for a reader of that table; a dedicated action value needs a migration
// and is tracked separately.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CarriedEvidencePrefix opens the review_evidence of every carried row.
//
// Durable on the ROW, for the reason UnprovenEvidencePrefix is: it is
// the only thing telling a reader -- of the row, or of the audit table
// where the action reads `enable` -- that this row is not a fresh
// enablement decision but a copy of one taken at another digest. A log
// line written at the moment it happened is unreadable six weeks later,
// which is the failure this whole surface exists to end.
const CarriedEvidencePrefix = "CARRIED-FROM "

// CarriedEvidence renders what a carried row records.
//
// The source row's own evidence is kept INTACT behind the prefix,
// including an ACKNOWLEDGED-UNPROVEN prefix of its own: a row enabled
// without proof must still say so at the new digest. Nothing is
// truncated -- an over-long result is refused rather than trimmed,
// because the audit table is append-only and a silently trimmed reason
// has no later remedy.
func CarriedEvidence(liveSchemaDigest, build string, at time.Time, source string) string {
	return fmt.Sprintf("%s%s build=%s at=%s: %s",
		CarriedEvidencePrefix, liveSchemaDigest, build, at.UTC().Format(time.RFC3339), source)
}

// The outcome vocabulary. Every row at the live digest gets exactly one
// of these and ALL of them are reported -- a skipped row is named with
// its reason rather than silently missing from the output, which is how
// a dead enablement stays invisible.
const (
	// CarryActionCarry: the row was (or would be) written at the target
	// digest.
	CarryActionCarry = "CARRY"
	// CarryActionUnchanged: an identical row is already there, so a rerun
	// reports it and writes nothing.
	CarryActionUnchanged = "UNCHANGED"
	// CarryActionSkip: the row carries no reachability to preserve, or is
	// already unreachable at the live digest. Reported, never fatal.
	CarryActionSkip = "SKIP"
	// CarryActionRefuse: the row is reachable NOW and cannot be carried
	// faithfully. One of these refuses the whole run.
	CarryActionRefuse = "REFUSE"
)

// ErrCarryRequestRefused classifies every validate() refusal, the way
// ErrEnableRequestRefused does for `enable`: a classification for
// errors.Is, never a sentence to print.
var ErrCarryRequestRefused = errors.New("goapiproof: carry refuses this request")

type carryRefusal struct{ err error }

func (e carryRefusal) Error() string   { return e.err.Error() }
func (e carryRefusal) Unwrap() []error { return []error{e.err, ErrCarryRequestRefused} }

// ErrCarryDigestUnchanged reports that this binary's SDL digest is the
// one the deployed process already computes.
//
// A refusal rather than an empty success: with both digests equal a
// "carry" would be an `enable` with none of `enable`'s preflights,
// writing rows whose proof and registry agreement nobody checked. There
// is also nothing to preserve -- the rows already sit at the digest the
// fleet computes.
var ErrCarryDigestUnchanged = errors.New("goapiproof: the target schema digest is the live one, so there is nothing to carry")

// ErrCarryNoLiveRows reports that no row exists at the live digest.
// "The table is empty" and "every row died" must not read alike -- that
// is the whole lesson of CHAOS-5416.
var ErrCarryNoLiveRows = errors.New("goapiproof: no routing row exists at the live schema digest")

// ErrCarryNothingReachable reports that rows exist at the live digest but
// none is reachable, so the roll would un-route nothing.
//
// Still a refusal: an operator runs this verb believing operations are
// being served, and "nothing is on" is an answer they must read, not
// infer from a run that reported success over an empty list.
var ErrCarryNothingReachable = errors.New("goapiproof: no reachable row at the live schema digest")

// ErrCarryDocumentMoved reports operations whose registered document is
// changed or absent in the image this binary was built from.
//
// The verb rests on the document being the SAME text at both digests:
// the row's key includes document_digest, and the edge looks a row up by
// the digest of the request text it received. A carried row under a
// digest the new image does not serve is a dead row written on purpose,
// so the operation is refused BY NAME and the run stops.
var ErrCarryDocumentMoved = errors.New("goapiproof: the registered document changed or is absent at the target schema digest")

// ErrCarryImageDisagrees reports that the two artifacts this image
// carries -- the registered-document dump built from its own
// query_route.go, and the checked-in edge catalog -- do not give an
// operation the same document digest.
//
// Reachability after the roll is a conjunction of both planes, so this
// image cannot serve that operation end to end whichever of the two a
// carry believed. Refusing names the artifact to regenerate instead of
// silently picking one.
var ErrCarryImageDisagrees = errors.New("goapiproof: this image's registered documents and edge catalog disagree")

// ErrCarryBuildNotRunning reports that a row being carried names a build
// the deployed process is not running. The fix is the verb that exists
// for it: run `repoint` first, then carry.
var ErrCarryBuildNotRunning = errors.New("goapiproof: a routing row names a build the deployed process is not running -- run `repoint` first")

// ErrCarryTargetRowExists reports that the target digest already holds a
// row for this operation that differs from the one being carried.
//
// Somebody decided something at the target digest. A carry that
// overwrote it would silently revert that decision -- and `carry` is the
// one verb with no operator intent of its own to justify doing so.
var ErrCarryTargetRowExists = errors.New("goapiproof: a different routing row already exists at the target schema digest")

// ErrCarryUnknownOperation reports an -operations name with no row at the
// live digest, matching `repoint`'s refusal of the same shape.
var ErrCarryUnknownOperation = errors.New("goapiproof: no routing row at the live schema digest for a named operation")

// ErrCarrySourceRowChanged reports that a row at the LIVE schema digest
// -- one this run was copying -- was changed or removed by somebody else
// after this run read it and before it could commit.
//
// Its own sentinel, not folded into ErrCarryRacedAnotherWriter, because
// the two name opposite ends of the copy and an operator does different
// things about them: a changed TARGET row means somebody decided
// something at the digest about to roll, and is resolved there; a
// changed SOURCE row means the decision being copied is no longer the
// standing one, and is resolved by simply running `carry` again, which
// then copies what the operator actually wants now.
var ErrCarrySourceRowChanged = errors.New("goapiproof: a routing row at the live schema digest changed while this carry was preparing")

// ErrCarryRacedAnotherWriter reports that a row appeared at the target
// digest between this verb's survey and its write, and then could not be
// read back. Nothing is written.
var ErrCarryRacedAnotherWriter = errors.New("goapiproof: a routing row at the target schema digest changed while this carry was preparing")

// CarryRow is one routing row, read wide enough to be copied.
//
// EligibleOrgs is the column's text, or nil for SQL NULL, and the
// distinction is load-bearing: NULL ("no restriction recorded") and an
// empty JSON container are different recorded intents, and a copy that
// collapsed them would rewrite one as the other.
type CarryRow struct {
	Operation         string
	DocumentDigest    string
	Mode              string
	Build             string
	Owner             string
	RolloutPercentage int
	EligibleOrgs      *string
	ReviewEvidence    string
}

// sameCarriedState reports whether two rows agree on every column a
// carry copies. review_evidence and updated_at are deliberately NOT
// compared: a carried row's evidence carries CarriedEvidencePrefix by
// design, so comparing it would make every rerun look like a conflict.
func sameCarriedState(a, b CarryRow) bool {
	if a.DocumentDigest != b.DocumentDigest || a.Mode != b.Mode || a.Build != b.Build ||
		a.Owner != b.Owner || a.RolloutPercentage != b.RolloutPercentage {
		return false
	}
	switch {
	case a.EligibleOrgs == nil && b.EligibleOrgs == nil:
		return true
	case a.EligibleOrgs == nil || b.EligibleOrgs == nil:
		return false
	default:
		return *a.EligibleOrgs == *b.EligibleOrgs
	}
}

// CarryInputs are the facts OUTSIDE the row that decide its outcome,
// gathered by the caller from the deployed process and this image's own
// artifacts.
type CarryInputs struct {
	// LiveDocumentDigest is what the DEPLOYED process registers, read
	// from its /registry. It decides what is reachable RIGHT NOW: a row
	// whose document digest is not what the running binary serves is
	// already dead, whatever its mode says.
	LiveDocumentDigest map[string]string
	// TargetDocumentDigest is what the image this binary was built from
	// registers, computed over its own registered-document dump with the
	// same function the running process uses.
	TargetDocumentDigest map[string]string
	// CatalogDocumentDigest is what THIS image's edge catalog carries --
	// the other half of reachability after the roll, owned by the Python
	// plane.
	CatalogDocumentDigest map[string]string
	// TargetRows are the rows already at the target digest, keyed by
	// OPERATION rather than by full row identity: a row for this
	// operation at the target digest under another document digest is not
	// a row to carry alongside, it is a second row of which at most one
	// can ever be looked up.
	TargetRows map[string]CarryRow
}

// CarryOutcome is what happened, or would happen, to one live row.
type CarryOutcome struct {
	Operation      string
	DocumentDigest string
	Action         string
	// Reason is filled for SKIP and REFUSE and says which fact decided
	// it, in words an operator can act on.
	Reason string
	// Refusal is the sentinel a REFUSE outcome carries, so a caller can
	// tell "the documents moved" (stop and look at the change) from
	// "this image's own artifacts disagree" (regenerate the catalog)
	// with errors.Is rather than by reading English.
	Refusal error
	// Mode/RolloutPercentage/Build/Owner/EligibleOrgs are the values
	// being preserved, echoed so the report SHOWS what was carried rather
	// than asserting that something was.
	Mode              string
	RolloutPercentage int
	Build             string
	Owner             string
	EligibleOrgs      *string
	// ReviewEvidence is what was (or would be) written, prefix included.
	ReviewEvidence string
}

// DecideCarry is the whole decision for ONE row, as a pure function of
// the row and the facts around it.
//
// Pure and exported so the decision can be enumerated over its entire
// input domain without a database: the axes are the mode vocabulary, the
// row's live reachability, the document's state at the target digest and
// what is already at the target digest. A decision reachable only
// through a Postgres fixture is one nobody enumerates.
//
// The order of the checks is the order of the FACTS: what is reachable
// now, then what the new image serves, then what is already at the
// target digest. Each returns the most specific true statement about the
// row.
func DecideCarry(row CarryRow, inputs CarryInputs) CarryOutcome {
	outcome := CarryOutcome{
		Operation:         row.Operation,
		DocumentDigest:    row.DocumentDigest,
		Mode:              row.Mode,
		RolloutPercentage: row.RolloutPercentage,
		Build:             row.Build,
		Owner:             row.Owner,
		EligibleOrgs:      row.EligibleOrgs,
	}
	skip := func(format string, args ...any) CarryOutcome {
		outcome.Action, outcome.Reason = CarryActionSkip, fmt.Sprintf(format, args...)
		return outcome
	}
	refuse := func(sentinel error, format string, args ...any) CarryOutcome {
		outcome.Action, outcome.Reason, outcome.Refusal = CarryActionRefuse, fmt.Sprintf(format, args...), sentinel
		return outcome
	}

	// Reachability is the mode vocabulary BOTH planes agree on
	// (go_api_dispatcher's _REACHABLE_MODES, routeswitch's reachable
	// set). python and disabled are the safe default a missing row
	// already gives; shadow is not served to a client by either plane. So
	// none of the three has a reachability to preserve, and writing a row
	// for one would be this verb inventing an enablement nobody decided.
	if row.Mode != TargetModeCanary && row.Mode != TargetModePrimary {
		return skip("mode=%s is not served to a client by either plane, so the roll un-routes nothing here", row.Mode)
	}
	live, registered := inputs.LiveDocumentDigest[row.Operation]
	if !registered {
		return skip("the deployed process does not register this operation, so this row is already unreachable")
	}
	if live != row.DocumentDigest {
		// The dead-row shape `status` reports as an unreachable document
		// digest: present in psql, never looked up. Carrying it would
		// manufacture a second dead row at the target digest.
		return skip("this row's document digest is not the one the deployed process registers (%s), so the row is already unreachable", live)
	}

	target, serves := inputs.TargetDocumentDigest[row.Operation]
	if !serves {
		return refuse(ErrCarryDocumentMoved, "the image this binary was built from does not register this operation at all")
	}
	if target != row.DocumentDigest {
		return refuse(ErrCarryDocumentMoved, "the registered document changed: this row is keyed to %s, the target image registers %s", row.DocumentDigest, target)
	}
	catalog, inCatalog := inputs.CatalogDocumentDigest[row.Operation]
	if !inCatalog {
		return refuse(ErrCarryImageDisagrees, "this image's edge catalog does not carry this operation, so the edge could not dispatch it after the roll")
	}
	if catalog != target {
		return refuse(ErrCarryImageDisagrees, "this image's edge catalog says %s and its registered documents say %s -- regenerate the catalog with scripts/go_api/generate_operation_catalog.py against this revision", catalog, target)
	}

	existing, present := inputs.TargetRows[row.Operation]
	switch {
	case !present:
		outcome.Action = CarryActionCarry
		return outcome
	case sameCarriedState(row, existing):
		outcome.Action = CarryActionUnchanged
		return outcome
	default:
		return refuse(ErrCarryTargetRowExists,
			"the target digest already holds a row for this operation (document %s, mode %s, rollout %d, build %s) that differs from the one being carried -- resolve it there first",
			existing.DocumentDigest, existing.Mode, existing.RolloutPercentage, existing.Build)
	}
}

// CarryRequest is one invocation.
type CarryRequest struct {
	// LiveSchemaDigest is what the DEPLOYED process reported from its
	// /registry, never an operator-supplied value: the rows worth
	// preserving are the ones the running fleet actually reads.
	LiveSchemaDigest string
	// TargetSchemaDigest is the digest of the SDL compiled into the
	// binary running this call. It is the one value here that cannot be
	// wrong by configuration -- go:embed makes it a function of the
	// build.
	TargetSchemaDigest string
	// RunningBuild is what the deployed process's /buildinfo reported. It
	// is NOT written: it is the guard that every row being carried
	// already names the build that is actually running.
	RunningBuild string
	// ExpectBuild, when non-empty, must equal RunningBuild. An operator
	// cross-check that can only FAIL a run.
	ExpectBuild string

	Inputs CarryInputs

	// Operations restricts the run; empty means every row at the live
	// digest.
	Operations []string

	RecordedBy     string
	ReviewEvidence string
	// PrincipalID is the effective-principal envelope's subject. Required
	// for the reason `enable` requires it: this verb reads the
	// authenticated /buildinfo, so the envelope it presented was verified
	// by the process that owns that check, and the audit row can name the
	// subject that credential carried.
	PrincipalID string

	DryRun bool
}

func (r CarryRequest) validate() error {
	if err := r.validateFields(); err != nil {
		return carryRefusal{err}
	}
	return nil
}

func (r CarryRequest) validateFields() error {
	switch {
	case r.LiveSchemaDigest == "":
		return errors.New("goapiproof: the live schema digest is required and must come from the deployed process's /registry")
	case r.TargetSchemaDigest == "":
		return errors.New("goapiproof: the target schema digest is required and is computed from this binary's embedded SDL")
	case r.LiveSchemaDigest == r.TargetSchemaDigest:
		return fmt.Errorf("%w: both are %s -- build this binary from the commit about to roll, or use `enable`",
			ErrCarryDigestUnchanged, r.LiveSchemaDigest)
	case r.RunningBuild == "":
		return errors.New("goapiproof: the running build is required and must come from /buildinfo, never a flag")
	case r.ExpectBuild != "" && r.ExpectBuild != r.RunningBuild:
		return fmt.Errorf("goapiproof: cross-check build %q does not match the running build %q -- the flag is a cross-check, never the source", r.ExpectBuild, r.RunningBuild)
	case r.RecordedBy == "":
		return errors.New("goapiproof: recorded-by is required")
	case len(r.RecordedBy) > auditRecordedByMax:
		return fmt.Errorf("goapiproof: recorded-by is %d characters, the audit column holds %d -- refusing rather than truncating an identity in an append-only table", len(r.RecordedBy), auditRecordedByMax)
	case r.ReviewEvidence == "":
		return errors.New("goapiproof: review-evidence is required: carrying a rollout across a schema change is a decision, and a decision with no durable reason is unreadable weeks later")
	case len(r.ReviewEvidence) > auditReviewEvidenceMax:
		return fmt.Errorf("goapiproof: review-evidence is %d characters, the audit column holds %d -- shorten it rather than have it silently trimmed in an append-only table",
			len(r.ReviewEvidence), auditReviewEvidenceMax)
	case r.PrincipalID == "":
		return errors.New("goapiproof: principal id is required: `carry` reads the authenticated /buildinfo, so the envelope it presented was verified and the audit row must name the subject that credential carried")
	case len(r.Inputs.LiveDocumentDigest) == 0:
		return errors.New("goapiproof: the deployed process registers no operations -- there is nothing reachable to preserve")
	case len(r.Inputs.TargetDocumentDigest) == 0:
		return errors.New("goapiproof: this image registers no documents -- the registered-document dump is empty or was not read")
	case len(r.Inputs.CatalogDocumentDigest) == 0:
		return errors.New("goapiproof: the edge catalog is empty -- nothing could be dispatched to Go after the roll")
	}
	return nil
}

// carryRoutingRowSQL writes ONE row at the target digest and never
// touches an existing one: ON CONFLICT DO NOTHING, never DO UPDATE. A
// conflict means something is already there, which is a decision this
// verb must not overwrite -- the caller re-reads the conflicting row and
// either reports it UNCHANGED or refuses by name.
//
// owner, mode, eligible_orgs and rollout_percentage are written from the
// SOURCE ROW's own values, not from constants: each is a recorded
// decision, and a preserving verb that substituted its own default for
// any of them would change what the operator decided while claiming to
// preserve it. The json cast keeps eligible_orgs the type it was read
// as, NULL included.
const carryRoutingRowSQL = `
INSERT INTO public.go_api_routing_state
	(schema_digest, document_digest, selected_operation, current_candidate_build,
	 owner, mode, eligible_orgs, rollout_percentage, review_evidence, recorded_by, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7::json, $8, $9, $10, $11)
ON CONFLICT (schema_digest, document_digest, selected_operation) DO NOTHING`

// selectCarryTargetRowSQL re-reads ONE target row under a lock, after a
// conflicting insert says it exists. Scoped to the row's full identity,
// so there is no ambiguity for an ORDER BY to resolve.
const selectCarryTargetRowSQL = `
SELECT ` + carryRowColumns + `
  FROM public.go_api_routing_state
 WHERE schema_digest = $1 AND document_digest = $2 AND selected_operation = $3
   FOR UPDATE`

// Carry copies every reachable routing row from the live schema digest to
// the target one, in ONE transaction.
//
// ALL OR NOTHING. A partial carry is the state this verb exists to
// prevent, half-shaped: some operations routed at the new digest and
// some not is exactly what the roll would have produced on its own.
//
// THE LOCK ORDER is the package's one rule, obeyed by construction: the
// candidate build is registered BEFORE any routing-row lock is taken, and
// rows are visited in (selected_operation, document_digest) order. Both
// surveys are UNLOCKED reads -- they only decide what to register -- and
// the only routing-row lock this verb takes is the one its own INSERT
// takes, after that row's registration. Nothing here can hold a
// routing-row lock while waiting for a candidate build.
func Carry(ctx context.Context, pool *pgxpool.Pool, request CarryRequest) ([]CarryOutcome, error) {
	if pool == nil {
		return nil, errors.New("goapiproof: nil pool")
	}
	if err := request.validate(); err != nil {
		return nil, err
	}

	wanted := map[string]bool{}
	for _, operation := range request.Operations {
		wanted[operation] = true
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	liveRows, err := readCarryRows(ctx, tx, surveyCarryRowsSQL, request.LiveSchemaDigest)
	if err != nil {
		return nil, err
	}
	if len(liveRows) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrCarryNoLiveRows, request.LiveSchemaDigest)
	}
	if len(wanted) > 0 {
		present := map[string]bool{}
		for _, row := range liveRows {
			present[row.Operation] = true
		}
		var missing []string
		for operation := range wanted {
			if !present[operation] {
				missing = append(missing, operation)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			return nil, fmt.Errorf("%w: %v at %s -- carrying the rest and saying nothing about these is how an operator learns too late that half a rollout moved",
				ErrCarryUnknownOperation, missing, request.LiveSchemaDigest)
		}
	}

	targetRows, err := readCarryRows(ctx, tx, surveyCarryRowsSQL, request.TargetSchemaDigest)
	if err != nil {
		return nil, err
	}
	inputs := request.Inputs
	inputs.TargetRows = make(map[string]CarryRow, len(targetRows))
	for _, row := range targetRows {
		// First wins, over a read whose order is total, so this is
		// deterministic: a SECOND row for the same operation at the
		// target digest is itself a difference DecideCarry refuses on,
		// because at most one of the two can ever be looked up.
		if _, seen := inputs.TargetRows[row.Operation]; !seen {
			inputs.TargetRows[row.Operation] = row
		}
	}

	now := time.Now().UTC()
	outcomes := make([]CarryOutcome, 0, len(liveRows))
	var refusals []CarryOutcome
	var staleBuilds []string
	for _, row := range liveRows {
		if len(wanted) > 0 && !wanted[row.Operation] {
			continue
		}
		outcome := DecideCarry(row, inputs)
		switch outcome.Action {
		case CarryActionCarry, CarryActionUnchanged:
			outcome.ReviewEvidence = CarriedEvidence(request.LiveSchemaDigest, row.Build, now, row.ReviewEvidence)
			// The build guard applies only to rows whose provenance is
			// actually being copied. Refusing a whole run over a SKIPPED
			// row's stale build would block a legitimate carry for a row
			// nothing will ever read.
			if row.Build != request.RunningBuild {
				staleBuilds = append(staleBuilds, fmt.Sprintf("%s names %s", row.Operation, row.Build))
			}
		case CarryActionRefuse:
			refusals = append(refusals, outcome)
		}
		outcomes = append(outcomes, outcome)
	}
	sortCarryOutcomes(outcomes)

	if len(refusals) > 0 {
		return outcomes, carryRefusalError(refusals)
	}
	if len(staleBuilds) > 0 {
		sort.Strings(staleBuilds)
		return outcomes, fmt.Errorf("%w: the deployed process runs %s, but %s.\n"+
			"  `carry` copies provenance rather than re-reading it, so a stale build would be copied to the new digest and outlive the roll that made it wrong",
			ErrCarryBuildNotRunning, request.RunningBuild, strings.Join(staleBuilds, "; "))
	}

	summary := SummarizeCarry(outcomes)
	if summary.Carried == 0 && summary.Unchanged == 0 {
		return outcomes, fmt.Errorf("%w: %d row(s) exist at %s and every one was skipped -- each outcome names why",
			ErrCarryNothingReachable, len(liveRows), request.LiveSchemaDigest)
	}
	if request.DryRun {
		return outcomes, nil
	}

	audit := RoutingAudit{
		// alembic 0130's CHECK admits enable|disable|repoint only, and a
		// carried row IS an enablement at a new key: same mode, same
		// rollout, same build, decided earlier. CarriedEvidencePrefix on
		// review_evidence is what tells a reader of this table which of
		// the two it is looking at.
		Action:          AuditActionEnable,
		CredentialClass: CredentialClassEnvelope,
		PrincipalID:     request.PrincipalID,
		RecordedBy:      request.RecordedBy,
		ReviewEvidence:  request.ReviewEvidence,
		SchemaDigest:    request.TargetSchemaDigest,
	}
	for index := range outcomes {
		outcome := &outcomes[index]
		if outcome.Action != CarryActionCarry {
			continue
		}
		wrote, err := carryOneRow(ctx, tx, request.TargetSchemaDigest, request.RecordedBy, now, outcome)
		if err != nil {
			return outcomes, err
		}
		if !wrote {
			continue
		}
		audit.Entries = append(audit.Entries, RoutingAuditEntry{
			DocumentDigest: outcome.DocumentDigest,
			Operation:      outcome.Operation,
			// No row existed at this digest, so both before-values are
			// NULL rather than an invented default -- "there was no row"
			// and "the row said python" are different facts.
			CandidateBuildBefore: nil,
			CandidateBuildAfter:  outcome.Build,
			ModeBefore:           nil,
			ModeAfter:            outcome.Mode,
		})
	}
	// THE SOURCE ROWS ARE RE-READ UNDER A LOCK, and only now.
	//
	// Everything above decided what to copy from an UNLOCKED read, which
	// under READ COMMITTED is a photograph of a moment that has since
	// passed. This pass locks those rows FOR SHARE and compares them to
	// what was actually written; from here to COMMIT they cannot move, so
	// the row committed at the target digest is the decision standing at
	// the live digest at the instant this transaction becomes real --
	// which is the only reading of "preserve" that is worth anything to
	// an operator about to roll.
	//
	// It runs after every write for the lock-order reason
	// lockCarrySourceRowsSQL's own comment gives: nothing is left to
	// register, so no routing-row lock is ever held while waiting for a
	// candidate build.
	//
	// A difference ROLLS THE WHOLE RUN BACK rather than carrying the rest
	// -- the same all-or-nothing rule the rest of this verb obeys. Half a
	// rollout moved is the state carry exists to prevent.
	if err := revalidateCarriedSourceRows(ctx, tx, request.LiveSchemaDigest, liveRows, outcomes); err != nil {
		return outcomes, err
	}

	// A run that carried nothing in the end (every row turned out to be
	// there already) writes no audit row: an audit entry for a write that
	// did not happen is a false entry in a table nothing can correct.
	if len(audit.Entries) > 0 {
		if _, err := writeRoutingAudit(ctx, tx, audit, now); err != nil {
			return outcomes, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return outcomes, fmt.Errorf("goapiproof: commit: %w", err)
	}
	return outcomes, nil
}

// carryOneRow registers the build and writes ONE row, and answers what
// happened to the row that is actually at that key.
//
// Split out of Carry because the branch that matters most here is the one
// hardest to reach from outside: an insert that affects NO row, because
// something arrived at this exact key after the survey read it. Behind
// the full verb that needs a concurrent writer landing inside a
// millisecond-wide window; at this seam a test commits the racing row
// first and calls this directly, so both of its answers -- "identical, so
// this is somebody else's carry of the same decision" and "different, so
// refuse" -- are pinned rather than argued about.
//
// Returns wrote=false when the row was already there and identical; the
// outcome is then rewritten to UNCHANGED, and its caller writes no audit
// entry for a write that did not happen.
func carryOneRow(ctx context.Context, tx pgx.Tx, targetSchemaDigest, recordedBy string, now time.Time, outcome *CarryOutcome) (bool, error) {
	// Candidate build FIRST: the routing row's 4-column foreign key makes
	// it mandatory, and registering before any routing-row lock is the
	// package's shared order. The key includes the SCHEMA digest, so this
	// registers the build the row already names under the NEW digest --
	// an append to an append-only table, never a change to the live
	// digest's own registration.
	if _, err := tx.Exec(ctx, registerCandidateBuildSQL,
		targetSchemaDigest, outcome.DocumentDigest, outcome.Operation, outcome.Build); err != nil {
		return false, fmt.Errorf("goapiproof: register candidate build for %s: %w", outcome.Operation, err)
	}
	tag, err := tx.Exec(ctx, carryRoutingRowSQL,
		targetSchemaDigest, outcome.DocumentDigest, outcome.Operation, outcome.Build,
		outcome.Owner, outcome.Mode, outcome.EligibleOrgs, outcome.RolloutPercentage,
		outcome.ReviewEvidence, recordedBy, now)
	if err != nil {
		return false, fmt.Errorf("goapiproof: carry %s: %w", outcome.Operation, err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	// Read the conflicting row back under a lock and answer on what is
	// actually there -- never on what the survey believed a moment ago.
	existing, readErr := scanCarryRow(tx.QueryRow(ctx, selectCarryTargetRowSQL,
		targetSchemaDigest, outcome.DocumentDigest, outcome.Operation))
	switch {
	case errors.Is(readErr, pgx.ErrNoRows):
		return false, fmt.Errorf("%w: %s conflicted on insert and then could not be read back",
			ErrCarryRacedAnotherWriter, outcome.Operation)
	case readErr != nil:
		return false, readErr
	}
	if !sameCarriedState(carriedRowOf(*outcome), existing) {
		return false, fmt.Errorf("%w: %s appeared at %s while this carry was preparing, with a different state (mode %s, rollout %d, build %s)",
			ErrCarryTargetRowExists, outcome.Operation, targetSchemaDigest,
			existing.Mode, existing.RolloutPercentage, existing.Build)
	}
	outcome.Action = CarryActionUnchanged
	return false, nil
}

// revalidateCarriedSourceRows re-reads the LIVE rows under a share lock
// and refuses if any row this run copied is no longer what was copied.
//
// It compares only the rows whose outcome was CARRY or UNCHANGED: a
// SKIPPED row was never copied, so a concurrent change to it changes
// nothing this transaction claims, and refusing over it would block a
// correct carry for a row nothing at the target digest will ever read.
//
// Both directions are a refusal, and for the same reason: a row whose
// carried columns CHANGED means the target digest would hold a
// superseded decision, and a row that VANISHED means the operator
// removed the decision entirely -- in both cases what is about to commit
// at the target digest is no longer what is standing at the live one.
func revalidateCarriedSourceRows(ctx context.Context, tx pgx.Tx, liveSchemaDigest string, surveyed []CarryRow, outcomes []CarryOutcome) error {
	copied := map[carryRowKey]CarryRow{}
	for _, outcome := range outcomes {
		switch outcome.Action {
		case CarryActionCarry, CarryActionUnchanged:
			copied[carryRowKey{Operation: outcome.Operation, DocumentDigest: outcome.DocumentDigest}] = carriedRowOf(outcome)
		}
	}
	// EVERY surveyed key, not only the copied ones (r2 F2). A row the
	// survey saw and deliberately skipped is accounted for; a row NOBODY
	// saw is the gap. Built even when nothing was copied, because a run
	// that carried nothing has already refused elsewhere and a run that
	// carried something must still be able to tell an old row from a new
	// one.
	seen := map[carryRowKey]bool{}
	for _, row := range surveyed {
		seen[carryRowKey{Operation: row.Operation, DocumentDigest: row.DocumentDigest}] = true
	}
	if len(copied) == 0 {
		return nil
	}
	locked, err := readCarryRows(ctx, tx, lockCarrySourceRowsSQL, liveSchemaDigest)
	if err != nil {
		return err
	}
	present := map[carryRowKey]CarryRow{}
	for _, row := range locked {
		present[carryRowKey{Operation: row.Operation, DocumentDigest: row.DocumentDigest}] = row
	}
	var changed []string
	for key, was := range copied {
		now, ok := present[key]
		switch {
		case !ok:
			changed = append(changed, fmt.Sprintf("%s (document %s) was REMOVED at the live digest", key.Operation, key.DocumentDigest))
		case !sameCarriedState(was, now):
			changed = append(changed, fmt.Sprintf("%s (document %s) now reads mode %s, rollout %d, build %s -- this run was copying mode %s, rollout %d, build %s",
				key.Operation, key.DocumentDigest, now.Mode, now.RolloutPercentage, now.Build, was.Mode, was.RolloutPercentage, was.Build))
		}
	}
	// A row that APPEARED during the run is the other way to break the
	// invariant, and it is the quiet one: the rows this run copied are
	// all still correct, so every check above passes, and the run
	// commits reporting success -- while an operation somebody enabled
	// thirty seconds ago has no row at the digest about to go live. It
	// would be un-routed by the roll, which is the precise harm this
	// verb exists to prevent, arrived at through the verb itself
	// reporting that it had prevented it.
	//
	// Refused rather than carried: this run never fetched the deployed
	// registry's or the image's opinion of that operation, so it has no
	// basis to decide whether the row COULD be carried faithfully.
	// Deciding on less than the verb's own preflights is how a carry
	// starts inventing rows. Re-running gathers everything properly.
	for key := range present {
		if !seen[key] {
			changed = append(changed, fmt.Sprintf("%s (document %s) APPEARED at the live digest after this run read it -- it has no row at the target digest and the roll would un-route it",
				key.Operation, key.DocumentDigest))
		}
	}
	if len(changed) == 0 {
		return nil
	}
	sort.Strings(changed)
	return fmt.Errorf("%w: %d row(s) at %s moved while this carry was preparing:\n  %s\n"+
		"  NOTHING was written -- the whole run rolled back. Run `carry` again; it will copy what the live digest says NOW.",
		ErrCarrySourceRowChanged, len(changed), liveSchemaDigest, strings.Join(changed, "\n  "))
}

// carryRowKey is a row's full identity WITHIN one schema digest -- the
// table's primary key minus the digest itself. Operation alone ties
// whenever an operation has rows under several document digests, and a
// tie here would silently compare one row against another.
type carryRowKey struct {
	Operation      string
	DocumentDigest string
}

// carryRefusalError renders every refused operation in one error, wrapped
// in the sentinel of the FIRST refusal in the report's own order -- so
// errors.Is names a real cause deterministically rather than whichever
// row the database returned first.
func carryRefusalError(refusals []CarryOutcome) error {
	lines := make([]string, 0, len(refusals))
	operations := make([]string, 0, len(refusals))
	for _, refusal := range refusals {
		lines = append(lines, fmt.Sprintf("%s: %s", refusal.Operation, refusal.Reason))
		operations = append(operations, refusal.Operation)
	}
	sort.Strings(lines)
	return fmt.Errorf("%w: %d operation(s) reachable right now cannot be carried faithfully:\n  %s%s",
		refusals[0].Refusal, len(refusals), strings.Join(lines, "\n  "), goOnlyEscalation(operations))
}

// goOnlyEscalation appends the sentence an operator must read before
// rolling: an operation whose Python execution path was deleted does not
// fall back when its row is missing -- it answers the deletion error to a
// real client. The ledger is embedded in this package, so this needs no
// file and no flag.
func goOnlyEscalation(operations []string) string {
	ledger, err := DefaultGoServedLedger()
	if err != nil {
		// The ledger is embedded, so this cannot fail for a reason an
		// operator can fix, and the refusal it would decorate is already
		// correct without it. Say nothing rather than replace a real
		// refusal with a parse error.
		return ""
	}
	var affected []string
	for _, operation := range operations {
		if _, ok := ledger.Entry(operation); ok {
			affected = append(affected, operation)
		}
	}
	if len(affected) == 0 {
		return ""
	}
	sort.Strings(affected)
	return fmt.Sprintf("\n  DO NOT ROLL: %v have no Python execution path left, so losing their routing rows does not fall back -- it answers the deletion error to real clients.", affected)
}

// carriedRowOf turns an outcome back into the row it describes, for the
// comparison against whatever is actually at the target digest.
func carriedRowOf(outcome CarryOutcome) CarryRow {
	return CarryRow{
		Operation:         outcome.Operation,
		DocumentDigest:    outcome.DocumentDigest,
		Mode:              outcome.Mode,
		Build:             outcome.Build,
		Owner:             outcome.Owner,
		RolloutPercentage: outcome.RolloutPercentage,
		EligibleOrgs:      outcome.EligibleOrgs,
	}
}

// sortCarryOutcomes reports in the order this table is always read in.
// TOTAL, not by operation alone: one operation can have several rows at
// a schema digest under different document digests, and a report whose
// row order is not a function of the data cannot be pinned by a test.
func sortCarryOutcomes(outcomes []CarryOutcome) {
	sort.Slice(outcomes, func(i, j int) bool {
		if outcomes[i].Operation != outcomes[j].Operation {
			return outcomes[i].Operation < outcomes[j].Operation
		}
		return outcomes[i].DocumentDigest < outcomes[j].DocumentDigest
	})
}

// CarrySummary counts what a run did, including the zeros -- "nothing
// needed carrying" and "nothing was looked at" must not read alike.
type CarrySummary struct {
	Total     int
	Carried   int
	Unchanged int
	Skipped   int
	Refused   int
}

// SummarizeCarry counts outcomes by action.
func SummarizeCarry(outcomes []CarryOutcome) CarrySummary {
	summary := CarrySummary{Total: len(outcomes)}
	for _, outcome := range outcomes {
		switch outcome.Action {
		case CarryActionCarry:
			summary.Carried++
		case CarryActionUnchanged:
			summary.Unchanged++
		case CarryActionSkip:
			summary.Skipped++
		case CarryActionRefuse:
			summary.Refused++
		}
	}
	return summary
}
