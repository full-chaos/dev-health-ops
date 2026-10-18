package syncdispatchruntime

import (
	"errors"
	"regexp"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/jackc/pgx/v5/pgconn"
)

var sqlStatePattern = regexp.MustCompile(`^[0-9A-Z]{5}$`)

// postSyncStage is the closed vocabulary of the places a post-sync fan-out reports ErrPostSyncUnavailable from.
type postSyncStage string

const (
	stageConstruct           postSyncStage = "construct"
	stageArgs                postSyncStage = "args"
	stageBegin               postSyncStage = "begin"
	stageCommit              postSyncStage = "commit"
	stageReference           postSyncStage = "reference_read"
	stagePlanRun             postSyncStage = "plan_run_read"
	stagePlanUnits           postSyncStage = "plan_units_query"
	stagePlanUnitScan        postSyncStage = "plan_units_scan"
	stagePlanUnitsRows       postSyncStage = "plan_units_rows"
	stagePlanAutoImport      postSyncStage = "plan_auto_import_read"
	stagePanic               postSyncStage = "panic"
	stageTouchedTake         postSyncStage = "touched_take"
	stageComplexity          postSyncStage = "complexity"
	stageDaily               postSyncStage = "daily"
	stageTouchedStart        postSyncStage = "touched_start"
	stageWorkGraph           postSyncStage = "workgraph"
	stageInvestment          postSyncStage = "investment"
	stageMembership          postSyncStage = "membership_backfill"
	stageDora                postSyncStage = "dora"
	stageTeamAutoimport      postSyncStage = "team_autoimport"
	stageOwnershipDerivation postSyncStage = "ownership_derivation"
	stageTouchedSetup        postSyncStage = "touched_setup"
	stageTouchedWindow       postSyncStage = "touched_window"
	stageTouchedPlan         postSyncStage = "touched_plan"
	stageTouchedBegin        postSyncStage = "touched_begin"
	stageDrainConstruct      postSyncStage = "drain_construct"
	stageDrainInFlightBeg    postSyncStage = "drain_in_flight_begin"
	stageDrainStartBegin     postSyncStage = "drain_start_begin"
	stageDrainStartLock      postSyncStage = "drain_start_lock"
	stageDrainStartCommit    postSyncStage = "drain_start_commit"
)

// postSyncUnavailableError is ErrPostSyncUnavailable with the place it came from and the error under it. errors.Is still
// matches the sentinel; errors.As / Unwrap reach the cause, so the log line's class, Go type and SQLSTATE (synclog.Failure) name
// the real failure. Error() is the fixed sentinel text: it never carries the cause's message.
type postSyncUnavailableError struct {
	stage postSyncStage
	cause error
	// sentinel is false for stagedFailure: a writer's own error keeps its text and does not match ErrPostSyncUnavailable.
	sentinel bool
}

func (err *postSyncUnavailableError) Error() (text string) {
	text = ErrPostSyncUnavailable.Error()
	if err.sentinel || err.cause == nil {
		return text
	}
	// the cause is a writer's own error: a typed nil receiver there must not panic the job error text
	defer func() {
		if recover() != nil {
			text = ErrPostSyncUnavailable.Error()
		}
	}()
	return err.cause.Error()
}
func (err *postSyncUnavailableError) Unwrap() error { return err.cause }
func (err *postSyncUnavailableError) Is(target error) bool {
	return err.sentinel && target == ErrPostSyncUnavailable
}

// unavailable is the one way a post-sync path returns ErrPostSyncUnavailable: cause may be nil (a failed precondition).
func unavailable(stage postSyncStage, cause error) error {
	return &postSyncUnavailableError{stage: stage, cause: cause, sentinel: true}
}

// stagedFailure names the stage of an error a writer returned, so the log line and the job error say where it came from. An
// error that already carries a stage is returned as it is; the chain, the text and every errors.Is / errors.As are unchanged.
func stagedFailure(stage postSyncStage, cause error) error {
	if cause == nil || postSyncStageOf(cause) != "" {
		return cause
	}
	return &postSyncUnavailableError{stage: stage, cause: cause}
}

// maxChainDepth bounds a walk down an error chain: a cyclic Unwrap, or a chain far deeper than any real one, must not stall it.
const maxChainDepth = 64

// boundedChain is err and what it unwraps to, at most maxChainDepth long. An Unwrap that panics (a typed nil receiver) ends the walk.
func boundedChain(err error) (chain []error) {
	defer func() { _ = recover() }()
	for depth := 0; err != nil && depth < maxChainDepth; depth++ {
		chain = append(chain, err)
		err = errors.Unwrap(err)
	}
	return chain
}

// postSyncStageOf is the stage of err, or "" when no error of its chain is a postSyncUnavailableError.
func postSyncStageOf(err error) postSyncStage {
	for _, link := range boundedChain(err) {
		if typed, ok := link.(*postSyncUnavailableError); ok && typed != nil {
			return typed.stage
		}
	}
	return ""
}

// failureDetail is the fixed-vocabulary suffix of the River job error text: the stage, the error class and, for Postgres, the SQLSTATE.
func failureDetail(err error) (text string) {
	// a typed nil receiver can panic in Unwrap/Is: the job error text must still be built
	defer func() {
		if recover() != nil {
			text = " class=other"
		}
	}()
	if stage := postSyncStageOf(err); stage != "" {
		text += " stage=" + string(stage)
	}
	text += " class=" + logging.ErrorClass(err)
	for _, link := range boundedChain(err) {
		if pgErr, ok := link.(*pgconn.PgError); ok && pgErr != nil && sqlStatePattern.MatchString(pgErr.Code) {
			text += " code=" + pgErr.Code
			break
		}
	}
	return text
}
