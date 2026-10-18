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
	stageConstruct        postSyncStage = "construct"
	stageArgs             postSyncStage = "args"
	stageBegin            postSyncStage = "begin"
	stageCommit           postSyncStage = "commit"
	stageReference        postSyncStage = "reference_read"
	stagePlanRun          postSyncStage = "plan_run_read"
	stagePlanUnits        postSyncStage = "plan_units_query"
	stagePlanUnitScan     postSyncStage = "plan_units_scan"
	stagePlanUnitsRows    postSyncStage = "plan_units_rows"
	stagePlanAutoImport   postSyncStage = "plan_auto_import_read"
	stageTouchedSetup     postSyncStage = "touched_setup"
	stageTouchedWindow    postSyncStage = "touched_window"
	stageTouchedPlan      postSyncStage = "touched_plan"
	stageTouchedBegin     postSyncStage = "touched_begin"
	stageDrainConstruct   postSyncStage = "drain_construct"
	stageDrainInFlightBeg postSyncStage = "drain_in_flight_begin"
	stageDrainStartBegin  postSyncStage = "drain_start_begin"
	stageDrainStartLock   postSyncStage = "drain_start_lock"
	stageDrainStartCommit postSyncStage = "drain_start_commit"
)

// postSyncUnavailableError is ErrPostSyncUnavailable with the place it came from and the error under it. errors.Is still
// matches the sentinel; errors.As / Unwrap reach the cause, so the log line's class, Go type and SQLSTATE (synclog.Failure) name
// the real failure. Error() is the fixed sentinel text: it never carries the cause's message.
type postSyncUnavailableError struct {
	stage postSyncStage
	cause error
}

func (err *postSyncUnavailableError) Error() string { return ErrPostSyncUnavailable.Error() }
func (err *postSyncUnavailableError) Unwrap() error { return err.cause }
func (err *postSyncUnavailableError) Is(target error) bool {
	return target == ErrPostSyncUnavailable
}

// unavailable is the one way a post-sync path returns ErrPostSyncUnavailable: cause may be nil (a failed precondition).
func unavailable(stage postSyncStage, cause error) error {
	return &postSyncUnavailableError{stage: stage, cause: cause}
}

// postSyncStageOf is the stage of err, or "" when err is not a postSyncUnavailableError.
func postSyncStageOf(err error) postSyncStage {
	var typed *postSyncUnavailableError
	if errors.As(err, &typed) {
		return typed.stage
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
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && sqlStatePattern.MatchString(pgErr.Code) {
		text += " code=" + pgErr.Code
	}
	return text
}
