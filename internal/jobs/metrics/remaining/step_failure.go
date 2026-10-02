package remaining

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// sqlStateShape is the only SQLSTATE form that may reach a log: five characters of [0-9A-Z]. A server error code
// from any other source (a proxy, a wrapped driver, a test double) is dropped, not truncated or escaped.
var sqlStateShape = regexp.MustCompile(`^[0-9A-Z]{5}$`)

// stepLabel is the CLOSED set of step names a safe cause may carry (CHAOS-8020). It is an unexported named type and every value is
// a constant declared below: an untyped string constant converts implicitly, a non-constant string ("step "+err.Error(), fmt.Sprint(err),
// strings.Join(...)) does NOT, so such a label is refused by the COMPILER and driver text cannot reach the cause through the label.
// An explicit conversion of a string to this type would still compile inside this package: TestNoStepLabelConversion bans it.
type stepLabel string

const (
	stepQueryScopedWatermarks                             stepLabel = "query scoped watermarks"
	stepScanScopedWatermark                               stepLabel = "scan scoped watermark"
	stepQueryScopeChanges                                 stepLabel = "query scope changes"
	stepScanScopeChange                                   stepLabel = "scan scope change"
	stepQueryMaxUpdatedAt                                 stepLabel = "query max updated_at"
	stepScanMaxUpdatedAt                                  stepLabel = "scan max updated_at"
	stepQueryMaxEffectiveChangedAt                        stepLabel = "query max effective changed_at"
	stepScanMaxEffectiveChangedAt                         stepLabel = "scan max effective changed_at"
	stepQueryWorkItems                                    stepLabel = "query work_items"
	stepScanWorkItemsRow                                  stepLabel = "scan work_items row"
	stepQueryWorkItemDependencies                         stepLabel = "query work_item_dependencies"
	stepScanWorkItemDependenciesRow                       stepLabel = "scan work_item_dependencies row"
	stepQueryWorkItemDependenciesReverseClosure           stepLabel = "query work_item_dependencies (reverse closure)"
	stepScanWorkItemDependenciesRowReverseClosure         stepLabel = "scan work_item_dependencies row (reverse closure)"
	stepCountWorkItems                                    stepLabel = "count work_items"
	stepQueryAlreadyCoveredTodayAttributions              stepLabel = "query already-covered-today attributions"
	stepScanAlreadyCoveredTodayRow                        stepLabel = "scan already-covered-today row"
	stepPrepareWorkItemTeamAttributionsBatch              stepLabel = "prepare work_item_team_attributions batch"
	stepAppendWorkItemTeamAttributionsRow                 stepLabel = "append work_item_team_attributions row"
	stepSendWorkItemTeamAttributionsBatch                 stepLabel = "send work_item_team_attributions batch"
	stepPrepareWorkItemAttributionBackstopRunsBatch       stepLabel = "prepare work_item_attribution_backstop_runs batch"
	stepAppendWorkItemAttributionBackstopRunsRow          stepLabel = "append work_item_attribution_backstop_runs row"
	stepSendWorkItemAttributionBackstopRunsBatch          stepLabel = "send work_item_attribution_backstop_runs batch"
	stepPrepareWorkItemAttributionBackstopScopedRunsBatch stepLabel = "prepare work_item_attribution_backstop_scoped_runs batch"
	stepAppendWorkItemAttributionBackstopScopedRunsRow    stepLabel = "append work_item_attribution_backstop_scoped_runs row"
	stepSendWorkItemAttributionBackstopScopedRunsBatch    stepLabel = "send work_item_attribution_backstop_scoped_runs batch"
	stepClaimPartition                                    stepLabel = "claim_partition"
	stepLoadRun                                           stepLabel = "load_run"
	stepComputePartition                                  stepLabel = "compute_partition"
	stepCompletePartition                                 stepLabel = "complete_partition"
)

// stepFailure returns err with its message unchanged (fmt.Errorf("<step>: %w")) and a safe log cause that names
// the step and, when the error carries them, the ClickHouse exception code and the Postgres SQLSTATE.
//
// CHAOS-8020: a retryable failure of the partition handler leaves River only the bounded text
// `dev-health job failed [retryable]` (jobruntime/errors.go safeError). The runtime logs a cause only when the
// handler opts in (WithSafeCauseText), and the ClickHouse/Postgres step errors never did, so a daily failure of
// work_item_attribution (three attempts, then a discard) was unrecoverable. The cause text built here is made
// ONLY of the closed step label (a constant of the unexported stepLabel type), the integer ClickHouse code and a validated SQLSTATE:
// never the driver message, a query, a table value or an identifier of the data.
func stepFailure(step stepLabel, err error) error {
	if err == nil {
		return nil
	}
	return jobruntime.WithSafeCauseText(fmt.Errorf("%s: %w", string(step), err), safeStepCause(step, err))
}

// safeStepCause is stepFailure's cause text: `step=<label>` plus ` ch_code=<n>` and ` sqlstate=<xxxxx>` when present.
func safeStepCause(step stepLabel, err error) string {
	cause := "step=" + string(step)
	var exception *clickhouse.Exception
	if errors.As(err, &exception) && exception != nil {
		cause += " ch_code=" + strconv.FormatInt(int64(exception.Code), 10)
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && pgError != nil && sqlStateShape.MatchString(pgError.Code) {
		cause += " sqlstate=" + pgError.Code
	}
	return cause
}
