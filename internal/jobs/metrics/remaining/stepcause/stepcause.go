// Package stepcause builds the safe log cause of a retryable partition failure (CHAOS-8020).
//
// A retryable failure of the work_item_attribution partition leaves River only the bounded text
// `dev-health job failed [retryable]` (jobruntime/errors.go safeError); the runtime logs a cause only when the handler
// opts in with jobruntime.WithSafeCauseText. Failure does that with a cause made ONLY of a closed step label, the integer
// ClickHouse exception code and a validated SQLSTATE: never driver text, a query, a table value or an identifier.
//
// The step is the opaque struct Step. Its values are the exported variables below, declared in this one place. Its only
// field is unexported, so a caller outside this package cannot put text in one: a string is not assignable to it, a
// conversion does not exist, and a generic ~string helper or fmt.Sscan has no field to fill. The zero Step fails closed to
// the fixed label unknown_step.
package stepcause

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// Step names the failing step. It is an opaque struct: its only field is unexported, so code outside this package can
// build a Step only from the exported values below (or the zero Step, which renders as unknownStep). No conversion, no
// generic ~string helper and no fmt.Sscan can put new text into one.
type Step struct{ label string }

// unknownStep is the label of the zero Step: a caller that passes Step{} fails closed to a fixed, known text.
const unknownStep = "unknown_step"

// The closed set of steps.
var (
	QueryScopedWatermarks                             = Step{label: "query scoped watermarks"}
	ScanScopedWatermark                               = Step{label: "scan scoped watermark"}
	QueryScopeChanges                                 = Step{label: "query scope changes"}
	ScanScopeChange                                   = Step{label: "scan scope change"}
	QueryMaxUpdatedAt                                 = Step{label: "query max updated_at"}
	ScanMaxUpdatedAt                                  = Step{label: "scan max updated_at"}
	QueryMaxEffectiveChangedAt                        = Step{label: "query max effective changed_at"}
	ScanMaxEffectiveChangedAt                         = Step{label: "scan max effective changed_at"}
	QueryWorkItems                                    = Step{label: "query work_items"}
	ScanWorkItemsRow                                  = Step{label: "scan work_items row"}
	QueryWorkItemDependencies                         = Step{label: "query work_item_dependencies"}
	ScanWorkItemDependenciesRow                       = Step{label: "scan work_item_dependencies row"}
	QueryWorkItemDependenciesReverseClosure           = Step{label: "query work_item_dependencies (reverse closure)"}
	ScanWorkItemDependenciesRowReverseClosure         = Step{label: "scan work_item_dependencies row (reverse closure)"}
	CountWorkItems                                    = Step{label: "count work_items"}
	QueryAlreadyCoveredTodayAttributions              = Step{label: "query already-covered-today attributions"}
	ScanAlreadyCoveredTodayRow                        = Step{label: "scan already-covered-today row"}
	PrepareWorkItemTeamAttributionsBatch              = Step{label: "prepare work_item_team_attributions batch"}
	AppendWorkItemTeamAttributionsRow                 = Step{label: "append work_item_team_attributions row"}
	SendWorkItemTeamAttributionsBatch                 = Step{label: "send work_item_team_attributions batch"}
	PrepareWorkItemAttributionBackstopRunsBatch       = Step{label: "prepare work_item_attribution_backstop_runs batch"}
	AppendWorkItemAttributionBackstopRunsRow          = Step{label: "append work_item_attribution_backstop_runs row"}
	SendWorkItemAttributionBackstopRunsBatch          = Step{label: "send work_item_attribution_backstop_runs batch"}
	PrepareWorkItemAttributionBackstopScopedRunsBatch = Step{label: "prepare work_item_attribution_backstop_scoped_runs batch"}
	AppendWorkItemAttributionBackstopScopedRunsRow    = Step{label: "append work_item_attribution_backstop_scoped_runs row"}
	SendWorkItemAttributionBackstopScopedRunsBatch    = Step{label: "send work_item_attribution_backstop_scoped_runs batch"}
	ClaimPartition                                    = Step{label: "claim_partition"}
	LoadRun                                           = Step{label: "load_run"}
	ComputePartition                                  = Step{label: "compute_partition"}
	CompletePartition                                 = Step{label: "complete_partition"}
)

// sqlStateShape is the only SQLSTATE form that may reach a log: five characters of [0-9A-Z]. A code from any other
// source (a proxy, a wrapped driver, a test double) is dropped, not truncated or escaped.
var sqlStateShape = regexp.MustCompile(`^[0-9A-Z]{5}$`)

// Failure returns err with its message unchanged (fmt.Errorf("<step>: %w")) and a safe log cause that names the step
// and, when the error carries them, the ClickHouse exception code and the Postgres SQLSTATE.
func Failure(step Step, err error) error {
	if err == nil {
		return nil
	}
	return jobruntime.WithSafeCauseText(fmt.Errorf("%s: %w", step.text(), err), cause(step, err))
}

// text is the step's label, or unknownStep for the zero Step.
func (step Step) text() string {
	if step.label == "" {
		return unknownStep
	}
	return step.label
}

// cause is Failure's cause text: `step=<label>` plus ` ch_code=<n>` and ` sqlstate=<xxxxx>` when present.
func cause(step Step, err error) string {
	text := "step=" + step.text()
	var exception *clickhouse.Exception
	if errors.As(err, &exception) && exception != nil {
		text += " ch_code=" + strconv.FormatInt(int64(exception.Code), 10)
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && pgError != nil && sqlStateShape.MatchString(pgError.Code) {
		text += " sqlstate=" + pgError.Code
	}
	return text
}
