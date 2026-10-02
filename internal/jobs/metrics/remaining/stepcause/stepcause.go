// Package stepcause builds the safe log cause of a retryable partition failure (CHAOS-8020).
//
// A retryable failure of the work_item_attribution partition leaves River only the bounded text
// `dev-health job failed [retryable]` (jobruntime/errors.go safeError); the runtime logs a cause only when the handler
// opts in with jobruntime.WithSafeCauseText. Failure does that with a cause made ONLY of a closed step label, the integer
// ClickHouse exception code and a validated SQLSTATE: never driver text, a query, a table value or an identifier.
//
// The step label is the unexported type label. Its values are the exported constants below, declared in this one place.
// A caller outside this package cannot name the type, so it cannot convert a string to it; a non-constant string
// ("step "+err.Error(), fmt.Sprint(err), strings.Join(...)) is not assignable to it and the call does not COMPILE.
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

type label string

// The closed set of step labels.
const (
	QueryScopedWatermarks                             label = "query scoped watermarks"
	ScanScopedWatermark                               label = "scan scoped watermark"
	QueryScopeChanges                                 label = "query scope changes"
	ScanScopeChange                                   label = "scan scope change"
	QueryMaxUpdatedAt                                 label = "query max updated_at"
	ScanMaxUpdatedAt                                  label = "scan max updated_at"
	QueryMaxEffectiveChangedAt                        label = "query max effective changed_at"
	ScanMaxEffectiveChangedAt                         label = "scan max effective changed_at"
	QueryWorkItems                                    label = "query work_items"
	ScanWorkItemsRow                                  label = "scan work_items row"
	QueryWorkItemDependencies                         label = "query work_item_dependencies"
	ScanWorkItemDependenciesRow                       label = "scan work_item_dependencies row"
	QueryWorkItemDependenciesReverseClosure           label = "query work_item_dependencies (reverse closure)"
	ScanWorkItemDependenciesRowReverseClosure         label = "scan work_item_dependencies row (reverse closure)"
	CountWorkItems                                    label = "count work_items"
	QueryAlreadyCoveredTodayAttributions              label = "query already-covered-today attributions"
	ScanAlreadyCoveredTodayRow                        label = "scan already-covered-today row"
	PrepareWorkItemTeamAttributionsBatch              label = "prepare work_item_team_attributions batch"
	AppendWorkItemTeamAttributionsRow                 label = "append work_item_team_attributions row"
	SendWorkItemTeamAttributionsBatch                 label = "send work_item_team_attributions batch"
	PrepareWorkItemAttributionBackstopRunsBatch       label = "prepare work_item_attribution_backstop_runs batch"
	AppendWorkItemAttributionBackstopRunsRow          label = "append work_item_attribution_backstop_runs row"
	SendWorkItemAttributionBackstopRunsBatch          label = "send work_item_attribution_backstop_runs batch"
	PrepareWorkItemAttributionBackstopScopedRunsBatch label = "prepare work_item_attribution_backstop_scoped_runs batch"
	AppendWorkItemAttributionBackstopScopedRunsRow    label = "append work_item_attribution_backstop_scoped_runs row"
	SendWorkItemAttributionBackstopScopedRunsBatch    label = "send work_item_attribution_backstop_scoped_runs batch"
	ClaimPartition                                    label = "claim_partition"
	LoadRun                                           label = "load_run"
	ComputePartition                                  label = "compute_partition"
	CompletePartition                                 label = "complete_partition"
)

// sqlStateShape is the only SQLSTATE form that may reach a log: five characters of [0-9A-Z]. A code from any other
// source (a proxy, a wrapped driver, a test double) is dropped, not truncated or escaped.
var sqlStateShape = regexp.MustCompile(`^[0-9A-Z]{5}$`)

// Failure returns err with its message unchanged (fmt.Errorf("<step>: %w")) and a safe log cause that names the step
// and, when the error carries them, the ClickHouse exception code and the Postgres SQLSTATE.
func Failure(step label, err error) error {
	if err == nil {
		return nil
	}
	return jobruntime.WithSafeCauseText(fmt.Errorf("%s: %w", string(step), err), cause(step, err))
}

// cause is Failure's cause text: `step=<label>` plus ` ch_code=<n>` and ` sqlstate=<xxxxx>` when present.
func cause(step label, err error) string {
	text := "step=" + string(step)
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
