package pooledmttr

import (
	"fmt"
	"time"

	"context"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/operationalordering"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// HoursSample is the column name of a repair or deploy sample in hours, shared by every delivery pooled source.
const HoursSample = "delivery_hours_sample"

// BugRepairsSource holds the repair samples of mttr_hours (repouser.MTTRByRepo over the loader's bug work items): the
// samples the repo_user_commit writer stored with the newest repo_metrics_daily row of each repository-day. A
// repository-day whose newest row holds no samples (written before migration 161) reads the raw rows with the writer's
// rule: a work item of type bug with both lifecycle times and a terminal status (done or canceled) counts on the UTC day
// it completed, and its sample is completed_at - started_at in hours, a negative one included, as the writer's mean
// includes it.
var BugRepairsSource = `(
    SELECT org_id, repo_id, day, ` + HoursSample + `
    FROM (
        SELECT org_id, repo_id, day, stored_hours AS ` + HoursSample + `
        FROM ` + testopspooled.NewestStoredRows("repo_metrics_daily", "pooled_bug_repair_hours") + `
        ARRAY JOIN pooled_bug_repair_hours AS stored_hours
        WHERE pooled_samples_stored = 1
        UNION ALL
        SELECT
            org_id,
            repo_id,
            toDate(completed_at, 'UTC') AS day,
            dateDiff('millisecond', started_at, completed_at) / 3600000 AS ` + HoursSample + `
        FROM work_items FINAL
        WHERE org_id = {org_id:String}
          AND type = 'bug' AND started_at IS NOT NULL AND completed_at IS NOT NULL
          AND status IN ('done', 'canceled')
          AND toDate(completed_at, 'UTC') >= {start_date:Date} AND toDate(completed_at, 'UTC') < {end_date:Date}
          AND (repo_id, toDate(completed_at, 'UTC')) IN (` + testopspooled.LegacyRepoDays("repo_metrics_daily") + `)
    )
) AS pooled_bug_repairs`

// IncidentRepairsSource holds the samples of the incident writer (daily.LoadIncidentsStarted and
// computeIncidentMetricsDaily): the samples it stored with the newest incident_metrics_daily row of each repository-day,
// read through the service mappings the writer read at its run. A repository-day whose newest row holds no samples
// (written before migration 161) reads the raw rows with the writer's rule: the current row of each incident, not
// deleted, joined to the repositories its service maps to (the current active mappings valid at {as_of}), counts on the
// UTC day it started when it was resolved on that same day, once per repository; its sample is resolved_at - started_at
// in hours, to the microsecond the timestamps hold, when not negative.
var IncidentRepairsSource = `(
    SELECT org_id, repo_id, day, ` + HoursSample + `
    FROM (
        SELECT org_id, repo_id, day, stored_hours AS ` + HoursSample + `
        FROM ` + testopspooled.NewestStoredRows("incident_metrics_daily", "pooled_repair_hours") + `
        ARRAY JOIN pooled_repair_hours AS stored_hours
        WHERE pooled_samples_stored = 1
        UNION ALL
        SELECT org_id, repo_id, day, ` + HoursSample + `
        FROM (
            SELECT
                incident.org_id AS org_id,
                assumeNotNull(mapping.repo_id) AS repo_id,
                incident.id AS incident_id,
                toDate(incident.started_at) AS day,
                if(incident.resolved_at >= incident.started_at,
                    dateDiff('microsecond', incident.started_at, incident.resolved_at) / 3600000000, NULL) AS ` + HoursSample + `
            FROM ` + operationalordering.RevisionCurrentRows("operational_incidents", "org_id = {org_id:String}", []string{
	"is_deleted = 0",
	"toDate(started_at) >= {start_date:Date}",
	"toDate(started_at) < {end_date:Date}",
	"resolved_at IS NOT NULL",
	"toDate(resolved_at) = toDate(started_at)",
}) + ` AS incident
            INNER JOIN ` + operationalordering.RevisionCurrentRows("operational_service_repository_mappings", "org_id = {org_id:String}", []string{
	"repo_id IS NOT NULL",
	"is_active = 1",
	"(valid_from IS NULL OR valid_from <= {as_of:DateTime64(6, 'UTC')})",
	"(valid_to IS NULL OR valid_to > {as_of:DateTime64(6, 'UTC')})",
}) + ` AS mapping
                ON incident.org_id = mapping.org_id AND incident.service_id = mapping.service_id
            INNER JOIN repos AS repo FINAL
                ON mapping.org_id = repo.org_id AND mapping.repo_id = repo.id
            WHERE mapping.repo_id IS NOT NULL AND repo.org_id = {org_id:String}
            LIMIT 1 BY repo_id, incident_id
        )
        WHERE (repo_id, day) IN (` + testopspooled.LegacyRepoDays("incident_metrics_daily") + `)
    )
) AS pooled_incident_repairs`

// The writers' median, p90 (linear between closest ranks) and mean, NULL for a group with no sample.
const (
	HoursMedian = "if(count(" + HoursSample + ") = 0, NULL, quantileExactInclusive(0.5)(" + HoursSample + "))"
	HoursP90    = "if(count(" + HoursSample + ") = 0, NULL, quantileExactInclusive(0.9)(" + HoursSample + "))"
	HoursMean   = "if(count(" + HoursSample + ") = 0, NULL, avg(" + HoursSample + "))"
)

// AsOfFormat is the literal a DateTime64(6, 'UTC') mapping instant is bound as.
const AsOfFormat = "2006-01-02 15:04:05.000000"

// Querier is the query boundary of a tile client.
type Querier interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// MTTR is the pooled MTTR of [start, end) over the samples the report charts: the incident median (nil when no incident
// sample) and the bug repair mean (nil when no bug sample). asOf is the instant the incident mappings of a repository-day
// without stored samples are read at.
func MTTR(ctx context.Context, client Querier, orgID string, start, end, asOf time.Time) (p50, mean *float64, err error) {
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: start.Format(time.DateOnly)},
		{Name: "end_date", Value: end.Format(time.DateOnly)},
	}
	p50, err = scorecard(ctx, client, IncidentRepairsSource, HoursMedian, append(bindings, clickhouse.Binding{Name: "as_of", Value: asOf.UTC().Format(AsOfFormat)}))
	if err != nil {
		return nil, nil, err
	}
	mean, err = scorecard(ctx, client, BugRepairsSource, HoursMean, bindings)
	if err != nil {
		return nil, nil, err
	}
	return p50, mean, nil
}

func scorecard(ctx context.Context, client Querier, source, expression string, bindings []clickhouse.Binding) (value *float64, err error) {
	statement := fmt.Sprintf(`SELECT
        CAST('total', 'String') AS x,
        CAST(NULL, 'Nullable(String)') AS group_value,
        %s AS y
    FROM %s
    WHERE
        org_id = {org_id:String}
    SETTINGS max_rows_to_read = %d, read_overflow_mode = 'throw'`, expression, source, testopspooled.MaxRowsToRead)
	rows, err := client.Query(ctx, statement, bindings)
	if err != nil {
		return nil, fmt.Errorf("pooledmttr: mttr query: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	for rows.Next() {
		var x string
		var group *string
		var y *float64
		if scanErr := rows.Scan(&x, &group, &y); scanErr != nil {
			return nil, fmt.Errorf("pooledmttr: mttr scan: %w", scanErr)
		}
		value = y
	}
	return value, rows.Err()
}
