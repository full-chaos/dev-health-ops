package report

import (
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/deploymentstatus"
	"github.com/full-chaos/dev-health-ops/internal/operationalordering"
)

// The deploy time median, the pull request size percentiles and the MTTR metrics are charted over the window's raw rows
// with the daily writers' own sample rules (delivery_pooled_test.go pins each rule to the writer text it restates). Only
// the repository-days the writer computed are read (the semi-join on the daily table's keys), so a bucket of one
// repository-day is that day's stored value.
const (
	deliveryHoursSample = "delivery_hours_sample"
	deliveryLOCSample   = "delivery_loc_sample"
)

// deploymentsSource holds the samples of the deploy writer (internal/jobs/metrics/daily/deploy_native_clickhouse.go and
// numerical.ComputeDeployMetrics): a deployment counts on the UTC day of deployed_at, a cancelled one is no deployment,
// and its sample is finished_at - started_at in hours when both are set and it is not negative. The newest synced version
// of a deployment (FINAL) is the deployment, counted once.
var deploymentsSource = `(
    SELECT
        org_id,
        repo_id,
        toDate(deployed_at) AS day,
        if(finished_at >= started_at, dateDiff('millisecond', started_at, finished_at) / 3600000, NULL) AS ` + deliveryHoursSample + `
    FROM deployments FINAL
    WHERE org_id = {org_id:String}
      AND toDate(deployed_at) >= {start_date:Date} AND toDate(deployed_at) < {end_date:Date}
      AND ` + deploymentstatus.CanceledExclusionSQL("status") + `
      AND (repo_id, toDate(deployed_at)) IN (
          SELECT repo_id, day FROM deploy_metrics_daily
          WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date})
) AS pooled_deployments`

// pullRequestSizesSource holds the size samples of the repo_user_commit writer (repouser.Compute): a pull request counts
// on the UTC day it merged, its size is its additions plus its deletions, and a size of 0 is no sample. The newest synced
// version of a pull request (FINAL) is the pull request, counted once.
const pullRequestSizesSource = `(
    SELECT
        org_id,
        repo_id,
        toDate(merged_at) AS day,
        toFloat64(ifNull(additions, 0) + ifNull(deletions, 0)) AS ` + deliveryLOCSample + `
    FROM git_pull_requests FINAL
    WHERE org_id = {org_id:String}
      AND toDate(merged_at) >= {start_date:Date} AND toDate(merged_at) < {end_date:Date}
      AND ifNull(additions, 0) + ifNull(deletions, 0) > 0
      AND (repo_id, toDate(merged_at)) IN (
          SELECT repo_id, day FROM repo_metrics_daily
          WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date})
) AS pooled_pull_requests`

// bugRepairsSource holds the repair samples of mttr_hours (repouser.MTTRByRepo over the loader's bug work items): a work
// item of type bug with both lifecycle times counts on the UTC day it completed, and its sample is completed_at -
// started_at in hours, a negative one included, as the writer's mean includes it.
const bugRepairsSource = `(
    SELECT
        org_id,
        repo_id,
        toDate(completed_at, 'UTC') AS day,
        dateDiff('millisecond', started_at, completed_at) / 3600000 AS ` + deliveryHoursSample + `
    FROM work_items FINAL
    WHERE org_id = {org_id:String}
      AND type = 'bug' AND started_at IS NOT NULL AND completed_at IS NOT NULL
      AND toDate(completed_at, 'UTC') >= {start_date:Date} AND toDate(completed_at, 'UTC') < {end_date:Date}
      AND (repo_id, toDate(completed_at, 'UTC')) IN (
          SELECT repo_id, day FROM repo_metrics_daily
          WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date})
) AS pooled_bug_repairs`

// incidentRepairsSource holds the samples of the incident writer (daily.LoadIncidentsStarted and
// computeIncidentMetricsDaily): the current row of each incident, not deleted, joined to the repositories its service maps
// to (the current active mappings valid at the request), counts on the UTC day it started when it was resolved on that
// same day, once per repository; its sample is resolved_at - started_at in hours when not negative.
var incidentRepairsSource = `(
    SELECT org_id, repo_id, day, ` + deliveryHoursSample + `
    FROM (
        SELECT
            incident.org_id AS org_id,
            assumeNotNull(mapping.repo_id) AS repo_id,
            incident.id AS incident_id,
            toDate(incident.started_at) AS day,
            if(incident.resolved_at >= incident.started_at,
                dateDiff('millisecond', incident.started_at, incident.resolved_at) / 3600000, NULL) AS ` + deliveryHoursSample + `
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
        WHERE mapping.repo_id IS NOT NULL
        LIMIT 1 BY repo_id, incident_id
    )
    WHERE (repo_id, day) IN (
        SELECT repo_id, day FROM incident_metrics_daily
        WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date})
) AS pooled_incident_repairs`

// The writers' median, p90 (linear between closest ranks) and mean, NULL for a group with no sample.
const (
	deliveryHoursMedian = "if(count(" + deliveryHoursSample + ") = 0, NULL, quantileExactInclusive(0.5)(" + deliveryHoursSample + "))"
	deliveryHoursP90    = "if(count(" + deliveryHoursSample + ") = 0, NULL, quantileExactInclusive(0.9)(" + deliveryHoursSample + "))"
	deliveryHoursMean   = "if(count(" + deliveryHoursSample + ") = 0, NULL, avg(" + deliveryHoursSample + "))"
	deliveryLOCMedian   = "if(count(" + deliveryLOCSample + ") = 0, NULL, quantileExactInclusive(0.5)(" + deliveryLOCSample + "))"
	deliveryLOCP90      = "if(count(" + deliveryLOCSample + ") = 0, NULL, quantileExactInclusive(0.9)(" + deliveryLOCSample + "))"
)

func bindCanceledStatuses(time.Time) []any {
	return []any{clickhouse.Named("canceled_statuses", deploymentstatus.CanceledStatuses)}
}

// bindMappingAsOf binds the instant the incident mappings are read at: the request, as the team owners are.
func bindMappingAsOf(asOf time.Time) []any {
	return []any{clickhouse.Named("as_of", asOf.UTC().Format("2006-01-02 15:04:05.000000"))}
}
