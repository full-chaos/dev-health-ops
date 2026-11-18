package report

import (
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/deploymentstatus"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pooledmttr"
)

// The deploy time median, the pull request size percentiles and the MTTR metrics are charted over the window's raw rows
// with the daily writers' own sample rules (delivery_pooled_test.go pins each rule to the writer text it restates). Only
// the repository-days the writer computed are read (the semi-join on the daily table's keys), so a bucket of one
// repository-day is that day's stored value.
const (
	deliveryHoursSample = pooledmttr.HoursSample
	deliveryLOCSample   = "delivery_loc_sample"
)

// deploymentsSource holds the samples of the deploy writer (internal/jobs/metrics/daily/deploy_native_clickhouse.go and
// numerical.ComputeDeployMetrics): a deployment counts on the UTC day of deployed_at, a cancelled one is no deployment,
// and its sample is finished_at - started_at in hours when both are set and it is not negative. The stored rows are read
// without FINAL, as the writer reads them, so a version not yet merged away is a sample of both.
var deploymentsSource = `(
    SELECT
        org_id,
        repo_id,
        toDate(deployed_at) AS day,
        if(finished_at >= started_at, dateDiff('millisecond', started_at, finished_at) / 3600000, NULL) AS ` + deliveryHoursSample + `
    FROM deployments
    WHERE org_id = {org_id:String}
      AND toDate(deployed_at) >= {start_date:Date} AND toDate(deployed_at) < {end_date:Date}
      AND ` + deploymentstatus.CanceledExclusionSQL("status") + `
      AND (repo_id, toDate(deployed_at)) IN (
          SELECT repo_id, day FROM deploy_metrics_daily
          WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date})
) AS pooled_deployments`

// pullRequestSizesSource holds the size samples of the repo_user_commit writer (repouser.Compute): a pull request counts
// on the UTC day it merged, its size is its additions plus its deletions, and a size of 0 is no sample. The stored rows
// are read without FINAL, as the writer reads them.
const pullRequestSizesSource = `(
    SELECT
        org_id,
        repo_id,
        toDate(merged_at) AS day,
        toFloat64(ifNull(additions, 0) + ifNull(deletions, 0)) AS ` + deliveryLOCSample + `
    FROM git_pull_requests
    WHERE org_id = {org_id:String}
      AND toDate(merged_at) >= {start_date:Date} AND toDate(merged_at) < {end_date:Date}
      AND ifNull(additions, 0) + ifNull(deletions, 0) > 0
      AND (repo_id, toDate(merged_at)) IN (
          SELECT repo_id, day FROM repo_metrics_daily
          WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date})
) AS pooled_pull_requests`

// The MTTR sources and the hours expressions live in pooledmttr, which the operating review tile reads too.
const (
	bugRepairsSource    = pooledmttr.BugRepairsSource
	deliveryHoursMedian = pooledmttr.HoursMedian
	deliveryHoursP90    = pooledmttr.HoursP90
	deliveryHoursMean   = pooledmttr.HoursMean
)

var incidentRepairsSource = pooledmttr.IncidentRepairsSource

// The writers' LOC percentiles, NULL for a group with no sample.
const (
	deliveryLOCMedian = "if(count(" + deliveryLOCSample + ") = 0, NULL, quantileExactInclusive(0.5)(" + deliveryLOCSample + "))"
	deliveryLOCP90    = "if(count(" + deliveryLOCSample + ") = 0, NULL, quantileExactInclusive(0.9)(" + deliveryLOCSample + "))"
)

func bindCanceledStatuses(time.Time) []any {
	return []any{clickhouse.Named("canceled_statuses", deploymentstatus.CanceledStatuses)}
}

// bindMappingAsOf binds the instant the incident mappings are read at: the request, as the team owners are.
func bindMappingAsOf(asOf time.Time) []any {
	return []any{clickhouse.Named("as_of", asOf.UTC().Format(pooledmttr.AsOfFormat))}
}
