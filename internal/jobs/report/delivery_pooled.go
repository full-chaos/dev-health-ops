package report

import (
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/deploymentstatus"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pooledmttr"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// The deploy time median, the pull request size percentiles and the MTTR metrics are charted over the samples the daily
// writers stored with the newest row of each repository-day, so a bucket of one repository-day is that day's stored value
// and a row synced after the writer ran counts after its next run. A repository-day whose newest row holds no samples
// (written before migration 161) reads its raw rows with the writers' own sample rules (delivery_pooled_test.go pins each
// rule to the writer text it restates). A retired repository-day (newest row retracted = 1) is in neither arm
// (testopspooled.LiveRepoDays).
const (
	deliveryHoursSample = pooledmttr.HoursSample
	deliveryLOCSample   = "delivery_loc_sample"
)

// deploymentsSource holds the samples of the deploy writer (internal/jobs/metrics/daily/deploy_native_clickhouse.go and
// numerical.ComputeDeployMetrics) stored with its newest row. Raw rule of a repository-day without stored samples: a
// deployment counts on the UTC day of deployed_at, a cancelled one is no deployment, and its sample is finished_at -
// started_at in hours when both are set and it is not negative. The raw rows are read without FINAL, as the writer reads
// them, so a version not yet merged away is a sample of both.
var deploymentsSource = `(
    SELECT org_id, repo_id, day, ` + deliveryHoursSample + `
    FROM (
        SELECT org_id, repo_id, day, stored_hours AS ` + deliveryHoursSample + `
        FROM ` + testopspooled.NewestStoredRows("deploy_metrics_daily", "pooled_deploy_hours") + `
        ARRAY JOIN pooled_deploy_hours AS stored_hours
        WHERE pooled_samples_stored = 1
        UNION ALL
        SELECT
            org_id,
            repo_id,
            toDate(deployed_at) AS day,
            if(finished_at >= started_at, dateDiff('millisecond', started_at, finished_at) / 3600000, NULL) AS ` + deliveryHoursSample + `
        FROM deployments
        WHERE org_id = {org_id:String}
          AND toDate(deployed_at) >= {start_date:Date} AND toDate(deployed_at) < {end_date:Date}
          AND ` + deploymentstatus.CanceledExclusionSQL("status") + `
          AND (repo_id, toDate(deployed_at)) IN (` + testopspooled.LegacyRepoDays("deploy_metrics_daily") + `)
    )
    WHERE ` + testopspooled.LiveRepoDays("deploy_metrics_daily") + `
) AS pooled_deployments`

// pullRequestSizesSource holds the size samples of the repo_user_commit writer (repouser.Compute) stored with its newest
// row. Raw rule of a repository-day without stored samples: a pull request counts on the UTC day it merged, its size is
// its additions plus its deletions, and a size of 0 is no sample. The raw rows are read without FINAL, as the writer
// reads them.
var pullRequestSizesSource = `(
    SELECT org_id, repo_id, day, ` + deliveryLOCSample + `
    FROM (
        SELECT org_id, repo_id, day, stored_loc AS ` + deliveryLOCSample + `
        FROM ` + testopspooled.NewestStoredRows("repo_metrics_daily", "pooled_pr_size_loc") + `
        ARRAY JOIN pooled_pr_size_loc AS stored_loc
        WHERE pooled_samples_stored = 1
        UNION ALL
        SELECT
            org_id,
            repo_id,
            toDate(merged_at) AS day,
            toFloat64(ifNull(additions, 0) + ifNull(deletions, 0)) AS ` + deliveryLOCSample + `
        FROM git_pull_requests
        WHERE org_id = {org_id:String}
          AND toDate(merged_at) >= {start_date:Date} AND toDate(merged_at) < {end_date:Date}
          AND ifNull(additions, 0) + ifNull(deletions, 0) > 0
          AND (repo_id, toDate(merged_at)) IN (` + testopspooled.LegacyRepoDays("repo_metrics_daily") + `)
    )
    WHERE ` + testopspooled.LiveRepoDays("repo_metrics_daily") + `
) AS pooled_pull_requests`

// The MTTR sources and the hours expressions live in pooledmttr, which the operating review tile reads too.
const (
	deliveryHoursMedian = pooledmttr.HoursMedian
	deliveryHoursP90    = pooledmttr.HoursP90
	deliveryHoursMean   = pooledmttr.HoursMean
)

var (
	bugRepairsSource      = pooledmttr.BugRepairsSource
	incidentRepairsSource = pooledmttr.IncidentRepairsSource
)

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
