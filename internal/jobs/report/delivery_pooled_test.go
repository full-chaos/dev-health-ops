package report

import (
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/deploymentstatus"
)

// The deploy time, pull request size and MTTR pooled sources hold the daily writers' own rules. Each pooled clause below
// is pinned to the writer text it restates, so a change on either side fails here. The executed cells
// (delivery_pooled_registry_integration_test.go) hold the two to one value.
func TestDeliveryPooledRulesAreTheWritersOwn(t *testing.T) {
	t.Parallel()
	read := func(path string) string {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	writers := map[string]string{
		"deploy loader":    read("../metrics/daily/deploy_native_clickhouse.go"),
		"deploy compute":   read("../metrics/numerical/deploy.go"),
		"repouser compute": read("../metrics/daily/repouser/compute.go"),
		"repouser loader":  read("../metrics/daily/repouser/clickhouse.go"),
		"repouser quality": read("../metrics/daily/repouser/quality.go"),
		"incident loader":  read("../metrics/daily/incident_native_clickhouse.go"),
		"incident compute": read("../metrics/daily/incident_native_executor.go"),
	}
	pins := []struct{ rule, writer, writerText, source, pooled string }{
		{"a deployment counts on the day it deployed", "deploy loader",
			"AND deployed_at >= {start:DateTime64(3, 'UTC')} AND deployed_at < {end:DateTime64(3, 'UTC')}", deploymentsSource, "toDate(deployed_at) AS day"},
		{"a cancelled deployment is no deployment", "deploy loader",
			"AND ` + deploymentstatus.CanceledExclusionSQL(\"status\")", deploymentsSource, "AND " + deploymentstatus.CanceledExclusionSQL("status")},
		{"deploy duration sample", "deploy compute",
			"duration := row.FinishedAt.UTC().Sub(row.StartedAt.UTC()).Hours()\n\t\t\tif duration >= 0 {", deploymentsSource,
			"if(finished_at >= started_at, dateDiff('millisecond', started_at, finished_at) / 3600000, NULL) AS " + deliveryHoursSample},
		{"the deployment rows of the legacy arm (raw: the writer reads FINAL, the legacy arm stands for days written before it did)", "deploy loader", "\nFROM deployments FINAL\nWHERE org_id = {org_id:String}", deploymentsSource,
			"        FROM deployments\n        WHERE org_id = {org_id:String}"},
		{"the pull request rows of the legacy arm (raw: the writer reads FINAL, the legacy arm stands for days written before it did)", "repouser loader", "FROM git_pull_requests FINAL\nWHERE\n", pullRequestSizesSource,
			"        FROM git_pull_requests\n        WHERE org_id = {org_id:String}"},
		{"deploy median", "deploy compute", "value := deployPercentile(bucket.durations, 50.0)", deliveryHoursMedian, "quantileExactInclusive(0.5)("},
		{"a pull request counts on the day it merged", "repouser compute",
			"if mergedAt.Before(start) || !mergedAt.Before(end) {", pullRequestSizesSource, "toDate(merged_at) AS day"},
		{"pull request size sample", "repouser compute",
			"additions := maxInt(0, pr.Additions)\n\t\tdeletions := maxInt(0, pr.Deletions)\n\t\ttotalLOC := additions + deletions", pullRequestSizesSource,
			"toFloat64(ifNull(additions, 0) + ifNull(deletions, 0)) AS " + deliveryLOCSample},
		{"a size of 0 is no sample", "repouser compute", "if totalLOC > 0 {\n\t\t\trepoPRSizes[pr.RepoID]", pullRequestSizesSource,
			"AND ifNull(additions, 0) + ifNull(deletions, 0) > 0"},
		{"pull request size median", "repouser compute", "prSizeP50 = ptr(percentile(repoSizes, 50.0))", deliveryLOCMedian, "quantileExactInclusive(0.5)("},
		{"pull request size p90", "repouser compute", "prSizeP90 = ptr(percentile(repoSizes, 90.0))", deliveryLOCP90, "quantileExactInclusive(0.9)("},
		{"a repair is a bug with both lifecycle times", "repouser loader",
			"FROM work_items FINAL\nWHERE type = 'bug' AND repo_id IN {repo_ids:Array(UUID)} AND org_id = {org_id:String}\n  AND started_at IS NOT NULL AND completed_at IS NOT NULL",
			bugRepairsSource, "FROM work_items FINAL"},
		{"a repair is a bug with both lifecycle times", "repouser loader", "AND started_at IS NOT NULL AND completed_at IS NOT NULL", bugRepairsSource,
			"AND type = 'bug' AND started_at IS NOT NULL AND completed_at IS NOT NULL"},
		{"a repair counts on the day it completed", "repouser quality", "if completedAt.Before(start) || !completedAt.Before(end) {", bugRepairsSource,
			"toDate(completed_at, 'UTC') AS day"},
		{"repair sample, a negative one included", "repouser quality", "hours := completedAt.Sub(item.StartedAt.UTC()).Seconds() / 3600.0\n", bugRepairsSource,
			"dateDiff('millisecond', started_at, completed_at) / 3600000 AS " + deliveryHoursSample},
		{"mttr_hours is the mean", "repouser quality", "result[repoID] = mean(hours)", deliveryHoursMean, "avg(" + deliveryHoursSample + ")"},
		{"a deleted incident is no incident", "incident loader", `"is_deleted = 0",`, incidentRepairsSource, "WHERE is_deleted = 0 AND"},
		{"an active mapping", "incident loader", `"is_active = 1",`, incidentRepairsSource, "AND is_active = 1 AND"},
		{"a mapping valid from the instant", "incident loader", `"(valid_from IS NULL OR valid_from <= {as_of:DateTime64(6, 'UTC')})"`, incidentRepairsSource,
			"(valid_from IS NULL OR valid_from <= {as_of:DateTime64(6, 'UTC')})"},
		{"a mapping valid to the instant", "incident loader", `"(valid_to IS NULL OR valid_to > {as_of:DateTime64(6, 'UTC')})"`, incidentRepairsSource,
			"(valid_to IS NULL OR valid_to > {as_of:DateTime64(6, 'UTC')})"},
		{"an incident joins its service's repositories", "incident loader",
			"ON incident.org_id = mapping.org_id AND incident.service_id = mapping.service_id\n    INNER JOIN repos AS repo FINAL", incidentRepairsSource,
			"ON incident.org_id = mapping.org_id AND incident.service_id = mapping.service_id\n            INNER JOIN repos AS repo FINAL"},
		{"an incident counts once per repository", "incident loader", "LIMIT 1 BY mapping.repo_id, incident.id", incidentRepairsSource, "LIMIT 1 BY repo_id, incident_id"},
		{"an incident counts when resolved on the day it started", "incident compute",
			"if resolvedAt.Before(dayStart) || !resolvedAt.Before(dayEnd) {", incidentRepairsSource, "toDate(resolved_at) = toDate(started_at)"},
		{"incident repair sample", "incident compute", "mttr := resolvedAt.Sub(row.StartedAt.UTC()).Hours()\n\t\tif mttr >= 0 {", incidentRepairsSource,
			"if(incident.resolved_at >= incident.started_at,\n                    dateDiff('microsecond', incident.started_at, incident.resolved_at) / 3600000000, NULL) AS " + deliveryHoursSample},
		{"incident median", "incident compute", "p50 := incidentPercentile(b.mttrHours, 50.0)", deliveryHoursMedian, "quantileExactInclusive(0.5)("},
		{"incident p90", "incident compute", "p90 := incidentPercentile(b.mttrHours, 90.0)", deliveryHoursP90, "quantileExactInclusive(0.9)("},
		{"the stored deploy samples are the median's", "deploy compute", "DeployHoursSamples:     append([]float64{}, bucket.durations...)", deploymentsSource,
			"ARRAY JOIN pooled_deploy_hours AS stored_hours"},
		{"the stored pull request sizes are the percentiles'", "repouser compute", "PRSizeLOCSamples:           append([]float64{}, repoSizes...)", pullRequestSizesSource,
			"ARRAY JOIN pooled_pr_size_loc AS stored_loc"},
		{"the stored repair samples are the mean's", "repouser quality", "hoursByRepo := RepairHoursByRepo(day, bugItems)", bugRepairsSource,
			"ARRAY JOIN pooled_bug_repair_hours AS stored_hours"},
		{"the stored incident samples are the percentiles'", "incident compute", "record.RepairHoursSamples = append([]float64{}, b.mttrHours...)", incidentRepairsSource,
			"ARRAY JOIN pooled_repair_hours AS stored_hours"},
	}
	for _, pin := range pins {
		if !strings.Contains(writers[pin.writer], pin.writerText) {
			t.Errorf("%s: the %s no longer holds %q: move the pooled rule with it", pin.rule, pin.writer, pin.writerText)
		}
		if !strings.Contains(pin.source, pin.pooled) {
			t.Errorf("%s: the pooled text no longer holds %q", pin.rule, pin.pooled)
		}
	}
	// Every percentile kernel is linear between closest ranks at rank (n-1)*p: quantileExactInclusive.
	for _, writer := range []string{"deploy compute", "repouser compute", "incident compute"} {
		if !strings.Contains(writers[writer], "(len(sorted)-1) * (") {
			t.Errorf("the %s percentile is no longer linear between closest ranks", writer)
		}
	}
}

// Each pooled statement reads only the repository-days its writer computed, under the pooled row bound, and binds what
// its source names.
func TestDeliveryPooledStatementsReadComputedDaysUnderTheBound(t *testing.T) {
	t.Parallel()
	for metric, want := range map[string]struct{ table, binding string }{
		"deploy_time_p50_hours": {"deploy_metrics_daily", "canceled_statuses"},
		"pr_size_p50_loc":       {"repo_metrics_daily", ""},
		"pr_size_p90_loc":       {"repo_metrics_daily", ""},
		"mttr_hours":            {"repo_metrics_daily", ""},
		"mttr_p50_hours":        {"incident_metrics_daily", "as_of"},
		"mttr_p90_hours":        {"incident_metrics_daily", "as_of"},
	} {
		statement, parameters, err := buildChartQuery(ChartSpec{ChartType: "bar", Metric: metric, OrganizationID: "org-1",
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-06"}, pooledDefinition(t, metric))
		if err != nil {
			t.Fatal(err)
		}
		for _, text := range []string{"FROM " + want.table + "\n", "WHERE pooled_samples_stored = 1", "WHERE pooled_samples_stored != 1", "max_rows_to_read = 5000000, read_overflow_mode = 'throw'", "HAVING y IS NOT NULL"} {
			if !strings.Contains(statement, text) {
				t.Errorf("%s: the statement has no %q:\n%s", metric, text, statement)
			}
		}
		count := 3
		if want.binding != "" {
			count = 4
			if !strings.Contains(statement, "{"+want.binding+":") {
				t.Errorf("%s: the statement names no %s", metric, want.binding)
			}
		}
		if len(parameters) != count {
			t.Errorf("%s: %d parameters, want %d", metric, len(parameters), count)
		}
	}
}
