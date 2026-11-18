package report

import (
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/cicd"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// The CI/CD pooled source holds the cicd daily writer's own rules. The writer's run rules are SQL (cicd.PipelineRunsSource)
// and its sample rules Go (compute.go); each pooled clause below is pinned to the writer text it restates, so a change on
// either side fails here. The executed cells (cicd_pooled_registry_integration_test.go) hold the two to one value.
func TestCicdPooledRulesAreTheWritersOwn(t *testing.T) {
	t.Parallel()
	compute, err := os.ReadFile("../metrics/daily/cicd/compute.go")
	if err != nil {
		t.Fatal(err)
	}
	writerCompute := string(compute)
	deleted := "lowerUTF8(trimBoth(ifNull(status, ''))) != '" + testops.StatusDeleted + "'"
	if cicdCountedRunPredicate != deleted {
		t.Fatalf("pooled predicate %q, want the writer's %q", cicdCountedRunPredicate, deleted)
	}
	pins := []struct{ rule, writer, writerText, pooled string }{
		{"newest version of a run", "PipelineRunsSource", "FROM ci_pipeline_runs FINAL", "FROM ci_pipeline_runs FINAL"},
		{"a deleted run is no run", "PipelineRunsSource", "AND " + deleted, "AND " + cicdCountedRunPredicate},
		{"the run finishes on the day", "PipelineRunsSource",
			"WHERE finished_at >= {start:DateTime64(3, 'UTC')} AND finished_at < {end:DateTime64(3, 'UTC')}",
			"AND finished_at IS NOT NULL AND toDate(finished_at) = toDate(started_at)"},
		{"the run starts on the day", "PipelineRunsSource",
			"AND started_at >= {start:DateTime64(3, 'UTC')} AND started_at < {end:DateTime64(3, 'UTC')}",
			"toDate(started_at) AS day"},
		{"duration sample", "compute.go", "durationMin := row.FinishedAt.UTC().Sub(startedAt).Minutes()\n\t\t\tif durationMin >= 0 {",
			"if(finished_at >= started_at, dateDiff('millisecond', started_at, finished_at) / 60000, NULL) AS " + cicdRunMinutes},
		{"queue sample", "compute.go", "if row.QueuedAt != nil {\n\t\t\tqueueMin := startedAt.Sub(row.QueuedAt.UTC()).Minutes()\n\t\t\tif queueMin >= 0 {",
			"if(queued_at IS NOT NULL AND started_at >= queued_at, dateDiff('millisecond', queued_at, started_at) / 60000, NULL) AS " + cicdQueueMinutes},
	}
	for _, pin := range pins {
		writer := cicd.PipelineRunsSource
		if pin.writer == "compute.go" {
			writer = writerCompute
		}
		if !strings.Contains(writer, pin.writerText) {
			t.Errorf("%s: the writer's %s no longer holds %q: move the pooled rule with it", pin.rule, pin.writer, pin.writerText)
		}
		if !strings.Contains(cicdRunsSource, pin.pooled) {
			t.Errorf("%s: the pooled source no longer holds %q", pin.rule, pin.pooled)
		}
	}
	aggregates := []struct{ writerText, pooled string }{
		{"avgDuration = ptr(mean(bucket.durations))", cicdDurationMean},
		{"p90Duration = ptr(percentile(bucket.durations, 90.0))", cicdDurationP90},
		{"avgQueue = ptr(mean(bucket.queues))", cicdQueueMean},
		// The writer's percentile is linear between closest ranks at rank (n-1)*p: quantileExactInclusive.
		{"rank := float64(float64(len(sorted)-1) * (pct / 100.0))", "quantileExactInclusive(0.9)(" + cicdRunMinutes + ")"},
	}
	for _, aggregate := range aggregates {
		if !strings.Contains(writerCompute, aggregate.writerText) {
			t.Errorf("the writer's compute.go no longer holds %q", aggregate.writerText)
		}
		if !strings.Contains(cicdDurationMean+cicdDurationP90+cicdQueueMean, aggregate.pooled) {
			t.Errorf("no pooled expression holds %q", aggregate.pooled)
		}
	}
	// The writer reads only the timestamps: a stored duration or queue column is no sample.
	for _, column := range []string{"duration_seconds", "queue_seconds", "DurationSeconds", "QueueSeconds"} {
		if strings.Contains(writerCompute, column) || strings.Contains(cicd.PipelineRunsSource, column) {
			t.Errorf("the writer now reads %s: the pooled source does not", column)
		}
		if strings.Contains(cicdRunsSource, column) {
			t.Errorf("the pooled source reads %s, the writer does not", column)
		}
	}
	for status, excluded := range map[string]bool{"deleted": true, " Deleted ": true, "DELETED": true, "success": false, "": false, "cancelled": false} {
		if testops.ExcludedFromPipelineCounts(status) != excluded {
			t.Fatalf("ExcludedFromPipelineCounts(%q) = %v: the writer's rule moved, move the pinned predicate with it", status, !excluded)
		}
	}
}

// The statement reads only the repository-days the writer computed, under the pooled row bound.
func TestCicdPooledStatementReadsComputedDaysUnderTheBound(t *testing.T) {
	t.Parallel()
	for _, metric := range []string{"avg_duration_minutes", "p90_duration_minutes", "avg_queue_minutes"} {
		statement, _, err := buildChartQuery(ChartSpec{ChartType: "bar", Metric: metric, OrganizationID: "org-1",
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-06"}, pooledDefinition(t, metric))
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"SELECT repo_id, day FROM cicd_metrics_daily", "max_rows_to_read = 5000000, read_overflow_mode = 'throw'", "HAVING y IS NOT NULL"} {
			if !strings.Contains(statement, want) {
				t.Errorf("%s: the statement has no %q:\n%s", metric, want, statement)
			}
		}
	}
}

// A metric without the team (or service, or repo) dimension is charted as buildChartQuery charts it: a group by it is one
// 'unscoped' bucket and a filter on it does not narrow. A metric with the dimension keeps the owner join. No registry
// pooled metric lacks the team dimension, so the cell takes one without it.
func TestPooledChartKeepsTheRegistryDimensions(t *testing.T) {
	t.Parallel()
	noTeam := pooledDefinition(t, "avg_duration_minutes")
	noTeam.Dimensions = []string{"repo", "day"}
	for _, groupBy := range []string{"team", "service"} {
		for _, teams := range [][]string{nil, {"team-1"}} {
			statement, parameters, err := buildChartQuery(ChartSpec{ChartType: "bar", Metric: "avg_duration_minutes", GroupBy: groupBy,
				OrganizationID: "org-1", FilterTeams: teams}, noTeam)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(statement, "'unscoped' AS x") || strings.Contains(statement, "team_repo_ownership") || strings.Contains(statement, "service_id, ''") {
				t.Errorf("group %s, teams %v: not one unscoped bucket:\n%s", groupBy, teams, statement)
			}
			if len(parameters) != 3 {
				t.Errorf("group %s, teams %v: %d parameters, want org and the two dates only", groupBy, teams, len(parameters))
			}
		}
	}
	statement, _, err := buildChartQuery(ChartSpec{ChartType: "bar", Metric: "avg_duration_minutes", OrganizationID: "org-1",
		FilterTeams: []string{"team-1"}}, noTeam)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(statement, "team_repo_ownership") {
		t.Errorf("a team filter narrows a metric with no team dimension:\n%s", statement)
	}
	statement, _, err = buildChartQuery(ChartSpec{ChartType: "bar", Metric: "avg_duration_minutes", GroupBy: "repo", OrganizationID: "org-1",
		FilterRepos: []string{"repo-1"}}, pooledDefinition(t, "avg_duration_minutes"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(statement, "toString(repo_id) AS x") || !strings.Contains(statement, "toString(repo_id) IN {filter_repos:Array(String)}") {
		t.Errorf("the repo dimension is not read:\n%s", statement)
	}
	for _, metric := range []string{"p95_duration_seconds", "suite_duration_p95_seconds"} {
		grouped, _, err := buildChartQuery(ChartSpec{ChartType: "bar", Metric: metric, GroupBy: "team", OrganizationID: "org-1"}, pooledDefinition(t, metric))
		if err != nil {
			t.Fatal(err)
		}
		filtered, _, err := buildChartQuery(ChartSpec{ChartType: "bar", Metric: metric, OrganizationID: "org-1", FilterTeams: []string{"team-1"}}, pooledDefinition(t, metric))
		if err != nil {
			t.Fatal(err)
		}
		service, _, err := buildChartQuery(ChartSpec{ChartType: "bar", Metric: metric, GroupBy: "service", OrganizationID: "org-1"}, pooledDefinition(t, metric))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(grouped, "own.owner_team_id AS x") || !strings.Contains(filtered, "team_repo_ownership") || !strings.Contains(service, "ifNull(service_id, '') AS x") {
			t.Errorf("%s lost a dimension it has", metric)
		}
	}
}
