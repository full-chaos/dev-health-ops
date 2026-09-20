package datahealth

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

const deploymentCoverageSQL = `
SELECT coalesce(nullIf(r.repo, ''), toString(d.repo_id)) AS repo_name,
       count() AS total,
       countIf(d.pull_request_number IS NOT NULL OR d.release_ref != '') AS covered
FROM deployments d
LEFT JOIN repos r ON r.id = d.repo_id AND r.org_id = d.org_id
WHERE d.org_id = {org_id:String}
GROUP BY repo_name
ORDER BY repo_name`

const workItemCoverageSQL = `
SELECT coalesce(nullIf(r.repo, ''), toString(w.repo_id)) AS repo_name,
       count() AS total,
       countIf(w.provider != '' AND w.project_key != '') AS covered
FROM work_items AS w FINAL
LEFT JOIN repos r ON r.id = w.repo_id AND r.org_id = w.org_id
WHERE w.org_id = {org_id:String}
GROUP BY repo_name
ORDER BY repo_name`

type coverageRow struct {
	repoName string
	covered  uint64
}

// MappingCoverage ports resolve_mapping_coverage: per-repo coverage of the
// deployment-to-release/PR mapping and of the work-item project/provider
// mapping. A repo is covered when at least one of its rows is mapped.
func (r *Reader) MappingCoverage(ctx context.Context, orgID string) *model.MappingCoverage {
	deployments := r.coverageRows(ctx, "coverage_deployments", deploymentCoverageSQL, orgID)
	workItems := r.coverageRows(ctx, "coverage_work_items", workItemCoverageSQL, orgID)
	return &model.MappingCoverage{
		Deployments: coverageStat(deployments, "No deployment-to-release/PR mapping"),
		WorkItems:   coverageStat(workItems, "No work-item project/provider mapping"),
	}
}

func (r *Reader) coverageRows(ctx context.Context, section, statement, orgID string) []coverageRow {
	var out []coverageRow
	r.queryRows(ctx, section, statement, []clickhouse.Binding{{Name: "org_id", Value: orgID}},
		func(rows clickhouse.RowScanner) error {
			var name string
			var total, covered uint64
			if err := rows.Scan(&name, &total, &covered); err != nil {
				return err
			}
			out = append(out, coverageRow{repoName: name, covered: covered})
			return nil
		})
	return out
}

func coverageStat(rows []coverageRow, reason string) *model.CoverageStat {
	total := len(rows)
	covered := 0
	missing := []model.MissingMapping{}
	for _, row := range rows {
		if row.covered > 0 {
			covered++
			continue
		}
		name := row.repoName
		if name == "" {
			name = "unknown"
		}
		missing = append(missing, model.MissingMapping{RepoName: name, Reason: reason})
	}
	pct := 100.0
	if total > 0 {
		pct = float64(covered) / float64(total) * 100.0
	}
	return &model.CoverageStat{TotalRepos: total, CoveredRepos: covered, CoveragePct: pct, Missing: missing}
}

// windowSpec is one lineage registry window.
type windowSpec struct {
	kind         string
	durationDays *int
}

type lineageEntry struct {
	tables []string
	window windowSpec
}

func days(n int) *int { return &n }

// lineageRegistry is the metric-to-source-tables registry (ported from the
// deleted Python METRIC_LINEAGE_REGISTRY).
var lineageRegistry = map[string]lineageEntry{
	"throughput":           {[]string{"work_item_metrics_daily"}, windowSpec{kind: "daily"}},
	"cycle_time":           {[]string{"work_item_metrics_daily"}, windowSpec{kind: "daily"}},
	"lead_time":            {[]string{"work_item_metrics_daily"}, windowSpec{kind: "daily"}},
	"wip":                  {[]string{"work_item_metrics_daily"}, windowSpec{kind: "daily"}},
	"review_load":          {[]string{"repo_metrics_daily"}, windowSpec{kind: "daily"}},
	"review_latency":       {[]string{"repo_metrics_daily"}, windowSpec{kind: "daily"}},
	"deployment_frequency": {[]string{"repo_metrics_daily"}, windowSpec{kind: "daily"}},
	"change_failure_rate":  {[]string{"repo_metrics_daily"}, windowSpec{kind: "daily"}},
	"after_hours_ratio":    {[]string{"team_metrics_daily"}, windowSpec{kind: "daily"}},
	"weekend_ratio":        {[]string{"team_metrics_daily"}, windowSpec{kind: "daily"}},
	"investment_mix":       {[]string{"work_unit_investments"}, windowSpec{kind: "rolling", durationDays: days(30)}},
}

// MetricLineage ports compute_metric_lineage: the freshness of the tables a
// metric is computed from. An unknown metric, or one whose tables all failed
// to answer, has no lineage. A table that answers with no rows reports the
// engine's zero time, as the Python read does.
func (r *Reader) MetricLineage(ctx context.Context, orgID, metricID string) *model.MetricLineage {
	entry, ok := lineageRegistry[metricID]
	if !ok {
		return nil
	}
	var newest time.Time
	found := false
	totalRows := 0
	for _, table := range entry.tables {
		statement := "\nSELECT argMax(computed_at, computed_at) AS computed_at, count() AS row_count\nFROM " + table + "\nWHERE org_id = {org_id:String}"
		r.queryRows(ctx, "lineage_"+table, statement, []clickhouse.Binding{{Name: "org_id", Value: orgID}},
			func(rows clickhouse.RowScanner) error {
				var computed *time.Time
				var count uint64
				if err := rows.Scan(&computed, &count); err != nil {
					return err
				}
				if computed != nil {
					if !found || computed.After(newest) {
						newest = *computed
					}
					found = true
				}
				totalRows += int(count)
				return nil
			})
	}
	if !found {
		return nil
	}
	rowCount := totalRows
	return &model.MetricLineage{
		MetricID:      metricID,
		SourceTables:  append([]string(nil), entry.tables...),
		ComputeWindow: &model.WindowSpec{Kind: entry.window.kind, DurationDays: entry.window.durationDays},
		ComputedAt:    newest.UTC(),
		RowCount:      &rowCount,
	}
}
