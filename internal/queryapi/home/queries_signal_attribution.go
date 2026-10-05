package home

import (
	"context"
	"fmt"
	"sort"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// signalAttributionSQL counts the latest primary attribution for each
// in-window work item. The work_items join supplies repository scope because
// work_item_cycle_times is not repository keyed. It joins on provider too:
// two providers may both use the same external work-item identifier.
//
// The attribution subquery determines the latest record before its primary
// flag is applied. A later non-primary record therefore truthfully leaves the
// work item without current primary attribution instead of reviving an older
// primary row. FINAL is required for every ReplacingMergeTree read, and every
// such read carries its own org predicate at the same depth.
const signalAttributionSQL = `
    SELECT
        a.source AS source,
        a.confidence AS confidence,
        toInt64(uniqExact(tuple(wct.provider, wct.work_item_id))) AS items
    FROM work_item_cycle_times AS wct FINAL
    INNER JOIN work_items AS wi FINAL
      ON wi.work_item_id = wct.work_item_id
     AND wi.provider = wct.provider
    INNER JOIN (
        SELECT
            provider,
            work_item_id,
            source,
            confidence
        FROM work_item_team_attributions FINAL
        WHERE org_id = {org_id:String}
          AND is_primary = 1
          AND (provider, work_item_id, computed_at) IN (
              SELECT
                  provider,
                  work_item_id,
                  max(computed_at)
              FROM work_item_team_attributions FINAL
              WHERE org_id = {org_id:String}
              GROUP BY provider, work_item_id
          )
    ) AS a
      ON a.work_item_id = wct.work_item_id
     AND a.provider = wct.provider
    WHERE wct.org_id = {org_id:String}
      AND wi.org_id = {org_id:String}
      AND wct.day >= {start_day:Date}
      AND wct.day < {end_day:Date}%s
    GROUP BY a.source, a.confidence
    ORDER BY a.source ASC, a.confidence ASC
    SETTINGS max_execution_time = 30
`

// fetchSignalAttribution returns nil when no in-window work item has a
// current primary attribution. UNASSIGNED and NONE remain real buckets, not
// an absent result. For a team scope, repoScopeFilter resolves ownership via
// teamscope.RepoCondition against wi.repo_id; it never uses legacy team_id
// fields on a metric row.
func fetchSignalAttribution(ctx context.Context, client QueryClient, f Filters, startDay, endDay time.Time, orgID string, asOf time.Time) (*SignalAttribution, error) {
	scopeFilter, scopeBindings, err := repoScopeFilter(ctx, client, f, orgID, "toString(wi.repo_id)", asOf)
	if err != nil {
		return nil, fmt.Errorf("home: resolve signal attribution scope: %w", err)
	}

	bindings := append([]dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
	}, scopeBindings...)
	rows, err := client.Query(ctx, fmt.Sprintf(signalAttributionSQL, scopeFilter), bindings)
	if err != nil {
		return nil, fmt.Errorf("home: fetch signal attribution query: %w", err)
	}
	defer rows.Close()

	sourceItems := make(map[string]int)
	confidenceItems := make(map[string]int)
	total := 0
	for rows.Next() {
		var source, confidence string
		var items int64
		if err := rows.Scan(&source, &confidence, &items); err != nil {
			return nil, fmt.Errorf("home: scan signal attribution row: %w", err)
		}
		if items < 0 {
			return nil, fmt.Errorf("home: signal attribution returned negative item count: %d", items)
		}
		if items == 0 {
			continue
		}
		count := int(items)
		sourceItems[source] += count
		confidenceItems[confidence] += count
		total += count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("home: fetch signal attribution rows: %w", err)
	}
	if total == 0 {
		return nil, nil
	}

	attribution := &SignalAttribution{
		Items:      total,
		Sources:    signalAttributionSourceCounts(sourceItems, total),
		Confidence: signalAttributionConfidenceCounts(confidenceItems, total),
	}
	return attribution, nil
}

func signalAttributionSourceCounts(counts map[string]int, total int) []SignalAttributionSourceCount {
	keys := sortedSignalAttributionKeys(counts)
	out := make([]SignalAttributionSourceCount, 0, len(keys))
	for _, key := range keys {
		out = append(out, SignalAttributionSourceCount{
			Source: key,
			Items:  counts[key],
			Share:  float64(counts[key]) / float64(total),
		})
	}
	return out
}

func signalAttributionConfidenceCounts(counts map[string]int, total int) []SignalAttributionConfidenceCount {
	keys := sortedSignalAttributionKeys(counts)
	out := make([]SignalAttributionConfidenceCount, 0, len(keys))
	for _, key := range keys {
		out = append(out, SignalAttributionConfidenceCount{
			Confidence: key,
			Items:      counts[key],
			Share:      float64(counts[key]) / float64(total),
		})
	}
	return out
}

func sortedSignalAttributionKeys(counts map[string]int) []string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
