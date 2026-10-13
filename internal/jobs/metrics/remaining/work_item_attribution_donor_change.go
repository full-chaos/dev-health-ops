package remaining

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining/stepcause"
	"github.com/full-chaos/dev-health-ops/internal/teamactive"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// workItemAttributionNewestPrimary is an item's newest primary row
// (work_item_team_attributions, by computed_at).
type workItemAttributionNewestPrimary struct {
	TeamID, Source, Evidence string
	ComputedAt               time.Time
}

// loadDonorChangedDependents returns the items whose linked_issue row is owed a
// rederive because a donor's row changed after it (CHAOS-9478): a linked item
// copies its donor's stored row, and a donor's row also changes for reasons no
// ownership watermark sees (the item moved to another project, its native team
// key changed), written by the daily family or the provider sync. The trigger
// reads the stored rows on every run, so it needs no watermark: after the
// rederive the item's row is newer than every donor row and the next run finds
// nothing (donorChangedDependents).
func (executor *WorkItemAttributionExecutor) loadDonorChangedDependents(
	ctx context.Context, orgID string,
) ([]string, error) {
	edges, err := executor.loadInheritableDependencyEdges(ctx, orgID)
	if err != nil {
		return nil, err
	}
	if len(edges) == 0 {
		return nil, nil
	}
	idSet := map[string]struct{}{}
	keySet := map[string]struct{}{}
	for _, edge := range edges {
		idSet[edge.SourceWorkItemID] = struct{}{}
		if key, ok := strings.CutPrefix(edge.TargetWorkItemID, "extkey:"); ok {
			keySet[strings.ToUpper(strings.TrimSpace(key))] = struct{}{}
		} else {
			idSet[edge.TargetWorkItemID] = struct{}{}
		}
	}
	keyIndex := map[string]string{}
	if len(keySet) > 0 {
		keyed, err := executor.loadDonorSubjects(ctx, orgID, nil, sortedWorkItemAttributionKeys(keySet))
		if err != nil {
			return nil, err
		}
		keyIndex = workItemAttributionKeyIndex(keyed)
		for _, id := range keyIndex {
			idSet[id] = struct{}{}
		}
	}
	rows, err := executor.loadNewestPrimaryRows(ctx, orgID, sortedWorkItemAttributionKeys(idSet))
	if err != nil {
		return nil, err
	}
	inactive, err := teamactive.LoadInactive(ctx, executor.conn, orgID)
	if err != nil {
		return nil, stepcause.Failure(stepcause.LoadInactiveTeamsDonorChange, err)
	}
	return donorChangedDependents(edges, keyIndex, rows, inactive), nil
}

// donorChangedDependents returns, sorted, each item that inherits through an
// inheritable dependency edge and is owed a rederive: one of its donors has a
// newest primary row newer than the item's own, and the donor the item would
// copy now differs from the one its newest primary row records. The donor it
// would copy is the cascade's winner (BuildLinkedIssueIndexFromRows): of its
// donors whose newest primary row has a team of its own (an own source) that
// is active, the lowest target id, then relationship, with that row's team. An
// item whose own primary row has a team of its own (an own source: native
// team, project or repository ownership) outranks linked_issue, so a donor
// never gives it its primary row. A donor recomputed
// with the same team triggers nothing, and a rederive makes the item newer than
// its donors, so a trigger never repeats for the same donor row.
func donorChangedDependents(
	edges []teamattribution.GithubWorkItemDerivationDependencyEdge,
	keyIndex map[string]string,
	rows map[string]workItemAttributionNewestPrimary,
	inactive teamactive.Inactive,
) []string {
	type donor struct{ target, relationship string }
	donors := map[string][]donor{}
	for _, edge := range teamattribution.LatestGitHubWorkItemDerivationDependencies(edges) {
		if !teamattribution.GithubWorkItemDerivationInheritableRelationships[edge.RelationshipType] {
			continue
		}
		target := edge.TargetWorkItemID
		if key, ok := strings.CutPrefix(target, "extkey:"); ok {
			target = keyIndex[strings.ToUpper(strings.TrimSpace(key))]
		}
		if target == "" || target == edge.SourceWorkItemID {
			continue
		}
		donors[edge.SourceWorkItemID] = append(donors[edge.SourceWorkItemID], donor{target, edge.RelationshipType})
	}
	var owed []string
	for item, linked := range donors {
		own, ok := rows[item]
		if !ok || teamattribution.IsLinkedIssueDonorSource(own.Source) {
			continue
		}
		sort.Slice(linked, func(left, right int) bool {
			if linked[left].target != linked[right].target {
				return linked[left].target < linked[right].target
			}
			return linked[left].relationship < linked[right].relationship
		})
		changed, wantDonor, wantTeam := false, "", ""
		for _, candidate := range linked {
			row, ok := rows[candidate.target]
			if !ok {
				continue
			}
			changed = changed || row.ComputedAt.After(own.ComputedAt)
			if wantDonor == "" && row.TeamID != "" && teamattribution.IsLinkedIssueDonorSource(row.Source) && !inactive.Has(row.TeamID) {
				wantDonor, wantTeam = candidate.target, row.TeamID
			}
		}
		if !changed {
			continue
		}
		recordedDonor, recordedTeam := "", ""
		if own.Source == "linked_issue" {
			recordedDonor, recordedTeam = linkedIssueEvidenceDonor(own.Evidence), own.TeamID
		}
		if wantDonor != recordedDonor || wantTeam != recordedTeam {
			owed = append(owed, item)
		}
	}
	sort.Strings(owed)
	return owed
}

// linkedIssueEvidenceDonor is the donor id a linked_issue row's evidence names:
// "linked_issue=<donor id>[;...]".
func linkedIssueEvidenceDonor(evidence string) string {
	donor, ok := strings.CutPrefix(evidence, "linked_issue=")
	if !ok {
		return ""
	}
	donor, _, _ = strings.Cut(donor, ";")
	return donor
}

// workItemAttributionKeyIndex maps an issue key to its one Jira or Linear item,
// as BuildLinkedIssueIndexFromRows resolves an extkey: target; a key that two
// items hold resolves to none.
func workItemAttributionKeyIndex(subjects map[string]teamattribution.GithubWorkItemDerivationSubject) map[string]string {
	index := map[string]string{}
	ambiguous := map[string]bool{}
	for _, subject := range subjects {
		if (subject.Provider != "linear" && subject.Provider != "jira") || !strings.Contains(subject.WorkItemID, ":") {
			continue
		}
		_, suffix, _ := strings.Cut(subject.WorkItemID, ":")
		key := strings.ToUpper(strings.TrimSpace(suffix))
		if ambiguous[key] {
			continue
		}
		if existing, exists := index[key]; exists && existing != subject.WorkItemID {
			delete(index, key)
			ambiguous[key] = true
			continue
		}
		index[key] = subject.WorkItemID
	}
	return index
}

func sortedWorkItemAttributionKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// loadInheritableDependencyEdges reads every dependency edge of the
// organization; the inheritable relationships are kept by
// donorChangedDependents. One statement with no list of ids.
func (executor *WorkItemAttributionExecutor) loadInheritableDependencyEdges(
	ctx context.Context, orgID string,
) ([]teamattribution.GithubWorkItemDerivationDependencyEdge, error) {
	rows, err := executor.conn.Query(ctx, `
SELECT source_work_item_id, target_work_item_id, relationship_type, last_synced
FROM work_item_dependencies FINAL
WHERE org_id = ?`, orgID)
	if err != nil {
		return nil, stepcause.Failure(stepcause.QueryWorkItemDependenciesDonorChange, err)
	}
	defer func() { _ = rows.Close() }()
	var edges []teamattribution.GithubWorkItemDerivationDependencyEdge
	for rows.Next() {
		var edge teamattribution.GithubWorkItemDerivationDependencyEdge
		if err := rows.Scan(&edge.SourceWorkItemID, &edge.TargetWorkItemID, &edge.RelationshipType, &edge.LastSynced); err != nil {
			return nil, stepcause.Failure(stepcause.ScanWorkItemDependenciesRowDonorChange, err)
		}
		edge.OrgID = orgID
		edges = append(edges, edge)
	}
	return edges, stepcause.Failure(stepcause.IterateWorkItemDependenciesDonorChange, rows.Err())
}

// loadNewestPrimaryRows reads the newest primary row of each item of ids, one
// statement per bounded chunk (workItemAttributionMaxArrayBytes).
func (executor *WorkItemAttributionExecutor) loadNewestPrimaryRows(
	ctx context.Context, orgID string, ids []string,
) (map[string]workItemAttributionNewestPrimary, error) {
	result := map[string]workItemAttributionNewestPrimary{}
	for _, chunk := range chunkStringsByRenderedBytes(ids, workItemAttributionMaxArrayBytes) {
		rows, err := executor.conn.Query(ctx, `
SELECT work_item_id,
       argMax(ifNull(team_id, ''), computed_at),
       argMax(toString(source), computed_at),
       argMax(evidence, computed_at),
       max(computed_at)
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND has(?, work_item_id) AND is_primary = 1
GROUP BY work_item_id`, orgID, chunk)
		if err != nil {
			return nil, stepcause.Failure(stepcause.QueryNewestPrimaryRowsDonorChange, err)
		}
		for rows.Next() {
			var id string
			var row workItemAttributionNewestPrimary
			if err := rows.Scan(&id, &row.TeamID, &row.Source, &row.Evidence, &row.ComputedAt); err != nil {
				_ = rows.Close()
				return nil, stepcause.Failure(stepcause.ScanNewestPrimaryRowDonorChange, err)
			}
			result[id] = row
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, stepcause.Failure(stepcause.IterateNewestPrimaryRowsDonorChange, err)
		}
	}
	return result, nil
}
