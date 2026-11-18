package remaining

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining/stepcause"
	"github.com/full-chaos/dev-health-ops/internal/teamactive"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// workItemAttributionNewestPrimary is an item's newest primary row
// (work_item_team_attributions, by computed_at), and its newest linked_issue
// row, primary or not (an issue's linked_issue row is provenance only).
type workItemAttributionNewestPrimary struct {
	TeamID, Source, Evidence     string
	ComputedAt                   time.Time
	LinkedTeamID, LinkedEvidence string
	LinkedComputedAt             time.Time
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
	kindIDs := make(map[string]struct{}, len(idSet))
	for id := range idSet {
		kindIDs[id] = struct{}{}
	}
	for _, row := range rows {
		if recorded, _ := recordedDonorOf(row); recorded != "" {
			kindIDs[recorded] = struct{}{}
		}
	}
	kinds, err := executor.loadDonorKinds(ctx, orgID, sortedWorkItemAttributionKeys(kindIDs))
	if err != nil {
		return nil, err
	}
	inactive, err := teamactive.LoadInactive(ctx, executor.conn, orgID)
	if err != nil {
		return nil, stepcause.Failure(stepcause.LoadInactiveTeamsDonorChange, err)
	}
	refused := map[string]int{}
	owed := donorChangedDependents(edges, keyIndex, rows, kinds, inactive, refused)
	if len(refused) > 0 {
		slog.Default().InfoContext(ctx, "work_item_attribution.linked_issue_donor_refused",
			"org_id", orgID, "pr_kind", refused[teamattribution.LinkedIssueDonorRefusedPullKind],
			"empty_type", refused[teamattribution.LinkedIssueDonorRefusedEmptyType],
			"unknown", refused[teamattribution.LinkedIssueDonorRefusedUnknown])
	}
	return owed, nil
}

// donorChangedDependents returns, sorted, each item that inherits through an
// inheritable dependency edge and is owed a rederive: one of its donors has a
// newest primary row newer than the item's own, and the donor the item would
// copy now differs from the one its newest snapshot records (its primary
// linked_issue row, or the provenance linked_issue row of the same snapshot). The donor it
// would copy is the cascade's winner (BuildLinkedIssueIndexFromRows): of its
// donors whose newest primary row has a team of its own (an own source) that
// is active, the lowest target id, then relationship, with that row's team. An
// item whose own primary row has a team of its own (an own source: native
// team, project or repository ownership) outranks linked_issue, so a donor
// never gives it its primary row. A donor recomputed
// with the same team triggers nothing, and a rederive makes the item newer than
// its donors, so a trigger never repeats for the same donor row.
//
// The donor must be an issue (teamattribution.LinkedIssueDonorRefusal): a refused donor gives no team and does
// not advance the staleness signal, and an item whose stored snapshot records a refused donor is owed a
// rederive whatever the timestamps and its own primary source. refused, when not nil, receives the count by
// reason of the distinct refused donors the items evaluated. An item that no edge links any more is not seen.
func donorChangedDependents(
	edges []teamattribution.GithubWorkItemDerivationDependencyEdge,
	keyIndex map[string]string,
	rows map[string]workItemAttributionNewestPrimary,
	kinds map[string]teamattribution.GithubWorkItemDerivationSubject,
	inactive teamactive.Inactive,
	refused map[string]int,
) []string {
	type donor struct{ target, relationship string }
	donors := map[string][]donor{}
	unresolved := map[string][]string{}
	for _, edge := range teamattribution.LatestGitHubWorkItemDerivationDependencies(edges) {
		if !teamattribution.GithubWorkItemDerivationInheritableRelationships[edge.RelationshipType] {
			continue
		}
		target := edge.TargetWorkItemID
		if key, ok := strings.CutPrefix(target, "extkey:"); ok {
			target = keyIndex[strings.ToUpper(strings.TrimSpace(key))]
		}
		if target == "" {
			// A key no item resolves to (or two items hold) is a donor that is not known.
			unresolved[edge.SourceWorkItemID] = append(unresolved[edge.SourceWorkItemID], edge.TargetWorkItemID)
			if _, ok := donors[edge.SourceWorkItemID]; !ok {
				donors[edge.SourceWorkItemID] = nil
			}
			continue
		}
		if target == edge.SourceWorkItemID {
			continue
		}
		donors[edge.SourceWorkItemID] = append(donors[edge.SourceWorkItemID], donor{target, edge.RelationshipType})
	}
	var owed []string
	refusedBy := map[string]string{}
	refuse := func(target, reason string) {
		if refused != nil {
			refusedBy[target] = reason
		}
	}
	for item, linked := range donors {
		own, ok := rows[item]
		if !ok {
			continue
		}
		recordedDonor, recordedTeam := recordedDonorOf(own)
		// A stored link whose recorded donor is no longer an issue is repaired, whatever the timestamps and
		// whatever the item's own primary source: the row the item holds names a donor the cascade refuses.
		if recordedDonor != "" {
			kind, known := kinds[recordedDonor]
			if reason := teamattribution.LinkedIssueDonorRefusal(kind, known); reason != "" {
				refuse(recordedDonor, reason)
				owed = append(owed, item)
				continue
			}
		}
		if teamattribution.IsLinkedIssueDonorSource(own.Source) {
			continue
		}
		for _, raw := range unresolved[item] {
			refuse(raw, teamattribution.LinkedIssueDonorRefusedUnknown)
		}
		sort.Slice(linked, func(left, right int) bool {
			if linked[left].target != linked[right].target {
				return linked[left].target < linked[right].target
			}
			return linked[left].relationship < linked[right].relationship
		})
		changed, wantDonor, wantTeam := false, "", ""
		for _, candidate := range linked {
			// A refused donor is no donor: it neither gives a team nor advances the staleness signal.
			kind, known := kinds[candidate.target]
			if reason := teamattribution.LinkedIssueDonorRefusal(kind, known); reason != "" {
				refuse(candidate.target, reason)
				continue
			}
			row, ok := rows[candidate.target]
			if !ok {
				continue
			}
			changed = changed || row.ComputedAt.After(own.ComputedAt)
			if row.TeamID == "" || !teamattribution.IsLinkedIssueDonorSource(row.Source) {
				continue
			}
			if wantDonor == "" && !inactive.Has(row.TeamID) {
				wantDonor, wantTeam = candidate.target, row.TeamID
			}
		}
		if !changed {
			continue
		}
		if wantDonor != recordedDonor || wantTeam != recordedTeam {
			owed = append(owed, item)
		}
	}
	for _, reason := range refusedBy {
		refused[reason]++
	}
	sort.Strings(owed)
	return owed
}

// recordedDonorOf is the donor, and its team, that the item's newest snapshot records: its primary linked_issue
// row, or the provenance linked_issue row of the same snapshot.
func recordedDonorOf(own workItemAttributionNewestPrimary) (string, string) {
	switch {
	case own.Source == "linked_issue":
		return linkedIssueEvidenceDonor(own.Evidence), own.TeamID
	case own.LinkedEvidence != "" && own.LinkedComputedAt.Equal(own.ComputedAt):
		return linkedIssueEvidenceDonor(own.LinkedEvidence), own.LinkedTeamID
	}
	return "", ""
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

// loadDonorKinds reads the provider and Type of each item of ids (work_items), one statement per bounded
// chunk: the donor of a linked_issue row must be an issue (teamattribution.LinkedIssueDonorRefusal). An id
// with no work_items row is absent from the result.
func (executor *WorkItemAttributionExecutor) loadDonorKinds(
	ctx context.Context, orgID string, ids []string,
) (map[string]teamattribution.GithubWorkItemDerivationSubject, error) {
	result := map[string]teamattribution.GithubWorkItemDerivationSubject{}
	for _, chunk := range chunkStringsByRenderedBytes(ids, workItemAttributionMaxArrayBytes) {
		rows, err := executor.conn.Query(ctx, `
SELECT work_item_id, toString(provider), toString(ifNull(type, ''))
FROM work_items FINAL
WHERE org_id = ? AND has(?, work_item_id)`, orgID, chunk)
		if err != nil {
			return nil, stepcause.Failure(stepcause.QueryDonorKindsDonorChange, err)
		}
		for rows.Next() {
			var subject teamattribution.GithubWorkItemDerivationSubject
			if err := rows.Scan(&subject.WorkItemID, &subject.Provider, &subject.Type); err != nil {
				_ = rows.Close()
				return nil, stepcause.Failure(stepcause.ScanDonorKindDonorChange, err)
			}
			result[subject.WorkItemID] = subject
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, stepcause.Failure(stepcause.IterateDonorKindsDonorChange, err)
		}
	}
	return result, nil
}

// loadNewestPrimaryRows reads the newest primary row of each item of ids, and
// its newest linked_issue row, primary or not, one statement per bounded chunk
// (workItemAttributionMaxArrayBytes).
func (executor *WorkItemAttributionExecutor) loadNewestPrimaryRows(
	ctx context.Context, orgID string, ids []string,
) (map[string]workItemAttributionNewestPrimary, error) {
	result := map[string]workItemAttributionNewestPrimary{}
	for _, chunk := range chunkStringsByRenderedBytes(ids, workItemAttributionMaxArrayBytes) {
		rows, err := executor.conn.Query(ctx, `
SELECT work_item_id,
       argMaxIf(ifNull(team_id, ''), computed_at, is_primary = 1),
       argMaxIf(toString(source), computed_at, is_primary = 1),
       argMaxIf(evidence, computed_at, is_primary = 1),
       maxIf(computed_at, is_primary = 1),
       argMaxIf(ifNull(team_id, ''), computed_at, source = 'linked_issue'),
       argMaxIf(evidence, computed_at, source = 'linked_issue'),
       maxIf(computed_at, source = 'linked_issue')
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND has(?, work_item_id) AND (is_primary = 1 OR source = 'linked_issue')
GROUP BY work_item_id
HAVING countIf(is_primary = 1) > 0`, orgID, chunk)
		if err != nil {
			return nil, stepcause.Failure(stepcause.QueryNewestPrimaryRowsDonorChange, err)
		}
		for rows.Next() {
			var id string
			var row workItemAttributionNewestPrimary
			if err := rows.Scan(&id, &row.TeamID, &row.Source, &row.Evidence, &row.ComputedAt,
				&row.LinkedTeamID, &row.LinkedEvidence, &row.LinkedComputedAt); err != nil {
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
