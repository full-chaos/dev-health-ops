package investment

// Package investment: the deterministic per-component assembly (CHAOS-4441,
// plan.md section 1.2's "deterministic plane"). Ports the non-LLM half of
// materialize_investments' per-component body (materialize.py:1320-1791) --
// time-bounds/text-bundle preprocessing (:1320-1361), and the record
// construction that runs once an outcome exists (:1691-1791), MINUS
// everything that outcome (the LLM categorize_bundle result) supplies:
// theme_distribution_json, subcategory_distribution_json,
// categorization_status/errors_json/model_version.
//
// This file does NOT fetch anything and does NOT write anything -- it is a
// pure function over already-fetched entity maps, matching the shape every
// other units/ helper in this port takes. The chquery fetch orchestration
// and the chwrite dispatch are the executor-wiring PR that follows this one
// (plan.md section 1.2's narrow LLM bridge, section 9 Q1, still unruled).

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// CategorizationStatusNotYetCategorized is the placeholder
// work_unit_investments.categorization_status this deterministic-only
// assembly writes. The LLM categorization plane (plan.md section 1.2 item M,
// the narrow categorize_bundle bridge) has not landed -- every record this
// function produces carries this status until that plane exists and a
// follow-up PR replaces it with a real outcome.status.
const CategorizationStatusNotYetCategorized = "not_yet_categorized"

// MaterializeComponentInput carries everything ONE component's deterministic
// computation reads. All maps are the SAME org-wide fetch results
// materialize_investments builds once and shares across every component
// (materialize.py:1251-1272) -- a missing key reads as the zero value,
// matching Python's `.get(id, {})` / `.get(id, 0.0)`.
type MaterializeComponentInput struct {
	Component units.Component
	// WorkItems is keyed by work_item_id (the plain issue node id).
	WorkItems map[string]chquery.WorkItem
	// PRs and Commits are keyed by the canonical composite id
	// ("{repo}#pr{number}" / "{repo}@{hash}", units.ParsePRFromID/
	// ParseCommitFromID's own format) -- the same keys the component's PR/
	// commit node ids already are.
	PRs     map[string]chquery.PullRequest
	Commits map[string]chquery.Commit
	// EdgeRepoIDs maps edge_id -> repo_id across the FULL fetched edge set
	// (chquery.EdgeRow.RepoID), not just this component's edges --
	// materialize.py's _collect_repo_ids reads repo_id directly off each
	// edge dict, which units.Edge (the stripped type BuildComponents groups)
	// does not carry. The caller builds this once from the same
	// []chquery.EdgeRow FetchWorkGraphEdges returned.
	EdgeRepoIDs map[string]string
	// CascadeRepoID/CascadeRepoSource (CHAOS-5359) is the issue-hierarchy
	// cascade's result for THIS component, computed by the caller once,
	// org-wide, via computeRepoHierarchyCascade -- nil/"" when the component's
	// own edges already resolved a repo, or when the cascade found no signal
	// either. Applied ONLY as a fallback: collectSingleRepoID's own-edges
	// result always wins when both exist.
	CascadeRepoID     *uuid.UUID
	CascadeRepoSource string
	PRChurn           map[string]float64
	CommitChurn       map[string]float64
	ActiveHours       map[string]float64
	ParentTitles      map[string]string
	EpicTitles        map[string]string
	// FromTS/ToTS is the run's window (config.from_ts/to_ts). A component
	// whose computed time bounds fall entirely outside it is skipped, same
	// as materialize.py:1335-1336.
	FromTS time.Time
	ToTS   time.Time
}

// MaterializeComponentResult is one component's outcome. Skipped names WHY
// no record was produced, matching materialize.py's two `continue` sites
// (:1333-1336) -- "" means a record and repo-effort rows were produced.
type MaterializeComponentResult struct {
	// Investment is chwrite-shaped, but ComputedAt, CategorizationRunID and
	// OrgID are the CALLER's to fill in -- they are run-level, not
	// per-component, and materialize_investments only assigns them once,
	// outside this loop (materialize.py:1292-1293). WorkUnitID IS computed
	// here (units.WorkUnitID over the component's node set).
	Investment chwrite.InvestmentRecord
	// RepoEffort rows carry the same caller-filled ComputedAt/
	// CategorizationRunID/OrgID gap as Investment. WorkUnitID matches
	// Investment.WorkUnitID.
	RepoEffort []chwrite.RepoEffortRecord
	Skipped    string
	// Bundle is the text bundle this component's assembly built.
	//
	// Returned rather than recomputed by the caller because the ORCHESTRATOR
	// needs it too -- materialize.py's preprocess loop reads text_char_count
	// and text_source_count to decide LLM-vs-fallback (:1363-1381) and hands
	// source_block/source_texts/handle_map to categorize_text_bundle. Building
	// it twice would put two BuildTextBundle call sites on the parity path,
	// and input_hash (which gates every skip-existing lookup, at real LLM cost
	// when it drifts) is derived from it -- so the two copies would have to
	// stay byte-identical forever by convention rather than by construction.
	// Zero value on either Skipped path: no bundle is built there.
	Bundle units.TextBundle
}

const (
	skippedNoTimeBounds = "no_time_bounds"
	skippedOutOfWindow  = "out_of_window"
)

// MaterializeComponent ports the deterministic half of one iteration of
// materialize_investments' component loop.
func MaterializeComponent(input MaterializeComponentInput) (MaterializeComponentResult, error) {
	unitNodes := dedupeNodeKeys(input.Component.Nodes)
	issueNodeIDs, prNodeIDs, commitNodeIDs := splitNodeIDsByType(unitNodes)

	nodeTimes := buildNodeTimes(issueNodeIDs, prNodeIDs, commitNodeIDs, input.WorkItems, input.PRs, input.Commits)
	bounds, ok := units.ComputeTimeBounds(unitNodes, nodeTimes)
	if !ok {
		return MaterializeComponentResult{Skipped: skippedNoTimeBounds}, nil
	}
	// NO EMPTY-INTERVAL SKIP -- deliberately, and this is PARITY, not an
	// oversight.
	//
	// The two checks below skip a component lying WHOLLY before or after the
	// interval, so a component STRADDLING an empty interval is retained and its
	// row written: with FromTS=Jan20, ToTS=Jan10 and bounds Jan1-Jan30, both
	// halves are false. That looks wrong, and an earlier version of this file
	// skipped it on the stated grounds that "Python filters in SQL and returns
	// nothing". THAT CLAIM WAS FALSE.
	//
	// materialize.py:1335 is the ONLY date filter in materialize_investments --
	// `if bounds.end < config.from_ts or bounds.start >= config.to_ts: continue`
	// -- and it is structurally identical to the two checks below. Every fetch
	// feeding it is id/org-scoped, never date-scoped. Python therefore retains
	// the same straddling component and writes the same row.
	//
	// So skipping here would make the port DIVERGE from the plane it replaces.
	// The zero-width-window behaviour is a real defect, filed as its own ticket
	// against the Python plane; fixing it inside a faithful port would change
	// behaviour under cover of a cutover, which is exactly what this PR must not
	// do.
	if bounds.End.Before(input.FromTS) || !bounds.Start.Before(input.ToTS) {
		return MaterializeComponentResult{Skipped: skippedOutOfWindow}, nil
	}

	unitID := units.WorkUnitID(unitNodes)

	bundle, err := units.BuildTextBundle(units.BuildTextBundleInput{
		IssueIDs:     issueNodeIDs,
		PRIDs:        prNodeIDs,
		CommitIDs:    commitNodeIDs,
		WorkItemMap:  workItemMapForBundle(issueNodeIDs, input.WorkItems),
		PRMap:        prMapForBundle(prNodeIDs, input.PRs),
		CommitMap:    commitMapForBundle(commitNodeIDs, input.Commits),
		ParentTitles: input.ParentTitles,
		EpicTitles:   input.EpicTitles,
		WorkUnitID:   unitID,
	})
	if err != nil {
		return MaterializeComponentResult{}, err
	}

	confidences := make([]any, len(input.Component.Edges))
	for i, edge := range input.Component.Edges {
		confidences[i] = edge.Confidence
	}
	evidenceQuality := units.ComputeEvidenceQuality(units.EvidenceQualityInput{
		TextSourceCount: bundle.TextSourceCount,
		TextCharCount:   bundle.TextCharCount,
		SourceTexts:     bundle.SourceTexts,
		NodesCount:      len(unitNodes),
		Confidences:     confidences,
	})
	evidenceBand := units.EvidenceQualityBand(evidenceQuality)

	effort := units.ComputeEffort(units.EffortInput{
		IssueIDs: issueNodeIDs, PRIDs: prNodeIDs, CommitIDs: commitNodeIDs,
		PRChurn: input.PRChurn, CommitChurn: input.CommitChurn, ActiveHours: input.ActiveHours,
	})

	structuralEvidenceJSON, err := marshalStructuralEvidence(issueNodeIDs, prNodeIDs, commitNodeIDs, input.Component.Edges)
	if err != nil {
		return MaterializeComponentResult{}, err
	}

	repoID := collectSingleRepoID(input.Component.Edges, input.EdgeRepoIDs)
	var repoSource *string
	switch {
	case repoID != nil:
		source := RepoSourceOwnEdges
		repoSource = &source
	case input.CascadeRepoID != nil:
		repoID = input.CascadeRepoID
		source := input.CascadeRepoSource
		repoSource = &source
	}
	provider := collectProvider(issueNodeIDs, input.WorkItems)
	labelType, labelName := units.ResolveWorkUnitLabel(units.ResolveWorkUnitLabelInput{
		IssueIDs: issueNodeIDs, PRIDs: prNodeIDs, CommitIDs: commitNodeIDs,
		WorkItems: workItemLabelFields(input.WorkItems), PRs: prLabelFields(input.PRs), Commits: commitLabelFields(input.Commits),
	})
	investment := chwrite.InvestmentRecord{
		WorkUnitID:               unitID,
		WorkUnitType:             labelType,
		WorkUnitName:             labelName,
		FromTS:                   bounds.Start,
		ToTS:                     bounds.End,
		RepoID:                   repoID,
		RepoSource:               repoSource,
		Provider:                 provider,
		EffortMetric:             effort.Metric,
		EffortValue:              effort.Value,
		ThemeDistribution:        map[string]float64{},
		SubcategoryDistribution:  map[string]float64{},
		StructuralEvidenceJSON:   structuralEvidenceJSON,
		EvidenceQuality:          evidenceQuality,
		EvidenceQualityBand:      evidenceBand,
		CategorizationStatus:     CategorizationStatusNotYetCategorized,
		CategorizationErrorsJSON: "[]",
		CategorizationInputHash:  bundle.InputHash,
	}

	allocations := units.AllocateRepoEffort(units.AllocateRepoEffortInput{
		IssueIDs: issueNodeIDs, PRIDs: prNodeIDs, CommitIDs: commitNodeIDs,
		PRChurn: input.PRChurn, CommitChurn: input.CommitChurn, ActiveHours: input.ActiveHours,
		EffortMetric: effort.Metric, EffortValue: effort.Value,
	})
	// CHAOS-5359: sankeycoverage.go prefers work_unit_repo_effort's repo_id
	// over work_unit_investments' the moment ANY row exists for this
	// work_unit_id -- and AllocateRepoEffort always returns at least one row,
	// even its bottom "empty" tier (RepoID nil). So a cascade that patched
	// only the investment row above would be permanently shadowed here: the
	// empty-tier row already "exists" and wins the query's `wure.work_unit_id
	// != ''` check regardless of its own repo_id being NULL. The cascade must
	// therefore also override THIS row when it is the empty tier -- the one
	// case with no per-repo signal of its own to defer to.
	repoEffortRecords := make([]chwrite.RepoEffortRecord, len(allocations))
	for i, allocation := range allocations {
		record := chwrite.RepoEffortRecord{
			WorkUnitID:       unitID,
			RepoID:           allocation.RepoID,
			EffortMetric:     effort.Metric,
			EffortValue:      allocation.RepoEffort,
			AllocationWeight: allocation.AllocationWeight,
			AllocationSource: allocation.AllocationSource,
		}
		if allocation.AllocationSource == units.AllocationSourceEmpty && input.CascadeRepoID != nil {
			source := input.CascadeRepoSource
			record.RepoID = input.CascadeRepoID
			record.RepoSource = &source
			record.AllocationWeight = 1.0
			record.AllocationSource = units.AllocationSourceHierarchyCascade
		}
		repoEffortRecords[i] = record
	}

	return MaterializeComponentResult{Investment: investment, RepoEffort: repoEffortRecords, Bundle: bundle}, nil
}

// dedupeNodeKeys is `list(dict.fromkeys(nodes))` (materialize.py:1321) --
// first-occurrence order preserved, duplicates dropped.
func dedupeNodeKeys(nodes []units.NodeKey) []units.NodeKey {
	seen := make(map[units.NodeKey]struct{}, len(nodes))
	out := make([]units.NodeKey, 0, len(nodes))
	for _, node := range nodes {
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		out = append(out, node)
	}
	return out
}

func splitNodeIDsByType(nodes []units.NodeKey) (issueIDs, prIDs, commitIDs []string) {
	for _, node := range nodes {
		switch node.Type {
		case "issue":
			issueIDs = append(issueIDs, node.ID)
		case "pr":
			prIDs = append(prIDs, node.ID)
		case "commit":
			commitIDs = append(commitIDs, node.ID)
		}
	}
	return issueIDs, prIDs, commitIDs
}

func buildNodeTimes(
	issueIDs, prIDs, commitIDs []string,
	workItems map[string]chquery.WorkItem, prs map[string]chquery.PullRequest, commits map[string]chquery.Commit,
) map[units.NodeKey]units.NodeTimes {
	times := make(map[units.NodeKey]units.NodeTimes, len(issueIDs)+len(prIDs)+len(commitIDs))
	for _, id := range issueIDs {
		if item, ok := workItems[id]; ok {
			times[units.NodeKey{Type: "issue", ID: id}] = units.NodeTimes{
				CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt, CompletedAt: item.CompletedAt, Present: true,
			}
		}
	}
	for _, id := range prIDs {
		if pr, ok := prs[id]; ok {
			times[units.NodeKey{Type: "pr", ID: id}] = units.NodeTimes{
				CreatedAt: pr.CreatedAt, MergedAt: pr.MergedAt, ClosedAt: pr.ClosedAt, Present: true,
			}
		}
	}
	for _, id := range commitIDs {
		if commit, ok := commits[id]; ok {
			times[units.NodeKey{Type: "commit", ID: id}] = units.NodeTimes{
				AuthorWhen: commit.AuthorWhen, Present: true,
			}
		}
	}
	return times
}

func workItemMapForBundle(issueIDs []string, workItems map[string]chquery.WorkItem) map[string]map[string]any {
	out := make(map[string]map[string]any, len(issueIDs))
	for _, id := range issueIDs {
		item, ok := workItems[id]
		if !ok {
			continue
		}
		labels := make([]any, len(item.Labels))
		for i, label := range item.Labels {
			labels[i] = label
		}
		out[id] = map[string]any{
			"title": item.Title, "description": item.Description, "type": item.Type,
			"labels": labels, "parent_id": item.ParentID, "epic_id": item.EpicID,
		}
	}
	return out
}

func prMapForBundle(prIDs []string, prs map[string]chquery.PullRequest) map[string]map[string]any {
	out := make(map[string]map[string]any, len(prIDs))
	for _, id := range prIDs {
		pr, ok := prs[id]
		if !ok {
			continue
		}
		out[id] = map[string]any{"title": pr.Title, "body": pr.Body}
	}
	return out
}

func commitMapForBundle(commitIDs []string, commits map[string]chquery.Commit) map[string]map[string]any {
	out := make(map[string]map[string]any, len(commitIDs))
	for _, id := range commitIDs {
		commit, ok := commits[id]
		if !ok {
			continue
		}
		out[id] = map[string]any{"message": commit.Message}
	}
	return out
}

func workItemLabelFields(workItems map[string]chquery.WorkItem) map[string]units.WorkItemLabelFields {
	out := make(map[string]units.WorkItemLabelFields, len(workItems))
	for id, item := range workItems {
		out[id] = units.WorkItemLabelFields{Title: item.Title, Type: item.Type}
	}
	return out
}

func prLabelFields(prs map[string]chquery.PullRequest) map[string]units.PRLabelFields {
	out := make(map[string]units.PRLabelFields, len(prs))
	for id, pr := range prs {
		out[id] = units.PRLabelFields{Title: pr.Title}
	}
	return out
}

func commitLabelFields(commits map[string]chquery.Commit) map[string]units.CommitLabelFields {
	out := make(map[string]units.CommitLabelFields, len(commits))
	for id, commit := range commits {
		out[id] = units.CommitLabelFields{Message: commit.Message}
	}
	return out
}

// structuralEvidence mirrors materialize.py:1711-1722's dict shape and key
// order -- issues, prs, commits, edges, each sorted.
type structuralEvidence struct {
	Issues  []string `json:"issues"`
	PRs     []string `json:"prs"`
	Commits []string `json:"commits"`
	Edges   []string `json:"edges"`
}

// marshalStructuralEvidence produces structural_evidence_json. Uses plain
// encoding/json rather than a byte-exact Python encoder deliberately: unlike
// categorization_input_hash (bundle.InputHash, which gates LLM
// re-categorization and MUST be byte-identical), this column is read back
// only through ClickHouse's JSONExtract by every consumer
// (workUnitEvidenceWorkItemRefsExpr and siblings in
// cmd/query-api/internal/analytics/investment.go) -- valid JSON with the
// right keys and values is sufficient; nothing compares its raw bytes across
// planes. Every value here is a plain ASCII id (uuid/hash), so there is no
// unicode-escaping divergence to worry about either.
func marshalStructuralEvidence(issueIDs, prIDs, commitIDs []string, edges []units.Edge) (string, error) {
	edgeIDs := make([]string, 0, len(edges))
	for _, edge := range edges {
		if edge.EdgeID != "" {
			edgeIDs = append(edgeIDs, edge.EdgeID)
		}
	}
	payload := structuralEvidence{
		Issues:  sortedStrings(issueIDs),
		PRs:     sortedStrings(prIDs),
		Commits: sortedStrings(commitIDs),
		Edges:   sortedStrings(edgeIDs),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func sortedStrings(values []string) []string {
	out := make([]string, len(values))
	copy(out, values)
	sort.Strings(out)
	return out
}

// collectSingleRepoID ports materialize.py:1724-1727: a repo_id is set ONLY
// when every edge naming a repo_id names the SAME one -- zero distinct
// values or more than one both leave it unset. edgeRepoIDs is keyed by
// edge_id across the FULL fetch (see MaterializeComponentInput's own doc
// comment for why units.Edge alone cannot carry this).
func collectSingleRepoID(edges []units.Edge, edgeRepoIDs map[string]string) *uuid.UUID {
	distinct := map[string]struct{}{}
	for _, edge := range edges {
		repoID := edgeRepoIDs[edge.EdgeID]
		if repoID != "" {
			distinct[repoID] = struct{}{}
		}
	}
	if len(distinct) != 1 {
		return nil
	}
	for repoID := range distinct {
		return units.ParseRepoID(repoID)
	}
	return nil
}

// collectProvider ports materialize._collect_provider: the ONE distinct
// non-empty provider among the given issue ids' work items, or nil for zero
// or more than one distinct value.
func collectProvider(issueIDs []string, workItems map[string]chquery.WorkItem) *string {
	distinct := map[string]struct{}{}
	for _, id := range issueIDs {
		if item, ok := workItems[id]; ok && item.Provider != "" {
			distinct[item.Provider] = struct{}{}
		}
	}
	if len(distinct) != 1 {
		return nil
	}
	for provider := range distinct {
		return &provider
	}
	return nil
}
