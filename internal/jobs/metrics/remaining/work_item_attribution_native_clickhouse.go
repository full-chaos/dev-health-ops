package remaining

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// WorkItemAttributionOutcome reports what one ComputeOrg call did, for
// telemetry and for the caller to decide whether a run marker is needed.
type WorkItemAttributionOutcome struct {
	OrgWide     bool
	RepoIDs     []string
	ProjectKeys []string
	ItemsSeen   int
	RowsWritten int
	SkippedNoop bool
}

// ComputeOrg re-derives work_item_team_attributions for one org's AFFECTED
// SCOPE and writes it, following the CHAOS-2433 write-then-marker protocol.
//
// # SCOPING RULE (CHAOS-3092 PR-B, team-lead ruling)
//
// Unlike the sync-time deriver (per-provider, re-derives on every item sync
// -- but only within an incremental watermark window, which is exactly the
// gap this backstop exists to close) and unlike the retired Python daily
// sweep (job_daily.py's unconditional compute_work_item_team_attributions,
// which re-derived every work item loaded that day regardless of whether
// anything about it could have changed), this backstop re-derives ONLY the
// scope whose OWNERSHIP changed since the last backstop run:
//
//   - A team_repo_ownership change since that repo's last covered run scopes
//     the rederive to that repo's items.
//   - A team_project_ownership change scopes to that project's items.
//   - An identities/teams (admin membership) change is ALWAYS org-wide:
//     admin membership has no single-repo/project scope to key on, and
//     "team = ownership" means a membership change can retarget ANY item's
//     assignee_membership/author_membership candidate ([[feedback_team_vs_member_attribution]]).
//
// A run that finds nothing changed is a genuine no-op: it writes NOTHING,
// not even a marker -- the EXISTING watermark is still the correct
// high-water mark, and publishing a fresh one would just be a needless
// write with no new coverage behind it.
func (executor *WorkItemAttributionExecutor) ComputeOrg(
	ctx context.Context, orgID string,
) (WorkItemAttributionOutcome, error) {
	if executor == nil || executor.conn == nil {
		return WorkItemAttributionOutcome{}, ErrWorkItemAttributionUnavailable
	}
	if strings.TrimSpace(orgID) == "" {
		return WorkItemAttributionOutcome{}, ErrWorkItemAttributionWriteInvalidState
	}
	now := executor.wallClock()()

	scope, err := executor.detectScope(ctx, orgID, now)
	if err != nil {
		return WorkItemAttributionOutcome{}, err
	}
	if scope.orgWide == false && len(scope.repoIDs) == 0 && len(scope.projectKeys) == 0 {
		return WorkItemAttributionOutcome{SkippedNoop: true}, nil
	}

	subjects, err := executor.loadAffectedSubjects(ctx, orgID, scope)
	if err != nil {
		return WorkItemAttributionOutcome{}, err
	}
	outcome := WorkItemAttributionOutcome{
		OrgWide: scope.orgWide, RepoIDs: scope.repoIDs, ProjectKeys: scope.projectKeys,
		ItemsSeen: len(subjects),
	}
	if len(subjects) == 0 {
		// The scope's ownership changed, but nothing in it has any work
		// items yet -- a real, if unusual, outcome (e.g. a repo/project was
		// just given team ownership before any item synced). Still a
		// completed run: publish the marker so this scope is not
		// re-detected as changed forever.
		if err := executor.publishRunMarkers(ctx, orgID, scope, now); err != nil {
			return outcome, err
		}
		return outcome, nil
	}

	// affectedIDs pins which work items THIS run was actually asked to
	// touch, before donors (pulled in only to feed linked_issue inheritance)
	// are merged into the same lookup map below -- a donor is never itself
	// written, since writing it would attribute an item nothing in this
	// run's scope asked to be re-verified, stamped with a computed_at that
	// makes it look freshly checked when it was not.
	affectedIDs := make(map[string]struct{}, len(subjects))
	for id := range subjects {
		affectedIDs[id] = struct{}{}
	}

	facts, err := executor.loadFacts(ctx, orgID, now)
	if err != nil {
		return outcome, err
	}
	dependencies, err := executor.loadDependencyEdges(ctx, orgID, subjects)
	if err != nil {
		return outcome, err
	}
	donorIDs, donorKeys := workItemAttributionDonorTargets(dependencies, subjects)
	donors, err := executor.loadDonorSubjects(ctx, orgID, donorIDs, donorKeys)
	if err != nil {
		return outcome, err
	}
	for id, donor := range donors {
		if _, exists := subjects[id]; !exists {
			subjects[id] = donor
		}
	}

	derived := teamattribution.NewGitHubWorkItemDerivationContext(facts)
	linkedIssue, _, _ := derived.BuildLinkedIssueIndex("", subjects, dependencies, nil)
	derived.LinkedIssue = linkedIssue

	rows := BuildWorkItemAttributionRows(orgID, now, affectedIDs, subjects, derived)

	written, err := executor.writer.WriteAttributions(ctx, rows)
	if err != nil {
		return outcome, err
	}
	outcome.RowsWritten = written
	if err := executor.publishRunMarkers(ctx, orgID, scope, now); err != nil {
		return outcome, err
	}
	return outcome, nil
}

// BuildWorkItemAttributionRows maps resolved candidates onto the
// work_item_team_attributions row shape, for exactly the affected work items
// (never a donor pulled in only for linked_issue inheritance -- see
// ComputeOrg's affectedIDs doc comment). EXPORTED so the CHAOS-3092 PR-B
// differential test (internal/providersync's oracle harness) can drive this
// SAME mapping against a subjects/derivation-context pair built from an
// oracle case, rather than a hand-copied duplicate of ComputeOrg's former
// inline loop -- a divergence between the tested mapping and the one that
// actually writes to ClickHouse would defeat the point of the proof.
//
// affectedIDs is iterated in SORTED order: a map range would make row order
// (and therefore the dedupe/last-wins tie-break in
// workItemAttributionSortingKeyDedupe, and the oracle differential test's
// column-positional comparison) nondeterministic between runs, for no
// benefit -- ClickHouse itself does not care what order a batch's rows
// arrive in.
func BuildWorkItemAttributionRows(
	orgID string,
	computedAt time.Time,
	affectedIDs map[string]struct{},
	subjects map[string]teamattribution.GithubWorkItemDerivationSubject,
	derived teamattribution.GithubWorkItemDerivationContext,
) []WorkItemAttributionRow {
	ids := make([]string, 0, len(affectedIDs))
	for id := range affectedIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	rows := make([]WorkItemAttributionRow, 0, len(affectedIDs))
	for _, id := range ids {
		subject := subjects[id]
		_, _, candidates := derived.Resolve(subject)
		for _, candidate := range candidates {
			var repoID *uuid.UUID
			if subject.RepoID != nil {
				if parsed, err := uuid.Parse(*subject.RepoID); err == nil {
					repoID = &parsed
				}
			}
			rows = append(rows, WorkItemAttributionRow{
				WorkItemID: subject.WorkItemID,
				Provider:   subject.Provider,
				Source:     candidate.Source,
				IsPrimary:  candidate.IsPrimary,
				Confidence: candidate.Confidence,
				Evidence:   candidate.Evidence,
				ComputedAt: computedAt,
				RepoID:     repoID,
				TeamID:     candidate.TeamID,
				TeamName:   candidate.TeamName,
				OrgID:      orgID,
			})
		}
	}
	return rows
}

// workItemAttributionScopeDecision is the affected set detectScope computes:
// EITHER org-wide (identities/teams changed -- team = ownership, admin
// membership has no single-repo/project scope to key on) OR a set of
// repo/project scopes, never both (an org-wide run is a strict superset).
//
// KNOWN LIMITATION, not yet addressed: repo scoping keys ONLY on
// team_repo_ownership.repo_id (a non-null UUID); a repo-name-only ownership
// row (repo_id NULL, repo_full_name set) is invisible to this detector.
// Project scoping keys ONLY on project_key (non-empty); a project_id-only
// ownership row is likewise invisible. Both are the same shape as the
// no-repo/no-project item this backstop already cannot scope by identity --
// a real gap, not yet sized, left for a fast-follow once this lands.
type workItemAttributionScopeDecision struct {
	orgWide     bool
	repoIDs     []string
	projectKeys []string
}

// detectScope reads the org-wide and scoped watermarks, compares them
// against ownership-table activity, and returns the affected set. See
// ComputeOrg's doc comment for the scoping rule itself.
func (executor *WorkItemAttributionExecutor) detectScope(
	ctx context.Context, orgID string, now time.Time,
) (workItemAttributionScopeDecision, error) {
	orgWatermark, err := executor.orgWatermark(ctx, orgID)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}
	repoWatermarks, projectWatermarks, err := executor.scopedWatermarks(ctx, orgID)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}

	identitiesChanged, err := executor.maxUpdatedAt(ctx,
		"SELECT max(updated_at) FROM identities FINAL WHERE org_id = ?", orgID)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}
	teamsChanged, err := executor.maxUpdatedAt(ctx,
		"SELECT max(updated_at) FROM teams FINAL WHERE org_id = ?", orgID)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}
	if identitiesChanged.After(orgWatermark) || teamsChanged.After(orgWatermark) {
		return workItemAttributionScopeDecision{orgWide: true}, nil
	}

	repoChanges, err := executor.scopeChanges(ctx, orgID,
		"SELECT toString(repo_id), max(updated_at) FROM team_repo_ownership WHERE org_id = ? AND repo_id IS NOT NULL GROUP BY repo_id")
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}
	projectChanges, err := executor.scopeChanges(ctx, orgID,
		"SELECT project_key, max(updated_at) FROM team_project_ownership WHERE org_id = ? AND project_key != '' GROUP BY project_key")
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}

	var decision workItemAttributionScopeDecision
	for scopeID, changedAt := range repoChanges {
		floor := orgWatermark
		if watermark, ok := repoWatermarks[scopeID]; ok && watermark.After(floor) {
			floor = watermark
		}
		if changedAt.After(floor) {
			decision.repoIDs = append(decision.repoIDs, scopeID)
		}
	}
	for scopeID, changedAt := range projectChanges {
		floor := orgWatermark
		if watermark, ok := projectWatermarks[scopeID]; ok && watermark.After(floor) {
			floor = watermark
		}
		if changedAt.After(floor) {
			decision.projectKeys = append(decision.projectKeys, scopeID)
		}
	}
	return decision, nil
}

// orgWatermark is the org's org-wide "attribution guaranteed correct as of"
// timestamp -- the zero time if no org-wide run has ever completed.
func (executor *WorkItemAttributionExecutor) orgWatermark(
	ctx context.Context, orgID string,
) (time.Time, error) {
	return executor.maxUpdatedAt(ctx,
		"SELECT max(completed_at) FROM work_item_attribution_backstop_runs WHERE org_id = ?", orgID)
}

// scopedWatermarks reads every repo- and project-scoped watermark for the
// org, keyed by scope_id.
func (executor *WorkItemAttributionExecutor) scopedWatermarks(
	ctx context.Context, orgID string,
) (repos map[string]time.Time, projects map[string]time.Time, err error) {
	rows, err := executor.conn.Query(ctx, `
SELECT scope_kind, scope_id, max(completed_at)
FROM work_item_attribution_backstop_scoped_runs
WHERE org_id = ?
GROUP BY scope_kind, scope_id`, orgID)
	if err != nil {
		return nil, nil, fmt.Errorf("query scoped watermarks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	repos = map[string]time.Time{}
	projects = map[string]time.Time{}
	for rows.Next() {
		var kind, id string
		var completedAt time.Time
		if err := rows.Scan(&kind, &id, &completedAt); err != nil {
			return nil, nil, fmt.Errorf("scan scoped watermark: %w", err)
		}
		switch kind {
		case "repo":
			repos[id] = completedAt
		case "project":
			projects[id] = completedAt
		}
	}
	return repos, projects, rows.Err()
}

// scopeChanges runs a `SELECT scope_id, max(updated_at) ... GROUP BY
// scope_id` query and returns the result as a map.
func (executor *WorkItemAttributionExecutor) scopeChanges(
	ctx context.Context, orgID string, query string,
) (map[string]time.Time, error) {
	rows, err := executor.conn.Query(ctx, query, orgID)
	if err != nil {
		return nil, fmt.Errorf("query scope changes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := map[string]time.Time{}
	for rows.Next() {
		var id string
		var changedAt time.Time
		if err := rows.Scan(&id, &changedAt); err != nil {
			return nil, fmt.Errorf("scan scope change: %w", err)
		}
		result[id] = changedAt
	}
	return result, rows.Err()
}

// maxUpdatedAt runs a single-row `SELECT max(...)` query, treating a
// ClickHouse NULL (nothing matched) as the zero time -- "nothing has ever
// happened here" is not an error.
func (executor *WorkItemAttributionExecutor) maxUpdatedAt(
	ctx context.Context, query string, orgID string,
) (time.Time, error) {
	rows, err := executor.conn.Query(ctx, query, orgID)
	if err != nil {
		return time.Time{}, fmt.Errorf("query max updated_at: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return time.Time{}, rows.Err()
	}
	var value *time.Time
	if err := rows.Scan(&value); err != nil {
		return time.Time{}, fmt.Errorf("scan max updated_at: %w", err)
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, err
	}
	if value == nil {
		return time.Time{}, nil
	}
	return *value, nil
}

// loadAffectedSubjects loads every work item in the affected scope,
// org-wide or repo/project-scoped, keyed by work_item_id.
func (executor *WorkItemAttributionExecutor) loadAffectedSubjects(
	ctx context.Context, orgID string, scope workItemAttributionScopeDecision,
) (map[string]teamattribution.GithubWorkItemDerivationSubject, error) {
	query := `SELECT work_item_id, provider, type, toString(repo_id), native_team_key,
       project_key, project_id, project_name, assignees, reporter, org_id
FROM work_items FINAL
WHERE org_id = ?`
	args := []any{orgID}
	if !scope.orgWide {
		if len(scope.repoIDs) == 0 && len(scope.projectKeys) == 0 {
			return map[string]teamattribution.GithubWorkItemDerivationSubject{}, nil
		}
		query += " AND (has(?, toString(repo_id)) OR has(?, project_key))"
		args = append(args, scope.repoIDs, scope.projectKeys)
	}
	return executor.querySubjects(ctx, query, args...)
}

// loadDonorSubjects loads work items referenced as linked-issue donor
// TARGETS that fall outside the affected scope -- the same donor-loading
// shape as the sync-time deriver's loadDonors
// (internal/providersync/github_work_items_derivation_context.go), adapted
// to a plain org-wide/multi-provider read since this backstop has no
// per-provider claim to scope by.
func (executor *WorkItemAttributionExecutor) loadDonorSubjects(
	ctx context.Context, orgID string, donorIDs, donorKeys []string,
) (map[string]teamattribution.GithubWorkItemDerivationSubject, error) {
	if len(donorIDs) == 0 && len(donorKeys) == 0 {
		return map[string]teamattribution.GithubWorkItemDerivationSubject{}, nil
	}
	query := `SELECT work_item_id, provider, type, toString(repo_id), native_team_key,
       project_key, project_id, project_name, assignees, reporter, org_id
FROM work_items FINAL
WHERE org_id = ? AND (
  has(?, work_item_id)
  OR (provider IN ('linear', 'jira') AND has(?, upper(splitByChar(':', work_item_id)[-1])))
)`
	return executor.querySubjects(ctx, query, orgID, donorIDs, donorKeys)
}

func (executor *WorkItemAttributionExecutor) querySubjects(
	ctx context.Context, query string, args ...any,
) (map[string]teamattribution.GithubWorkItemDerivationSubject, error) {
	rows, err := executor.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query work_items: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := map[string]teamattribution.GithubWorkItemDerivationSubject{}
	for rows.Next() {
		var subject teamattribution.GithubWorkItemDerivationSubject
		var repoID string
		if err := rows.Scan(
			&subject.WorkItemID, &subject.Provider, &subject.Type, &repoID, &subject.NativeTeamKey,
			&subject.ProjectKey, &subject.ProjectID, &subject.ProjectName,
			&subject.Assignees, &subject.Reporter, &subject.OrgID,
		); err != nil {
			return nil, fmt.Errorf("scan work_items row: %w", err)
		}
		if repoID != "" && repoID != uuid.Nil.String() {
			subject.RepoID = &repoID
		}
		result[subject.WorkItemID] = subject
	}
	return result, rows.Err()
}

// loadDependencyEdges loads work_item_dependencies rows whose SOURCE is one
// of the affected subjects, converted to teamattribution's narrow
// dependency-edge shape.
func (executor *WorkItemAttributionExecutor) loadDependencyEdges(
	ctx context.Context, orgID string, subjects map[string]teamattribution.GithubWorkItemDerivationSubject,
) ([]teamattribution.GithubWorkItemDerivationDependencyEdge, error) {
	if len(subjects) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(subjects))
	for id := range subjects {
		ids = append(ids, id)
	}
	rows, err := executor.conn.Query(ctx, `
SELECT source_work_item_id, target_work_item_id, relationship_type, last_synced
FROM work_item_dependencies FINAL
WHERE org_id = ? AND has(?, source_work_item_id)`, orgID, ids)
	if err != nil {
		return nil, fmt.Errorf("query work_item_dependencies: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []teamattribution.GithubWorkItemDerivationDependencyEdge
	for rows.Next() {
		var edge teamattribution.GithubWorkItemDerivationDependencyEdge
		if err := rows.Scan(&edge.SourceWorkItemID, &edge.TargetWorkItemID, &edge.RelationshipType, &edge.LastSynced); err != nil {
			return nil, fmt.Errorf("scan work_item_dependencies row: %w", err)
		}
		edge.OrgID = orgID
		result = append(result, edge)
	}
	return result, rows.Err()
}

// workItemAttributionDonorTargets mirrors githubWorkItemDerivationDonorTargets
// (internal/providersync/github_work_items_derivation_context.go): the
// dependency targets not already present among the affected subjects,
// split into plain work-item IDs and extkey: cross-provider issue keys.
func workItemAttributionDonorTargets(
	dependencies []teamattribution.GithubWorkItemDerivationDependencyEdge,
	subjects map[string]teamattribution.GithubWorkItemDerivationSubject,
) ([]string, []string) {
	ids := map[string]struct{}{}
	keys := map[string]struct{}{}
	for _, dependency := range teamattribution.LatestGitHubWorkItemDerivationDependencies(dependencies) {
		if !teamattribution.GithubWorkItemDerivationInheritableRelationships[dependency.RelationshipType] {
			continue
		}
		target := strings.TrimSpace(dependency.TargetWorkItemID)
		if _, exists := subjects[target]; exists {
			continue
		}
		if strings.HasPrefix(target, "extkey:") {
			key := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(target, "extkey:")))
			if key != "" {
				keys[key] = struct{}{}
			}
		} else if target != "" {
			ids[target] = struct{}{}
		}
	}
	idList := make([]string, 0, len(ids))
	for id := range ids {
		idList = append(idList, id)
	}
	keyList := make([]string, 0, len(keys))
	for key := range keys {
		keyList = append(keyList, key)
	}
	return idList, keyList
}

// loadFacts loads the org-wide, provider-neutral ownership facts via
// teamattribution's own ClickHouseFactSource (PR-A) -- the SAME loader the
// sync-time deriver's providersync.githubWorkItemClickHouseDerivationContextSource
// wraps, so both writers read identical facts for identical inputs.
func (executor *WorkItemAttributionExecutor) loadFacts(
	ctx context.Context, orgID string, asOf time.Time,
) (teamattribution.GithubWorkItemDerivationFacts, error) {
	loader := teamattribution.ClickHouseFactSource{Conn: executor.conn}
	facts := teamattribution.GithubWorkItemDerivationFacts{}
	var err error
	if facts.Teams, err = loader.LoadTeams(ctx, orgID); err != nil {
		return facts, err
	}
	if facts.Projects, err = loader.LoadProjects(ctx, orgID, asOf); err != nil {
		return facts, err
	}
	if facts.Repos, err = loader.LoadRepos(ctx, orgID, asOf); err != nil {
		return facts, err
	}
	var providerTaggedRosterMembers []teamattribution.GithubWorkItemDerivationMemberFact
	if facts.Members, facts.UntypedMembers, facts.ProviderUntypedMembers, providerTaggedRosterMembers, err = loader.LoadMembers(ctx, orgID, asOf); err != nil {
		return facts, err
	}
	if facts.ProviderMembers, err = loader.LoadProviderMembers(ctx, orgID, asOf); err != nil {
		return facts, err
	}
	facts.ProviderMembers = append(facts.ProviderMembers, providerTaggedRosterMembers...)
	if facts.ManualFallbacks, err = loader.LoadManualFallbacks(ctx, orgID, asOf); err != nil {
		return facts, err
	}
	return facts, nil
}

// publishRunMarkers writes the completion marker(s) for a completed run:
// one org-wide marker, or one scoped marker per repo/project actually
// processed. CHAOS-2433 protocol: called only after WriteAttributions has
// already returned successfully.
func (executor *WorkItemAttributionExecutor) publishRunMarkers(
	ctx context.Context, orgID string, scope workItemAttributionScopeDecision, completedAt time.Time,
) error {
	runID := uuid.NewString()
	if scope.orgWide {
		return executor.writer.WriteAttributionRun(ctx, WorkItemAttributionRunRecord{
			OrgID: orgID, RunID: runID, CompletedAt: completedAt,
		})
	}
	records := make([]WorkItemAttributionScopedRunRecord, 0, len(scope.repoIDs)+len(scope.projectKeys))
	for _, repoID := range scope.repoIDs {
		records = append(records, WorkItemAttributionScopedRunRecord{
			OrgID: orgID, ScopeKind: "repo", ScopeID: repoID, RunID: runID, CompletedAt: completedAt,
		})
	}
	for _, projectKey := range scope.projectKeys {
		records = append(records, WorkItemAttributionScopedRunRecord{
			OrgID: orgID, ScopeKind: "project", ScopeID: projectKey, RunID: runID, CompletedAt: completedAt,
		})
	}
	return executor.writer.WriteScopedAttributionRuns(ctx, records)
}

// ComputePartition satisfies CompatibilityExecutor: the seam the partition
// handler drives.
//
// Unlike membership_backfill's ComputePartition, the decoded scope is NOT
// fed into the compute call: membership's caller-supplied repo_ids come from
// an upstream planner that already knows what changed, but this backstop's
// whole design point (team-lead's PR-B ruling) is that IT determines the
// affected scope itself, from ClickHouse watermarks, at compute time --
// staleness a scheduler-time planner cannot see coming. The partition's
// workItemAttributionScope is decoded only to fail the partition loudly on a
// shape a future producer got wrong (scopes.go's contract), matching the
// validation discipline every other decode in this package applies; ComputeOrg
// alone decides what is actually re-derived.
func (executor *WorkItemAttributionExecutor) ComputePartition(
	ctx context.Context, run Run, partition Partition,
) (CompatibilityOutcome, error) {
	if executor == nil || executor.conn == nil {
		return CompatibilityOutcome{}, ErrWorkItemAttributionUnavailable
	}
	if strings.TrimSpace(run.OrganizationID) == "" {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s has no organization", ErrInvalidState, partition.ID))
	}

	var scope workItemAttributionScope
	if err := json.Unmarshal(partition.Scope, &scope); err != nil {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s scope: %v", ErrInvalidState, partition.ID, err))
	}

	outcome, err := executor.ComputeOrg(ctx, run.OrganizationID)
	written := outcome.RowsWritten
	if err != nil {
		return CompatibilityOutcome{RowsWritten: &written}, err
	}
	return CompatibilityOutcome{RowsWritten: &written}, nil
}

// A compile-time pin that this executor IS the seam the handler drives.
var _ CompatibilityExecutor = (*WorkItemAttributionExecutor)(nil)
