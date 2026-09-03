package providersync

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// This file is providersync's half of the CHAOS-3092 PR-A extraction: the
// provider-neutral cascade, its fact loaders, and the linked-issue donor
// index now live in internal/teamattribution (github_work_items_derivation_context.go's
// former contents, extracted as teamattribution/cascade.go). What stays here
// is everything typed on providersync's own Claim/githubWorkItemRows/
// githubWorkItemDependencyRow: the row-shape adapters, the orchestrator entry
// point, and the stored-edge-merge/donor helpers -- plus the small converters
// at the seam where a STAYS function hands data to a MOVES one.

type githubWorkItemDerivationContextSource interface {
	Load(
		context.Context,
		Claim,
		teamattribution.GithubWorkItemDerivationLoadRequest,
	) (teamattribution.GithubWorkItemDerivationFacts, error)

	// LoadStoredInheritableEdges returns the STORED inheritable dependency
	// edges whose SOURCE is one of the given work items, for the claim's
	// tenant. CHAOS-3978: without it this route sees only the edges its own
	// provider extracted THIS run, so an edge minted by a DIFFERENT provider's
	// sync -- a Linear attachment linking a Linear issue to a GitHub PR,
	// `relationship_type_raw='linear_attachment'` -- is structurally invisible
	// to the side that would inherit from it, and the PR is rebuilt
	// `unassigned` on every run despite a valid, teamed donor.
	//
	// It is a REQUIRED method rather than an optional interface on purpose: a
	// double that silently kept the fresh-only behaviour would make every test
	// using it pass while production stayed blind, which is the failure mode
	// this ticket exists to remove.
	LoadStoredInheritableEdges(
		context.Context,
		Claim,
		[]string,
	) ([]githubWorkItemDependencyRow, error)
}

type githubWorkItemClickHouseDerivationContextSource struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

// Load assembles the full fact set: the provider-neutral facts (teams,
// projects, repos, members, manual fallbacks) come from teamattribution's
// ClickHouseFactSource; donor items stay a providersync-local read
// (loadDonors below), since they are the one fact loaded via this route's
// own githubWorkItemDerivationLoadRequest-driven donor-id/key lookup rather
// than a flat org-scoped scan.
func (source githubWorkItemClickHouseDerivationContextSource) Load(
	ctx context.Context,
	claim Claim,
	request teamattribution.GithubWorkItemDerivationLoadRequest,
) (teamattribution.GithubWorkItemDerivationFacts, error) {
	if ctx == nil || source.Conn == nil || source.Lease == nil || claim.Validate() != nil ||
		!isDerivedWorkItemProvider(claim.Provider) || claim.Dataset != "work-items" || request.AsOf.IsZero() {
		return teamattribution.GithubWorkItemDerivationFacts{}, ErrInvalidConfiguration
	}
	if err := source.Lease.Assert(ctx); err != nil {
		return teamattribution.GithubWorkItemDerivationFacts{}, err
	}
	loader := teamattribution.ClickHouseFactSource{Conn: source.Conn, Lease: source.Lease}
	facts := teamattribution.GithubWorkItemDerivationFacts{}
	var err error
	if facts.Teams, err = loader.LoadTeams(ctx, claim.OrgID); err != nil {
		return teamattribution.GithubWorkItemDerivationFacts{}, err
	}
	if facts.Projects, err = loader.LoadProjects(ctx, claim.OrgID, request.AsOf); err != nil {
		return teamattribution.GithubWorkItemDerivationFacts{}, err
	}
	if facts.Repos, err = loader.LoadRepos(ctx, claim.OrgID, request.AsOf); err != nil {
		return teamattribution.GithubWorkItemDerivationFacts{}, err
	}
	var providerTaggedRosterMembers []teamattribution.GithubWorkItemDerivationMemberFact
	if facts.Members, facts.UntypedMembers, facts.ProviderUntypedMembers, providerTaggedRosterMembers, err = loader.LoadMembers(ctx, claim.OrgID, request.AsOf); err != nil {
		return teamattribution.GithubWorkItemDerivationFacts{}, err
	}
	if facts.ProviderMembers, err = loader.LoadProviderMembers(ctx, claim.OrgID, request.AsOf); err != nil {
		return teamattribution.GithubWorkItemDerivationFacts{}, err
	}
	// CHAOS-4321 round 3 (team-lead ruling, 2026-08-26): teams.members
	// facets LoadMembers could provider-tag (via identities.
	// provider_identities) join the SAME ProviderMembers pool real
	// team_memberships rows populate, so they resolve through the SAME
	// provider-scoped attributionMapKey path -- not a parallel one.
	facts.ProviderMembers = append(facts.ProviderMembers, providerTaggedRosterMembers...)
	if facts.ManualFallbacks, err = loader.LoadManualFallbacks(ctx, claim.OrgID, request.AsOf); err != nil {
		return teamattribution.GithubWorkItemDerivationFacts{}, err
	}
	if facts.DonorItems, err = source.loadDonors(ctx, claim.OrgID, request); err != nil {
		return teamattribution.GithubWorkItemDerivationFacts{}, err
	}
	if err := source.Lease.Assert(ctx); err != nil {
		return teamattribution.GithubWorkItemDerivationFacts{}, err
	}
	return facts, nil
}

func loadGitHubWorkItemDerivationContext(
	ctx context.Context,
	claim Claim,
	rows githubWorkItemRows,
	source githubWorkItemDerivationContextSource,
	asOf time.Time,
) (teamattribution.GithubWorkItemDerivationContext, error) {
	return loadWorkItemDerivationContextForProvider(
		ctx, "github", claim, rows, source, asOf,
	)
}

// loadWorkItemDerivationContextForProvider is the provider-neutral context
// loader shared by the GitHub, GitLab, and Jira work-item families. The source
// facts are already provider-keyed, so the resolver can apply the same
// canonical precedence without copying or weakening the team-attribution
// contract.
func loadWorkItemDerivationContextForProvider(
	ctx context.Context,
	provider string,
	claim Claim,
	rows githubWorkItemRows,
	source githubWorkItemDerivationContextSource,
	asOf time.Time,
) (teamattribution.GithubWorkItemDerivationContext, error) {
	if ctx == nil || source == nil || claim.Validate() != nil ||
		claim.Provider != provider || claim.Dataset != "work-items" || asOf.IsZero() {
		return teamattribution.GithubWorkItemDerivationContext{}, ErrInvalidConfiguration
	}
	for _, row := range rows.WorkItems {
		if row.OrgID != claim.OrgID {
			return teamattribution.GithubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
	}
	for _, dependency := range rows.Dependencies {
		if dependency.OrgID != claim.OrgID {
			return teamattribution.GithubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
	}
	// D17 (owner ruling, 2026-08-06) ratifies failing closed on BOTH branches
	// below, as a deliberate divergence from job_work_items.py:1196-1210, which
	// catches a donor-load failure, logs it, and continues with whatever
	// inheritance it has left.
	//
	// This is deliberately NOT the optional-data class the same ruling governs
	// for fetches. A missing donor does not omit a row: it writes a DIFFERENT
	// team onto work_item_team_attributions and every derived surface
	// downstream, with nothing in the row saying the attribution was computed
	// blind. There is no "land what you have and record what you lost" shape
	// available here, so the unit fails instead. The Python-side silent
	// degradation is tracked as CHAOS-3467.
	//
	// The limit is the same rail for the same reason: a truncated donor set
	// produces confidently wrong attribution, not absent attribution.
	//
	// CHAOS-3978: the same ruling decides the STORED-edge read below. A
	// failure there is not "inherit a little less this run": the recompute
	// re-stamps every item it touched, so a run that could not see the stored
	// cross-provider edge writes `unassigned` OVER a correct `linked_issue`
	// attribution -- confidently wrong, silently. Python degrades and
	// continues at the equivalent site; that degradation is on the CHAOS-4150
	// silent-fail list, i.e. the defect inventory, not the precedent. A failed
	// unit is loud, retryable and self-healing; a silently team-less item is
	// none of those.
	subjectIDs := githubWorkItemDerivationSubjectIDs(rows.WorkItems)
	storedEdges, err := source.LoadStoredInheritableEdges(ctx, claim, subjectIDs)
	if err != nil {
		return teamattribution.GithubWorkItemDerivationContext{}, err
	}
	for _, dependency := range storedEdges {
		if dependency.OrgID != claim.OrgID {
			return teamattribution.GithubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
	}
	dependencies, storedOnly, merged := mergeStoredInheritableWorkItemEdges(
		rows.Dependencies, storedEdges,
	)
	donorIDs, donorKeys := githubWorkItemDerivationDonorTargets(dependencies)
	if len(donorIDs)+len(donorKeys) > teamattribution.GithubWorkItemDerivationContextLimit {
		return teamattribution.GithubWorkItemDerivationContext{}, ErrEffectRecoveryUnsafe
	}
	facts, err := source.Load(ctx, claim, teamattribution.GithubWorkItemDerivationLoadRequest{
		AsOf: asOf, DonorWorkItemIDs: donorIDs, DonorIssueKeys: donorKeys,
	})
	if err != nil {
		return teamattribution.GithubWorkItemDerivationContext{}, err
	}
	for _, donor := range facts.DonorItems {
		if donor.OrgID != claim.OrgID {
			return teamattribution.GithubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
	}
	derived := teamattribution.NewGitHubWorkItemDerivationContext(facts)
	subjects := make(map[string]teamattribution.GithubWorkItemDerivationSubject, len(facts.DonorItems)+len(rows.WorkItems))
	for _, donor := range facts.DonorItems {
		subjects[donor.WorkItemID] = donor
	}
	for _, row := range rows.WorkItems {
		subject := githubWorkItemDerivationSubjectFromRow(row)
		if subject.OrgID != claim.OrgID {
			return teamattribution.GithubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
		// Fresh rows are authoritative over a persisted donor version.
		subjects[subject.WorkItemID] = subject
	}
	linkedIssue, rescues, crossProviderRescues := derived.BuildLinkedIssueIndex(
		provider, subjects, toDerivationDependencyEdges(dependencies), storedOnly,
	)
	derived.LinkedIssue = linkedIssue
	derived.StoredEdgeMerge = teamattribution.GithubWorkItemStoredEdgeMergeObservation{
		StoredEdgesMerged:    merged,
		DonorRescues:         rescues,
		CrossProviderRescues: crossProviderRescues,
	}
	return derived, nil
}

func githubWorkItemDerivationSubjectIDs(rows []githubWorkItemRow) []string {
	ids := map[string]struct{}{}
	for _, row := range rows {
		if identifier := strings.TrimSpace(row.WorkItemID); identifier != "" {
			ids[identifier] = struct{}{}
		}
	}
	return sortedStringSet(ids)
}

// mergeStoredInheritableWorkItemEdges unions this run's fresh edges with the
// STORED inheritable edges for the same source items, minus the ones this
// run's provider snapshot proves were removed.
//
// The pruning proof is keyed on (source_work_item_id, relationship_type_raw),
// EXACTLY as _merge_stored_inheritable_edges does in Python
// (metrics/job_work_items.py). That key shape is load-bearing, not incidental:
//
//   - Per ITEM would be unsound. One item's edges come from several
//     independently-gated extractors (a GitHub PR body is always parsed;
//     Linear linkback COMMENTS are gated by GITHUB_FETCH_COMMENTS and capped
//     by GITHUB_COMMENTS_LIMIT), so a fresh body edge is no evidence that
//     comment extraction ran -- and treating it as evidence would delete the
//     stored linkback edges, decaying exactly the population CHAOS-4112
//     protects.
//   - Per (source, target, relationship_type) alone would never prune, so a
//     link genuinely removed upstream would donate forever.
//
// A DIVERGENCE between this key and Python's would undo CHAOS-4112 from
// whichever side drifted, since both writers stamp the same rows; the key
// shape is therefore pinned by test on both sides.
//
// Removals stay expressible without a tombstone because every provider
// re-extracts an item's links on each sync, so a link still present upstream
// reappears among the fresh edges. Residual (identical to Python's, and
// tracked in CHAOS-4129): an item that loses its LAST edge of a given
// provenance emits no fresh edge of that provenance, so its stored one keeps
// donating until another appears -- erring toward PRESERVING a team.
//
// Returns the merged edges, the set of keys that came from the store ALONE
// (for the rescue observation), and how many stored edges were added.
func mergeStoredInheritableWorkItemEdges(
	fresh []githubWorkItemDependencyRow,
	stored []githubWorkItemDependencyRow,
) ([]githubWorkItemDependencyRow, map[teamattribution.GithubWorkItemDerivationEdgeKey]bool, int) {
	freshKeys := make(map[teamattribution.GithubWorkItemDerivationEdgeKey]bool, len(fresh))
	resyncedProvenances := make(map[[2]string]bool, len(fresh))
	for _, dependency := range fresh {
		freshKeys[teamattribution.GithubWorkItemDerivationEdgeKey{
			Source:           dependency.SourceWorkItemID,
			Target:           dependency.TargetWorkItemID,
			RelationshipType: dependency.RelationshipType,
		}] = true
		resyncedProvenances[[2]string{
			dependency.SourceWorkItemID, dependency.RelationshipTypeRaw,
		}] = true
	}
	merged := append([]githubWorkItemDependencyRow(nil), fresh...)
	storedOnly := map[teamattribution.GithubWorkItemDerivationEdgeKey]bool{}
	added := 0
	for _, dependency := range stored {
		if !teamattribution.GithubWorkItemDerivationInheritableRelationships[dependency.RelationshipType] {
			continue
		}
		key := teamattribution.GithubWorkItemDerivationEdgeKey{
			Source:           dependency.SourceWorkItemID,
			Target:           dependency.TargetWorkItemID,
			RelationshipType: dependency.RelationshipType,
		}
		// A fresh edge is authoritative for its OWN identity. A stored edge
		// differing only by relationship_type is kept so the latest-edge
		// recency collapse can settle the pair by last_synced -- which is what
		// protects the retype case (relates_to -> blocked_by) that motivated
		// the fresh-only rule in the first place.
		if freshKeys[key] || storedOnly[key] {
			continue
		}
		if resyncedProvenances[[2]string{
			dependency.SourceWorkItemID, dependency.RelationshipTypeRaw,
		}] {
			// That extractor ran for this item this run and did not re-emit
			// this link: the provider removed it.
			continue
		}
		merged = append(merged, dependency)
		storedOnly[key] = true
		added++
	}
	return merged, storedOnly, added
}

func githubWorkItemDerivationDonorTargets(
	dependencies []githubWorkItemDependencyRow,
) ([]string, []string) {
	ids := map[string]struct{}{}
	keys := map[string]struct{}{}
	for _, dependency := range teamattribution.LatestGitHubWorkItemDerivationDependencies(toDerivationDependencyEdges(dependencies)) {
		if !teamattribution.GithubWorkItemDerivationInheritableRelationships[dependency.RelationshipType] {
			continue
		}
		target := strings.TrimSpace(dependency.TargetWorkItemID)
		if strings.HasPrefix(target, "extkey:") {
			key := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(target, "extkey:")))
			if key != "" {
				keys[key] = struct{}{}
			}
		} else if target != "" {
			ids[target] = struct{}{}
		}
	}
	return sortedStringSet(ids), sortedStringSet(keys)
}

// toDerivationDependencyEdges converts providersync's canonical dependency
// row (used far beyond the attribution cascade -- extraction, storage) to the
// narrow shape teamattribution.BuildLinkedIssueIndex and
// teamattribution.LatestGitHubWorkItemDerivationDependencies need.
func toDerivationDependencyEdges(rows []githubWorkItemDependencyRow) []teamattribution.GithubWorkItemDerivationDependencyEdge {
	edges := make([]teamattribution.GithubWorkItemDerivationDependencyEdge, len(rows))
	for index, row := range rows {
		edges[index] = teamattribution.GithubWorkItemDerivationDependencyEdge{
			SourceWorkItemID: row.SourceWorkItemID,
			TargetWorkItemID: row.TargetWorkItemID,
			RelationshipType: row.RelationshipType,
			LastSynced:       row.LastSynced,
		}
	}
	return edges
}

func sortedStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func githubWorkItemDerivationSubjectFromRow(row githubWorkItemRow) teamattribution.GithubWorkItemDerivationSubject {
	var repoID *string
	if row.RepoID != nil {
		value := row.RepoID.String()
		repoID = &value
	}
	return teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: row.WorkItemID, Provider: row.Provider, Type: row.Type, RepoID: repoID,
		NativeTeamKey: row.NativeTeamKey, ProjectKey: row.ProjectKey,
		ProjectID: row.ProjectID, ProjectName: row.ProjectName,
		Assignees: append([]string(nil), row.Assignees...),
		Reporter:  row.Reporter, OrgID: row.OrgID,
	}
}

// LoadStoredInheritableEdges reads the stored inheritable edges whose SOURCE is
// one of this run's items (CHAOS-3978).
//
// Bounded exactly like every other read here: tenant-scoped, keyed on the
// subject ids, restricted to the inheritable relationship types, and capped by
// the same context limit -- a keyed lookup, never a history scan.
//
// ONE bounded retry, then FAIL CLOSED (D17). A transient ClickHouse blip
// otherwise silently downgrades the whole unit's attribution to fresh-window
// inheritance and re-stamps `unassigned` over correct rows. Python retries and
// then CONTINUES at the equivalent site; that continue is catalogued as a
// silent-degradation defect (CHAOS-4150), so it is not the precedent to copy.
func (source githubWorkItemClickHouseDerivationContextSource) LoadStoredInheritableEdges(
	ctx context.Context,
	claim Claim,
	sourceWorkItemIDs []string,
) ([]githubWorkItemDependencyRow, error) {
	if ctx == nil || source.Conn == nil || source.Lease == nil || claim.Validate() != nil ||
		!isDerivedWorkItemProvider(claim.Provider) || claim.Dataset != "work-items" {
		return nil, ErrInvalidConfiguration
	}
	if len(sourceWorkItemIDs) == 0 {
		return []githubWorkItemDependencyRow{}, nil
	}
	if err := source.Lease.Assert(ctx); err != nil {
		return nil, err
	}
	relationshipTypes := make([]string, 0, len(teamattribution.GithubWorkItemDerivationInheritableRelationships))
	for relationshipType := range teamattribution.GithubWorkItemDerivationInheritableRelationships {
		relationshipTypes = append(relationshipTypes, relationshipType)
	}
	sort.Strings(relationshipTypes)
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		result, err := source.queryStoredInheritableEdges(
			ctx, claim.OrgID, sourceWorkItemIDs, relationshipTypes,
		)
		if err == nil {
			return result, nil
		}
		// The cap is a correctness rail, not a transient fault: retrying an
		// over-limit read just spends another query to fail the same way.
		if errors.Is(err, ErrEffectRecoveryUnsafe) {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (source githubWorkItemClickHouseDerivationContextSource) queryStoredInheritableEdges(
	ctx context.Context,
	orgID string,
	sourceWorkItemIDs []string,
	relationshipTypes []string,
) ([]githubWorkItemDependencyRow, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT source_work_item_id, target_work_item_id, relationship_type,
       relationship_type_raw, relationship_semantics_version, last_synced, org_id
FROM work_item_dependencies FINAL
WHERE org_id = ? AND has(?, source_work_item_id) AND has(?, relationship_type)
ORDER BY source_work_item_id, target_work_item_id, relationship_type
LIMIT ?`, orgID, sourceWorkItemIDs, relationshipTypes,
		teamattribution.GithubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDependencyRow{}
	for rows.Next() {
		var row githubWorkItemDependencyRow
		if err := rows.Scan(
			&row.SourceWorkItemID, &row.TargetWorkItemID, &row.RelationshipType,
			&row.RelationshipTypeRaw, &row.RelationshipSemanticsVersion,
			&row.LastSynced, &row.OrgID,
		); err != nil {
			return nil, err
		}
		result = append(result, row)
		if len(result) > teamattribution.GithubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (source githubWorkItemClickHouseDerivationContextSource) loadDonors(
	ctx context.Context, orgID string, request teamattribution.GithubWorkItemDerivationLoadRequest,
) ([]teamattribution.GithubWorkItemDerivationSubject, error) {
	if len(request.DonorWorkItemIDs) == 0 && len(request.DonorIssueKeys) == 0 {
		return []teamattribution.GithubWorkItemDerivationSubject{}, nil
	}
	maximum := len(request.DonorWorkItemIDs) + len(request.DonorIssueKeys)*2
	rows, err := source.Conn.Query(ctx, `
SELECT work_item_id, provider, type, toString(repo_id), native_team_key, project_key,
       project_id, project_name, assignees, org_id
FROM work_items FINAL
WHERE org_id = ? AND (
  has(?, work_item_id)
  OR (provider IN ('linear', 'jira') AND has(?, upper(splitByChar(':', work_item_id)[-1])))
)
LIMIT ?`, orgID, request.DonorWorkItemIDs, request.DonorIssueKeys, maximum+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []teamattribution.GithubWorkItemDerivationSubject{}
	for rows.Next() {
		var subject teamattribution.GithubWorkItemDerivationSubject
		if err := rows.Scan(
			&subject.WorkItemID, &subject.Provider, &subject.Type, &subject.RepoID, &subject.NativeTeamKey,
			&subject.ProjectKey, &subject.ProjectID, &subject.ProjectName,
			&subject.Assignees, &subject.OrgID,
		); err != nil {
			return nil, err
		}
		result = append(result, subject)
		if len(result) > maximum {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

var _ githubWorkItemDerivationContextSource = githubWorkItemClickHouseDerivationContextSource{}
