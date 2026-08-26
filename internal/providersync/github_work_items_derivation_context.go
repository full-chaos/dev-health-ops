package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const githubWorkItemDerivationContextLimit = 100_000

var githubWorkItemIssueKeyPattern = regexp.MustCompile(`^([A-Za-z]{2,})-\d+$`)

var githubWorkItemDerivationInheritableRelationships = map[string]bool{
	"relates_to": true, "relates": true, "duplicates": true, "external_issue_key": true,
}

type githubWorkItemDerivationCandidate struct {
	Source      string    `json:"source"`
	TeamID      *string   `json:"team_id"`
	TeamName    *string   `json:"team_name"`
	Confidence  string    `json:"confidence"`
	Evidence    string    `json:"evidence"`
	IsPrimary   int       `json:"is_primary"`
	Specificity int       `json:"specificity"`
	Priority    int       `json:"priority"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type githubWorkItemDerivationSubject struct {
	WorkItemID string
	Provider   string
	// Type is the item's own native kind (e.g. "pr", "merge_request", "bug",
	// "issue"). Consulted by resolve() to gate author_membership: only a
	// PR/MR-shaped item (per githubWorkItemDerivationIsPullOrMergeRequestType,
	// which checks Provider+Type, never WorkItemID shape -- codex round-5,
	// 2026-08-25) is eligible for the author (Reporter) membership signal.
	// CHAOS-4321 (chris, 08:30 PT) restored this path after an earlier round
	// of the same ticket removed it; also kept for callers outside this file
	// (e.g. persisted work_items rows).
	Type          string
	RepoID        *string
	NativeTeamKey *string
	ProjectKey    *string
	ProjectID     *string
	ProjectName   *string
	Assignees     []string
	// Reporter is the item's author (e.g. a PR's opener). CHAOS-4244 added an
	// author_membership path (rank 6, below linked_issue) fed through the
	// same resolveMembership two-layer lookup as assignee_membership; an
	// earlier round of CHAOS-4321 removed it, then chris's 08:30 PT ruling
	// restored it, gated on the SAME two-layer admin/provider resolution
	// (see the CHAOS-4321 comment above resolve()'s reporter block) --
	// resolve() DOES read Reporter for team attribution. See
	// docs/contribute/architecture/team-attribution.md §0.
	Reporter *string
	OrgID    string
}

type githubWorkItemDerivationTeamFact struct {
	Provider    string
	TeamID      string
	TeamName    string
	ProjectKeys []string
	UpdatedAt   time.Time
}

type githubWorkItemDerivationProjectFact struct {
	Provider    string
	TeamID      string
	TeamName    string
	ProjectID   string
	ProjectKey  *string
	IsPrimary   int
	Specificity int
	Priority    int
	UpdatedAt   time.Time
}

type githubWorkItemDerivationRepoFact struct {
	Provider     string
	TeamID       string
	TeamName     string
	RepoID       *string
	RepoFullName string
	IsPrimary    int
	Specificity  int
	Priority     int
	UpdatedAt    time.Time
}

type githubWorkItemDerivationMemberFact struct {
	Provider          string
	TeamID            string
	TeamName          string
	MemberID          string
	RawProviderUserID *string
	RawEmail          *string
	IdentityFacets    []string
	IsPrimary         int
	Specificity       int
	Priority          int
	UpdatedAt         time.Time
}

type githubWorkItemDerivationManualFallback struct {
	Provider  string
	ScopeType string
	ScopeID   string
	TeamID    string
	TeamName  string
	Reason    string
	Priority  int
}

// githubWorkItemDerivationUntypedMemberFact is one `teams.members` facet
// entry with no backing `identities` row (CHAOS-4321, team-lead correction):
// adding a member directly on `/org/admin/teams/[id]/edit` is one of the two
// admin surfaces chris named, so this must still resolve a team even absent
// an identities row. Untyped (no Provider field) -- `teams.members` carries
// no provider column, so it is matched against an item's assignee/reporter
// facet by normalized equality alone, regardless of provider.
type githubWorkItemDerivationUntypedMemberFact struct {
	TeamID    string
	TeamName  string
	Facet     string
	UpdatedAt time.Time
}

type githubWorkItemDerivationFacts struct {
	Teams    []githubWorkItemDerivationTeamFact
	Projects []githubWorkItemDerivationProjectFact
	Repos    []githubWorkItemDerivationRepoFact
	// Members is the ADMIN layer (CHAOS-4321): sourced from the
	// `identities`/`teams` catalog, provider-scoped via
	// `identities.provider_identities`. Authoritative -- consulted first,
	// and an ambiguous match here does NOT fall through to ProviderMembers.
	Members []githubWorkItemDerivationMemberFact
	// UntypedMembers is ALSO the admin layer: bare `teams.members` facets
	// with no backing `identities` row, matched without a provider tag.
	UntypedMembers []githubWorkItemDerivationUntypedMemberFact
	// ProviderMembers is the FALLBACK layer (chris, 2026-08-26 08:30 PT:
	// "manual is override -- if the override exists, use it, else use
	// attribution from providers"): sourced from provider auto-import
	// `team_memberships`, consulted ONLY when the admin layer (Members ∪
	// UntypedMembers) has ZERO candidates for a given identity.
	ProviderMembers []githubWorkItemDerivationMemberFact
	ManualFallbacks []githubWorkItemDerivationManualFallback
	DonorItems      []githubWorkItemDerivationSubject
}

type githubWorkItemDerivationLoadRequest struct {
	AsOf             time.Time
	DonorWorkItemIDs []string
	DonorIssueKeys   []string
}

type githubWorkItemDerivationContextSource interface {
	Load(
		context.Context,
		Claim,
		githubWorkItemDerivationLoadRequest,
	) (githubWorkItemDerivationFacts, error)

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

// githubWorkItemStoredEdgeMergeObservation is the ledger-side record of what
// the stored-edge union did on one unit. providersync is a pure effect-ledger
// package with no logger and no metrics registry, so this travels out through
// the route's existing `Result["observations"]` map and lands on the unit's
// persisted payload (providerunit.Handler), which is where an operator can see
// the CHAOS-3978 population recovering after deploy.
type githubWorkItemStoredEdgeMergeObservation struct {
	// StoredEdgesMerged counts stored edges unioned into this run's fresh set
	// (CHAOS-4112's decay fix, ported here).
	StoredEdgesMerged int `json:"stored_edges_merged"`
	// DonorRescues counts items whose inherited team came from an edge that
	// was ONLY in the store -- i.e. attributions that would have been rebuilt
	// `unassigned` without this union.
	DonorRescues int `json:"donor_rescues"`
	// CrossProviderRescues is the CHAOS-3978 subset of DonorRescues: the donor
	// item belongs to a DIFFERENT provider than the claim being derived, which
	// is exactly the population no per-provider fresh edge set can see.
	CrossProviderRescues int `json:"cross_provider_rescues"`
}

type githubWorkItemClickHouseDerivationContextSource struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type githubWorkItemDerivationContext struct {
	projectKeyTeams map[string]githubWorkItemDerivationTeamFact
	projectByID     map[string][]githubWorkItemDerivationCandidate
	projectByKey    map[string][]githubWorkItemDerivationCandidate
	repoByID        map[string][]githubWorkItemDerivationCandidate
	repoByName      map[string][]githubWorkItemDerivationCandidate
	// memberByID (admin layer, provider-scoped) and memberByUntypedFacet
	// (admin layer, no provider tag) together form the AUTHORITATIVE
	// membership layer; providerMemberByID (auto-import team_memberships)
	// is the FALLBACK layer, consulted only when the admin layer has
	// nothing for an identity (CHAOS-4321, chris 08:30 PT).
	memberByID           map[string][]githubWorkItemDerivationCandidate
	memberByUntypedFacet map[string][]githubWorkItemDerivationCandidate
	providerMemberByID   map[string][]githubWorkItemDerivationCandidate
	manualFallbacks      []githubWorkItemDerivationManualFallback
	linkedIssue          map[string][2]string
	storedEdgeMerge      githubWorkItemStoredEdgeMergeObservation
}

func loadGitHubWorkItemDerivationContext(
	ctx context.Context,
	claim Claim,
	rows githubWorkItemRows,
	source githubWorkItemDerivationContextSource,
	asOf time.Time,
) (githubWorkItemDerivationContext, error) {
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
) (githubWorkItemDerivationContext, error) {
	if ctx == nil || source == nil || claim.Validate() != nil ||
		claim.Provider != provider || claim.Dataset != "work-items" || asOf.IsZero() {
		return githubWorkItemDerivationContext{}, ErrInvalidConfiguration
	}
	for _, row := range rows.WorkItems {
		if row.OrgID != claim.OrgID {
			return githubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
	}
	for _, dependency := range rows.Dependencies {
		if dependency.OrgID != claim.OrgID {
			return githubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
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
		return githubWorkItemDerivationContext{}, err
	}
	for _, dependency := range storedEdges {
		if dependency.OrgID != claim.OrgID {
			return githubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
	}
	dependencies, storedOnly, merged := mergeStoredInheritableWorkItemEdges(
		rows.Dependencies, storedEdges,
	)
	donorIDs, donorKeys := githubWorkItemDerivationDonorTargets(dependencies)
	if len(donorIDs)+len(donorKeys) > githubWorkItemDerivationContextLimit {
		return githubWorkItemDerivationContext{}, ErrEffectRecoveryUnsafe
	}
	facts, err := source.Load(ctx, claim, githubWorkItemDerivationLoadRequest{
		AsOf: asOf, DonorWorkItemIDs: donorIDs, DonorIssueKeys: donorKeys,
	})
	if err != nil {
		return githubWorkItemDerivationContext{}, err
	}
	for _, donor := range facts.DonorItems {
		if donor.OrgID != claim.OrgID {
			return githubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
	}
	derived := newGitHubWorkItemDerivationContext(facts)
	subjects := make(map[string]githubWorkItemDerivationSubject, len(facts.DonorItems)+len(rows.WorkItems))
	for _, donor := range facts.DonorItems {
		subjects[donor.WorkItemID] = donor
	}
	for _, row := range rows.WorkItems {
		subject := githubWorkItemDerivationSubjectFromRow(row)
		if subject.OrgID != claim.OrgID {
			return githubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
		// Fresh rows are authoritative over a persisted donor version.
		subjects[subject.WorkItemID] = subject
	}
	linkedIssue, rescues, crossProviderRescues := derived.buildLinkedIssueIndex(
		provider, subjects, dependencies, storedOnly,
	)
	derived.linkedIssue = linkedIssue
	derived.storedEdgeMerge = githubWorkItemStoredEdgeMergeObservation{
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

// githubWorkItemDerivationEdgeKey is the identity a fresh edge is authoritative
// for. It is deliberately the ClickHouse sorting key of work_item_dependencies
// (source, target, relationship_type), matching Python's merged_deps key in
// job_work_items.py.
type githubWorkItemDerivationEdgeKey struct {
	source, target, relationshipType string
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
) ([]githubWorkItemDependencyRow, map[githubWorkItemDerivationEdgeKey]bool, int) {
	freshKeys := make(map[githubWorkItemDerivationEdgeKey]bool, len(fresh))
	resyncedProvenances := make(map[[2]string]bool, len(fresh))
	for _, dependency := range fresh {
		freshKeys[githubWorkItemDerivationEdgeKey{
			source:           dependency.SourceWorkItemID,
			target:           dependency.TargetWorkItemID,
			relationshipType: dependency.RelationshipType,
		}] = true
		resyncedProvenances[[2]string{
			dependency.SourceWorkItemID, dependency.RelationshipTypeRaw,
		}] = true
	}
	merged := append([]githubWorkItemDependencyRow(nil), fresh...)
	storedOnly := map[githubWorkItemDerivationEdgeKey]bool{}
	added := 0
	for _, dependency := range stored {
		if !githubWorkItemDerivationInheritableRelationships[dependency.RelationshipType] {
			continue
		}
		key := githubWorkItemDerivationEdgeKey{
			source:           dependency.SourceWorkItemID,
			target:           dependency.TargetWorkItemID,
			relationshipType: dependency.RelationshipType,
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
	for _, dependency := range latestGitHubWorkItemDerivationDependencies(dependencies) {
		if !githubWorkItemDerivationInheritableRelationships[dependency.RelationshipType] {
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

func latestGitHubWorkItemDerivationDependencies(
	dependencies []githubWorkItemDependencyRow,
) map[struct{ source, target string }]githubWorkItemDependencyRow {
	type edgeKey struct{ source, target string }
	latest := map[edgeKey]githubWorkItemDependencyRow{}
	for _, dependency := range dependencies {
		key := edgeKey{dependency.SourceWorkItemID, dependency.TargetWorkItemID}
		current, exists := latest[key]
		if !exists || dependency.LastSynced.After(current.LastSynced) ||
			(dependency.LastSynced.Equal(current.LastSynced) && dependency.RelationshipType < current.RelationshipType) {
			latest[key] = dependency
		}
	}
	result := make(map[struct{ source, target string }]githubWorkItemDependencyRow, len(latest))
	for key, dependency := range latest {
		result[struct{ source, target string }{key.source, key.target}] = dependency
	}
	return result
}

func sortedStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func githubWorkItemDerivationSubjectFromRow(row githubWorkItemRow) githubWorkItemDerivationSubject {
	var repoID *string
	if row.RepoID != nil {
		value := row.RepoID.String()
		repoID = &value
	}
	return githubWorkItemDerivationSubject{
		WorkItemID: row.WorkItemID, Provider: row.Provider, Type: row.Type, RepoID: repoID,
		NativeTeamKey: row.NativeTeamKey, ProjectKey: row.ProjectKey,
		ProjectID: row.ProjectID, ProjectName: row.ProjectName,
		Assignees: append([]string(nil), row.Assignees...),
		Reporter:  row.Reporter, OrgID: row.OrgID,
	}
}

func newGitHubWorkItemDerivationContext(
	facts githubWorkItemDerivationFacts,
) githubWorkItemDerivationContext {
	result := githubWorkItemDerivationContext{
		projectKeyTeams:      map[string]githubWorkItemDerivationTeamFact{},
		projectByID:          map[string][]githubWorkItemDerivationCandidate{},
		projectByKey:         map[string][]githubWorkItemDerivationCandidate{},
		repoByID:             map[string][]githubWorkItemDerivationCandidate{},
		repoByName:           map[string][]githubWorkItemDerivationCandidate{},
		memberByID:           map[string][]githubWorkItemDerivationCandidate{},
		memberByUntypedFacet: map[string][]githubWorkItemDerivationCandidate{},
		providerMemberByID:   map[string][]githubWorkItemDerivationCandidate{},
		manualFallbacks:      append([]githubWorkItemDerivationManualFallback(nil), facts.ManualFallbacks...),
		linkedIssue:          map[string][2]string{},
	}
	for _, team := range facts.Teams {
		for _, rawKey := range append(append([]string(nil), team.ProjectKeys...), team.TeamID) {
			key := strings.TrimSpace(rawKey)
			if key == "" {
				continue
			}
			if _, exists := result.projectKeyTeams[key]; !exists {
				result.projectKeyTeams[key] = team
			}
		}
	}
	for _, fact := range facts.Projects {
		candidate := githubWorkItemDerivationCandidateFromFact(
			"project_ownership", fact.TeamID, fact.TeamName,
			fmt.Sprintf("project_ownership=%s", githubWorkItemDerivationFirstNonEmpty(fact.ProjectID, githubWorkItemDerivationStringValue(fact.ProjectKey))),
			fact.IsPrimary, fact.Specificity, fact.Priority, fact.UpdatedAt,
		)
		if fact.ProjectID != "" {
			appendDerivationCandidate(result.projectByID, attributionMapKey(fact.Provider, fact.ProjectID), candidate)
		}
		if fact.ProjectKey != nil && strings.TrimSpace(*fact.ProjectKey) != "" {
			appendDerivationCandidate(result.projectByKey, attributionMapKey(fact.Provider, *fact.ProjectKey), candidate)
		}
	}
	for _, fact := range facts.Repos {
		candidate := githubWorkItemDerivationCandidateFromFact(
			"repo_ownership", fact.TeamID, fact.TeamName,
			fmt.Sprintf("repo_ownership=%s", githubWorkItemDerivationFirstNonEmpty(githubWorkItemDerivationStringValue(fact.RepoID), fact.RepoFullName)),
			fact.IsPrimary, fact.Specificity, fact.Priority, fact.UpdatedAt,
		)
		if fact.RepoID != nil && strings.TrimSpace(*fact.RepoID) != "" {
			appendDerivationCandidate(result.repoByID, attributionMapKey(fact.Provider, *fact.RepoID), candidate)
		}
		if fact.RepoFullName != "" {
			appendDerivationCandidate(result.repoByName, attributionMapKey(fact.Provider, fact.RepoFullName), candidate)
		}
	}
	// CHAOS-4321: team_name for a membership candidate is now resolved from
	// the admin-authored `teams` catalog (ONE canonical name per team_id),
	// not carried per-row on the old `team_memberships` fact -- the Python
	// loader's `admin_teams` dict has exactly one name per team_id (first
	// FROM-teams-FINAL row wins; ClickHouse `teams` is itself
	// ReplacingMergeTree-deduped to one row per id). Mirror that here with a
	// first-fixture-order-wins map so two Members facts naming the SAME
	// team_id with DIFFERENT TeamName strings (a shape that could exist
	// under the old per-row team_memberships.team_name column) collapse to
	// ONE name, exactly as the live pipeline (loadMembers -> newGitHubWorkItemDerivationContext)
	// now guarantees -- not two divergent provenance rows for the same team.
	memberTeamNames := map[string]string{}
	for _, fact := range facts.Members {
		teamID := strings.TrimSpace(fact.TeamID)
		if teamID == "" {
			continue
		}
		if _, exists := memberTeamNames[teamID]; !exists {
			memberTeamNames[teamID] = githubWorkItemDerivationFirstNonEmpty(fact.TeamName, teamID)
		}
	}
	for _, fact := range facts.Members {
		// CHAOS-4321: IsPrimary/Specificity are no longer per-row auto-import
		// data (the old `team_memberships.is_primary`/`.specificity` columns,
		// provider-supplied); they are a fixed protocol constant now that
		// membership comes from the admin-authored `identities`/`teams`
		// catalog, which carries no such per-membership signal.
		// `loadMembers` already stamps every fact IsPrimary=1/Specificity=60
		// (matching the Python loader's load_team_attribution_context
		// exactly), so this is belt-and-suspenders for the real pipeline --
		// but it is load-bearing for tests that construct `Facts.Members`
		// directly (bypassing loadMembers): fact.IsPrimary/fact.Specificity/
		// fact.TeamName are deliberately ignored here (TeamName resolved via
		// memberTeamNames above) so such a fixture can't silently diverge
		// from the value production actually emits.
		candidate := githubWorkItemDerivationCandidateFromFact(
			"assignee_membership", fact.TeamID, memberTeamNames[strings.TrimSpace(fact.TeamID)],
			fmt.Sprintf("assignee_membership=%s", githubWorkItemDerivationFirstNonEmpty(fact.MemberID, githubWorkItemDerivationStringValue(fact.RawEmail))),
			1, 60, fact.Priority, fact.UpdatedAt,
		)
		identities := []string{fact.MemberID, githubWorkItemDerivationStringValue(fact.RawProviderUserID), githubWorkItemDerivationStringValue(fact.RawEmail)}
		identities = append(identities, fact.IdentityFacets...)
		seen := map[string]struct{}{}
		for _, identity := range identities {
			key := normalizeDerivationIdentity(identity)
			if key == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			appendDerivationCandidate(result.memberByID, attributionMapKey(fact.Provider, key), candidate)
		}
	}
	// CHAOS-4321 (team-lead correction): a `teams.members` facet with no
	// backing `identities` row is STILL an admin mapping -- adding a member
	// directly on `/org/admin/teams/[id]/edit` is one of the two admin
	// surfaces chris named. Matched WITHOUT a provider tag, unlike
	// memberByID above (`teams.members` carries no provider column).
	for _, fact := range facts.UntypedMembers {
		facet := normalizeDerivationIdentity(fact.Facet)
		if facet == "" {
			continue
		}
		teamID := strings.TrimSpace(fact.TeamID)
		if teamID == "" {
			continue
		}
		candidate := githubWorkItemDerivationCandidateFromFact(
			"assignee_membership", teamID, githubWorkItemDerivationFirstNonEmpty(fact.TeamName, teamID),
			fmt.Sprintf("assignee_membership=%s", fact.Facet),
			1, 60, 0, fact.UpdatedAt,
		)
		result.memberByUntypedFacet[facet] = append(result.memberByUntypedFacet[facet], candidate)
	}
	// CHAOS-4321 (chris, 08:30 PT): provider auto-import fallback layer --
	// unchanged shape from before this ticket (fact.IsPrimary/.Specificity
	// ARE real per-row auto-import data here, unlike the admin layer above,
	// so they are used as-is, not overridden).
	for _, fact := range facts.ProviderMembers {
		candidate := githubWorkItemDerivationCandidateFromFact(
			"assignee_membership", fact.TeamID, fact.TeamName,
			fmt.Sprintf("assignee_membership=%s", githubWorkItemDerivationFirstNonEmpty(fact.MemberID, githubWorkItemDerivationStringValue(fact.RawEmail))),
			fact.IsPrimary, fact.Specificity, fact.Priority, fact.UpdatedAt,
		)
		identities := []string{fact.MemberID, githubWorkItemDerivationStringValue(fact.RawProviderUserID), githubWorkItemDerivationStringValue(fact.RawEmail)}
		identities = append(identities, fact.IdentityFacets...)
		seen := map[string]struct{}{}
		for _, identity := range identities {
			key := normalizeDerivationIdentity(identity)
			if key == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			appendDerivationCandidate(result.providerMemberByID, attributionMapKey(fact.Provider, key), candidate)
		}
	}
	return result
}

func githubWorkItemDerivationCandidateFromFact(
	source, teamID, teamName, evidence string,
	isPrimary, specificity, priority int,
	updatedAt time.Time,
) githubWorkItemDerivationCandidate {
	if teamName == "" {
		teamName = teamID
	}
	// An ownership row's team_id is passed through RAW by Python, so a blank
	// team id persists as "" on work_item_team_attributions -- NOT as NULL.
	// githubWorkItemDerivationStringPointer maps "" to nil, which made Go write
	// NULL where Python writes "", and in a Nullable(String) column those are
	// different values. The two daily rollups normalise both to "unassigned",
	// so the attribution table is the only surface that can see it. Pinned by
	// the empty_string_team_id_is_not_null oracle case.
	//
	// Only this FACT-derived path is corrected, because only it is measured.
	// The native_team, issue_project, manual_fallback and linked_issue
	// candidates still route through the nil-on-empty helper: a blank team id
	// is not reachable there from any oracle case in this PR, and changing
	// them would be an unmeasured claim.
	return githubWorkItemDerivationCandidate{
		Source: source, TeamID: &teamID, TeamName: &teamName,
		Confidence: confidenceForPrimary(isPrimary), Evidence: evidence,
		IsPrimary: isPrimary, Specificity: specificity, Priority: priority,
		UpdatedAt: normalizedDerivationTime(updatedAt),
	}
}

func confidenceForPrimary(primary int) string {
	if primary != 0 {
		return "high"
	}
	return "medium"
}

func appendDerivationCandidate(
	target map[string][]githubWorkItemDerivationCandidate,
	key string,
	candidate githubWorkItemDerivationCandidate,
) {
	target[key] = append(target[key], candidate)
}

func attributionMapKey(provider, key string) string {
	return strings.TrimSpace(provider) + "\x00" + strings.TrimSpace(key)
}

func normalizeDerivationIdentity(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func normalizedDerivationTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return value.UTC()
}

func (derived githubWorkItemDerivationContext) resolve(
	subject githubWorkItemDerivationSubject,
) (*string, *string, []githubWorkItemDerivationCandidate) {
	bySource := map[string][]githubWorkItemDerivationCandidate{}
	if candidate := derived.nativeTeamCandidate(subject); candidate != nil {
		bySource[candidate.Source] = append(bySource[candidate.Source], *candidate)
	}
	if inherited, exists := derived.linkedIssue[subject.WorkItemID]; exists {
		bySource["linked_issue"] = append(bySource["linked_issue"], githubWorkItemDerivationCandidate{
			Source: "linked_issue", TeamID: githubWorkItemDerivationStringPointer(inherited[0]), TeamName: githubWorkItemDerivationStringPointer(inherited[1]),
			Confidence: "medium", Evidence: "linked_issue=" + subject.WorkItemID,
			IsPrimary: 1, Specificity: 90, UpdatedAt: normalizedDerivationTime(time.Time{}),
		})
	}
	issueProjectTeams := map[string]struct{}{}
	if candidate := derived.issueProjectCandidate(subject); candidate != nil {
		bySource[candidate.Source] = append(bySource[candidate.Source], *candidate)
		if candidate.TeamID != nil {
			issueProjectTeams[*candidate.TeamID] = struct{}{}
		}
	}
	bySource["project_ownership"] = append(
		bySource["project_ownership"],
		derived.projectByID[attributionMapKey(subject.Provider, githubWorkItemDerivationStringValue(subject.ProjectID))]...,
	)
	for _, candidate := range derived.projectByKey[attributionMapKey(subject.Provider, githubWorkItemDerivationStringValue(subject.ProjectKey))] {
		if candidate.TeamID != nil {
			if _, duplicate := issueProjectTeams[*candidate.TeamID]; duplicate {
				continue
			}
		}
		bySource["project_ownership"] = append(bySource["project_ownership"], candidate)
	}
	bySource["repo_ownership"] = append(
		bySource["repo_ownership"],
		derived.repoByID[attributionMapKey(subject.Provider, githubWorkItemDerivationStringValue(subject.RepoID))]...,
	)
	bySource["repo_ownership"] = append(
		bySource["repo_ownership"],
		derived.repoByName[attributionMapKey(subject.Provider, githubWorkItemDerivationStringValue(subject.ProjectID))]...,
	)
	// CHAOS-4321 (chris's ruling, 2026-08-26, refined 08:30 PT): membership-
	// based attribution -- assignee AND author alike -- is a TWO-LAYER
	// resolution via resolveMembership: layer 1 (admin: memberByID ∪
	// memberByUntypedFacet, from identities/teams) is authoritative,
	// including when ambiguous (no fallthrough on admin ambiguity); layer 2
	// (providerMemberByID, from auto-import team_memberships) is consulted
	// only when layer 1 has zero candidates for that identity ("manual is
	// override -- if the override exists, use it, else use attribution
	// from providers"). Both layers apply the SAME exactly-one-team gate --
	// previously this gate applied ONLY to the reporter/author path
	// (CHAOS-4110); assignee had none and let rankDerivationCandidates's
	// specificity/priority ordering silently pick an arbitrary winner among
	// an ambiguous member's teams -- exactly the defect this ticket removes.
	membershipSkipReasons := map[string]struct{}{}
	for _, assignee := range subject.Assignees {
		assigneeCandidates, reason := derived.resolveMembership(subject.Provider, assignee)
		if len(assigneeCandidates) > 0 {
			bySource["assignee_membership"] = append(bySource["assignee_membership"], assigneeCandidates...)
		} else if reason != "" {
			membershipSkipReasons[reason] = struct{}{}
		}
	}
	// CHAOS-4244/CHAOS-4321: mirrors compute_work_items.py's
	// resolve_team_attribution -- the reporter (author) is a membership
	// signal GitHub's "assignee" field never carries. Stamps its OWN
	// source/rank (author_membership, rank 6 below linked_issue -- chris's
	// ruling, 2026-08-24: an author is a PERSON signal, "at best a
	// low-precedence fallback"). Restored by the 07:09 PT ruling above:
	// author_membership is NOT removed, only gated on the SAME two-layer
	// resolveMembership the assignee loop above now uses -- CHAOS-4321 did
	// not change the bot filter or the PR/MR type gate, both unchanged from
	// CHAOS-4244.
	var reporterSkipReason string
	if githubWorkItemDerivationIsPullOrMergeRequestType(subject.Provider, subject.Type) &&
		subject.Reporter != nil && strings.TrimSpace(*subject.Reporter) != "" {
		if githubWorkItemDerivationIsBotIdentity(*subject.Reporter) {
			reporterSkipReason = "bot_author"
		} else {
			reporterCandidates, reason := derived.resolveMembership(subject.Provider, *subject.Reporter)
			if len(reporterCandidates) > 0 {
				// Source AND Evidence are rewritten (not passed through
				// verbatim): reporterCandidates come from the SAME
				// resolveMembership the assignee loop above uses,
				// pre-stamped Source "assignee_membership" at fact-load
				// time, so the override must happen here, at the point of
				// use.
				relabeled := make([]githubWorkItemDerivationCandidate, len(reporterCandidates))
				for index, candidate := range reporterCandidates {
					candidate.Source = "author_membership"
					candidate.Evidence = "reporter=" + *subject.Reporter
					relabeled[index] = candidate
				}
				bySource["author_membership"] = append(bySource["author_membership"], relabeled...)
			} else if reason != "" {
				membershipSkipReasons[reason] = struct{}{}
			}
		}
	}
	bySource["manual_fallback"] = append(
		bySource["manual_fallback"], derived.manualCandidates(subject)...,
	)

	order := []string{
		"native_team", "issue_project", "project_ownership", "repo_ownership",
		"assignee_membership", "linked_issue", "author_membership",
		"manual_fallback", "unassigned",
	}
	var primary *githubWorkItemDerivationCandidate
	all := make([]githubWorkItemDerivationCandidate, 0)
	for _, source := range order {
		// No team_id collapse here, deliberately (unlike the Python mirror):
		// this route's write path already dedupes by the EXACT ClickHouse
		// sorting key -- (repo_id, work_item_id, team_id, source), evidence
		// excluded -- via githubWorkItemDerivedSortingKeyDedupe in
		// WriteGitHubWorkItemEffect, with a deterministic last-wins tie-break
		// on computed_at. Collapsing here too would be redundant AND wrong:
		// it would also erase legitimate SEPARATE-team-name-same-team_id or
		// same-team-different-rule candidates (e.g. two manual_fallback
		// rules naming the same team) that the provenance contract still
		// wants recorded pre-write. Python has no such write-time dedup
		// (sinks/clickhouse/core.py inserts verbatim), which is why
		// _collapse_by_team_id lives THERE instead.
		candidates := rankDerivationCandidates(dedupeDerivationCandidates(bySource[source]))
		if primary == nil && len(candidates) > 0 {
			value := candidates[0]
			primary = &value
		}
		all = append(all, candidates...)
	}
	if primary == nil {
		// CHAOS-4321 (ticket step 4/6, telemetry; refined 08:30 PT): mirrors
		// compute_work_items.py's final evidence composition exactly --
		// several reasons can coexist (e.g. an ambiguous admin-mapped
		// assignee AND a reporter with no mapping anywhere), so pick the
		// single most actionable one. `ambiguous_admin_membership` names a
		// fixable problem in the AUTHORITATIVE layer and wins over
		// everything, including a provider-layer ambiguity (a lower-
		// priority problem the admin mapping, once added, would settle).
		// `bot_author` is a definitive "this can never resolve" answer.
		// `ambiguous_provider_membership` is next. `no_membership` is the
		// least specific (nobody involved has any mapping in either layer)
		// and loses to all of the above. This exact precedence and string
		// set must stay byte-identical to Python's membership_reason
		// composition or the live-python-oracle gate (ci/check_go.sh)
		// fails -- see AGENTS.md "Anything cross-implementation needs a
		// differential oracle."
		if reporterSkipReason != "" {
			membershipSkipReasons[reporterSkipReason] = struct{}{}
		}
		membershipReason := githubWorkItemDerivationReasonWithPrefix(membershipSkipReasons, "ambiguous_admin_membership")
		if membershipReason == "" {
			if githubWorkItemDerivationHasReason(membershipSkipReasons, "bot_author") {
				membershipReason = "bot_author"
			} else {
				membershipReason = githubWorkItemDerivationReasonWithPrefix(membershipSkipReasons, "ambiguous_provider_membership")
			}
		}
		if membershipReason == "" && githubWorkItemDerivationHasReason(membershipSkipReasons, "no_membership") {
			membershipReason = "no_membership"
		}
		evidence := "no_candidate"
		if membershipReason != "" {
			evidence = "no_candidate:" + membershipReason
		}
		value := githubWorkItemDerivationCandidate{
			Source: "unassigned", Confidence: "none", Evidence: evidence,
			IsPrimary: 1, UpdatedAt: normalizedDerivationTime(time.Time{}),
		}
		primary = &value
		all = append(all, value)
	}
	marked := make([]githubWorkItemDerivationCandidate, len(all))
	for index, candidate := range all {
		candidate.IsPrimary = 0
		if sameDerivationCandidate(candidate, *primary) {
			candidate.IsPrimary = 1
			primary = &candidate
		}
		marked[index] = candidate
	}
	return primary.TeamID, primary.TeamName, marked
}

func (derived githubWorkItemDerivationContext) nativeTeamCandidate(
	subject githubWorkItemDerivationSubject,
) *githubWorkItemDerivationCandidate {
	if subject.NativeTeamKey == nil {
		return nil
	}
	team, exists := derived.projectKeyTeams[strings.TrimSpace(*subject.NativeTeamKey)]
	if !exists {
		return nil
	}
	return &githubWorkItemDerivationCandidate{
		Source: "native_team", TeamID: githubWorkItemDerivationStringPointer(team.TeamID), TeamName: githubWorkItemDerivationStringPointer(githubWorkItemDerivationFirstNonEmpty(team.TeamName, team.TeamID)),
		Confidence: "high", Evidence: "native_team_key=" + *subject.NativeTeamKey,
		IsPrimary: 1, Specificity: 100, UpdatedAt: normalizedDerivationTime(time.Time{}),
	}
}

func (derived githubWorkItemDerivationContext) issueProjectCandidate(
	subject githubWorkItemDerivationSubject,
) *githubWorkItemDerivationCandidate {
	keys := []string{workItemDerivationScope(subject)}
	if subject.ProjectKey != nil && *subject.ProjectKey != keys[0] {
		keys = append(keys, *subject.ProjectKey)
	}
	for _, key := range keys {
		// Python looks the key up STRIPPED (providers/teams.py:88,
		// `work_scope_id.strip()`) but reports the RAW key in its evidence, so
		// the trim belongs on the lookup alone. Trimming the evidence too
		// would swap one divergence for another; both halves are pinned by the
		// issue_project_scope_needs_trimming oracle case.
		team, exists := derived.projectKeyTeams[strings.TrimSpace(key)]
		if !exists {
			continue
		}
		return &githubWorkItemDerivationCandidate{
			Source: "issue_project", TeamID: githubWorkItemDerivationStringPointer(team.TeamID), TeamName: githubWorkItemDerivationStringPointer(githubWorkItemDerivationFirstNonEmpty(team.TeamName, team.TeamID)),
			Confidence: "high", Evidence: "issue_project_key=" + key,
			IsPrimary: 1, Specificity: 50, UpdatedAt: normalizedDerivationTime(time.Time{}),
		}
	}
	return nil
}

func workItemDerivationScope(subject githubWorkItemDerivationSubject) string {
	if subject.Provider == "jira" && subject.ProjectKey != nil {
		return *subject.ProjectKey
	}
	for _, value := range []*string{subject.ProjectID, subject.ProjectName, subject.NativeTeamKey, subject.ProjectKey} {
		if value != nil && *value != "" {
			return *value
		}
	}
	return ""
}

func (derived githubWorkItemDerivationContext) manualCandidates(
	subject githubWorkItemDerivationSubject,
) []githubWorkItemDerivationCandidate {
	result := []githubWorkItemDerivationCandidate{}
	for _, rule := range derived.manualFallbacks {
		if rule.Provider != "" && rule.Provider != subject.Provider && rule.ScopeType != "issue_key_prefix" {
			continue
		}
		scopeID := strings.TrimSpace(rule.ScopeID)
		matched := false
		switch rule.ScopeType {
		case "repo":
			// Python compares against a SET built by dropping falsy values
			// (compute_work_items.py:304-309), so a blank scope_id matches
			// nothing. Comparing directly against stringValue(nil) == ""
			// instead made a blank rule match EVERY item with a null repo --
			// one empty config row silently attributing the whole tenant to
			// one team. githubWorkItemDerivationScopeMatch reproduces the set
			// semantics: empty candidates are dropped and an empty scope_id
			// can never match.
			matched = githubWorkItemDerivationScopeMatch(scopeID,
				githubWorkItemDerivationStringValue(subject.RepoID),
				githubWorkItemDerivationStringValue(subject.ProjectID))
		case "project":
			matched = githubWorkItemDerivationScopeMatch(scopeID,
				githubWorkItemDerivationStringValue(subject.ProjectID),
				githubWorkItemDerivationStringValue(subject.ProjectKey),
				workItemDerivationScope(subject))
		case "member":
			for _, assignee := range subject.Assignees {
				// Python's member_ids drops falsy assignees, so a blank
				// assignee cannot be matched by a blank rule either.
				if scopeID == "" || assignee == "" {
					continue
				}
				matched = matched || normalizeDerivationIdentity(scopeID) == normalizeDerivationIdentity(assignee)
			}
		case "issue_key_prefix":
			prefix := githubWorkItemIssueKeyPrefix(subject.WorkItemID)
			matched = prefix != "" && prefix == strings.ToUpper(scopeID)
		}
		if !matched {
			continue
		}
		evidence := fmt.Sprintf("manual_fallback:%s=%s", rule.ScopeType, rule.ScopeID)
		if rule.Reason != "" {
			evidence += " (" + rule.Reason + ")"
		}
		result = append(result, githubWorkItemDerivationCandidate{
			Source: "manual_fallback", TeamID: githubWorkItemDerivationStringPointer(rule.TeamID), TeamName: githubWorkItemDerivationStringPointer(githubWorkItemDerivationFirstNonEmpty(rule.TeamName, rule.TeamID)),
			Confidence: "manual", Evidence: evidence, IsPrimary: 1,
			Priority: rule.Priority, UpdatedAt: normalizedDerivationTime(time.Time{}),
		})
	}
	return result
}

// githubWorkItemDerivationScopeMatch mirrors Python's `scope_id in {...}`
// where the set is built by dropping falsy values: an empty scope_id matches
// nothing, and an empty candidate is not a member.
func githubWorkItemDerivationScopeMatch(scopeID string, candidates ...string) bool {
	if scopeID == "" {
		return false
	}
	for _, candidate := range candidates {
		if candidate != "" && strings.TrimSpace(candidate) == scopeID {
			return true
		}
	}
	return false
}

func githubWorkItemIssueKeyPrefix(workItemID string) string {
	_, suffix, found := strings.Cut(workItemID, ":")
	if !found {
		return ""
	}
	match := githubWorkItemIssueKeyPattern.FindStringSubmatch(strings.TrimSpace(suffix))
	if len(match) != 2 {
		return ""
	}
	return strings.ToUpper(match[1])
}

func rankDerivationCandidates(
	candidates []githubWorkItemDerivationCandidate,
) []githubWorkItemDerivationCandidate {
	sort.SliceStable(candidates, func(left, right int) bool {
		a, b := candidates[left], candidates[right]
		if a.IsPrimary != b.IsPrimary {
			return a.IsPrimary > b.IsPrimary
		}
		if a.Specificity != b.Specificity {
			return a.Specificity > b.Specificity
		}
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		if !a.UpdatedAt.Equal(b.UpdatedAt) {
			return a.UpdatedAt.After(b.UpdatedAt)
		}
		if leftTeamID, rightTeamID := githubWorkItemDerivationStringValue(a.TeamID), githubWorkItemDerivationStringValue(b.TeamID); leftTeamID != rightTeamID {
			return leftTeamID < rightTeamID
		}
		if leftTeamName, rightTeamName := githubWorkItemDerivationStringValue(a.TeamName), githubWorkItemDerivationStringValue(b.TeamName); leftTeamName != rightTeamName {
			return leftTeamName < rightTeamName
		}
		if a.Confidence != b.Confidence {
			return a.Confidence < b.Confidence
		}
		if a.Evidence != b.Evidence {
			return a.Evidence < b.Evidence
		}
		return a.Source < b.Source
	})
	return candidates
}

func dedupeDerivationCandidates(
	candidates []githubWorkItemDerivationCandidate,
) []githubWorkItemDerivationCandidate {
	result := make([]githubWorkItemDerivationCandidate, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		key := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00%d\x00%s",
			candidate.Source, githubWorkItemDerivationStringValue(candidate.TeamID), githubWorkItemDerivationStringValue(candidate.TeamName),
			candidate.Confidence, candidate.Evidence, candidate.IsPrimary,
			candidate.Specificity, candidate.Priority, candidate.UpdatedAt.UTC().Format(time.RFC3339Nano))
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, candidate)
	}
	return result
}

func sameDerivationCandidate(left, right githubWorkItemDerivationCandidate) bool {
	return left.Source == right.Source && githubWorkItemDerivationStringValue(left.TeamID) == githubWorkItemDerivationStringValue(right.TeamID) &&
		githubWorkItemDerivationStringValue(left.TeamName) == githubWorkItemDerivationStringValue(right.TeamName) && left.Confidence == right.Confidence &&
		left.Evidence == right.Evidence && left.Specificity == right.Specificity &&
		left.Priority == right.Priority && left.UpdatedAt.Equal(right.UpdatedAt)
}

// buildLinkedIssueIndex resolves each team-less item to the team of a linked,
// first-class-attributed donor.
//
// It additionally reports how many of those inheritances rest on an edge that
// exists ONLY in the store (rescues) and how many of THOSE reach a donor in a
// different provider (the CHAOS-3978 population). The counts are derived from
// the same winning edge the attribution uses, never re-estimated, so an
// observation of N is a claim about N specific attributions.
func (derived githubWorkItemDerivationContext) buildLinkedIssueIndex(
	provider string,
	subjects map[string]githubWorkItemDerivationSubject,
	dependencies []githubWorkItemDependencyRow,
	storedOnly map[githubWorkItemDerivationEdgeKey]bool,
) (map[string][2]string, int, int) {
	donors := map[string][2]string{}
	baseNative := map[string]bool{}
	keyIndex := map[string]string{}
	ambiguous := map[string]bool{}
	allowedDonorSources := map[string]bool{
		"native_team": true, "issue_project": true, "project_ownership": true,
		"repo_ownership": true, "assignee_membership": true,
	}
	for _, subject := range subjects {
		baseNative[subject.WorkItemID] = derived.nativeTeamCandidate(subject) != nil
		teamID, teamName, candidates := derived.resolveWithoutLinked(subject)
		primarySource := ""
		for _, candidate := range candidates {
			if candidate.IsPrimary == 1 {
				primarySource = candidate.Source
				break
			}
		}
		if teamID != nil && allowedDonorSources[primarySource] {
			donors[subject.WorkItemID] = [2]string{*teamID, githubWorkItemDerivationFirstNonEmpty(githubWorkItemDerivationStringValue(teamName), *teamID)}
		}
		if (subject.Provider == "linear" || subject.Provider == "jira") && strings.Contains(subject.WorkItemID, ":") {
			_, suffix, _ := strings.Cut(subject.WorkItemID, ":")
			key := strings.ToUpper(strings.TrimSpace(suffix))
			if ambiguous[key] {
				continue
			}
			if existing, exists := keyIndex[key]; exists && existing != subject.WorkItemID {
				delete(keyIndex, key)
				ambiguous[key] = true
			} else {
				keyIndex[key] = subject.WorkItemID
			}
		}
	}
	candidates := map[string][]struct {
		target     string
		team       [2]string
		storedOnly bool
	}{}
	for _, dependency := range latestGitHubWorkItemDerivationDependencies(dependencies) {
		if !githubWorkItemDerivationInheritableRelationships[dependency.RelationshipType] || baseNative[dependency.SourceWorkItemID] {
			continue
		}
		target := dependency.TargetWorkItemID
		if strings.HasPrefix(target, "extkey:") {
			target = keyIndex[strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(target, "extkey:")))]
		}
		if target == "" {
			continue
		}
		if donor, exists := donors[target]; exists {
			candidates[dependency.SourceWorkItemID] = append(candidates[dependency.SourceWorkItemID], struct {
				target     string
				team       [2]string
				storedOnly bool
			}{
				target: target, team: donor,
				storedOnly: storedOnly[githubWorkItemDerivationEdgeKey{
					source:           dependency.SourceWorkItemID,
					target:           dependency.TargetWorkItemID,
					relationshipType: dependency.RelationshipType,
				}],
			})
		}
	}
	result := map[string][2]string{}
	rescues := 0
	crossProviderRescues := 0
	for source, possible := range candidates {
		sort.Slice(possible, func(left, right int) bool { return possible[left].target < possible[right].target })
		winner := possible[0]
		result[source] = winner.team
		if !winner.storedOnly {
			continue
		}
		rescues++
		// Cross-provider is decided by the DONOR's provider, not by the edge's
		// provenance string: `linear_attachment` is today's whole population,
		// but the defect is structural (one provider's sync mints the edge,
		// another provider's writer needs it), so the observation must count
		// the structure rather than one extractor's name.
		if donor, exists := subjects[winner.target]; exists &&
			strings.TrimSpace(donor.Provider) != strings.TrimSpace(provider) {
			crossProviderRescues++
		}
	}
	return result, rescues, crossProviderRescues
}

func (derived githubWorkItemDerivationContext) resolveWithoutLinked(
	subject githubWorkItemDerivationSubject,
) (*string, *string, []githubWorkItemDerivationCandidate) {
	saved := derived.linkedIssue
	derived.linkedIssue = nil
	teamID, teamName, candidates := derived.resolve(subject)
	derived.linkedIssue = saved
	return teamID, teamName, candidates
}

func (source githubWorkItemClickHouseDerivationContextSource) Load(
	ctx context.Context,
	claim Claim,
	request githubWorkItemDerivationLoadRequest,
) (githubWorkItemDerivationFacts, error) {
	if ctx == nil || source.Conn == nil || source.Lease == nil || claim.Validate() != nil ||
		!isDerivedWorkItemProvider(claim.Provider) || claim.Dataset != "work-items" || request.AsOf.IsZero() {
		return githubWorkItemDerivationFacts{}, ErrInvalidConfiguration
	}
	if err := source.Lease.Assert(ctx); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	facts := githubWorkItemDerivationFacts{}
	var err error
	if facts.Teams, err = source.loadTeams(ctx, claim.OrgID); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.Projects, err = source.loadProjects(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.Repos, err = source.loadRepos(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.Members, facts.UntypedMembers, err = source.loadMembers(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.ProviderMembers, err = source.loadProviderMembers(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.ManualFallbacks, err = source.loadManualFallbacks(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.DonorItems, err = source.loadDonors(ctx, claim.OrgID, request); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if err := source.Lease.Assert(ctx); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	return facts, nil
}

func (source githubWorkItemClickHouseDerivationContextSource) loadTeams(
	ctx context.Context, orgID string,
) ([]githubWorkItemDerivationTeamFact, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT provider, id,
       argMax(name, (updated_at, last_synced, name)),
       argMax(project_keys, (updated_at, last_synced, toJSONString(project_keys))),
       max(updated_at)
FROM teams
WHERE org_id = ?
GROUP BY provider, id, org_id
ORDER BY provider, id
LIMIT ?`, orgID, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDerivationTeamFact{}
	for rows.Next() {
		var fact githubWorkItemDerivationTeamFact
		if err := rows.Scan(&fact.Provider, &fact.TeamID, &fact.TeamName, &fact.ProjectKeys, &fact.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, fact)
		if len(result) > githubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func (source githubWorkItemClickHouseDerivationContextSource) loadProjects(
	ctx context.Context, orgID string, asOf time.Time,
) ([]githubWorkItemDerivationProjectFact, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT g.provider, g.team_id, ifNull(nullIf(t.name, ''), g.team_id),
       g.project_id, g.project_key, g.is_primary, g.specificity, g.priority, g.updated_at
FROM (
  SELECT o.org_id, o.provider, o.project_id, o.team_id,
         argMax(o.project_key, (o.updated_at, o.valid_from)) AS project_key,
         argMax(o.is_primary, (o.updated_at, o.valid_from)) AS is_primary,
         argMax(o.specificity, (o.updated_at, o.valid_from)) AS specificity,
         argMax(o.priority, (o.updated_at, o.valid_from)) AS priority,
         max(o.updated_at) AS updated_at
  FROM team_project_ownership AS o
  WHERE o.org_id = ? AND o.valid_from <= ? AND (o.valid_to IS NULL OR o.valid_to > ?)
  GROUP BY o.org_id, o.provider, o.project_id, o.team_id
) AS g
LEFT JOIN (
  SELECT org_id, id, argMax(name, (updated_at, last_synced, name)) AS name
  FROM teams
  GROUP BY org_id, id
) AS t ON t.org_id = g.org_id AND t.id = g.team_id
ORDER BY g.provider, g.project_id, g.team_id
LIMIT ?`, orgID, asOf, asOf, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDerivationProjectFact{}
	for rows.Next() {
		var fact githubWorkItemDerivationProjectFact
		var isPrimary uint8
		var specificity uint16
		var priority int32
		if err := rows.Scan(&fact.Provider, &fact.TeamID, &fact.TeamName, &fact.ProjectID, &fact.ProjectKey, &isPrimary, &specificity, &priority, &fact.UpdatedAt); err != nil {
			return nil, err
		}
		fact.IsPrimary = int(isPrimary)
		fact.Specificity = int(specificity)
		fact.Priority = int(priority)
		result = append(result, fact)
		if len(result) > githubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func (source githubWorkItemClickHouseDerivationContextSource) loadRepos(
	ctx context.Context, orgID string, asOf time.Time,
) ([]githubWorkItemDerivationRepoFact, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT g.provider, g.team_id, ifNull(nullIf(t.name, ''), g.team_id),
       toString(g.repo_id), g.repo_full_name, g.is_primary, g.specificity, g.priority, g.updated_at
FROM (
  SELECT o.org_id, o.provider, o.repo_full_name, o.team_id,
         argMax(o.repo_id, (o.updated_at, o.valid_from)) AS repo_id,
         argMax(o.is_primary, (o.updated_at, o.valid_from)) AS is_primary,
         argMax(o.specificity, (o.updated_at, o.valid_from)) AS specificity,
         argMax(o.priority, (o.updated_at, o.valid_from)) AS priority,
         max(o.updated_at) AS updated_at
  FROM team_repo_ownership AS o
  WHERE o.org_id = ? AND o.valid_from <= ? AND (o.valid_to IS NULL OR o.valid_to > ?)
  GROUP BY o.org_id, o.provider, o.repo_full_name, o.team_id
) AS g
LEFT JOIN (
  SELECT org_id, id, argMax(name, (updated_at, last_synced, name)) AS name
  FROM teams
  GROUP BY org_id, id
) AS t ON t.org_id = g.org_id AND t.id = g.team_id
ORDER BY g.provider, g.repo_full_name, g.team_id
LIMIT ?`, orgID, asOf, asOf, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDerivationRepoFact{}
	for rows.Next() {
		var fact githubWorkItemDerivationRepoFact
		var isPrimary uint8
		var specificity uint16
		var priority int32
		if err := rows.Scan(&fact.Provider, &fact.TeamID, &fact.TeamName, &fact.RepoID, &fact.RepoFullName, &isPrimary, &specificity, &priority, &fact.UpdatedAt); err != nil {
			return nil, err
		}
		fact.IsPrimary = int(isPrimary)
		fact.Specificity = int(specificity)
		fact.Priority = int(priority)
		result = append(result, fact)
		if len(result) > githubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

// githubWorkItemDerivationAdminIdentity is one row of the ClickHouse
// `identities` table (canonical_id -> team_ids, provider_identities) --
// admin-authored via `/org/admin/identities`.
type githubWorkItemDerivationAdminIdentity struct {
	CanonicalID        string
	Email              *string
	ProviderIdentities string
	TeamIDs            []string
	UpdatedAt          time.Time
}

// githubWorkItemDerivationAdminTeam is one row of the ClickHouse `teams`
// table (id -> members facet roster) -- admin-authored via
// `/org/admin/teams`.
type githubWorkItemDerivationAdminTeam struct {
	TeamID  string
	Name    string
	Members []string
}

// loadMembers builds membership-attribution facts EXCLUSIVELY from
// admin-authored data. CHAOS-4321 (chris's ruling, 2026-08-26 07:09 PT):
// membership-based team attribution (assignee AND author alike) is
// legitimate ONLY for mappings an operator wrote through the admin surface
// (`/org/admin/teams`, `/org/admin/identities`) -- never inferred from
// provider auto-import. Those admin screens write the `identities`
// (canonical_id -> team_ids, provider_identities) and `teams` (id -> members
// facet roster) tables; they never write `team_memberships` (populated
// exclusively by the 4 provider auto-import workers -- see
// src/dev_health_ops/workers/team_autoimport_{github,gitlab,jira,linear}.py
// -- and read by drift/conflict review only, never by attribution).
// `team_memberships` is therefore no longer queried here at all. Mirrors
// Python's load_team_attribution_context identity_rows/admin_team_rows
// (metrics/loaders/clickhouse.py) exactly -- required for the
// live-python-oracle gate (ci/check_go.sh).
//
// An identity's admin-authorized team set is the UNION of:
//   - (a) `identities.team_ids` (its own declared teams), and
//   - (b) any active team whose `teams.members` roster contains one of the
//     identity's facets (canonical_id / email / any provider raw id).
//
// (b) matters because the drift-approval admin action
// (apply_identity_membership_change,
// api/services/configuration/clickhouse_identity_drift.py) writes
// `teams.members` directly without also updating `identities.team_ids` --
// reading `team_ids` alone would silently drop that admin decision. A bare
// `teams.members` facet with no matching `identities` row is not usable on
// its own: it carries no provider tag, so there is no safe way to scope it
// to a work item's Provider for the memberByID lookup in resolve().
//
// `asOf` is accepted for interface parity with the other load* methods but
// unused: `identities` and `teams` are current-state ReplacingMergeTree
// tables (FINAL-resolved), not temporally versioned facts with a
// valid_from/valid_to window like `team_memberships` was.
func (source githubWorkItemClickHouseDerivationContextSource) loadMembers(
	ctx context.Context, orgID string, _ time.Time,
) ([]githubWorkItemDerivationMemberFact, []githubWorkItemDerivationUntypedMemberFact, error) {
	identityRows, err := source.Conn.Query(ctx, `
SELECT canonical_id, email, provider_identities, team_ids, updated_at
FROM identities FINAL
WHERE org_id = ? AND is_active = 1
LIMIT ?`, orgID, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, nil, err
	}
	defer identityRows.Close()
	identities := []githubWorkItemDerivationAdminIdentity{}
	for identityRows.Next() {
		var identity githubWorkItemDerivationAdminIdentity
		if err := identityRows.Scan(
			&identity.CanonicalID, &identity.Email, &identity.ProviderIdentities,
			&identity.TeamIDs, &identity.UpdatedAt,
		); err != nil {
			return nil, nil, err
		}
		identities = append(identities, identity)
		if len(identities) > githubWorkItemDerivationContextLimit {
			return nil, nil, ErrEffectRecoveryUnsafe
		}
	}
	if err := identityRows.Err(); err != nil {
		return nil, nil, err
	}

	teamRows, err := source.Conn.Query(ctx, `
SELECT id, name, members
FROM teams FINAL
WHERE org_id = ? AND is_active = 1
LIMIT ?`, orgID, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, nil, err
	}
	defer teamRows.Close()
	adminTeams := map[string]githubWorkItemDerivationAdminTeam{}
	teamCount := 0
	for teamRows.Next() {
		var team githubWorkItemDerivationAdminTeam
		if err := teamRows.Scan(&team.TeamID, &team.Name, &team.Members); err != nil {
			return nil, nil, err
		}
		adminTeams[team.TeamID] = team
		teamCount++
		if teamCount > githubWorkItemDerivationContextLimit {
			return nil, nil, ErrEffectRecoveryUnsafe
		}
	}
	if err := teamRows.Err(); err != nil {
		return nil, nil, err
	}

	teamMemberFacets := make(map[string]map[string]struct{}, len(adminTeams))
	for teamID, team := range adminTeams {
		facets := make(map[string]struct{}, len(team.Members))
		for _, member := range team.Members {
			if key := normalizeDerivationIdentity(member); key != "" {
				facets[key] = struct{}{}
			}
		}
		teamMemberFacets[teamID] = facets
	}

	result := []githubWorkItemDerivationMemberFact{}
	for _, identity := range identities {
		canonicalID := strings.TrimSpace(identity.CanonicalID)
		if canonicalID == "" {
			continue
		}
		providerIdentities := map[string][]string{}
		if raw := strings.TrimSpace(identity.ProviderIdentities); raw != "" {
			// A malformed JSON payload degrades to "no provider facets" for
			// this identity rather than failing the whole derivation run --
			// mirrors Python's _decode_provider_identities_json, which
			// returns {} on a decode error.
			_ = json.Unmarshal([]byte(raw), &providerIdentities)
		}
		email := githubWorkItemDerivationStringValue(identity.Email)

		facets := map[string]struct{}{}
		if key := normalizeDerivationIdentity(canonicalID); key != "" {
			facets[key] = struct{}{}
		}
		if key := normalizeDerivationIdentity(email); key != "" {
			facets[key] = struct{}{}
		}
		for _, rawIDs := range providerIdentities {
			for _, rawID := range rawIDs {
				if key := normalizeDerivationIdentity(rawID); key != "" {
					facets[key] = struct{}{}
				}
			}
		}

		resolvedTeamIDs := map[string]struct{}{}
		for _, teamID := range identity.TeamIDs {
			teamID = strings.TrimSpace(teamID)
			if _, exists := adminTeams[teamID]; exists {
				resolvedTeamIDs[teamID] = struct{}{}
			}
		}
		for teamID, memberFacets := range teamMemberFacets {
			for facet := range facets {
				if _, overlap := memberFacets[facet]; overlap {
					resolvedTeamIDs[teamID] = struct{}{}
					break
				}
			}
		}
		if len(resolvedTeamIDs) == 0 {
			continue
		}

		for provider, rawIDs := range providerIdentities {
			provider = strings.TrimSpace(provider)
			if provider == "" {
				continue
			}
			identityFacets := append([]string(nil), rawIDs...)
			var rawEmail *string
			if email != "" {
				emailCopy := email
				rawEmail = &emailCopy
			}
			for teamID := range resolvedTeamIDs {
				team := adminTeams[teamID]
				result = append(result, githubWorkItemDerivationMemberFact{
					Provider:       provider,
					TeamID:         teamID,
					TeamName:       githubWorkItemDerivationFirstNonEmpty(team.Name, teamID),
					MemberID:       canonicalID,
					RawEmail:       rawEmail,
					IdentityFacets: identityFacets,
					IsPrimary:      1,
					// 60, matching the Python loader
					// (metrics/loaders/clickhouse.py's
					// load_team_attribution_context): an explicit
					// per-identity admin mapping is a more curated signal
					// than a generic roster fallback and should win any
					// intra-source tie-break, if one is ever added on the
					// Go side (this resolver has no such second
					// assignee_membership source today, unlike Python's
					// legacy `_resolve_team` YAML/store path).
					Specificity: 60,
					Priority:    0,
					UpdatedAt:   identity.UpdatedAt,
				})
				if len(result) > githubWorkItemDerivationContextLimit {
					return nil, nil, ErrEffectRecoveryUnsafe
				}
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Provider != result[j].Provider {
			return result[i].Provider < result[j].Provider
		}
		if result[i].MemberID != result[j].MemberID {
			return result[i].MemberID < result[j].MemberID
		}
		return result[i].TeamID < result[j].TeamID
	})

	// CHAOS-4321 (team-lead correction): every `teams.members` facet is ALSO
	// an untyped admin-mapping candidate, independent of whether it has a
	// backing `identities` row above -- adding a member directly on
	// `/org/admin/teams/[id]/edit` is one of the two admin surfaces chris
	// named.
	untyped := []githubWorkItemDerivationUntypedMemberFact{}
	for _, teamID := range githubWorkItemDerivationSortedAdminTeamIDs(adminTeams) {
		team := adminTeams[teamID]
		for _, facet := range team.Members {
			if strings.TrimSpace(facet) == "" {
				continue
			}
			untyped = append(untyped, githubWorkItemDerivationUntypedMemberFact{
				TeamID:   teamID,
				TeamName: githubWorkItemDerivationFirstNonEmpty(team.Name, teamID),
				Facet:    facet,
			})
			if len(untyped) > githubWorkItemDerivationContextLimit {
				return nil, nil, ErrEffectRecoveryUnsafe
			}
		}
	}
	return result, untyped, nil
}

// githubWorkItemDerivationSortedAdminTeamIDs returns adminTeams' keys sorted,
// so loadMembers's UntypedMembers output is deterministic (map iteration
// order is not) -- required for the live-python-oracle byte-for-byte gate.
func githubWorkItemDerivationSortedAdminTeamIDs(
	adminTeams map[string]githubWorkItemDerivationAdminTeam,
) []string {
	ids := make([]string, 0, len(adminTeams))
	for id := range adminTeams {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// loadProviderMembers is the FALLBACK membership layer (chris, 2026-08-26
// 08:30 PT: "manual is override -- if the override exists, use it, else use
// attribution from providers"): unchanged from before CHAOS-4321, reading
// provider auto-import `team_memberships` directly. Consulted by
// resolveMembership only when the admin layer (loadMembers's two return
// values) has zero candidates for a given identity.
func (source githubWorkItemClickHouseDerivationContextSource) loadProviderMembers(
	ctx context.Context, orgID string, asOf time.Time,
) ([]githubWorkItemDerivationMemberFact, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT g.provider, g.team_id, ifNull(nullIf(t.name, ''), g.team_id),
       g.member_id, g.raw_provider_user_id, g.raw_email, g.identity_facets,
       g.is_primary, g.specificity, g.priority, g.updated_at
FROM (
  SELECT o.org_id, o.provider, o.team_id, o.member_id,
         argMax(o.raw_provider_user_id, (o.updated_at, o.valid_from)) AS raw_provider_user_id,
         argMax(o.raw_email, (o.updated_at, o.valid_from)) AS raw_email,
         argMax(o.identity_facets, (o.updated_at, o.valid_from)) AS identity_facets,
         argMax(o.is_primary, (o.updated_at, o.valid_from)) AS is_primary,
         argMax(o.specificity, (o.updated_at, o.valid_from)) AS specificity,
         argMax(o.priority, (o.updated_at, o.valid_from)) AS priority,
         max(o.updated_at) AS updated_at
  FROM team_memberships AS o
  WHERE o.org_id = ? AND o.valid_from <= ? AND (o.valid_to IS NULL OR o.valid_to > ?)
  GROUP BY o.org_id, o.provider, o.team_id, o.member_id
) AS g
LEFT JOIN (
  SELECT org_id, id, argMax(name, (updated_at, last_synced, name)) AS name
  FROM teams
  GROUP BY org_id, id
) AS t ON t.org_id = g.org_id AND t.id = g.team_id
ORDER BY g.provider, g.member_id, g.team_id
LIMIT ?`, orgID, asOf, asOf, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDerivationMemberFact{}
	for rows.Next() {
		var fact githubWorkItemDerivationMemberFact
		var isPrimary uint8
		var specificity uint16
		var priority int32
		if err := rows.Scan(&fact.Provider, &fact.TeamID, &fact.TeamName, &fact.MemberID, &fact.RawProviderUserID, &fact.RawEmail, &fact.IdentityFacets, &isPrimary, &specificity, &priority, &fact.UpdatedAt); err != nil {
			return nil, err
		}
		fact.IsPrimary = int(isPrimary)
		fact.Specificity = int(specificity)
		fact.Priority = int(priority)
		result = append(result, fact)
		if len(result) > githubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func (source githubWorkItemClickHouseDerivationContextSource) loadManualFallbacks(
	ctx context.Context, orgID string, asOf time.Time,
) ([]githubWorkItemDerivationManualFallback, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT provider, scope_type, scope_id, team_id, ifNull(nullIf(team_name, ''), team_id), reason, priority
FROM manual_attribution_fallbacks FINAL
WHERE org_id = ? AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
ORDER BY provider, scope_type, scope_id, priority, team_id, team_name, reason
LIMIT ?`, orgID, asOf, asOf, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDerivationManualFallback{}
	for rows.Next() {
		var fact githubWorkItemDerivationManualFallback
		var priority int32
		if err := rows.Scan(&fact.Provider, &fact.ScopeType, &fact.ScopeID, &fact.TeamID, &fact.TeamName, &fact.Reason, &priority); err != nil {
			return nil, err
		}
		fact.Priority = int(priority)
		result = append(result, fact)
		if len(result) > githubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
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
	relationshipTypes := make([]string, 0, len(githubWorkItemDerivationInheritableRelationships))
	for relationshipType := range githubWorkItemDerivationInheritableRelationships {
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
		githubWorkItemDerivationContextLimit+1)
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
		if len(result) > githubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (source githubWorkItemClickHouseDerivationContextSource) loadDonors(
	ctx context.Context, orgID string, request githubWorkItemDerivationLoadRequest,
) ([]githubWorkItemDerivationSubject, error) {
	if len(request.DonorWorkItemIDs) == 0 && len(request.DonorIssueKeys) == 0 {
		return []githubWorkItemDerivationSubject{}, nil
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
	result := []githubWorkItemDerivationSubject{}
	for rows.Next() {
		var subject githubWorkItemDerivationSubject
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

func githubWorkItemDerivationStringPointer(value string) *string {
	if value == "" {
		return nil
	}
	copy := value
	return &copy
}

func githubWorkItemDerivationStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// resolveMembership mirrors compute_work_items.py's `_resolve_membership`
// (CHAOS-4321, chris 08:30 PT: "manual is override -- if the override
// exists, use it, else use attribution from providers"). Layer 1 (admin:
// `memberByID` ∪ `memberByUntypedFacet`, sourced from `identities`/`teams`)
// is AUTHORITATIVE when it has ANY candidate for this identity -- including
// when ambiguous: an ambiguous admin mapping does NOT fall through to layer
// 2, it needs fixing, not bypassing. Layer 2 (`providerMemberByID`, sourced
// from provider auto-import `team_memberships`) is consulted ONLY when
// layer 1 has ZERO candidates for this identity. Both layers apply the SAME
// exactly-one-team gate.
//
// Returns `(candidates, reason)`: `candidates` is the resolved list to use
// for the caller's source when exactly one team resolved (`reason` is
// `""`); otherwise `candidates` is nil and `reason` is one of
// `"ambiguous_admin_membership:<sorted team ids>"`,
// `"ambiguous_provider_membership:<sorted team ids>"`, or `"no_membership"`
// -- `""` only when there is no identity to look up at all (no assignee, no
// reporter).
func (derived githubWorkItemDerivationContext) resolveMembership(
	provider, identity string,
) ([]githubWorkItemDerivationCandidate, string) {
	if strings.TrimSpace(identity) == "" {
		return nil, ""
	}
	key := normalizeDerivationIdentity(identity)
	adminCandidates := append(
		[]githubWorkItemDerivationCandidate(nil),
		derived.memberByID[attributionMapKey(provider, key)]...,
	)
	adminCandidates = append(adminCandidates, derived.memberByUntypedFacet[key]...)
	adminTeams := map[string]struct{}{}
	for _, candidate := range adminCandidates {
		adminTeams[githubWorkItemDerivationStringValue(candidate.TeamID)] = struct{}{}
	}
	if len(adminTeams) == 1 {
		return adminCandidates, ""
	}
	if len(adminTeams) > 1 {
		return nil, "ambiguous_admin_membership:" + githubWorkItemDerivationSortedTeamIDs(adminTeams)
	}

	providerCandidates := derived.providerMemberByID[attributionMapKey(provider, key)]
	providerTeams := map[string]struct{}{}
	for _, candidate := range providerCandidates {
		providerTeams[githubWorkItemDerivationStringValue(candidate.TeamID)] = struct{}{}
	}
	if len(providerTeams) == 1 {
		return providerCandidates, ""
	}
	if len(providerTeams) > 1 {
		return nil, "ambiguous_provider_membership:" + githubWorkItemDerivationSortedTeamIDs(providerTeams)
	}
	return nil, "no_membership"
}

// githubWorkItemDerivationSortedTeamIDs renders a team-id set as a
// deterministic, comma-joined, sorted string for an ambiguous-membership
// evidence/reason suffix -- so an admin can act on the persisted evidence
// text (team-lead, CHAOS-4321: "list the team ids in the evidence string,
// so an admin can fix the mapping").
func githubWorkItemDerivationSortedTeamIDs(teams map[string]struct{}) string {
	ids := make([]string, 0, len(teams))
	for id := range teams {
		if id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

// githubWorkItemDerivationHasReason reports set membership for the
// membershipSkipReasons/reporterSkipReason evidence composition in
// resolve() -- a plain helper so that composition reads as a priority
// switch rather than repeated map-comma-ok checks.
func githubWorkItemDerivationHasReason(reasons map[string]struct{}, reason string) bool {
	_, exists := reasons[reason]
	return exists
}

// githubWorkItemDerivationReasonWithPrefix returns the (deterministic,
// sorted-first) reason in `reasons` that starts with `prefix` -- used for
// the ambiguous_admin_membership/ambiguous_provider_membership reasons,
// which carry a variable ":<team ids>" suffix so `githubWorkItemDerivationHasReason`'s
// exact-match check cannot be used for them. Returns "" if none match.
func githubWorkItemDerivationReasonWithPrefix(reasons map[string]struct{}, prefix string) string {
	matches := make([]string, 0, len(reasons))
	for reason := range reasons {
		if strings.HasPrefix(reason, prefix) {
			matches = append(matches, reason)
		}
	}
	if len(matches) == 0 {
		return ""
	}
	sort.Strings(matches)
	return matches[0]
}

// githubWorkItemDerivationIsBotIdentity reports whether an identity is a
// GitHub App/bot actor. Mirrors compute_work_items.py's _is_bot_identity:
// IdentityResolver.resolve falls back to "provider:username" for any
// identity with no email -- true of every bot -- so the raw login, brackets
// included, survives resolution (e.g. "github:dependabot[bot]"). "[bot]" is
// reserved: GitHub does not let a human register a bracketed login, the same
// invariant this repo's other bot detectors already rely on
// (isLinearIntegrationAuthor-equivalent checks, providers/_ai_detection.py's
// CI_BOTS/KNOWN_AI_BOTS) -- this reuses that convention rather than a second
// bot list.
func githubWorkItemDerivationIsBotIdentity(identity string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(identity)), "[bot]")
}

// githubWorkItemDerivationIsPullOrMergeRequestType gates author_membership
// (CHAOS-4244/CHAOS-4321) to PR/MR work items, by Provider+Type -- NOT by
// WorkItemID shape (codex R5, 2026-08-25, BLOCK): the prior `ghpr:`/
// `gitlab:...!...` ID-prefix gate had no way to notice a Provider that did
// not match the shape it was reading, so a malformed or legacy row whose
// WorkItemID happened to look like a GitLab MR (contains `!`) but whose
// Provider was NOT actually "gitlab" (e.g. "jira") would incorrectly pass.
// This resolver is provider-neutral (shared by GitHub, GitLab, and Jira via
// loadWorkItemDerivationContextForProvider), so Type must be checked
// together with Provider, never Type alone -- two providers could reuse the
// same Type string with different meaning.
//
//   - GitHub: Type "pr" (github_work_items_rows.go's PR row builder); a plain
//     issue is some other Type (e.g. "bug", "issue").
//   - GitLab: Type "merge_request" (gitlab_work_items_rows.go's MR row
//     builder); a plain issue is a different Type.
//   - Jira/Linear have no PR-equivalent Type, so neither Provider ever
//     matches and this gate simply never opens for them.
func githubWorkItemDerivationIsPullOrMergeRequestType(provider, itemType string) bool {
	switch provider {
	case "github":
		return itemType == "pr"
	case "gitlab":
		return itemType == "merge_request"
	default:
		return false
	}
}

func githubWorkItemDerivationFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ githubWorkItemDerivationContextSource = githubWorkItemClickHouseDerivationContextSource{}
