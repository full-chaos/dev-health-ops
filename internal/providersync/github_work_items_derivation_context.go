package providersync

import (
	"context"
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
	WorkItemID    string
	Provider      string
	RepoID        *string
	NativeTeamKey *string
	ProjectKey    *string
	ProjectID     *string
	ProjectName   *string
	Assignees     []string
	// Reporter is the item's author (e.g. a PR's opener). CHAOS-4244: GitHub's
	// "assignee" field is distinct from and far less commonly set than the
	// author, so a PR opened by a team member with no assignee, no
	// repo_patterns row, and no linked issue used to resolve unassigned. This
	// mirrors compute_work_items.py's resolve_team_attribution, which now
	// feeds item.reporter into the SAME assignee_membership candidate list.
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

type githubWorkItemDerivationFacts struct {
	Teams           []githubWorkItemDerivationTeamFact
	Projects        []githubWorkItemDerivationProjectFact
	Repos           []githubWorkItemDerivationRepoFact
	Members         []githubWorkItemDerivationMemberFact
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
	memberByID      map[string][]githubWorkItemDerivationCandidate
	manualFallbacks []githubWorkItemDerivationManualFallback
	linkedIssue     map[string][2]string
	storedEdgeMerge githubWorkItemStoredEdgeMergeObservation
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
		WorkItemID: row.WorkItemID, Provider: row.Provider, RepoID: repoID,
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
		projectKeyTeams: map[string]githubWorkItemDerivationTeamFact{},
		projectByID:     map[string][]githubWorkItemDerivationCandidate{},
		projectByKey:    map[string][]githubWorkItemDerivationCandidate{},
		repoByID:        map[string][]githubWorkItemDerivationCandidate{},
		repoByName:      map[string][]githubWorkItemDerivationCandidate{},
		memberByID:      map[string][]githubWorkItemDerivationCandidate{},
		manualFallbacks: append([]githubWorkItemDerivationManualFallback(nil), facts.ManualFallbacks...),
		linkedIssue:     map[string][2]string{},
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
	for _, fact := range facts.Members {
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
			appendDerivationCandidate(result.memberByID, attributionMapKey(fact.Provider, key), candidate)
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
	for _, assignee := range subject.Assignees {
		bySource["assignee_membership"] = append(
			bySource["assignee_membership"],
			derived.memberByID[attributionMapKey(subject.Provider, normalizeDerivationIdentity(assignee))]...,
		)
	}
	// CHAOS-4244: mirrors compute_work_items.py's resolve_team_attribution --
	// the reporter (author) is a membership signal GitHub's "assignee" field
	// never carries. Stamps its OWN source/rank (author_membership, rank 6 --
	// chris's ruling, 2026-08-24: an author is a PERSON signal, "at best a
	// low-precedence fallback", and must NOT beat a real linked_issue donor
	// (rank 5), so it cannot share assignee_membership's rank 4).
	//
	// Ambiguity gate (chris, CHAOS-4110, 2026-08-23): a person-shaped signal
	// is only usable "where the reporter's membership is unambiguous (exactly
	// one team)". Bot filter: a bot/App author (dependabot, github-actions,
	// ...) carries no team meaning -- see githubWorkItemDerivationIsBotIdentity.
	// An assignee keeps neither gate: it is the pre-existing rank-4 mechanism,
	// not the new one this ticket adds. reporterSkipReason records WHY, so the
	// eventual unassigned candidate (if nothing else resolves either) is
	// traceable instead of a bare "no_candidate".
	// No legacy-resolver reporter path exists here, deliberately (codex,
	// 2026-08-24): Go's memberByID is already the sole, org-scoped source
	// (loaded per-claim in loadWorkItemDerivationContextForProvider), so
	// there is no second, non-tenant-scoped lookup to bypass the gate above.
	var reporterSkipReason string
	if subject.Reporter != nil && strings.TrimSpace(*subject.Reporter) != "" {
		switch {
		case githubWorkItemDerivationIsBotIdentity(*subject.Reporter):
			reporterSkipReason = "bot_author"
		default:
			reporterCandidates := derived.memberByID[attributionMapKey(subject.Provider, normalizeDerivationIdentity(*subject.Reporter))]
			switch {
			case githubWorkItemDerivationSingleTeam(reporterCandidates):
				// Source AND Evidence are rewritten (not passed through
				// verbatim): reporterCandidates come from the SAME
				// memberByID map the assignee loop above reads, pre-stamped
				// Source "assignee_membership" at fact-load time, so the
				// override must happen here, at the point of use. This puts
				// the reporter-resolved row in its own author_membership
				// rank rather than assignee_membership's, and is
				// distinguishable from an assignee-resolved one for
				// provenance and for githubWorkItemTeamAttributionMetricSource.
				relabeled := make([]githubWorkItemDerivationCandidate, len(reporterCandidates))
				for index, candidate := range reporterCandidates {
					candidate.Source = "author_membership"
					candidate.Evidence = "reporter=" + *subject.Reporter
					relabeled[index] = candidate
				}
				bySource["author_membership"] = append(bySource["author_membership"], relabeled...)
			case len(reporterCandidates) > 0:
				reporterSkipReason = "ambiguous_membership"
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
		evidence := "no_candidate"
		if reporterSkipReason != "" {
			evidence = "no_candidate:" + reporterSkipReason
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
	if facts.Members, err = source.loadMembers(ctx, claim.OrgID, request.AsOf); err != nil {
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

func (source githubWorkItemClickHouseDerivationContextSource) loadMembers(
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
SELECT work_item_id, provider, toString(repo_id), native_team_key, project_key,
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
			&subject.WorkItemID, &subject.Provider, &subject.RepoID, &subject.NativeTeamKey,
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

// githubWorkItemDerivationSingleTeam reports whether a candidate list names
// exactly one distinct team_id. CHAOS-4244's reporter ambiguity gate: a
// person's membership resolving to zero candidates or to two+ different
// teams gives no signal to prefer one, so the caller must contribute
// nothing. An empty list is NOT single -- it is handled by the caller
// appending nothing either way, but this keeps the predicate's name honest.
func githubWorkItemDerivationSingleTeam(candidates []githubWorkItemDerivationCandidate) bool {
	teams := map[string]struct{}{}
	for _, candidate := range candidates {
		teams[githubWorkItemDerivationStringValue(candidate.TeamID)] = struct{}{}
	}
	return len(teams) == 1
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

func githubWorkItemDerivationFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ githubWorkItemDerivationContextSource = githubWorkItemClickHouseDerivationContextSource{}
