// Package teamattribution implements the provider-neutral team-attribution
// cascade shared by GitHub, GitLab, Jira and Linear work items: the
// precedence resolver (Resolve), its ClickHouse fact loaders
// (ClickHouseFactSource), and the linked-issue donor index
// (BuildLinkedIssueIndex).
//
// Extracted from internal/providersync/github_work_items_derivation_context.go
// (CHAOS-3092 PR-A) as a pure mechanical move: identifiers keep their
// original (github-prefixed) names, only capitalized where a providersync
// caller needs them. A follow-up cosmetic pass may drop the redundant
// "GithubWorkItem" prefix; that rename is deliberately out of scope here to
// keep this extraction diff reviewable and byte-identical in behavior.
package teamattribution

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// ErrEffectRecoveryUnsafe is providerfoundation's sentinel, aliased under this
// package's original (providersync-era) name so the moved loader code below
// is unchanged. providersync's own ErrEffectRecoveryUnsafe (lease.go) is
// aliased to the SAME providerfoundation value, so errors.Is comparisons
// against either name see the same identity.
var ErrEffectRecoveryUnsafe = providerfoundation.ErrRecoveryUnsafe

const GithubWorkItemDerivationContextLimit = 100_000

var GithubWorkItemIssueKeyPattern = regexp.MustCompile(`^([A-Za-z]{2,})-\d+$`)

var GithubWorkItemDerivationInheritableRelationships = map[string]bool{
	"relates_to": true, "relates": true, "duplicates": true, "external_issue_key": true,
}

type GithubWorkItemDerivationCandidate struct {
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

type GithubWorkItemDerivationSubject struct {
	WorkItemID string
	Provider   string
	// Type is the item's own native kind (e.g. "pr", "merge_request", "bug",
	// "issue"). Consulted by Resolve() to gate author_membership: only a
	// PR/MR-shaped item (per GithubWorkItemDerivationIsPullOrMergeRequestType,
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
	// same ResolveMembership two-layer lookup as assignee_membership; an
	// earlier round of CHAOS-4321 removed it, then chris's 08:30 PT ruling
	// restored it, gated on the SAME two-layer admin/provider resolution
	// (see the CHAOS-4321 comment above Resolve()'s reporter block) --
	// Resolve() DOES read Reporter for team attribution. See
	// docs/contribute/architecture/team-attribution.md §0.
	Reporter *string
	OrgID    string
}

type GithubWorkItemDerivationTeamFact struct {
	Provider    string
	TeamID      string
	TeamName    string
	ProjectKeys []string
	UpdatedAt   time.Time
}

type GithubWorkItemDerivationProjectFact struct {
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

type GithubWorkItemDerivationRepoFact struct {
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

type GithubWorkItemDerivationMemberFact struct {
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

type GithubWorkItemDerivationManualFallback struct {
	Provider  string
	ScopeType string
	ScopeID   string
	TeamID    string
	TeamName  string
	Reason    string
	Priority  int
}

// GithubWorkItemDerivationUntypedMemberFact is one `teams.members` facet
// entry with no backing `identities` row (CHAOS-4321, team-lead correction):
// adding a member directly on `/org/admin/teams/[id]/edit` is one of the two
// admin surfaces chris named, so this must still Resolve a team even absent
// an identities row. Untyped (no Provider field) -- `teams.members` carries
// no provider column, so it is matched against an item's assignee/reporter
// facet by normalized equality alone, regardless of provider.
type GithubWorkItemDerivationUntypedMemberFact struct {
	TeamID    string
	TeamName  string
	Facet     string
	UpdatedAt time.Time
}

type GithubWorkItemDerivationFacts struct {
	Teams    []GithubWorkItemDerivationTeamFact
	Projects []GithubWorkItemDerivationProjectFact
	Repos    []GithubWorkItemDerivationRepoFact
	// Members is the ADMIN layer (CHAOS-4321): sourced from the
	// `identities`/`teams` catalog, provider-scoped via
	// `identities.provider_identities`. Authoritative -- consulted first,
	// and an ambiguous match here does NOT fall through to ProviderMembers.
	Members []GithubWorkItemDerivationMemberFact
	// UntypedMembers is ALSO the admin layer: bare `teams.manual_members`
	// facets with no backing `identities` row, matched without a provider
	// tag.
	UntypedMembers []GithubWorkItemDerivationUntypedMemberFact
	// ProviderMembers is the FALLBACK layer (chris, 2026-08-26 08:30 PT:
	// "manual is override -- if the override exists, use it, else use
	// attribution from providers"): sourced from provider auto-import
	// `team_memberships`, consulted ONLY when the admin layer (Members ∪
	// UntypedMembers) has ZERO candidates for a given identity.
	ProviderMembers []GithubWorkItemDerivationMemberFact
	// ProviderUntypedMembers is ALSO the fallback layer (chris, 2026-08-26
	// 10:39 PT, after a codex adversarial review HIGH finding: "the new
	// membership layer can turn provider-imported rosters into
	// authoritative, provider-neutral admin overrides"): `teams.members`
	// mixes admin-curated entries (mirrored into `manual_members`, the
	// UntypedMembers source above) with UNREVIEWED provider auto-import
	// roster writes, so it is NOT admin-exclusive and cannot be layer 1.
	// Matched WITHOUT a provider tag for the same reason UntypedMembers is
	// untyped (no provider column on `teams.members`).
	ProviderUntypedMembers []GithubWorkItemDerivationUntypedMemberFact
	ManualFallbacks        []GithubWorkItemDerivationManualFallback
	DonorItems             []GithubWorkItemDerivationSubject
}

type GithubWorkItemDerivationLoadRequest struct {
	AsOf             time.Time
	DonorWorkItemIDs []string
	DonorIssueKeys   []string
}

// GithubWorkItemStoredEdgeMergeObservation is the ledger-side record of what
// the stored-edge union did on one unit. providersync is a pure effect-ledger
// package with no logger and no metrics registry, so this travels out through
// the route's existing `Result["observations"]` map and lands on the unit's
// persisted payload (providerunit.Handler), which is where an operator can see
// the CHAOS-3978 population recovering after deploy.
type GithubWorkItemStoredEdgeMergeObservation struct {
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

// ClickHouseFactSource loads the provider-neutral team-attribution facts
// (teams/projects/repos/members/manual fallbacks) from ClickHouse. It does
// NOT load donor items or stored inheritable edges: those stay
// provider-scoped in providersync (githubWorkItemClickHouseDerivationContextSource),
// which composes this loader with its own loadDonors/LoadStoredInheritableEdges.
type ClickHouseFactSource struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type GithubWorkItemDerivationContext struct {
	projectKeyTeams map[string]GithubWorkItemDerivationTeamFact
	projectByID     map[string][]GithubWorkItemDerivationCandidate
	projectByKey    map[string][]GithubWorkItemDerivationCandidate
	repoByID        map[string][]GithubWorkItemDerivationCandidate
	repoByName      map[string][]GithubWorkItemDerivationCandidate
	// memberByID (admin layer, provider-scoped) and memberByUntypedFacet
	// (admin layer, no provider tag) together form the AUTHORITATIVE
	// membership layer; providerMemberByID (auto-import team_memberships)
	// is the FALLBACK layer, consulted only when the admin layer has
	// nothing for an identity (CHAOS-4321, chris 08:30 PT).
	memberByID                   map[string][]GithubWorkItemDerivationCandidate
	memberByUntypedFacet         map[string][]GithubWorkItemDerivationCandidate
	providerMemberByID           map[string][]GithubWorkItemDerivationCandidate
	providerMemberByUntypedFacet map[string][]GithubWorkItemDerivationCandidate
	manualFallbacks              []GithubWorkItemDerivationManualFallback
	LinkedIssue                  map[string][2]string
	StoredEdgeMerge              GithubWorkItemStoredEdgeMergeObservation
}

// GithubWorkItemDerivationEdgeKey is the identity a fresh edge is authoritative
// for. It is deliberately the ClickHouse sorting key of work_item_dependencies
// (source, target, relationship_type), matching Python's merged_deps key in
// job_work_items.py. Exported: providersync's mergeStoredInheritableWorkItemEdges
// (STAYS, operates on the full providersync dependency-row type) constructs
// this key and BuildLinkedIssueIndex (MOVES) reads it, so the type must be
// visible from both packages.
type GithubWorkItemDerivationEdgeKey struct {
	Source, Target, RelationshipType string
}

// GithubWorkItemDerivationDependencyEdge is the narrow, provider-neutral shape
// BuildLinkedIssueIndex and LatestGitHubWorkItemDerivationDependencies need
// from a dependency row. providersync's canonical githubWorkItemDependencyRow
// stays in providersync (it is used far beyond the attribution cascade --
// extraction and storage), so the STAYS-side callers here
// (mergeStoredInheritableWorkItemEdges, githubWorkItemDerivationDonorTargets,
// loadWorkItemDerivationContextForProvider) convert to this shape at the call
// boundary.
type GithubWorkItemDerivationDependencyEdge struct {
	SourceWorkItemID string
	TargetWorkItemID string
	RelationshipType string
	LastSynced       time.Time
	// OrgID is carried for test-literal compatibility with the providersync
	// dependency row this type narrows; BuildLinkedIssueIndex and
	// LatestGitHubWorkItemDerivationDependencies do not read it (scoping is
	// enforced by the STAYS-side caller before conversion).
	OrgID string
}

func LatestGitHubWorkItemDerivationDependencies(
	dependencies []GithubWorkItemDerivationDependencyEdge,
) map[struct{ source, target string }]GithubWorkItemDerivationDependencyEdge {
	type edgeKey struct{ source, target string }
	latest := map[edgeKey]GithubWorkItemDerivationDependencyEdge{}
	for _, dependency := range dependencies {
		key := edgeKey{dependency.SourceWorkItemID, dependency.TargetWorkItemID}
		current, exists := latest[key]
		if !exists || dependency.LastSynced.After(current.LastSynced) ||
			(dependency.LastSynced.Equal(current.LastSynced) && dependency.RelationshipType < current.RelationshipType) {
			latest[key] = dependency
		}
	}
	result := make(map[struct{ source, target string }]GithubWorkItemDerivationDependencyEdge, len(latest))
	for key, dependency := range latest {
		result[struct{ source, target string }{key.source, key.target}] = dependency
	}
	return result
}

func NewGitHubWorkItemDerivationContext(
	facts GithubWorkItemDerivationFacts,
) GithubWorkItemDerivationContext {
	result := GithubWorkItemDerivationContext{
		projectKeyTeams:              map[string]GithubWorkItemDerivationTeamFact{},
		projectByID:                  map[string][]GithubWorkItemDerivationCandidate{},
		projectByKey:                 map[string][]GithubWorkItemDerivationCandidate{},
		repoByID:                     map[string][]GithubWorkItemDerivationCandidate{},
		repoByName:                   map[string][]GithubWorkItemDerivationCandidate{},
		memberByID:                   map[string][]GithubWorkItemDerivationCandidate{},
		memberByUntypedFacet:         map[string][]GithubWorkItemDerivationCandidate{},
		providerMemberByID:           map[string][]GithubWorkItemDerivationCandidate{},
		providerMemberByUntypedFacet: map[string][]GithubWorkItemDerivationCandidate{},
		manualFallbacks:              append([]GithubWorkItemDerivationManualFallback(nil), facts.ManualFallbacks...),
		LinkedIssue:                  map[string][2]string{},
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
		candidate := GithubWorkItemDerivationCandidateFromFact(
			"project_ownership", fact.TeamID, fact.TeamName,
			fmt.Sprintf("project_ownership=%s", GithubWorkItemDerivationFirstNonEmpty(fact.ProjectID, GithubWorkItemDerivationStringValue(fact.ProjectKey))),
			fact.IsPrimary, fact.Specificity, fact.Priority, fact.UpdatedAt,
		)
		if fact.ProjectID != "" {
			AppendDerivationCandidate(result.projectByID, AttributionMapKey(fact.Provider, fact.ProjectID), candidate)
		}
		if fact.ProjectKey != nil && strings.TrimSpace(*fact.ProjectKey) != "" {
			AppendDerivationCandidate(result.projectByKey, AttributionMapKey(fact.Provider, *fact.ProjectKey), candidate)
		}
	}
	for _, fact := range facts.Repos {
		candidate := GithubWorkItemDerivationCandidateFromFact(
			"repo_ownership", fact.TeamID, fact.TeamName,
			fmt.Sprintf("repo_ownership=%s", GithubWorkItemDerivationFirstNonEmpty(GithubWorkItemDerivationStringValue(fact.RepoID), fact.RepoFullName)),
			fact.IsPrimary, fact.Specificity, fact.Priority, fact.UpdatedAt,
		)
		if fact.RepoID != nil && strings.TrimSpace(*fact.RepoID) != "" {
			AppendDerivationCandidate(result.repoByID, AttributionMapKey(fact.Provider, *fact.RepoID), candidate)
		}
		if fact.RepoFullName != "" {
			AppendDerivationCandidate(result.repoByName, AttributionMapKey(fact.Provider, fact.RepoFullName), candidate)
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
	// ONE name, exactly as the live pipeline (LoadMembers -> NewGitHubWorkItemDerivationContext)
	// now guarantees -- not two divergent provenance rows for the same team.
	memberTeamNames := map[string]string{}
	for _, fact := range facts.Members {
		teamID := strings.TrimSpace(fact.TeamID)
		if teamID == "" {
			continue
		}
		if _, exists := memberTeamNames[teamID]; !exists {
			memberTeamNames[teamID] = GithubWorkItemDerivationFirstNonEmpty(fact.TeamName, teamID)
		}
	}
	for _, fact := range facts.Members {
		// CHAOS-4321: IsPrimary/Specificity are no longer per-row auto-import
		// data (the old `team_memberships.is_primary`/`.specificity` columns,
		// provider-supplied); they are a fixed protocol constant now that
		// membership comes from the admin-authored `identities`/`teams`
		// catalog, which carries no such per-membership signal.
		// `LoadMembers` already stamps every fact IsPrimary=1/Specificity=60
		// (matching the Python loader's load_team_attribution_context
		// exactly), so this is belt-and-suspenders for the real pipeline --
		// but it is load-bearing for tests that construct `Facts.Members`
		// directly (bypassing LoadMembers): fact.IsPrimary/fact.Specificity/
		// fact.TeamName are deliberately ignored here (TeamName resolved via
		// memberTeamNames above) so such a fixture can't silently diverge
		// from the value production actually emits.
		candidate := GithubWorkItemDerivationCandidateFromFact(
			"assignee_membership", fact.TeamID, memberTeamNames[strings.TrimSpace(fact.TeamID)],
			fmt.Sprintf("assignee_membership=%s", GithubWorkItemDerivationFirstNonEmpty(fact.MemberID, GithubWorkItemDerivationStringValue(fact.RawEmail))),
			1, 60, fact.Priority, fact.UpdatedAt,
		)
		identities := []string{fact.MemberID, GithubWorkItemDerivationStringValue(fact.RawProviderUserID), GithubWorkItemDerivationStringValue(fact.RawEmail)}
		identities = append(identities, fact.IdentityFacets...)
		seen := map[string]struct{}{}
		for _, identity := range identities {
			key := NormalizeDerivationIdentity(identity)
			if key == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			AppendDerivationCandidate(result.memberByID, AttributionMapKey(fact.Provider, key), candidate)
		}
	}
	// CHAOS-4321 (team-lead correction): a `teams.members` facet with no
	// backing `identities` row is STILL an admin mapping -- adding a member
	// directly on `/org/admin/teams/[id]/edit` is one of the two admin
	// surfaces chris named. Matched WITHOUT a provider tag, unlike
	// memberByID above (`teams.members` carries no provider column).
	for _, fact := range facts.UntypedMembers {
		facet := NormalizeDerivationIdentity(fact.Facet)
		if facet == "" {
			continue
		}
		teamID := strings.TrimSpace(fact.TeamID)
		if teamID == "" {
			continue
		}
		candidate := GithubWorkItemDerivationCandidateFromFact(
			"assignee_membership", teamID, GithubWorkItemDerivationFirstNonEmpty(fact.TeamName, teamID),
			fmt.Sprintf("assignee_membership=%s", fact.Facet),
			1, 60, 0, fact.UpdatedAt,
		)
		result.memberByUntypedFacet[facet] = append(result.memberByUntypedFacet[facet], candidate)
	}
	// CHAOS-4321 fix (chris, 2026-08-26 10:39 PT, after a codex adversarial
	// review HIGH finding: "the new membership layer can turn
	// provider-imported rosters into authoritative, provider-neutral admin
	// overrides"). `teams.members` mixes admin-curated entries (mirrored
	// into ProviderUntypedMembers's sibling, UntypedMembers, above) with
	// UNREVIEWED provider auto-import roster writes -- demoted here to the
	// provider-FALLBACK tier, matched WITHOUT a provider tag for the same
	// reason the admin-layer untyped loop above is untyped. Lower
	// specificity (50, the pre-CHAOS-4321 legacy roster-fallback
	// convention) than the admin layer's untyped candidates (60) so it
	// never wins an intra-source tie if a candidate from BOTH pools were
	// ever compared directly -- though in practice ResolveMembership never
	// lets that happen: this pool is consulted only when the admin layer
	// (layer 1) has zero candidates for the identity. Confidence comes out
	// "high" via ConfidenceForPrimary(1), matching Python's explicit choice
	// there -- this codebase's convention is that confidence mirrors
	// IsPrimary, not "how much do we trust the source"; specificity is what
	// actually distinguishes this tier.
	for _, fact := range facts.ProviderUntypedMembers {
		facet := NormalizeDerivationIdentity(fact.Facet)
		if facet == "" {
			continue
		}
		teamID := strings.TrimSpace(fact.TeamID)
		if teamID == "" {
			continue
		}
		// Priority 10, NOT 0: every provider-layer candidate must have
		// priority > 0 -- WriteGitHubWorkItemEffect's membership-layer
		// telemetry (chris/team-lead, 2026-08-26) derives admin_override
		// vs provider_fallback from Priority==0, and priority=0 is the
		// admin layer's fixed value (memberByID, memberByUntypedFacet).
		// Matches Python's provider_member_by_untyped_facet exactly (the
		// lowest real team_memberships priority in use, jira's).
		candidate := GithubWorkItemDerivationCandidateFromFact(
			"assignee_membership", teamID, GithubWorkItemDerivationFirstNonEmpty(fact.TeamName, teamID),
			fmt.Sprintf("assignee_membership=%s", fact.Facet),
			1, 50, 10, fact.UpdatedAt,
		)
		result.providerMemberByUntypedFacet[facet] = append(result.providerMemberByUntypedFacet[facet], candidate)
	}
	// CHAOS-4321 (chris, 08:30 PT): provider auto-import fallback layer --
	// unchanged shape from before this ticket (fact.IsPrimary/.Specificity
	// ARE real per-row auto-import data here, unlike the admin layer above,
	// so they are used as-is, not overridden).
	for _, fact := range facts.ProviderMembers {
		candidate := GithubWorkItemDerivationCandidateFromFact(
			"assignee_membership", fact.TeamID, fact.TeamName,
			fmt.Sprintf("assignee_membership=%s", GithubWorkItemDerivationFirstNonEmpty(fact.MemberID, GithubWorkItemDerivationStringValue(fact.RawEmail))),
			fact.IsPrimary, fact.Specificity, fact.Priority, fact.UpdatedAt,
		)
		identities := []string{fact.MemberID, GithubWorkItemDerivationStringValue(fact.RawProviderUserID), GithubWorkItemDerivationStringValue(fact.RawEmail)}
		identities = append(identities, fact.IdentityFacets...)
		seen := map[string]struct{}{}
		for _, identity := range identities {
			key := NormalizeDerivationIdentity(identity)
			if key == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			AppendDerivationCandidate(result.providerMemberByID, AttributionMapKey(fact.Provider, key), candidate)
		}
	}
	return result
}

func GithubWorkItemDerivationCandidateFromFact(
	source, teamID, teamName, evidence string,
	isPrimary, specificity, priority int,
	updatedAt time.Time,
) GithubWorkItemDerivationCandidate {
	if teamName == "" {
		teamName = teamID
	}
	// An ownership row's team_id is passed through RAW by Python, so a blank
	// team id persists as "" on work_item_team_attributions -- NOT as NULL.
	// GithubWorkItemDerivationStringPointer maps "" to nil, which made Go write
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
	return GithubWorkItemDerivationCandidate{
		Source: source, TeamID: &teamID, TeamName: &teamName,
		Confidence: ConfidenceForPrimary(isPrimary), Evidence: evidence,
		IsPrimary: isPrimary, Specificity: specificity, Priority: priority,
		UpdatedAt: NormalizedDerivationTime(updatedAt),
	}
}

func ConfidenceForPrimary(primary int) string {
	if primary != 0 {
		return "high"
	}
	return "medium"
}

func AppendDerivationCandidate(
	target map[string][]GithubWorkItemDerivationCandidate,
	key string,
	candidate GithubWorkItemDerivationCandidate,
) {
	target[key] = append(target[key], candidate)
}

func AttributionMapKey(provider, key string) string {
	return strings.TrimSpace(provider) + "\x00" + strings.TrimSpace(key)
}

func NormalizeDerivationIdentity(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func NormalizedDerivationTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return value.UTC()
}

func (derived GithubWorkItemDerivationContext) Resolve(
	subject GithubWorkItemDerivationSubject,
) (*string, *string, []GithubWorkItemDerivationCandidate) {
	bySource := map[string][]GithubWorkItemDerivationCandidate{}
	if candidate := derived.NativeTeamCandidate(subject); candidate != nil {
		bySource[candidate.Source] = append(bySource[candidate.Source], *candidate)
	}
	if inherited, exists := derived.LinkedIssue[subject.WorkItemID]; exists {
		bySource["linked_issue"] = append(bySource["linked_issue"], GithubWorkItemDerivationCandidate{
			Source: "linked_issue", TeamID: GithubWorkItemDerivationStringPointer(inherited[0]), TeamName: GithubWorkItemDerivationStringPointer(inherited[1]),
			Confidence: "medium", Evidence: "linked_issue=" + subject.WorkItemID,
			IsPrimary: 1, Specificity: 90, UpdatedAt: NormalizedDerivationTime(time.Time{}),
		})
	}
	issueProjectTeams := map[string]struct{}{}
	if candidate := derived.IssueProjectCandidate(subject); candidate != nil {
		bySource[candidate.Source] = append(bySource[candidate.Source], *candidate)
		if candidate.TeamID != nil {
			issueProjectTeams[*candidate.TeamID] = struct{}{}
		}
	}
	bySource["project_ownership"] = append(
		bySource["project_ownership"],
		derived.projectByID[AttributionMapKey(subject.Provider, GithubWorkItemDerivationStringValue(subject.ProjectID))]...,
	)
	for _, candidate := range derived.projectByKey[AttributionMapKey(subject.Provider, GithubWorkItemDerivationStringValue(subject.ProjectKey))] {
		if candidate.TeamID != nil {
			if _, duplicate := issueProjectTeams[*candidate.TeamID]; duplicate {
				continue
			}
		}
		bySource["project_ownership"] = append(bySource["project_ownership"], candidate)
	}
	bySource["repo_ownership"] = append(
		bySource["repo_ownership"],
		derived.repoByID[AttributionMapKey(subject.Provider, GithubWorkItemDerivationStringValue(subject.RepoID))]...,
	)
	bySource["repo_ownership"] = append(
		bySource["repo_ownership"],
		derived.repoByName[AttributionMapKey(subject.Provider, GithubWorkItemDerivationStringValue(subject.ProjectID))]...,
	)
	// CHAOS-4321 (chris's ruling, 2026-08-26, refined 08:30 PT): membership-
	// based attribution -- assignee AND author alike -- is a TWO-LAYER
	// resolution via ResolveMembership: layer 1 (admin: memberByID ∪
	// memberByUntypedFacet, from identities/teams) is authoritative,
	// including when ambiguous (no fallthrough on admin ambiguity); layer 2
	// (providerMemberByID, from auto-import team_memberships) is consulted
	// only when layer 1 has zero candidates for that identity ("manual is
	// override -- if the override exists, use it, else use attribution
	// from providers"). Both layers apply the SAME exactly-one-team gate --
	// previously this gate applied ONLY to the reporter/author path
	// (CHAOS-4110); assignee had none and let RankDerivationCandidates's
	// specificity/priority ordering silently pick an arbitrary winner among
	// an ambiguous member's teams -- exactly the defect this ticket removes.
	membershipSkipReasons := map[string]struct{}{}
	for _, assignee := range subject.Assignees {
		assigneeCandidates, reason := derived.ResolveMembership(subject.Provider, assignee)
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
	// ResolveMembership the assignee loop above now uses -- CHAOS-4321 did
	// not change the bot filter or the PR/MR type gate, both unchanged from
	// CHAOS-4244.
	var reporterSkipReason string
	if GithubWorkItemDerivationIsPullOrMergeRequestType(subject.Provider, subject.Type) &&
		subject.Reporter != nil && strings.TrimSpace(*subject.Reporter) != "" {
		if GithubWorkItemDerivationIsBotIdentity(*subject.Reporter) {
			reporterSkipReason = "bot_author"
		} else {
			reporterCandidates, reason := derived.ResolveMembership(subject.Provider, *subject.Reporter)
			if len(reporterCandidates) > 0 {
				// Source AND Evidence are rewritten (not passed through
				// verbatim): reporterCandidates come from the SAME
				// ResolveMembership the assignee loop above uses,
				// pre-stamped Source "assignee_membership" at fact-load
				// time, so the override must happen here, at the point of
				// use.
				relabeled := make([]GithubWorkItemDerivationCandidate, len(reporterCandidates))
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
		bySource["manual_fallback"], derived.ManualCandidates(subject)...,
	)

	order := []string{
		"native_team", "issue_project", "project_ownership", "repo_ownership",
		"assignee_membership", "linked_issue", "author_membership",
		"manual_fallback", "unassigned",
	}
	var primary *GithubWorkItemDerivationCandidate
	all := make([]GithubWorkItemDerivationCandidate, 0)
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
		candidates := RankDerivationCandidates(DedupeDerivationCandidates(bySource[source]))
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
		// `bot_author` is a definitive "this can never Resolve" answer.
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
		membershipReason := GithubWorkItemDerivationReasonWithPrefix(membershipSkipReasons, "ambiguous_admin_membership")
		if membershipReason == "" {
			if GithubWorkItemDerivationHasReason(membershipSkipReasons, "bot_author") {
				membershipReason = "bot_author"
			} else {
				membershipReason = GithubWorkItemDerivationReasonWithPrefix(membershipSkipReasons, "ambiguous_provider_membership")
			}
		}
		if membershipReason == "" && GithubWorkItemDerivationHasReason(membershipSkipReasons, "no_membership") {
			membershipReason = "no_membership"
		}
		evidence := "no_candidate"
		if membershipReason != "" {
			evidence = "no_candidate:" + membershipReason
		}
		value := GithubWorkItemDerivationCandidate{
			Source: "unassigned", Confidence: "none", Evidence: evidence,
			IsPrimary: 1, UpdatedAt: NormalizedDerivationTime(time.Time{}),
		}
		primary = &value
		all = append(all, value)
	}
	marked := make([]GithubWorkItemDerivationCandidate, len(all))
	for index, candidate := range all {
		candidate.IsPrimary = 0
		if SameDerivationCandidate(candidate, *primary) {
			candidate.IsPrimary = 1
			primary = &candidate
		}
		marked[index] = candidate
	}
	return primary.TeamID, primary.TeamName, marked
}

func (derived GithubWorkItemDerivationContext) NativeTeamCandidate(
	subject GithubWorkItemDerivationSubject,
) *GithubWorkItemDerivationCandidate {
	if subject.NativeTeamKey == nil {
		return nil
	}
	team, exists := derived.projectKeyTeams[strings.TrimSpace(*subject.NativeTeamKey)]
	if !exists {
		return nil
	}
	return &GithubWorkItemDerivationCandidate{
		Source: "native_team", TeamID: GithubWorkItemDerivationStringPointer(team.TeamID), TeamName: GithubWorkItemDerivationStringPointer(GithubWorkItemDerivationFirstNonEmpty(team.TeamName, team.TeamID)),
		Confidence: "high", Evidence: "native_team_key=" + *subject.NativeTeamKey,
		IsPrimary: 1, Specificity: 100, UpdatedAt: NormalizedDerivationTime(time.Time{}),
	}
}

func (derived GithubWorkItemDerivationContext) IssueProjectCandidate(
	subject GithubWorkItemDerivationSubject,
) *GithubWorkItemDerivationCandidate {
	keys := []string{WorkItemDerivationScope(subject)}
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
		return &GithubWorkItemDerivationCandidate{
			Source: "issue_project", TeamID: GithubWorkItemDerivationStringPointer(team.TeamID), TeamName: GithubWorkItemDerivationStringPointer(GithubWorkItemDerivationFirstNonEmpty(team.TeamName, team.TeamID)),
			Confidence: "high", Evidence: "issue_project_key=" + key,
			IsPrimary: 1, Specificity: 50, UpdatedAt: NormalizedDerivationTime(time.Time{}),
		}
	}
	return nil
}

func WorkItemDerivationScope(subject GithubWorkItemDerivationSubject) string {
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

func (derived GithubWorkItemDerivationContext) ManualCandidates(
	subject GithubWorkItemDerivationSubject,
) []GithubWorkItemDerivationCandidate {
	result := []GithubWorkItemDerivationCandidate{}
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
			// one team. GithubWorkItemDerivationScopeMatch reproduces the set
			// semantics: empty candidates are dropped and an empty scope_id
			// can never match.
			matched = GithubWorkItemDerivationScopeMatch(scopeID,
				GithubWorkItemDerivationStringValue(subject.RepoID),
				GithubWorkItemDerivationStringValue(subject.ProjectID))
		case "project":
			matched = GithubWorkItemDerivationScopeMatch(scopeID,
				GithubWorkItemDerivationStringValue(subject.ProjectID),
				GithubWorkItemDerivationStringValue(subject.ProjectKey),
				WorkItemDerivationScope(subject))
		case "member":
			for _, assignee := range subject.Assignees {
				// Python's member_ids drops falsy assignees, so a blank
				// assignee cannot be matched by a blank rule either.
				if scopeID == "" || assignee == "" {
					continue
				}
				matched = matched || NormalizeDerivationIdentity(scopeID) == NormalizeDerivationIdentity(assignee)
			}
		case "issue_key_prefix":
			prefix := GithubWorkItemIssueKeyPrefix(subject.WorkItemID)
			matched = prefix != "" && prefix == strings.ToUpper(scopeID)
		}
		if !matched {
			continue
		}
		evidence := fmt.Sprintf("manual_fallback:%s=%s", rule.ScopeType, rule.ScopeID)
		if rule.Reason != "" {
			evidence += " (" + rule.Reason + ")"
		}
		result = append(result, GithubWorkItemDerivationCandidate{
			Source: "manual_fallback", TeamID: GithubWorkItemDerivationStringPointer(rule.TeamID), TeamName: GithubWorkItemDerivationStringPointer(GithubWorkItemDerivationFirstNonEmpty(rule.TeamName, rule.TeamID)),
			Confidence: "manual", Evidence: evidence, IsPrimary: 1,
			Priority: rule.Priority, UpdatedAt: NormalizedDerivationTime(time.Time{}),
		})
	}
	return result
}

// GithubWorkItemDerivationScopeMatch mirrors Python's `scope_id in {...}`
// where the set is built by dropping falsy values: an empty scope_id matches
// nothing, and an empty candidate is not a member.
func GithubWorkItemDerivationScopeMatch(scopeID string, candidates ...string) bool {
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

func GithubWorkItemIssueKeyPrefix(workItemID string) string {
	_, suffix, found := strings.Cut(workItemID, ":")
	if !found {
		return ""
	}
	match := GithubWorkItemIssueKeyPattern.FindStringSubmatch(strings.TrimSpace(suffix))
	if len(match) != 2 {
		return ""
	}
	return strings.ToUpper(match[1])
}

func RankDerivationCandidates(
	candidates []GithubWorkItemDerivationCandidate,
) []GithubWorkItemDerivationCandidate {
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
		if leftTeamID, rightTeamID := GithubWorkItemDerivationStringValue(a.TeamID), GithubWorkItemDerivationStringValue(b.TeamID); leftTeamID != rightTeamID {
			return leftTeamID < rightTeamID
		}
		if leftTeamName, rightTeamName := GithubWorkItemDerivationStringValue(a.TeamName), GithubWorkItemDerivationStringValue(b.TeamName); leftTeamName != rightTeamName {
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

func DedupeDerivationCandidates(
	candidates []GithubWorkItemDerivationCandidate,
) []GithubWorkItemDerivationCandidate {
	result := make([]GithubWorkItemDerivationCandidate, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		key := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00%d\x00%s",
			candidate.Source, GithubWorkItemDerivationStringValue(candidate.TeamID), GithubWorkItemDerivationStringValue(candidate.TeamName),
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

func SameDerivationCandidate(left, right GithubWorkItemDerivationCandidate) bool {
	return left.Source == right.Source && GithubWorkItemDerivationStringValue(left.TeamID) == GithubWorkItemDerivationStringValue(right.TeamID) &&
		GithubWorkItemDerivationStringValue(left.TeamName) == GithubWorkItemDerivationStringValue(right.TeamName) && left.Confidence == right.Confidence &&
		left.Evidence == right.Evidence && left.Specificity == right.Specificity &&
		left.Priority == right.Priority && left.UpdatedAt.Equal(right.UpdatedAt)
}

// BuildLinkedIssueIndex resolves each team-less item to the team of a linked,
// first-class-attributed donor.
//
// It additionally reports how many of those inheritances rest on an edge that
// exists ONLY in the store (rescues) and how many of THOSE reach a donor in a
// different provider (the CHAOS-3978 population). The counts are derived from
// the same winning edge the attribution uses, never re-estimated, so an
// observation of N is a claim about N specific attributions.
func (derived GithubWorkItemDerivationContext) BuildLinkedIssueIndex(
	provider string,
	subjects map[string]GithubWorkItemDerivationSubject,
	dependencies []GithubWorkItemDerivationDependencyEdge,
	storedOnly map[GithubWorkItemDerivationEdgeKey]bool,
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
		baseNative[subject.WorkItemID] = derived.NativeTeamCandidate(subject) != nil
		teamID, teamName, candidates := derived.ResolveWithoutLinked(subject)
		primarySource := ""
		for _, candidate := range candidates {
			if candidate.IsPrimary == 1 {
				primarySource = candidate.Source
				break
			}
		}
		if teamID != nil && allowedDonorSources[primarySource] {
			donors[subject.WorkItemID] = [2]string{*teamID, GithubWorkItemDerivationFirstNonEmpty(GithubWorkItemDerivationStringValue(teamName), *teamID)}
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
	for _, dependency := range LatestGitHubWorkItemDerivationDependencies(dependencies) {
		if !GithubWorkItemDerivationInheritableRelationships[dependency.RelationshipType] || baseNative[dependency.SourceWorkItemID] {
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
				storedOnly: storedOnly[GithubWorkItemDerivationEdgeKey{
					Source:           dependency.SourceWorkItemID,
					Target:           dependency.TargetWorkItemID,
					RelationshipType: dependency.RelationshipType,
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

func (derived GithubWorkItemDerivationContext) ResolveWithoutLinked(
	subject GithubWorkItemDerivationSubject,
) (*string, *string, []GithubWorkItemDerivationCandidate) {
	saved := derived.LinkedIssue
	derived.LinkedIssue = nil
	teamID, teamName, candidates := derived.Resolve(subject)
	derived.LinkedIssue = saved
	return teamID, teamName, candidates
}

func (source ClickHouseFactSource) LoadTeams(
	ctx context.Context, orgID string,
) ([]GithubWorkItemDerivationTeamFact, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT provider, id,
       argMax(name, (updated_at, last_synced, name)),
       argMax(project_keys, (updated_at, last_synced, toJSONString(project_keys))),
       max(updated_at)
FROM teams
WHERE org_id = ?
GROUP BY provider, id, org_id
ORDER BY provider, id
LIMIT ?`, orgID, GithubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []GithubWorkItemDerivationTeamFact{}
	for rows.Next() {
		var fact GithubWorkItemDerivationTeamFact
		if err := rows.Scan(&fact.Provider, &fact.TeamID, &fact.TeamName, &fact.ProjectKeys, &fact.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, fact)
		if len(result) > GithubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func (source ClickHouseFactSource) LoadProjects(
	ctx context.Context, orgID string, asOf time.Time,
) ([]GithubWorkItemDerivationProjectFact, error) {
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
LIMIT ?`, orgID, asOf, asOf, GithubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []GithubWorkItemDerivationProjectFact{}
	for rows.Next() {
		var fact GithubWorkItemDerivationProjectFact
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
		if len(result) > GithubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func (source ClickHouseFactSource) LoadRepos(
	ctx context.Context, orgID string, asOf time.Time,
) ([]GithubWorkItemDerivationRepoFact, error) {
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
LIMIT ?`, orgID, asOf, asOf, GithubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []GithubWorkItemDerivationRepoFact{}
	for rows.Next() {
		var fact GithubWorkItemDerivationRepoFact
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
		if len(result) > GithubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

// GithubWorkItemDerivationAdminIdentity is one row of the ClickHouse
// `identities` table (canonical_id -> team_ids, provider_identities) --
// admin-authored via `/org/admin/identities`.
type GithubWorkItemDerivationAdminIdentity struct {
	CanonicalID        string
	Email              *string
	ProviderIdentities string
	TeamIDs            []string
	UpdatedAt          time.Time
}

// GithubWorkItemDerivationAdminTeam is one row of the ClickHouse `teams`
// table (id -> members facet roster). Members mixes admin edits (via
// `/org/admin/teams`/`/org/admin/identities`) with UNREVIEWED provider
// auto-import roster writes (CHAOS-4321 fix, chris 2026-08-26 10:39 PT, after
// a codex adversarial review HIGH finding) -- ManualMembers is the
// admin-EXCLUSIVE subset, written only by ClickHouseTeamAdminService.
type GithubWorkItemDerivationAdminTeam struct {
	TeamID        string
	Name          string
	Members       []string
	ManualMembers []string
}

// LoadMembers builds membership-attribution facts EXCLUSIVELY from
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
// to a work item's Provider for the memberByID lookup in Resolve().
//
// `asOf` is accepted for interface parity with the other load* methods but
// unused: `identities` and `teams` are current-state ReplacingMergeTree
// tables (FINAL-resolved), not temporally versioned facts with a
// valid_from/valid_to window like `team_memberships` was.
func (source ClickHouseFactSource) LoadMembers(
	ctx context.Context, orgID string, _ time.Time,
) (
	[]GithubWorkItemDerivationMemberFact,
	[]GithubWorkItemDerivationUntypedMemberFact,
	[]GithubWorkItemDerivationUntypedMemberFact,
	[]GithubWorkItemDerivationMemberFact,
	error,
) {
	identityRows, err := source.Conn.Query(ctx, `
SELECT canonical_id, email, provider_identities, team_ids, updated_at
FROM identities FINAL
WHERE org_id = ? AND is_active = 1
LIMIT ?`, orgID, GithubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer identityRows.Close()
	identities := []GithubWorkItemDerivationAdminIdentity{}
	for identityRows.Next() {
		var identity GithubWorkItemDerivationAdminIdentity
		if err := identityRows.Scan(
			&identity.CanonicalID, &identity.Email, &identity.ProviderIdentities,
			&identity.TeamIDs, &identity.UpdatedAt,
		); err != nil {
			return nil, nil, nil, nil, err
		}
		identities = append(identities, identity)
		if len(identities) > GithubWorkItemDerivationContextLimit {
			return nil, nil, nil, nil, ErrEffectRecoveryUnsafe
		}
	}
	if err := identityRows.Err(); err != nil {
		return nil, nil, nil, nil, err
	}

	// CHAOS-4321 fix (chris, 2026-08-26 10:39 PT, after a codex adversarial
	// review HIGH finding): manual_members is the admin-EXCLUSIVE subset of
	// members -- see GithubWorkItemDerivationAdminTeam's doc comment.
	teamRows, err := source.Conn.Query(ctx, `
SELECT id, name, members, manual_members
FROM teams FINAL
WHERE org_id = ? AND is_active = 1
LIMIT ?`, orgID, GithubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer teamRows.Close()
	adminTeams := map[string]GithubWorkItemDerivationAdminTeam{}
	teamCount := 0
	for teamRows.Next() {
		var team GithubWorkItemDerivationAdminTeam
		if err := teamRows.Scan(
			&team.TeamID, &team.Name, &team.Members, &team.ManualMembers,
		); err != nil {
			return nil, nil, nil, nil, err
		}
		adminTeams[team.TeamID] = team
		teamCount++
		if teamCount > GithubWorkItemDerivationContextLimit {
			return nil, nil, nil, nil, ErrEffectRecoveryUnsafe
		}
	}
	if err := teamRows.Err(); err != nil {
		return nil, nil, nil, nil, err
	}

	// CHAOS-4321 fix (chris, 2026-08-26 10:39 PT): (b) below used to match
	// on `team.Members` -- fixed to `team.ManualMembers`, since `members` is
	// NOT admin-exclusive (provider auto-import writes unreviewed roster
	// rows into it too; see AUTO_APPLY_POLICY in
	// clickhouse_team_drift_projector.py).
	teamMemberFacets := make(map[string]map[string]struct{}, len(adminTeams))
	for teamID, team := range adminTeams {
		facets := make(map[string]struct{}, len(team.ManualMembers))
		for _, member := range team.ManualMembers {
			if key := NormalizeDerivationIdentity(member); key != "" {
				facets[key] = struct{}{}
			}
		}
		teamMemberFacets[teamID] = facets
	}

	// CHAOS-4321 (team-lead ruling, 2026-08-26, codex round 3 adversarial
	// review HIGH finding): a `teams.members` fallback facet may only match
	// a work item if it is email-shaped (an email legitimately identifies
	// the same person on every provider -- CHAOS-2609) or provider-tagged
	// for the item's specific provider; a bare non-email facet (a display
	// name, a raw login/id) with no confirmed provider is ignored in the
	// fallback tier rather than matched against every provider. Without
	// this, a GitHub-imported roster login could still attribute a
	// Jira/GitLab/Linear item sharing the same raw string -- the exact
	// cross-provider leak class this ticket exists to close, just at lower
	// priority than before. identities.provider_identities is the only
	// place in this schema a raw facet string is genuinely tagged with a
	// provider, so it is the source of truth for facetProviderIndex below.
	facetProviderIndex := map[string]map[string]struct{}{}
	for _, identity := range identities {
		providerIdentities := map[string][]string{}
		if raw := strings.TrimSpace(identity.ProviderIdentities); raw != "" {
			_ = json.Unmarshal([]byte(raw), &providerIdentities)
		}
		for provider, rawIDs := range providerIdentities {
			provider = strings.TrimSpace(provider)
			if provider == "" {
				continue
			}
			for _, rawID := range rawIDs {
				key := NormalizeDerivationIdentity(rawID)
				if key == "" {
					continue
				}
				if facetProviderIndex[key] == nil {
					facetProviderIndex[key] = map[string]struct{}{}
				}
				facetProviderIndex[key][provider] = struct{}{}
			}
		}
	}

	result := []GithubWorkItemDerivationMemberFact{}
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
		email := GithubWorkItemDerivationStringValue(identity.Email)

		facets := map[string]struct{}{}
		if key := NormalizeDerivationIdentity(canonicalID); key != "" {
			facets[key] = struct{}{}
		}
		if key := NormalizeDerivationIdentity(email); key != "" {
			facets[key] = struct{}{}
		}
		for _, rawIDs := range providerIdentities {
			for _, rawID := range rawIDs {
				if key := NormalizeDerivationIdentity(rawID); key != "" {
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
				result = append(result, GithubWorkItemDerivationMemberFact{
					Provider:       provider,
					TeamID:         teamID,
					TeamName:       GithubWorkItemDerivationFirstNonEmpty(team.Name, teamID),
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
				if len(result) > GithubWorkItemDerivationContextLimit {
					return nil, nil, nil, nil, ErrEffectRecoveryUnsafe
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

	// CHAOS-4321 (team-lead correction, 2026-08-26; scope fixed 10:39 PT):
	// every `teams.manual_members` facet is ALSO an untyped admin-mapping
	// candidate, independent of whether it has a backing `identities` row
	// above -- adding a member via the admin Identities screen or the
	// drift-approval flow is one of the genuinely admin-exclusive writers
	// chris named. Used to iterate `team.Members` -- fixed to
	// `team.ManualMembers` after a codex adversarial review HIGH finding
	// (`members` is not admin-exclusive). The unreviewed `Members` roster is
	// handled separately, below, as the provider-fallback tier.
	untyped := []GithubWorkItemDerivationUntypedMemberFact{}
	for _, teamID := range GithubWorkItemDerivationSortedAdminTeamIDs(adminTeams) {
		team := adminTeams[teamID]
		for _, facet := range team.ManualMembers {
			if strings.TrimSpace(facet) == "" {
				continue
			}
			untyped = append(untyped, GithubWorkItemDerivationUntypedMemberFact{
				TeamID:   teamID,
				TeamName: GithubWorkItemDerivationFirstNonEmpty(team.Name, teamID),
				Facet:    facet,
			})
			if len(untyped) > GithubWorkItemDerivationContextLimit {
				return nil, nil, nil, nil, ErrEffectRecoveryUnsafe
			}
		}
	}

	// CHAOS-4321 fix (chris, 2026-08-26 10:39 PT, after a codex adversarial
	// review HIGH finding): `teams.members` mixes admin-curated entries
	// (mirrored into ManualMembers above) with UNREVIEWED provider
	// auto-import roster writes -- demoted here to the provider-FALLBACK
	// tier.
	//
	// CHAOS-4321 round 3 fix (team-lead ruling, 2026-08-26, codex
	// adversarial review HIGH finding): matched WITHOUT a provider tag ONLY
	// when the facet is email-shaped (facetProviderIndex above cannot and
	// should not gate an email -- CHAOS-2609 bare-email matching is
	// deliberately cross-provider). A non-email facet is provider-tagged
	// via facetProviderIndex and routed into providerTagged instead --
	// Load() appends providerTagged onto facts.ProviderMembers, so it is
	// consulted through the SAME provider-scoped AttributionMapKey path as
	// real team_memberships rows. A non-email facet with no confirmed
	// provider tag at all matches nothing and is dropped.
	providerUntyped := []GithubWorkItemDerivationUntypedMemberFact{}
	providerTagged := []GithubWorkItemDerivationMemberFact{}
	for _, teamID := range GithubWorkItemDerivationSortedAdminTeamIDs(adminTeams) {
		team := adminTeams[teamID]
		teamName := GithubWorkItemDerivationFirstNonEmpty(team.Name, teamID)
		for _, facet := range team.Members {
			if strings.TrimSpace(facet) == "" {
				continue
			}
			normalized := NormalizeDerivationIdentity(facet)
			if strings.Contains(normalized, "@") {
				providerUntyped = append(providerUntyped, GithubWorkItemDerivationUntypedMemberFact{
					TeamID: teamID, TeamName: teamName, Facet: facet,
				})
				if len(providerUntyped) > GithubWorkItemDerivationContextLimit {
					return nil, nil, nil, nil, ErrEffectRecoveryUnsafe
				}
				continue
			}
			for provider := range facetProviderIndex[normalized] {
				providerTagged = append(providerTagged, GithubWorkItemDerivationMemberFact{
					Provider: provider, TeamID: teamID, TeamName: teamName,
					MemberID: facet,
					// 50/10, matching the untyped fallback candidate's
					// specificity/priority exactly -- this pool differs
					// from that one only in HOW it is matched (provider-
					// scoped vs. untyped), not in how much it is trusted.
					// UpdatedAt left zero-value, matching providerUntyped's
					// existing (pre-round-3) sibling construction above,
					// which has never set it either.
					IsPrimary: 1, Specificity: 50, Priority: 10,
				})
				if len(providerTagged) > GithubWorkItemDerivationContextLimit {
					return nil, nil, nil, nil, ErrEffectRecoveryUnsafe
				}
			}
		}
	}
	return result, untyped, providerUntyped, providerTagged, nil
}

// GithubWorkItemDerivationSortedAdminTeamIDs returns adminTeams' keys sorted,
// so LoadMembers's UntypedMembers output is deterministic (map iteration
// order is not) -- required for the live-python-oracle byte-for-byte gate.
func GithubWorkItemDerivationSortedAdminTeamIDs(
	adminTeams map[string]GithubWorkItemDerivationAdminTeam,
) []string {
	ids := make([]string, 0, len(adminTeams))
	for id := range adminTeams {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// LoadProviderMembers is the FALLBACK membership layer (chris, 2026-08-26
// 08:30 PT: "manual is override -- if the override exists, use it, else use
// attribution from providers"): unchanged from before CHAOS-4321, reading
// provider auto-import `team_memberships` directly. Consulted by
// ResolveMembership only when the admin layer (LoadMembers's two return
// values) has zero candidates for a given identity.
func (source ClickHouseFactSource) LoadProviderMembers(
	ctx context.Context, orgID string, asOf time.Time,
) ([]GithubWorkItemDerivationMemberFact, error) {
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
LIMIT ?`, orgID, asOf, asOf, GithubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []GithubWorkItemDerivationMemberFact{}
	for rows.Next() {
		var fact GithubWorkItemDerivationMemberFact
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
		if len(result) > GithubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func (source ClickHouseFactSource) LoadManualFallbacks(
	ctx context.Context, orgID string, asOf time.Time,
) ([]GithubWorkItemDerivationManualFallback, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT provider, scope_type, scope_id, team_id, ifNull(nullIf(team_name, ''), team_id), reason, priority
FROM manual_attribution_fallbacks FINAL
WHERE org_id = ? AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
ORDER BY provider, scope_type, scope_id, priority, team_id, team_name, reason
LIMIT ?`, orgID, asOf, asOf, GithubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []GithubWorkItemDerivationManualFallback{}
	for rows.Next() {
		var fact GithubWorkItemDerivationManualFallback
		var priority int32
		if err := rows.Scan(&fact.Provider, &fact.ScopeType, &fact.ScopeID, &fact.TeamID, &fact.TeamName, &fact.Reason, &priority); err != nil {
			return nil, err
		}
		fact.Priority = int(priority)
		result = append(result, fact)
		if len(result) > GithubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func GithubWorkItemDerivationStringPointer(value string) *string {
	if value == "" {
		return nil
	}
	copy := value
	return &copy
}

func GithubWorkItemDerivationStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// ResolveMembership mirrors compute_work_items.py's `_resolve_membership`
// (CHAOS-4321, chris 08:30 PT: "manual is override -- if the override
// exists, use it, else use attribution from providers"; refined 2026-08-26
// 10:39 PT: "admin is an override, not a default -- it's the sync config
// mapping, but admin can override it in the panel"). Layer 1 (admin:
// `memberByID` ∪ `memberByUntypedFacet`, sourced from `identities.team_ids`
// and `teams.manual_members` -- NOT `teams.members`, which mixes in
// unreviewed provider auto-import rows) is AUTHORITATIVE when it has ANY
// candidate for this identity -- including when ambiguous: an ambiguous
// admin mapping does NOT fall through to layer 2, it needs fixing, not
// bypassing. Layer 2 (`providerMemberByID` ∪ `providerMemberByUntypedFacet`,
// sourced from provider auto-import `team_memberships` and `teams.members`
// respectively) is consulted ONLY when layer 1 has ZERO candidates for this
// identity. Both layers apply the SAME exactly-one-team gate.
//
// Returns `(candidates, reason)`: `candidates` is the resolved list to use
// for the caller's source when exactly one team resolved (`reason` is
// `""`); otherwise `candidates` is nil and `reason` is one of
// `"ambiguous_admin_membership:<sorted team ids>"`,
// `"ambiguous_provider_membership:<sorted team ids>"`, or `"no_membership"`
// -- `""` only when there is no identity to look up at all (no assignee, no
// reporter).
func (derived GithubWorkItemDerivationContext) ResolveMembership(
	provider, identity string,
) ([]GithubWorkItemDerivationCandidate, string) {
	if strings.TrimSpace(identity) == "" {
		return nil, ""
	}
	key := NormalizeDerivationIdentity(identity)
	adminCandidates := append(
		[]GithubWorkItemDerivationCandidate(nil),
		derived.memberByID[AttributionMapKey(provider, key)]...,
	)
	adminCandidates = append(adminCandidates, derived.memberByUntypedFacet[key]...)
	adminTeams := map[string]struct{}{}
	for _, candidate := range adminCandidates {
		adminTeams[GithubWorkItemDerivationStringValue(candidate.TeamID)] = struct{}{}
	}
	if len(adminTeams) == 1 {
		return adminCandidates, ""
	}
	if len(adminTeams) > 1 {
		return nil, "ambiguous_admin_membership:" + GithubWorkItemDerivationSortedTeamIDs(adminTeams)
	}

	providerCandidates := append(
		[]GithubWorkItemDerivationCandidate(nil),
		derived.providerMemberByID[AttributionMapKey(provider, key)]...,
	)
	providerCandidates = append(providerCandidates, derived.providerMemberByUntypedFacet[key]...)
	providerTeams := map[string]struct{}{}
	for _, candidate := range providerCandidates {
		providerTeams[GithubWorkItemDerivationStringValue(candidate.TeamID)] = struct{}{}
	}
	if len(providerTeams) == 1 {
		return providerCandidates, ""
	}
	if len(providerTeams) > 1 {
		return nil, "ambiguous_provider_membership:" + GithubWorkItemDerivationSortedTeamIDs(providerTeams)
	}
	return nil, "no_membership"
}

// GithubWorkItemDerivationSortedTeamIDs renders a team-id set as a
// deterministic, comma-joined, sorted string for an ambiguous-membership
// evidence/reason suffix -- so an admin can act on the persisted evidence
// text (team-lead, CHAOS-4321: "list the team ids in the evidence string,
// so an admin can fix the mapping").
func GithubWorkItemDerivationSortedTeamIDs(teams map[string]struct{}) string {
	ids := make([]string, 0, len(teams))
	for id := range teams {
		if id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

// GithubWorkItemDerivationHasReason reports set membership for the
// membershipSkipReasons/reporterSkipReason evidence composition in
// Resolve() -- a plain helper so that composition reads as a priority
// switch rather than repeated map-comma-ok checks.
func GithubWorkItemDerivationHasReason(reasons map[string]struct{}, reason string) bool {
	_, exists := reasons[reason]
	return exists
}

// GithubWorkItemDerivationReasonWithPrefix returns the (deterministic,
// sorted-first) reason in `reasons` that starts with `prefix` -- used for
// the ambiguous_admin_membership/ambiguous_provider_membership reasons,
// which carry a variable ":<team ids>" suffix so `GithubWorkItemDerivationHasReason`'s
// exact-match check cannot be used for them. Returns "" if none match.
func GithubWorkItemDerivationReasonWithPrefix(reasons map[string]struct{}, prefix string) string {
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

// GithubWorkItemDerivationIsBotIdentity reports whether an identity is a
// GitHub App/bot actor. Mirrors compute_work_items.py's _is_bot_identity:
// IdentityResolver.Resolve falls back to "provider:username" for any
// identity with no email -- true of every bot -- so the raw login, brackets
// included, survives resolution (e.g. "github:dependabot[bot]"). "[bot]" is
// reserved: GitHub does not let a human register a bracketed login, the same
// invariant this repo's other bot detectors already rely on
// (isLinearIntegrationAuthor-equivalent checks, providers/_ai_detection.py's
// CI_BOTS/KNOWN_AI_BOTS) -- this reuses that convention rather than a second
// bot list.
func GithubWorkItemDerivationIsBotIdentity(identity string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(identity)), "[bot]")
}

// GithubWorkItemDerivationIsPullOrMergeRequestType gates author_membership
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
func GithubWorkItemDerivationIsPullOrMergeRequestType(provider, itemType string) bool {
	switch provider {
	case "github":
		return itemType == "pr"
	case "gitlab":
		return itemType == "merge_request"
	default:
		return false
	}
}

func GithubWorkItemDerivationFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
