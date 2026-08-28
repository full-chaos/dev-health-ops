package providersync

import (
	"sort"
	"strings"
	"time"
)

// team_repo_ownership_derivation.go derives team_repo_ownership rows from
// data ALREADY synced by other providers' routes -- CHAOS-4365 item 1b.
// Never fetches anything from a provider API: team_project_ownership
// (already synced, any tracker: Linear/Jira/GitHub Projects) is joined
// against work_items' project<->repo linkage (already synced, any tracker
// that can carry a repo-bearing item -- e.g. a GitHub PR/issue) to derive
// which team owns which repo. Provider-agnostic on both ends per chris's
// 2026-08-28 ruling ("the graph associated VIA ANY TOOL THAT CAN MAP to
// github/gitlab objects -- the SOURCE github/gitlab/bitbucket ARE
// irrelevant").
//
// CHAOS-4321 hard rule: team = project/repo OWNERSHIP only, never
// person->membership inference. This producer only ever reads
// team_project_ownership (an ownership table) and repo/project linkage
// (structural, not people) -- it never touches identities/memberships.
//
// Two signal paths, both required (team-lead ruling 2026-08-28, "design
// check (a)" and "(b)"):
//
//  1. Own-project_id path: a work item that itself carries BOTH a repo_id
//     (it's a GitHub/GitLab-shaped repo-bearing item -- a PR or an issue
//     synced with repo context) AND a project_id (it's a member of a
//     tracked project) resolves directly: project_id -> team via
//     team_project_ownership, stamped on the item's own repo_id.
//  2. Dependency-donor walk: a repo-bearing item with NO project_id of its
//     own (e.g. a bare GitHub PR, which GitHub's own model has no concept
//     of "project membership" for) but a work_item_dependencies edge to a
//     DONOR item that DOES carry a project_id inherits that donor's
//     project_id. This is the EXACT SAME edge-walk shape, with the EXACT
//     SAME gating rules, as the existing linked-issue team resolver
//     (compute_work_items.py::build_linked_issue_team_resolver) -- landing
//     on team_repo_ownership instead of a per-work-item attribution:
//       - only INHERITANCE-SAFE relationship types transfer a team
//         (relates_to, relates, duplicates, external_issue_key); blocking
//         links (blocks/blocked_by/is_blocked_by) routinely span teams and
//         are never a donor edge (teamRepoOwnershipInheritableRelationshipTypes);
//       - per (source, target) pair, only the LATEST edge (by last_synced)
//         counts -- a relationship-type flip supersedes the stale row
//         instead of leaving an old inheritable edge alive alongside the
//         new one; ties break on the lexicographically smaller
//         relationship_type (deterministically prefers the safer blocking
//         type over inheriting);
//       - a cross-provider `extkey:KEY` target (emitted by PR parsers for a
//         Linear/Jira issue key mentioned in a PR body/branch name) is
//         resolved against a Linear/Jira work-item-key index; a key that
//         exists in BOTH providers is genuinely ambiguous and dropped
//         (never guessed) -- CHAOS-4321;
//       - when a source has multiple valid donor candidates, the
//         lexicographically smallest canonical target wins: a stable,
//         run-independent tie-break, since ClickHouse rows are unordered.
//
// PR inheritance ("design check (b)"): work_graph_issue_pr additionally
// links a work_item_id directly to a (repo_id, pr_number) -- this captures
// PRs that never became their own work_items row (or whose own project_id
// is empty) via whichever tracker's cross-provider link-capture already
// found the association (team-attribution.md Sec 2). Every PR reachable
// through this table inherits its linked work item's resolved team (own OR
// donor project_id, same resolver as path 1+2 above), using
// work_graph_issue_pr's OWN repo_id (the PR's repo, not necessarily the
// linked work item's repo_id -- a cross-repo link is possible and must use
// the PR's real repo).
//
// Never a hand seeder: this file has no direct-insert-to-team_repo_
// ownership fixture path anywhere in this repo. Fixtures must emit
// provider-shaped team_project_ownership + work_items + work_item_dependencies
// + work_graph_issue_pr rows and let THIS producer derive team_repo_ownership
// from them, exactly like a real sync would.

// TeamRepoOwnershipProjectLink is one already-synced team_project_ownership
// row for this org (any provider/tracker).
type TeamRepoOwnershipProjectLink struct {
	ProjectID string
	TeamID    string
	IsPrimary bool
}

// TeamRepoOwnershipWorkItem is one already-synced work_items row for this
// org (any provider). RepoID is empty for a tracker item with no repo
// context (e.g. a plain Jira story); ProjectID is empty for a repo-bearing
// item with no project membership of its own (e.g. a bare GitHub PR).
// Provider gates the extkey donor-index (only linear/jira work items ever
// carry a cross-provider issue key another tracker's PR parser can cite),
// exactly like compute_work_items.py's donor-index build.
type TeamRepoOwnershipWorkItem struct {
	WorkItemID string
	Provider   string
	RepoID     string
	ProjectID  string
}

// TeamRepoOwnershipDependencyEdge is one already-synced
// work_item_dependencies row: Source inherits team attribution FROM Target
// (the donor) -- same directionality, same relationship_type gating, and
// same latest-edge-per-pair collapse as the existing linked-issue resolver.
type TeamRepoOwnershipDependencyEdge struct {
	SourceWorkItemID string
	TargetWorkItemID string
	RelationshipType string
	LastSynced       time.Time
}

// TeamRepoOwnershipIssuePRLink is one already-synced work_graph_issue_pr
// row: WorkItemID's resolved team is inherited by the PR identified by
// RepoID (this table's own repo_id, which may differ from WorkItemID's
// own RepoID for a genuine cross-repo link) + PRNumber. PRNumber is not
// needed by the derivation itself (team_repo_ownership has no per-PR
// grain) but is kept on the struct for caller-side diagnostics/logging.
type TeamRepoOwnershipIssuePRLink struct {
	WorkItemID string
	RepoID     string
	PRNumber   uint32
}

// DerivedTeamRepoOwnershipRow is one team_repo_ownership row this producer
// would write. Source is always "inferred" (CHAOS-4365 item 1b ruling:
// reuse the existing, previously-unused enum value rather than add a new
// one). Specificity is deliberately LOWER than a hypothetical direct
// GitHub-team-derived row (never built in this PR -- "PR B", a separate
// ticket) would carry, so a future direct signal can outrank this one via
// the existing specificity/priority ownership-precedence read path,
// without this producer needing to know that signal exists.
type DerivedTeamRepoOwnershipRow struct {
	TeamID      string
	RepoID      string
	Specificity uint16
}

// teamRepoOwnershipInferredSpecificity is deliberately lower than every
// existing writer's specificity in this schema (provider_access rows use
// BASE_SPECIFICITY=100 upward, migration 051's default is 0) so a future
// direct signal always outranks an inferred one at the same priority tier.
const teamRepoOwnershipInferredSpecificity = 10

// teamRepoOwnershipInheritableRelationshipTypes mirrors
// compute_work_items.py's _INHERITABLE_RELATIONSHIP_TYPES verbatim: only
// edges that mean "this item does (or duplicates) the work of the linked
// issue" are sound to inherit a team through. A blocking relationship
// (blocks/blocked_by/is_blocked_by) connects items that are frequently
// owned by DIFFERENT teams, so it must never drive attribution.
var teamRepoOwnershipInheritableRelationshipTypes = map[string]bool{
	"relates_to":         true,
	"relates":            true,
	"duplicates":         true,
	"external_issue_key": true,
}

// deriveTeamRepoOwnership implements both signal paths plus PR inheritance
// against ALREADY-LOADED rows for one org (loading is the caller's job --
// this function does no I/O, so it is exhaustively unit-testable). Returns
// one row per distinct (team_id, repo_id) pair -- never a duplicate, and
// never a repo attributed to two different teams (CHAOS-4321: on a genuine
// conflict -- the same repo reachable from two DIFFERENT teams' owned
// projects -- neither wins; the repo is left unresolved rather than
// guessed, matching this schema's existing "never guess" precedent).
func deriveTeamRepoOwnership(
	projectLinks []TeamRepoOwnershipProjectLink,
	workItems []TeamRepoOwnershipWorkItem,
	dependencyEdges []TeamRepoOwnershipDependencyEdge,
	issuePRLinks []TeamRepoOwnershipIssuePRLink,
) []DerivedTeamRepoOwnershipRow {
	projectToTeam := resolveProjectToTeam(projectLinks)
	if len(projectToTeam) == 0 {
		return nil
	}

	byID := make(map[string]TeamRepoOwnershipWorkItem, len(workItems))
	for _, item := range workItems {
		byID[item.WorkItemID] = item
	}

	donorProjectID := buildDonorProjectIDResolver(byID, dependencyEdges)

	repoToTeam := map[string]string{}
	conflicted := map[string]bool{}
	assign := func(repoID, teamID string) {
		if repoID == "" || teamID == "" {
			return
		}
		if existing, ok := repoToTeam[repoID]; ok {
			if existing != teamID {
				conflicted[repoID] = true
			}
			return
		}
		repoToTeam[repoID] = teamID
	}

	// Path 1 + 2: every repo-bearing work item.
	for _, item := range workItems {
		if item.RepoID == "" {
			continue
		}
		projectID := donorProjectID(item.WorkItemID)
		if teamID, ok := projectToTeam[projectID]; ok {
			assign(item.RepoID, teamID)
		}
	}

	// PR inheritance: work_graph_issue_pr's OWN repo_id, resolved via the
	// linked work item's team (own or donor project_id, same resolver as
	// above) -- since the linked item's own repo_id (if any) may differ
	// from the PR's repo_id and must not gate this PR's resolution.
	for _, link := range issuePRLinks {
		if link.RepoID == "" {
			continue
		}
		projectID := donorProjectID(link.WorkItemID)
		if teamID, ok := projectToTeam[projectID]; ok {
			assign(link.RepoID, teamID)
		}
	}

	rows := make([]DerivedTeamRepoOwnershipRow, 0, len(repoToTeam))
	for repoID, teamID := range repoToTeam {
		if conflicted[repoID] {
			continue
		}
		rows = append(rows, DerivedTeamRepoOwnershipRow{
			TeamID:      teamID,
			RepoID:      repoID,
			Specificity: teamRepoOwnershipInferredSpecificity,
		})
	}
	return rows
}

// resolveProjectToTeam reduces a project's possibly-multiple ownership
// candidates to at most one team: the primary if exactly one is marked
// primary, else unresolved (never guessed) if zero or more than one claim
// primacy for the same project -- CHAOS-4321's "never guess" precedent.
func resolveProjectToTeam(links []TeamRepoOwnershipProjectLink) map[string]string {
	primaryByProject := map[string]string{}
	primaryCount := map[string]int{}
	for _, link := range links {
		if link.ProjectID == "" || link.TeamID == "" || !link.IsPrimary {
			continue
		}
		primaryByProject[link.ProjectID] = link.TeamID
		primaryCount[link.ProjectID]++
	}
	resolved := make(map[string]string, len(primaryByProject))
	for projectID, teamID := range primaryByProject {
		if primaryCount[projectID] == 1 {
			resolved[projectID] = teamID
		}
	}
	return resolved
}

// buildDonorProjectIDResolver returns a function mapping a work_item_id to
// the project_id its team should be derived from: its own, or (if it has
// none) the single deterministically-chosen dependency donor's project_id.
// Mirrors compute_work_items.py::build_linked_issue_team_resolver's edge
// handling exactly (see the package doc comment above for the gating
// rules), adapted from "resolves to a team" to "resolves to a project_id"
// since this producer's donor pool is team_project_ownership, not the
// richer multi-tier attribution ladder.
func buildDonorProjectIDResolver(
	byID map[string]TeamRepoOwnershipWorkItem,
	edges []TeamRepoOwnershipDependencyEdge,
) func(workItemID string) string {
	keyIndex := buildIssueKeyIndex(byID)

	type pair struct{ source, target string }
	latestEdge := make(map[pair]TeamRepoOwnershipDependencyEdge, len(edges))
	for _, edge := range edges {
		key := pair{edge.SourceWorkItemID, edge.TargetWorkItemID}
		current, ok := latestEdge[key]
		if !ok ||
			edge.LastSynced.After(current.LastSynced) ||
			(edge.LastSynced.Equal(current.LastSynced) && edge.RelationshipType < current.RelationshipType) {
			latestEdge[key] = edge
		}
	}

	candidateTargets := map[string][]string{}
	for _, edge := range latestEdge {
		if !teamRepoOwnershipInheritableRelationshipTypes[edge.RelationshipType] {
			continue
		}
		target := canonicalDependencyTarget(edge.TargetWorkItemID, keyIndex)
		if target == "" {
			continue
		}
		donor, ok := byID[target]
		if !ok || donor.ProjectID == "" {
			continue
		}
		candidateTargets[edge.SourceWorkItemID] = append(candidateTargets[edge.SourceWorkItemID], target)
	}

	// Deterministic tie-break: the lexicographically smallest canonical
	// target wins when a source has multiple valid donor candidates.
	donorProjectIDBySource := make(map[string]string, len(candidateTargets))
	for source, targets := range candidateTargets {
		sort.Strings(targets)
		donorProjectIDBySource[source] = byID[targets[0]].ProjectID
	}

	return func(workItemID string) string {
		if item, ok := byID[workItemID]; ok && item.ProjectID != "" {
			return item.ProjectID
		}
		return donorProjectIDBySource[workItemID]
	}
}

// buildIssueKeyIndex indexes Linear/Jira work items by their bare issue key
// (e.g. "PLAT-9" from "linear:PLAT-9") so a cross-provider `extkey:KEY`
// dependency target can be resolved. A key claimed by more than one work
// item is genuinely ambiguous and is dropped from the index entirely --
// CHAOS-4321 never guesses.
func buildIssueKeyIndex(byID map[string]TeamRepoOwnershipWorkItem) map[string]string {
	keyIndex := map[string]string{}
	ambiguous := map[string]bool{}
	for workItemID, item := range byID {
		if item.Provider != "linear" && item.Provider != "jira" {
			continue
		}
		idx := strings.Index(workItemID, ":")
		if idx < 0 {
			continue
		}
		key := strings.ToUpper(strings.TrimSpace(workItemID[idx+1:]))
		if key == "" || ambiguous[key] {
			continue
		}
		if existing, ok := keyIndex[key]; ok && existing != workItemID {
			delete(keyIndex, key)
			ambiguous[key] = true
			continue
		}
		keyIndex[key] = workItemID
	}
	return keyIndex
}

// canonicalDependencyTarget resolves an `extkey:KEY` dependency target
// (emitted by PR parsers for a cross-provider issue key) to the work_item_id
// it names, via keyIndex. A missing or ambiguous key returns "" -- no
// inheritance, never a guess. Every other target is already a work_item_id
// and passes through unchanged.
func canonicalDependencyTarget(targetID string, keyIndex map[string]string) string {
	const extkeyPrefix = "extkey:"
	if !strings.HasPrefix(targetID, extkeyPrefix) {
		return targetID
	}
	key := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(targetID, extkeyPrefix)))
	return keyIndex[key]
}
