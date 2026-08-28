package providersync

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
//     DONOR item that DOES carry a project_id -- same edge-walk shape as
//     the existing linked-issue team resolver (job_work_items.py's
//     build_linked_issue_team_resolver / this package's
//     github_work_items_derivation_context.go), but landing on
//     team_repo_ownership instead of a per-work-item attribution.
//
// PR inheritance ("design check (b)"): work_graph_issue_pr additionally
// links a work_item_id directly to a (repo_id, pr_number) -- this captures
// PRs that never became their own work_items row (or whose own project_id
// is empty) via whichever tracker's cross-provider link-capture already
// found the association (team-attribution.md Sec 2). Every PR reachable
// through this table inherits its linked work item's resolved team, using
// work_graph_issue_pr's OWN repo_id (the PR's repo, not necessarily the
// linked work item's repo_id -- a cross-repo link is possible and must use
// the PR's real repo).
//
// Never a hand seeder: this file has no direct-insert-to-team_repo_
// ownership fixture path anywhere in this repo. Fixtures must emit
// provider-shaped team_project_ownership + work_items + work_graph_issue_pr
// rows and let THIS producer derive team_repo_ownership from them, exactly
// like a real sync would.

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
type TeamRepoOwnershipWorkItem struct {
	WorkItemID string
	RepoID     string
	ProjectID  string
}

// TeamRepoOwnershipDependencyEdge is one already-synced
// work_item_dependencies row: Source inherits team attribution FROM Target
// (the donor) -- same directionality as the existing linked-issue resolver.
type TeamRepoOwnershipDependencyEdge struct {
	SourceWorkItemID string
	TargetWorkItemID string
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

	// donorProjectID resolves the project_id an item's team should be
	// derived from: its own, or (if empty) the FIRST dependency donor that
	// has one. Mirrors the existing linked-issue resolver's directionality
	// (source inherits from target) without needing that resolver's full
	// machinery, since this producer only needs project_id, not a richer
	// attribution record.
	donorProjectID := func(item TeamRepoOwnershipWorkItem) string {
		if item.ProjectID != "" {
			return item.ProjectID
		}
		for _, edge := range dependencyEdges {
			if edge.SourceWorkItemID != item.WorkItemID {
				continue
			}
			if donor, ok := byID[edge.TargetWorkItemID]; ok && donor.ProjectID != "" {
				return donor.ProjectID
			}
		}
		return ""
	}

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
		projectID := donorProjectID(item)
		if teamID, ok := projectToTeam[projectID]; ok {
			assign(item.RepoID, teamID)
		}
	}

	// PR inheritance: work_graph_issue_pr's OWN repo_id, resolved via the
	// linked work item's team (which itself may have come from path 1 or
	// 2 above -- resolved through byID + donorProjectID, not repoToTeam,
	// since the linked item's own repo_id (if any) may differ from the
	// PR's repo_id and must not gate this PR's resolution).
	for _, link := range issuePRLinks {
		if link.RepoID == "" {
			continue
		}
		item, ok := byID[link.WorkItemID]
		if !ok {
			continue
		}
		projectID := donorProjectID(item)
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
