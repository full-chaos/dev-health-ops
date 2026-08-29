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
// row for this org (any provider/tracker), already collapsed to one row per
// (provider, project_id, team_id) triple -- the caller
// (loadTeamRepoOwnershipProjectLinks) is responsible for deduping
// repeated-import generations before this point, same shape as
// metrics/loaders/clickhouse.py's load_team_attribution_context (GROUP BY +
// argMax(field, (updated_at, valid_from))). IsPrimary is NOT a reliable
// "this is THE owner" signal on its own: GitLab's provider_access writer
// (team_autoimport_gitlab.py's _project_ownership_rows) sets is_primary=0 on
// EVERY row it ever writes, so requiring IsPrimary==true here would
// silently derive nothing for every GitLab-sourced org. Specificity is the
// real tie-break; IsPrimary only wins between otherwise-equal candidates
// (codex adversarial review, 2026-08-28: confirmed high-severity finding,
// GitLab rows unconditionally dropped by the prior is_primary-required
// design).
//
// Provider is part of the match key, mirroring
// compute_work_items.py's project_by_id (keyed by (provider, project_id),
// not bare project_id) exactly: team_project_ownership.project_id values
// are namespaced per-writer (e.g. "{org_id}:jira:{key}"), but requiring an
// exact provider match too is the same defense-in-depth the canonical
// Python precedent already applies, rather than trusting every writer's
// namespacing convention to hold forever (codex adversarial review,
// 2026-08-28, confirmed finding).
type TeamRepoOwnershipProjectLink struct {
	Provider    string
	ProjectID   string
	TeamID      string
	IsPrimary   bool
	Specificity uint16
}

// teamRepoOwnershipProjectRef identifies a team_project_ownership row's
// join key: (provider, project_id), never bare project_id -- see
// TeamRepoOwnershipProjectLink's doc comment.
type teamRepoOwnershipProjectRef struct {
	Provider  string
	ProjectID string
}

// TeamRepoOwnershipWorkItem is one already-synced work_items row for this
// org (any provider). RepoID is empty for a tracker item with no repo
// context (e.g. a plain Jira story); ProjectID is empty for a repo-bearing
// item with no project membership of its own (e.g. a bare GitHub PR).
// Provider gates the extkey donor-index (only linear/jira work items ever
// carry a cross-provider issue key another tracker's PR parser can cite),
// exactly like compute_work_items.py's donor-index build.
//
// NativeTeamKey (Linear only; work_items.native_team_key, migration 050) is
// the raw Linear team key (issue.team.key) -- CHAOS-4458 part (b): Linear's
// team_project_ownership writer (team_autoimport_linear.py) and its
// work-item normalizer disagree on what project_id means. The ownership
// writer stamps project_id = "{org_id}:linear:{team_key}" (falling back to
// the team's own key when the team has no explicit Linear Project
// associations -- team_autoimport_linear.py:454-456,472,487), while the
// Linear work-item normalizer stamps ProjectID with the raw Linear Project
// UUID, a DELIBERATELY separate id space the same file's docstring calls
// out explicitly (team_autoimport_linear.py:309-314: "a SEPARATE id space
// and are unaffected"). These two values never intersect, so a Linear-only
// org's ownership never resolved via ProjectID alone (0 of 3168
// project-id-bearing Linear work items matched, locally, before CHAOS-4458b).
//
// CHAOS-4537: resolveWorkItemTeamID no longer retries this arm by
// reconstructing that "{org_id}:linear:{team_key}" identity and looking it
// up in team_project_ownership (linearTeamKeyProjectID, below) -- it trusts
// NativeTeamKey AS the resolved team_id directly. The two values were always
// byte-identical in practice: the ownership writer's fallback row stamps
// team_id = the team's own key (linear_reference_catalog_route.go's "The
// MATCHING team_project_ownership row below" block), the exact same
// issue.team.key this field already carries -- so the reconstruct-then-look-
// up step was pure indirection onto a value already in hand, and required a
// team_project_ownership row that this reader no longer needs to exist.
type TeamRepoOwnershipWorkItem struct {
	WorkItemID    string
	Provider      string
	RepoID        string
	ProjectID     string
	NativeTeamKey string
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
	// ResolutionArm names which identity resolved this row's team: either
	// TeamRepoOwnershipResolutionArmProjectID (the direct join, every
	// provider) or TeamRepoOwnershipResolutionArmLinearTeamKey (CHAOS-4458
	// part (b) -- see TeamRepoOwnershipWorkItem's doc comment). Telemetry
	// only; never written to team_repo_ownership itself. Set to whichever
	// arm resolved the FIRST work item/PR-link that claimed this repo (the
	// same first-wins row assign() already applies), so a repo reachable via
	// both arms still reports one deterministic arm.
	ResolutionArm string
}

// teamRepoOwnershipInferredSpecificity is deliberately lower than every
// existing writer's specificity in this schema (provider_access rows use
// BASE_SPECIFICITY=100 upward, migration 051's default is 0) so a future
// direct signal always outranks an inferred one at the same priority tier.
const teamRepoOwnershipInferredSpecificity = 10

// TeamRepoOwnershipResolutionArm* are the values deriveTeamRepoOwnership
// tags each DerivedTeamRepoOwnershipRow.ResolutionArm with (CHAOS-4458 part
// (b)). Mirrored (as a distinct, jobruntime-owned type) by
// jobruntime.TeamRepoOwnershipResolutionArm for telemetry -- kept as plain
// strings here since providersync must not import jobruntime.
const (
	TeamRepoOwnershipResolutionArmProjectID     = "project_id"
	TeamRepoOwnershipResolutionArmLinearTeamKey = "linear_team_key"
)

// teamRepoOwnershipResolutionArmPriority ranks the two arms so
// deriveTeamRepoOwnership's assign() can pick a deterministic winner when
// the SAME (repo, team) pair is reachable via both -- e.g. one work item
// resolves its own project_id directly while another, unrelated work item
// donating to the same repo only resolves via native_team_key. project_id
// outranks linear_team_key (the direct join is authoritative; the
// team-key-shaped fallback only exists because Linear's ownership writer
// and work-item normalizer disagree on project_id, per
// TeamRepoOwnershipWorkItem's doc comment) -- an unrecognized/empty arm
// ranks lowest so it is always replaced once any real arm is seen.
func teamRepoOwnershipResolutionArmPriority(arm string) int {
	switch arm {
	case TeamRepoOwnershipResolutionArmProjectID:
		return 2
	case TeamRepoOwnershipResolutionArmLinearTeamKey:
		return 1
	default:
		return 0
	}
}

// linearTeamKeyProjectID reconstructs the SAME project_id string
// team_autoimport_linear.py's ownership writer stamps for a team with no
// explicit Linear Project associations: `_project_id(org_id, "linear",
// project_key)` where project_key defaults to the team's own key
// (team_autoimport_linear.py:454-456,472,487) -- i.e. "{org_id}:linear:
// {team_key}".
//
// CHAOS-4537: no longer called by anything in this file --
// resolveWorkItemTeamID trusts NativeTeamKey directly instead of
// reconstructing this identity and looking it up in team_project_ownership
// (see TeamRepoOwnershipWorkItem's doc comment for why that indirection was
// safe to remove). Kept only so linear_reference_catalog_test.go's
// TestLinearReferenceCatalogTeamKeyOwnershipRowMatchesItsOneReader can still
// name the writer's own row shape by construction -- that row itself is
// still written (linear_reference_catalog_route.go, out of this ticket's
// scope) and is now vestigial from THIS reader's point of view; removing the
// write is a deliberate fast-follow, filed once this redirect is proven
// live, not bundled into CHAOS-4537.
func linearTeamKeyProjectID(orgID, teamKey string) string {
	return orgID + ":linear:" + teamKey
}

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
	// orgID is unused inside this function as of CHAOS-4537 (the last
	// internal use, reconstructing the linear_team_key identity, is gone --
	// see resolveWorkItemTeamID). Kept in the signature: every caller
	// (the ClickHouse-loading Derive, and every test below) already scopes
	// its inputs to one org and passes this for documentation/future-proofing
	// rather than threading it through, and changing the signature would
	// force an unrelated mechanical edit across every existing call site.
	orgID string,
	projectLinks []TeamRepoOwnershipProjectLink,
	workItems []TeamRepoOwnershipWorkItem,
	dependencyEdges []TeamRepoOwnershipDependencyEdge,
	issuePRLinks []TeamRepoOwnershipIssuePRLink,
) []DerivedTeamRepoOwnershipRow {
	projectToTeam := resolveProjectToTeam(projectLinks)
	// CHAOS-4537: no early return on len(projectToTeam) == 0 any more -- the
	// linear_team_key arm below resolves straight from a Linear work item's
	// own NativeTeamKey field and needs no team_project_ownership row at all,
	// so an org with zero project-ownership links can still legitimately
	// derive rows through it. The pre-CHAOS-4537 shortcut here was sound only
	// because every resolution path used to require a projectToTeam entry;
	// it is not any more, and keeping it would have silently zeroed out this
	// arm the moment CHAOS-4530's fast-follow stops writing the
	// team-key-shaped ownership row this arm no longer needs.

	byID := make(map[string]TeamRepoOwnershipWorkItem, len(workItems))
	for _, item := range workItems {
		byID[item.WorkItemID] = item
	}

	donorTeamID := buildDonorTeamIDResolver(byID, dependencyEdges, projectToTeam)

	repoToTeam := map[string]string{}
	repoArm := map[string]string{}
	conflicted := map[string]bool{}
	assign := func(repoID, teamID, arm string) {
		if repoID == "" || teamID == "" {
			return
		}
		if existing, ok := repoToTeam[repoID]; ok {
			if existing != teamID {
				conflicted[repoID] = true
				return
			}
			// Same repo, same team, resolved again via a (possibly
			// different) arm: keep whichever arm ranks higher by
			// teamRepoOwnershipResolutionArmPriority, deterministically,
			// rather than whichever candidate this loop happened to visit
			// first. loadTeamRepoOwnershipWorkItems has no ORDER BY, so an
			// unqualified first-wins policy let the recorded arm flicker
			// between identical runs over the same ClickHouse snapshot
			// (codex adversarial review, 2026-08-29, confirmed finding).
			if teamRepoOwnershipResolutionArmPriority(arm) > teamRepoOwnershipResolutionArmPriority(repoArm[repoID]) {
				repoArm[repoID] = arm
			}
			return
		}
		repoToTeam[repoID] = teamID
		repoArm[repoID] = arm
	}

	// Path 1 + 2: every repo-bearing work item.
	for _, item := range workItems {
		if item.RepoID == "" {
			continue
		}
		if teamID, arm := donorTeamID(item.WorkItemID); teamID != "" {
			assign(item.RepoID, teamID, arm)
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
		if teamID, arm := donorTeamID(link.WorkItemID); teamID != "" {
			assign(link.RepoID, teamID, arm)
		}
	}

	rows := make([]DerivedTeamRepoOwnershipRow, 0, len(repoToTeam))
	for repoID, teamID := range repoToTeam {
		if conflicted[repoID] {
			continue
		}
		rows = append(rows, DerivedTeamRepoOwnershipRow{
			TeamID:        teamID,
			RepoID:        repoID,
			Specificity:   teamRepoOwnershipInferredSpecificity,
			ResolutionArm: repoArm[repoID],
		})
	}
	return rows
}

// resolveWorkItemTeamID is the single identity-resolution step shared by the
// own-project_id path and the donor walk (CHAOS-4458 part (b) / CHAOS-4537):
// try the item's own project_id first via projectToTeam (works for every
// provider today, and for a future project-UUID-keyed Linear ownership row
// -- see TeamRepoOwnershipWorkItem's doc comment on FIX SHAPE case (2));
// only if that does not resolve, and only for a Linear work item carrying a
// native_team_key, fall back to NativeTeamKey AS the resolved team_id
// directly -- no team_project_ownership lookup for this arm any more (see
// TeamRepoOwnershipWorkItem's doc comment for why that indirection was safe
// to remove: the ownership writer's team-key-shaped row always stamped
// team_id to this exact same value). Never guesses between the two arms:
// the moment either resolves, the other is not consulted.
func resolveWorkItemTeamID(
	item TeamRepoOwnershipWorkItem,
	projectToTeam map[teamRepoOwnershipProjectRef]string,
) (string, string, bool) {
	if item.ProjectID != "" {
		ref := teamRepoOwnershipProjectRef{Provider: item.Provider, ProjectID: item.ProjectID}
		if teamID, ok := projectToTeam[ref]; ok {
			return teamID, TeamRepoOwnershipResolutionArmProjectID, true
		}
	}
	if item.Provider == "linear" && item.NativeTeamKey != "" {
		return item.NativeTeamKey, TeamRepoOwnershipResolutionArmLinearTeamKey, true
	}
	return "", "", false
}

// resolveProjectToTeam reduces a project's possibly-multiple ownership
// candidates to at most one team, ranked by (IsPrimary, Specificity) --
// IsPrimary breaks a tie, it is never a hard requirement, since GitLab's
// provider_access writer (team_autoimport_gitlab.py's
// _project_ownership_rows) sets is_primary=0 unconditionally on every row it
// writes; requiring IsPrimary==true here silently derived nothing for every
// GitLab-sourced org (codex adversarial review, 2026-08-28, confirmed
// high-severity finding). Mirrors the read-time ranking
// providers/teams.py::load_team_repo_ownership_map already applies to
// team_repo_ownership itself (ORDER BY is_primary DESC, specificity DESC) --
// team_project_ownership has no equivalent established ranking of its own
// for this producer to defer to.
//
// A genuine tie at the TOP rank between DIFFERENT teams for the same
// project resolves to nothing -- CHAOS-4321's "never guess" precedent.
// Duplicate (provider, project_id, team_id) entries (repeated-import
// generations re-asserting the SAME team's claim) never manufacture a false
// tie: the max-score selection below only compares DISTINCT team_ids at the
// current best rank, so re-seeing the same team a second time is a no-op,
// not a second competing claim.
//
// Keyed by (provider, project_id), never bare project_id -- see
// TeamRepoOwnershipProjectLink's doc comment (codex adversarial review,
// 2026-08-28, confirmed finding).
func resolveProjectToTeam(links []TeamRepoOwnershipProjectLink) map[teamRepoOwnershipProjectRef]string {
	score := func(link TeamRepoOwnershipProjectLink) int {
		value := int(link.Specificity)
		if link.IsPrimary {
			value += 1 << 20
		}
		return value
	}
	bestScore := map[teamRepoOwnershipProjectRef]int{}
	bestTeams := map[teamRepoOwnershipProjectRef]map[string]bool{}
	for _, link := range links {
		if link.ProjectID == "" || link.TeamID == "" {
			continue
		}
		ref := teamRepoOwnershipProjectRef{Provider: link.Provider, ProjectID: link.ProjectID}
		linkScore := score(link)
		switch existing, ok := bestScore[ref]; {
		case !ok || linkScore > existing:
			bestScore[ref] = linkScore
			bestTeams[ref] = map[string]bool{link.TeamID: true}
		case linkScore == existing:
			bestTeams[ref][link.TeamID] = true
		}
	}
	resolved := make(map[teamRepoOwnershipProjectRef]string, len(bestTeams))
	for ref, teams := range bestTeams {
		if len(teams) != 1 {
			continue
		}
		for teamID := range teams {
			resolved[ref] = teamID
		}
	}
	return resolved
}

// buildDonorTeamIDResolver returns a function mapping a work_item_id to the
// team_id it should be attributed to: its own (but ONLY if that resolves via
// resolveWorkItemTeamID), or (if it has none, or its own never resolves) the
// single deterministically-chosen dependency donor's team_id. Mirrors
// compute_work_items.py::build_linked_issue_team_resolver's edge handling
// exactly (see the package doc comment above for the gating rules).
//
// CHAOS-4537 renamed this from buildDonorProjectIDResolver and changed its
// return type from a team_project_ownership lookup key to the resolved
// team_id itself, since resolveWorkItemTeamID's linear_team_key arm no
// longer produces a project_id-shaped key to look up in the first place --
// see resolveWorkItemTeamID's doc comment. Every caller that used to
// re-derive a team_id via projectToTeam[ref] now gets it directly.
//
// The resolution gate on the own-project_id branch matters concretely:
// GitHub work items (github_work_items_rows.go) unconditionally set their
// own ProjectID to the repo's full name, which never appears in
// team_project_ownership (GitHub never writes that table) -- without this
// gate, a real GitHub PR/issue's non-resolving "own" project_id would
// permanently shadow a valid dependency-donor edge to a Linear/Jira issue
// that DOES resolve, silently defeating the donor-walk fallback for the
// primary real-world use case (codex adversarial review, 2026-08-28,
// confirmed high-severity finding).
func buildDonorTeamIDResolver(
	byID map[string]TeamRepoOwnershipWorkItem,
	edges []TeamRepoOwnershipDependencyEdge,
	projectToTeam map[teamRepoOwnershipProjectRef]string,
) func(workItemID string) (string, string) {
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

	type resolvedTeam struct {
		teamID string
		arm    string
	}

	// Only a donor that ITSELF already resolves to a team is a valid
	// candidate -- mirrors compute_work_items.py::build_linked_issue_team_resolver
	// exactly (its `donor_team` map is populated ONLY for items with a
	// resolved team_id; `candidates` only ever appends a target present in
	// `donor_team`). Without this gate, an unowned donor with a
	// lexicographically smaller work_item_id could win the tie-break over a
	// second donor that DOES resolve, silently suppressing a valid team
	// (codex adversarial review, 2026-08-28, confirmed finding). Uses the
	// SAME resolveWorkItemTeamID as the own-item path (CHAOS-4458 part (b) /
	// CHAOS-4537) so a donor whose own project_id never resolves (a Linear
	// issue donating through its native_team_key) is still a valid candidate.
	candidateTargets := map[string][]string{}
	targetTeam := map[string]resolvedTeam{}
	for _, edge := range latestEdge {
		if !teamRepoOwnershipInheritableRelationshipTypes[edge.RelationshipType] {
			continue
		}
		target := canonicalDependencyTarget(edge.TargetWorkItemID, keyIndex)
		if target == "" {
			continue
		}
		donor, ok := byID[target]
		if !ok {
			continue
		}
		teamID, arm, resolves := resolveWorkItemTeamID(donor, projectToTeam)
		if !resolves {
			continue
		}
		targetTeam[target] = resolvedTeam{teamID: teamID, arm: arm}
		candidateTargets[edge.SourceWorkItemID] = append(candidateTargets[edge.SourceWorkItemID], target)
	}

	// Deterministic tie-break: the lexicographically smallest canonical
	// target wins when a source has multiple valid donor candidates.
	donorTeamBySource := make(map[string]resolvedTeam, len(candidateTargets))
	for source, targets := range candidateTargets {
		sort.Strings(targets)
		donorTeamBySource[source] = targetTeam[targets[0]]
	}

	return func(workItemID string) (string, string) {
		if item, ok := byID[workItemID]; ok {
			if teamID, arm, resolves := resolveWorkItemTeamID(item, projectToTeam); resolves {
				return teamID, arm
			}
		}
		if entry, ok := donorTeamBySource[workItemID]; ok {
			return entry.teamID, entry.arm
		}
		return "", ""
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
