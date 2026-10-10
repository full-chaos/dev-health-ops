package providersync

import "time"

// The fact kinds of every writer that closes rows from a provider snapshot.
// Every kind is made here and nowhere else (TestSnapshotKindCensus): a new
// kind, or a change of what an empty answer of a kind means, is an edit of
// this file, of the census table and of the table in
// docs/contribute/architecture/team-attribution.md (section 0.4b), which
// TestSnapshotKindPolicyTableIsTheDocumentedOne compares with this one.
//
// The policy table.
//
// Scope: every kind closes only behind the one scope gate (ProveSoleScope:
// the run's integration is the only ACTIVE integration of its provider in the
// organization). No row of any kind carries an integration key, and an
// organization can hold two integrations of one provider, so a walk proves
// its own scope only: no kind is exempt.
//
// Empty answer: "closes nothing": the kind has ONE walk for the whole provider
// answer, and an answer with no row of the kind is more often an access
// change than a real empty state. "is an answer": the kind is read per team,
// each read proves its own end, and a team with no row is a real answer.
//
//	kind                          scope             empty answer    absence
//	linear_project_ownership      sole integration  closes nothing  cursor walk
//	linear_team_key_ownership     sole integration  closes nothing  cursor walk
//	jira_legacy_ownership         sole integration  closes nothing  one response or direct answer
//	atlassian_team_catalog        sole integration  closes nothing  cursor walk
//	atlassian_team_memberships    sole integration  is an answer    cursor walk
//	atlassian_team_project_links  sole integration  is an answer    cursor walk
//	gitlab_group_project_grants   sole integration  is an answer    one response or direct answer
//	github_team_repo_grants       sole integration  is an answer    one response or direct answer
//
// Absence: what proves that an open fact the run does not hold is gone
// (AbsenceProof, an argument of every kind snapshot). "one response or direct
// answer": the kind's listings are read by position (a page number, an
// offset); the walks prove what they do not hold only when EVERY walk that
// feeds the kind's held set was ONE response, and otherwise the provider's own
// answer for that one fact does, asked for every state the held set admits,
// inside a budget of AbsenceLookupBudget per run (AbsenceByListing;
// TestHeldSetWalkCensus names the walks of each kind). "cursor
// walk": the walk follows the provider's cursor to its proven end, and its
// answer is taken as the proof (AbsenceByWalk); the providers state no
// contract for a list that changes during such a walk, so it is a named risk.

// MembershipSnapshotRow is one team_memberships fact as the snapshot rule
// reads it: the team, the member and the stored valid_from.
type MembershipSnapshotRow struct {
	TeamID, MemberID string
	ValidFrom        time.Time
}

// MembershipSnapshotKey names a membership fact.
func MembershipSnapshotKey(row MembershipSnapshotRow) string {
	return row.TeamID + "\x00" + row.MemberID
}

// TeamSnapshotRow is one catalog team as the snapshot rule reads it.
type TeamSnapshotRow struct{ TeamID string }

// TeamSnapshotKey names a catalog team.
func TeamSnapshotKey(row TeamSnapshotRow) string { return row.TeamID }

// LinearProjectOwnershipKind is the ownership of real Linear projects: source
// native, an id that is not the team-key form. Its walk is the projects walk.
// An answer with no project ownership row closes nothing.
func LinearProjectOwnershipKind(orgID string) SnapshotKind[OwnershipSnapshotRow] {
	return NewSnapshotKind("linear_project_ownership", EmptyClosesNothing, func(row OwnershipSnapshotRow) bool {
		return row.Source == "native" && !row.ProjectID.IsLinearTeamKeyForm(orgID)
	})
}

// LinearTeamKeyOwnershipKind is the {org}:linear:{team key} row of each team
// (LinearTeamKeyProjectID). Its walk is the teams walk. An answer with no team
// closes nothing.
func LinearTeamKeyOwnershipKind(orgID string) SnapshotKind[OwnershipSnapshotRow] {
	return NewSnapshotKind("linear_team_key_ownership", EmptyClosesNothing, func(row OwnershipSnapshotRow) bool {
		return row.Source == "native" && row.ProjectID.IsLinearTeamKeyForm(orgID)
	})
}

// JiraLegacyOwnershipKind is the Jira catalog's own rows: source jira_legacy.
// Its walk is the project search and the legacy links read. An answer with no
// live ownership row closes nothing.
func JiraLegacyOwnershipKind() SnapshotKind[OwnershipSnapshotRow] {
	return NewSnapshotKind("jira_legacy_ownership", EmptyClosesNothing, func(row OwnershipSnapshotRow) bool {
		return row.Source == jiraTeamCatalogLegacySource
	})
}

// teamIn says whether a row's team is one of teams.
func teamIn(teams []string) func(teamID string) bool {
	allowed := make(map[string]bool, len(teams))
	for _, teamID := range teams {
		allowed[teamID] = true
	}
	return func(teamID string) bool { return allowed[teamID] }
}

// GitLabGroupProjectGrantKind is the GitLab provider_access grants of the
// teams whose own listing proved its end in a scope no other integration
// lists (decideOwnershipClose). Each team's listing is its own walk, so a team
// with no grant is an answer: its rows close. A team outside closable is of no
// kind, and its rows never close.
func GitLabGroupProjectGrantKind(closable []string) SnapshotKind[OwnershipSnapshotRow] {
	closes := teamIn(closable)
	return NewSnapshotKind("gitlab_group_project_grants", EmptyIsAnAnswer, func(row OwnershipSnapshotRow) bool {
		return row.Source == gitlabTeamCatalogSource && closes(row.TeamID)
	})
}

// GitHubTeamRepoGrantKind is the GitHub provider_access team_repo_ownership
// grants (a repository full name stands in for the project) of the teams
// decideOwnershipClose lets close, on the same terms as the GitLab kind.
func GitHubTeamRepoGrantKind(closable []string) SnapshotKind[OwnershipSnapshotRow] {
	closes := teamIn(closable)
	return NewSnapshotKind("github_team_repo_grants", EmptyIsAnAnswer, func(row OwnershipSnapshotRow) bool {
		return row.Source == githubTeamCatalogSource && closes(row.TeamID)
	})
}

// AtlassianTeamLinkKind is the Atlassian Teams project links: source native,
// of a team whose every Jira project link got a row (a team in unreadable has
// a link the provider still returns and this run could not write, so its rows
// are of no kind and never close). Each team's link read is its own walk, so
// a team with no link is an answer.
func AtlassianTeamLinkKind(source string, unreadable []string) SnapshotKind[OwnershipSnapshotRow] {
	skip := teamIn(unreadable)
	return NewSnapshotKind("atlassian_team_project_links", EmptyIsAnAnswer, func(row OwnershipSnapshotRow) bool {
		return row.Source == source && !skip(row.TeamID)
	})
}

// AtlassianTeamMembershipKind is the Atlassian Teams memberships. Each team's
// member read is its own walk, so a team with no member is an answer.
func AtlassianTeamMembershipKind() SnapshotKind[MembershipSnapshotRow] {
	return NewSnapshotKind("atlassian_team_memberships", EmptyIsAnAnswer, func(MembershipSnapshotRow) bool { return true })
}

// AtlassianTeamCatalogKind is the Atlassian teams of the catalog. Its walk is
// the team search. A search that answers no team closes nothing: no team is
// deactivated, and no team outside the answer is in scope for the membership
// and link closes.
func AtlassianTeamCatalogKind() SnapshotKind[TeamSnapshotRow] {
	return NewSnapshotKind("atlassian_team_catalog", EmptyClosesNothing, func(TeamSnapshotRow) bool { return true })
}
