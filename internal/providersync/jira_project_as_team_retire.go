package providersync

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The Jira team catalog used to make one team out of every Jira project (a
// "project-as-team" row: teams.id = the project key) and to point the project
// at that team with an ownership row and the project lead with a membership
// row. Team and Project are separate nouns: the Atlassian Teams are the Jira
// teams, and a project that no Atlassian team is connected to has no team. No
// writer builds these rows any more; this file retires the ones a store still
// holds. Nothing is deleted: a team row is written again inactive, an open
// ownership or membership row is written again with valid_to set, under the
// row's own sort key.

// jiraAtlassianTeamARIPrefix starts the native_team_key of every Atlassian
// team row (internal/atlassianteams writes the team ARI there).
const jiraAtlassianTeamARIPrefix = "ari:cloud:identity::team/"

// jiraProjectAsTeamRowPredicate is the shape of a project-as-team `teams` row,
// whatever its is_active: provider 'jira' and a native_team_key equal to the
// row's own id (normalizeJiraTeamRow stamped the project key into both). An
// Atlassian team row holds the team ARI there, which is never its id; the
// prefix clause says so a second time, so that no row with a team ARI can be
// retired even when its id were the same text.
const jiraProjectAsTeamRowPredicate = `provider = 'jira' AND id != '' AND ifNull(native_team_key, '') = id ` +
	`AND NOT startsWith(ifNull(native_team_key, ''), '` + jiraAtlassianTeamARIPrefix + `')`

const jiraProjectAsTeamIDsSubquery = `SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND ` + jiraProjectAsTeamRowPredicate

const jiraAtlassianTeamIDsSubquery = `SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND provider = 'jira' ` +
	`AND startsWith(ifNull(native_team_key, ''), '` + jiraAtlassianTeamARIPrefix + `')`

// jiraProjectAsTeamOwnershipShape is the shape of a project-as-team ownership
// row: provider 'jira', source 'native', and a team_id equal to the row's own
// project_key (the team WAS the project). An Atlassian team's row has the team
// id there and the project key in project_key; a team that the catalog knows
// as an Atlassian team is left out by id as well. A 'jira_legacy' row is
// another class and stays. It has no valid_to clause: the same shape selects
// the rows to close (with valid_to IS NULL added) and the team ids whose
// memberships and derived repository rows go with them, so a re-run after a
// part way failure still finds those.
const jiraProjectAsTeamOwnershipShape = `org_id = {org_id:String} AND provider = 'jira' AND source = 'native' ` +
	`AND team_id != '' AND team_id = ifNull(project_key, '') ` +
	`AND team_id NOT IN (` + jiraAtlassianTeamIDsSubquery + `)`

const jiraProjectAsTeamOwnershipPredicate = jiraProjectAsTeamOwnershipShape + ` AND valid_to IS NULL`

// jiraProjectAsTeamShapeIDsSubquery is the team ids the retired ownership rows
// name, open or closed. The team row of such an id may have been written again
// since (an admin edit leaves the provider empty; a Linear team of the same key
// replaces the row, teams holds one row per id), so the row alone cannot say
// whether the id was a project-as-team.
const jiraProjectAsTeamShapeIDsSubquery = `SELECT team_id FROM team_project_ownership FINAL WHERE ` + jiraProjectAsTeamOwnershipShape

// jiraProjectAsTeamIDsPredicate holds a team id that is a project-as-team: the
// current team row has the shape, or an ownership row of the shape names it.
const jiraProjectAsTeamIDsPredicate = `(team_id IN (` + jiraProjectAsTeamIDsSubquery + `) ` +
	`OR team_id IN (` + jiraProjectAsTeamShapeIDsSubquery + `))`

// jiraProjectAsTeamMembershipPredicate is the open membership row of a
// project-as-team (the project lead): provider 'jira', source 'native', and a
// team id that is a project-as-team. The provider clause keeps a membership of
// another provider's team with the same id.
const jiraProjectAsTeamMembershipPredicate = `org_id = {org_id:String} AND provider = 'jira' AND source = 'native' ` +
	`AND valid_to IS NULL AND ` + jiraProjectAsTeamIDsPredicate

// jiraProjectAsTeamOtherOpenLinkIDsSubquery is the team ids that keep an open
// project link after the ownership close: any open row of the organization, of
// any provider and any source, that is not a row of the retired shape.
const jiraProjectAsTeamOtherOpenLinkIDsSubquery = `SELECT team_id FROM team_project_ownership FINAL WHERE ` +
	`org_id = {org_id:String} AND valid_to IS NULL AND NOT (` + jiraProjectAsTeamOwnershipShape + `)`

// jiraProjectAsTeamRepoOwnershipPredicate is the open DERIVED repository row
// of a project-as-team (source 'inferred': TeamRepoOwnershipDerivationService
// derived it from the team's project links; its provider is the repository's,
// so it cannot name the team's). Three conditions keep a row out:
//   - the id's current team row is another provider's team (not the admin's,
//     whose provider is empty): the id names that team and its rows stay;
//   - the team keeps another open project link (a legacy link, an admin link):
//     the derivation reads every link and would open the row again at its next
//     run, so the retire leaves the rows to it, and it retracts the rows that
//     only the retired link supported;
//   - the row is of another source (a person's or a provider's statement): it
//     is not derived from the retired rows.
const jiraProjectAsTeamRepoOwnershipPredicate = `org_id = {org_id:String} AND source = 'inferred' ` +
	`AND valid_to IS NULL AND ` + jiraProjectAsTeamIDsPredicate + ` ` +
	`AND team_id NOT IN (SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND provider NOT IN ('', 'jira')) ` +
	`AND team_id NOT IN (` + jiraProjectAsTeamOtherOpenLinkIDsSubquery + `)`

const jiraProjectAsTeamActivePredicate = `org_id = {org_id:String} AND is_active = 1 AND ` + jiraProjectAsTeamRowPredicate

const jiraProjectAsTeamCountQuery = `SELECT ` +
	`(SELECT count() FROM teams FINAL WHERE ` + jiraProjectAsTeamActivePredicate + `), ` +
	`(SELECT count() FROM teams FINAL WHERE ` + jiraProjectAsTeamActivePredicate + ` AND notEmpty(manual_members)), ` +
	`(SELECT count() FROM teams FINAL WHERE ` + jiraProjectAsTeamActivePredicate +
	` AND id IN (SELECT team_id FROM team_sync_policies FINAL WHERE org_id = {org_id:String})), ` +
	`(SELECT count() FROM team_project_ownership FINAL WHERE ` + jiraProjectAsTeamOwnershipPredicate + `), ` +
	`(SELECT count() FROM team_memberships FINAL WHERE ` + jiraProjectAsTeamMembershipPredicate + `), ` +
	`(SELECT count() FROM team_repo_ownership FINAL WHERE ` + jiraProjectAsTeamRepoOwnershipPredicate + `)`

// A retraction keeps every value of the row it closes and its sort key; only
// valid_to and updated_at change. valid_to is never before valid_from, and
// updated_at is past the stored version so the closed row wins the merge.
const jiraProjectAsTeamCloseMemberships = `INSERT INTO team_memberships ` +
	`(org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) ` +
	`SELECT org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, ` +
	`greatest({at:DateTime64(3, 'UTC')}, valid_from), greatest({at:DateTime64(3, 'UTC')}, updated_at + toIntervalMillisecond(1)) ` +
	`FROM team_memberships FINAL WHERE ` + jiraProjectAsTeamMembershipPredicate

const jiraProjectAsTeamCloseOwnership = `INSERT INTO team_project_ownership ` +
	`(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) ` +
	`SELECT org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, ` +
	`greatest({at:DateTime64(3, 'UTC')}, valid_from), greatest({at:DateTime64(3, 'UTC')}, updated_at + toIntervalMillisecond(1)) ` +
	`FROM team_project_ownership FINAL WHERE ` + jiraProjectAsTeamOwnershipPredicate

const jiraProjectAsTeamCloseRepoOwnership = teamRepoOwnershipInsert + ` ` +
	`SELECT org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, ` +
	`greatest({at:DateTime64(3, 'UTC')}, valid_from), greatest({at:DateTime64(3, 'UTC')}, updated_at + toIntervalMillisecond(1)) ` +
	`FROM team_repo_ownership FINAL WHERE ` + jiraProjectAsTeamRepoOwnershipPredicate

const jiraProjectAsTeamDeactivateTeams = `INSERT INTO teams ` +
	`(id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id, source_id, created_at) ` +
	`SELECT id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, 0, ` +
	`greatest({at:DateTime64(3, 'UTC')}, updated_at + toIntervalMillisecond(1)), org_id, provider, native_team_key, parent_team_id, source_id, created_at ` +
	`FROM teams FINAL WHERE ` + jiraProjectAsTeamActivePredicate

// JiraProjectAsTeamRetireOutcome is counts only: no id, key or name of a team,
// a project or an organization leaves the store through it.
type JiraProjectAsTeamRetireOutcome struct {
	DryRun bool `json:"dry_run"`
	// Teams is the active project-as-team rows found.
	Teams uint64 `json:"teams"`
	// TeamsWithManualMembers and TeamsWithSyncPolicy count the found rows an
	// admin has put members on or a sync policy on. They are retired too.
	TeamsWithManualMembers uint64 `json:"teams_with_manual_members"`
	TeamsWithSyncPolicy    uint64 `json:"teams_with_sync_policy"`
	// OwnershipRows and MembershipRows are the open rows found.
	OwnershipRows  uint64 `json:"ownership_rows"`
	MembershipRows uint64 `json:"membership_rows"`
	// RepoOwnershipRows is the open derived repository rows found.
	RepoOwnershipRows uint64 `json:"repo_ownership_rows"`
	// The four below are what a real run wrote; 0 in a dry run.
	TeamsRetired        uint64 `json:"teams_retired"`
	OwnershipClosed     uint64 `json:"ownership_closed"`
	MembershipClosed    uint64 `json:"memberships_closed"`
	RepoOwnershipClosed uint64 `json:"repo_ownership_closed"`
}

// Found says the store holds something to retire.
func (outcome JiraProjectAsTeamRetireOutcome) Found() bool {
	return outcome.Teams > 0 || outcome.OwnershipRows > 0 || outcome.MembershipRows > 0 || outcome.RepoOwnershipRows > 0
}

// Retired is the number of rows a real run wrote.
func (outcome JiraProjectAsTeamRetireOutcome) Retired() uint64 {
	return outcome.TeamsRetired + outcome.OwnershipClosed + outcome.MembershipClosed + outcome.RepoOwnershipClosed
}

// RetireJiraProjectAsTeamRows retires every project-as-team row of one
// organization: the team rows go inactive, their open ownership and
// membership rows and the repository rows derived from that ownership are
// closed at `at`. It reads no provider answer and depends
// on none: the class is retired as a whole, so there is no snapshot that could
// be incomplete. A dry run counts and writes nothing. With nothing to retire
// it is one count read and no write, so a second run reports zero.
//
// The team rows are written last: after a failure part way the team is still
// found active, so a re-run closes the rows that are left.
func RetireJiraProjectAsTeamRows(
	ctx context.Context, conn driver.Conn, orgID string, at time.Time, dryRun bool,
) (JiraProjectAsTeamRetireOutcome, error) {
	orgID = strings.TrimSpace(orgID)
	if ctx == nil || conn == nil || orgID == "" || at.IsZero() {
		return JiraProjectAsTeamRetireOutcome{}, ErrInvalidConfiguration
	}
	org := clickhouse.Named("org_id", orgID)
	outcome := JiraProjectAsTeamRetireOutcome{DryRun: dryRun}
	if err := conn.QueryRow(ctx, jiraProjectAsTeamCountQuery, org).Scan(
		&outcome.Teams, &outcome.TeamsWithManualMembers, &outcome.TeamsWithSyncPolicy,
		&outcome.OwnershipRows, &outcome.MembershipRows, &outcome.RepoOwnershipRows,
	); err != nil {
		return JiraProjectAsTeamRetireOutcome{}, err
	}
	if dryRun || !outcome.Found() {
		return outcome, nil
	}
	stamp := clickhouse.Named("at", at.UTC().Truncate(time.Millisecond).Format("2006-01-02 15:04:05.000"))
	if outcome.MembershipRows > 0 {
		if err := conn.Exec(ctx, jiraProjectAsTeamCloseMemberships, org, stamp); err != nil {
			return outcome, err
		}
		outcome.MembershipClosed = outcome.MembershipRows
	}
	if outcome.RepoOwnershipRows > 0 {
		if err := conn.Exec(ctx, jiraProjectAsTeamCloseRepoOwnership, org, stamp); err != nil {
			return outcome, err
		}
		outcome.RepoOwnershipClosed = outcome.RepoOwnershipRows
	}
	if outcome.OwnershipRows > 0 {
		if err := conn.Exec(ctx, jiraProjectAsTeamCloseOwnership, org, stamp); err != nil {
			return outcome, err
		}
		outcome.OwnershipClosed = outcome.OwnershipRows
	}
	if outcome.Teams > 0 {
		if err := conn.Exec(ctx, jiraProjectAsTeamDeactivateTeams, org, stamp); err != nil {
			return outcome, err
		}
		outcome.TeamsRetired = outcome.Teams
	}
	return outcome, nil
}
