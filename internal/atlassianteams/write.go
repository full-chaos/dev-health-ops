package atlassianteams

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/teamcreated"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
)

// The column lists are the ones the project-as-team Jira catalog writes
// (providersync/jira_team_catalog_effects_clickhouse.go), so both writers fill
// the same physical tables the same way.
const (
	teamsInsert       = `INSERT INTO teams (id, team_uuid, name, description, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id, created_at, provider_team_id, provider_name, gone_at)`
	membershipsInsert = `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, provider_user_id)`
	// ownershipInsert names its columns and omits last_synced on purpose: the server stamps it at insert time.
	// last_synced = server insert time, not commit order across concurrent inserts. A reader must re-read a
	// window of at least 300 seconds behind its cursor and dedup by key with FINAL (migration 099).
	ownershipInsert = `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`

	existingProjectKeysQuery = "SELECT id, project_keys FROM teams FINAL WHERE org_id = {org_id:String} AND provider = {provider:String} AND id IN {team_ids:Array(String)}"
)

// Result counts what a write did: what it actually persisted, after the
// team_sync_policies / manual-membership-conflict guards (sync_guards.go)
// may have left some of the incoming rows untouched, and what it retracted
// (rows of members or project links that an Atlassian team had before and
// this snapshot no longer has).
type Result struct {
	// TeamsWritten/TeamKeys/MembershipsWritten/MembersWritten/
	// OwnershipWritten count only rows this call actually persisted -- never
	// the caller's pre-guard collected snapshot, which can be larger once a
	// sync_policy or membership-conflict guard skips a row. TeamKeys holds
	// each written team's native_team_key (the full Atlassian ARI), the same
	// column a readback verifier checks -- never the bare TeamRow.ID.
	TeamsWritten       int
	TeamKeys           []string
	MembershipsWritten int
	MembersWritten     int
	// OwnershipWritten is the project links this run found and holds open; OwnershipUnchanged of them repeat
	// an open row and were not written again.
	OwnershipWritten   int
	OwnershipUnchanged int
	ExpiredMemberships int
	// NewerMembershipFactsSkipped counts the facts (team and member) left open whole because an open row
	// of theirs was written after this run read the provider (CHAOS-9204).
	NewerMembershipFactsSkipped int
	ExpiredOwnership            int
	// UnreadableProjectLinkTeams counts the teams of which this call closed
	// no project link, because the provider returned a Jira project link of
	// the team that got no row (Rows.UnreadableProjectLinkTeams).
	UnreadableProjectLinkTeams int
	// ProjectLinks is the collection's own link counts (Rows.ProjectLinks):
	// links seen, skipped by reason, and team reads that failed. With
	// OwnershipWritten it says what became of every link the provider
	// returned. Set whenever the project links were selected, also when no
	// row was written.
	ProjectLinks ProjectLinkCounts
	// ProjectLinksIncomplete says the project links were selected and the
	// collection was not a complete snapshot (Rows.ProjectLinksComplete is
	// false): this call closed no project link.
	ProjectLinksIncomplete bool
	// DeactivatedTeams counts catalog rows of Atlassian teams the snapshot no
	// longer returns (deleted upstream), rewritten inactive.
	DeactivatedTeams int
	// CloseAbandoned holds, sorted and without repeats, the reason of every
	// fact kind (teams, memberships, project links) whose close this call gave
	// up while open rows of the kind were kept: the scope gate's reason
	// (scope_shared, ...), a read that did not reach its end, or an empty
	// team search. Empty when every selected kind could close.
	CloseAbandoned []string
}

const (
	atlassianTeamARIPrefix = "ari:cloud:identity::team/"

	activeMissingTeamsQuery = "SELECT id, team_uuid, name, description, manual_members, project_keys, repo_patterns, native_team_key, parent_team_id " +
		"FROM teams FINAL WHERE org_id = {org_id:String} AND provider = {provider:String} AND is_active = 1 AND id IN {team_ids:Array(String)}"

	knownTeamsQuery      = "SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND provider = {provider:String} AND startsWith(ifNull(native_team_key, ''), {ari_prefix:String})"
	openMembershipsQuery = "SELECT team_id, member_id, raw_provider_user_id, raw_email, identity_facets, toString(source), is_primary, specificity, priority, valid_from, provider_user_id, updated_at " +
		"FROM team_memberships FINAL WHERE org_id = {org_id:String} AND provider = {provider:String} AND source = 'native' AND team_id IN {team_ids:Array(String)} AND valid_to IS NULL"
	openOwnershipQuery = "SELECT team_id, project_id, project_key, toString(source), is_primary, specificity, priority, valid_from " +
		"FROM team_project_ownership FINAL WHERE org_id = {org_id:String} AND provider = {provider:String} AND source = 'native' AND team_id IN {team_ids:Array(String)} AND valid_to IS NULL"
)

// Write stores the rows of one collection and retracts what the snapshot no
// longer has.
//
// The team catalog row is written LAST: ClickHouse has no transaction across
// tables, so a failure part way leaves memberships and project links without
// their catalog row (a re-run repairs it) rather than a listed team without
// its members. The error names the stages already committed.
//
// Retraction: the members and project links an Atlassian team held before and
// the snapshot omits (a person who left, a project it stopped working on, an
// archived or deleted team) are closed with a replacement row (valid_to = now,
// the same sort key), because the attribution loaders read valid_to. Every
// close goes through the one snapshot rule (providersync.PlanSnapshot), one
// fact kind at a time, each on the proof of its own reads: the teams of the
// catalog (Rows.TeamSearchComplete; a search that answers no team deactivates
// none and puts no other team in scope), the memberships
// (Rows.MembershipsComplete) and the project links (Rows.ProjectLinksComplete).
// A kind that closes nothing while it holds open rows is logged and counted
// (providersync.ReportSnapshotPlan). Only rows
// of Atlassian teams (their catalog row carries the team ARI) with source
// native are touched; the project-as-team rows never are. Members and links
// that stay keep their original valid_from, so a re-run replaces a row instead
// of adding one. Teams are written only when the structure was selected; an
// existing team's manual members are carried over, and its project keys when
// the project links were not read this run, or not read for that team.
//
// scope is the scope gate's answer for the run (providersync.ProveSoleScope
// for provider jira): the reads below are every Atlassian team row of the
// organization, whatever site wrote them, so a run closes and deactivates
// only when the organization has no other active Jira integration. A scope
// that is not proven writes what the run found and closes nothing.
//
// prover is the gateway's own answer for one fact (absence.go): an open fact the
// run does not hold is a candidate, and it closes only when the prover says it is
// gone, inside the per-run budget of each kind. A nil prover answers nothing, so
// nothing closes (CHAOS-9124: the end of a cursor walk is no proof).
func Write(
	ctx context.Context, conn driver.Conn, orgID string, rows Rows, selections Selections, scope providersync.ScopeProof,
	prover AbsenceProver,
) (Result, error) {
	var result Result
	if conn == nil || orgID == "" {
		return result, ErrConfiguration
	}
	if err := checkTeamIDs(rows); err != nil {
		return result, err
	}
	teams, missing, catalog, err := teamsInScope(ctx, conn, orgID, rows, scope, prover)
	if err != nil {
		return result, fmt.Errorf("read known atlassian teams: %w", err)
	}
	abandoned := map[string]bool{}
	report := func(plan providersync.SnapshotPlan) {
		providersync.ReportSnapshotPlan(ctx, Provider, orgID, plan)
		for _, reason := range plan.SnapshotReasons() {
			abandoned[reason] = true
		}
	}
	report(catalog)
	var deactivate []inactiveTeam
	if selections.Structure {
		if deactivate, err = planDeactivations(ctx, conn, orgID, missing); err != nil {
			return result, fmt.Errorf("read the teams the snapshot no longer has: %w", err)
		}
	}
	now := time.Time{}
	for _, team := range rows.Teams {
		now = team.UpdatedAt
		break
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	// The two guards below scope EXACTLY like every other native collector's
	// own wrapper (jira_team_catalog_guards.go): the sync-policy guard only
	// ever filters the `teams` write (it runs against the FULL, unfiltered
	// rows.Teams computed above, so a guard-skipped team is never mistaken
	// by teamsInScope/planDeactivations for one the snapshot stopped
	// reporting); the membership-conflict guard runs before planMemberships
	// so a dropped conflicting row is retracted the same way any other
	// disappeared membership is.
	teamsToWrite := rows.Teams
	if selections.Structure && len(rows.Teams) > 0 {
		if teamsToWrite, err = filterTeamsBySyncPolicy(ctx, conn, orgID, rows.Teams, now); err != nil {
			return result, fmt.Errorf("apply team sync policy guard: %w", err)
		}
	}
	freshMemberships := rows.Memberships
	if selections.Members {
		observedTeamIDs := make([]string, 0, len(rows.Teams))
		for _, team := range rows.Teams {
			if team.IsActive != 0 {
				observedTeamIDs = append(observedTeamIDs, team.ID)
			}
		}
		if freshMemberships, err = filterMembershipsByManualConflict(ctx, conn, orgID, rows.Memberships, observedTeamIDs, now); err != nil {
			return result, fmt.Errorf("apply team membership conflict guard: %w", err)
		}
	}

	var memberships []MembershipRow
	var expiredMemberships []openMembership
	if selections.Members {
		var plan providersync.SnapshotPlan
		var newerSkipped int
		if memberships, expiredMemberships, plan, newerSkipped, err = planMemberships(ctx, conn, orgID, teams, freshMemberships, now,
			providersync.AtlassianTeamMembershipKind().Snapshot(scope, providersync.ProveSnapshot(
				providersync.SnapshotTerm{Holds: rows.MembershipsComplete, Reason: snapshotMemberReadsNotEnded}),
				providersync.AbsenceByListing[providersync.MembershipSnapshotRow](nil, membershipLookups(ctx, prover).Answer))); err != nil {
			return result, fmt.Errorf("read current team memberships: %w", err)
		}
		report(plan)
		result.NewerMembershipFactsSkipped = newerSkipped
		if newerSkipped > 0 {
			// Counts only: a fact is a team and a member, which no log line names (CHAOS-9149).
			slog.Default().WarnContext(ctx, "team_membership_close_skipped",
				"org_id", orgID, "provider", Provider, "row_newer_than_read", newerSkipped)
		}
	}
	var ownership []OwnershipRow
	var expiredOwnership []openOwnership
	ownershipUnchanged := 0
	if selections.Projects {
		var plan providersync.SnapshotPlan
		if ownership, expiredOwnership, plan, err = planOwnership(ctx, conn, orgID, teams, rows.Ownership, now,
			providersync.AtlassianTeamLinkKind(Source, rows.UnreadableProjectLinkTeams).Snapshot(scope, providersync.ProveSnapshot(
				providersync.SnapshotTerm{Holds: rows.ProjectLinksComplete, Reason: snapshotLinkReadsNotEnded}),
				providersync.AbsenceByListing[providersync.OwnershipSnapshotRow](nil, linkLookups(ctx, prover).Answer))); err != nil {
			return result, fmt.Errorf("read current team project ownership: %w", err)
		}
		report(plan)
		ownershipUnchanged = plan.UnchangedCount()
	}
	for reason := range abandoned {
		result.CloseAbandoned = append(result.CloseAbandoned, reason)
	}
	slices.Sort(result.CloseAbandoned)
	if selections.Projects {
		result.ProjectLinks = rows.ProjectLinks
		result.ProjectLinksIncomplete = !rows.ProjectLinksComplete
		result.UnreadableProjectLinkTeams = len(rows.UnreadableProjectLinkTeams)
	}
	var done []string
	fail := func(stage string, err error) (Result, error) {
		if len(done) > 0 {
			return result, fmt.Errorf("%s: %w (already written: %s; a re-run completes the rest)", stage, err, strings.Join(done, ", "))
		}
		return result, fmt.Errorf("%s: %w", stage, err)
	}
	// The links are written before the teams: the teams this run writes are
	// imported teams for them.
	var teamsWritten []string
	if selections.Structure {
		for _, team := range teamsToWrite {
			teamsWritten = append(teamsWritten, team.ID)
		}
	}
	if selections.Members && (len(memberships) > 0 || len(expiredMemberships) > 0) {
		if err := writeMemberships(ctx, conn, orgID, memberships, expiredMemberships, now, teamsWritten); err != nil {
			return fail("write team memberships", err)
		}
		result.MembershipsWritten = len(memberships)
		result.MembersWritten = distinctAtlassianMembers(memberships)
		result.ExpiredMemberships = len(expiredMemberships)
		done = append(done, "team memberships")
	}
	if selections.Projects && (len(ownership) > 0 || len(expiredOwnership) > 0) {
		if err := writeOwnership(ctx, conn, orgID, ownership, expiredOwnership, now, teamsWritten); err != nil {
			return fail("write team project ownership", err)
		}
		result.ExpiredOwnership = len(expiredOwnership)
		done = append(done, "team project ownership")
	}
	if selections.Projects {
		// Set after the write: a failed write returns above with nothing counted. The rows the run found are
		// counted whether or not any was written; the unchanged ones were not written again.
		result.OwnershipWritten = len(rows.Ownership)
		result.OwnershipUnchanged = ownershipUnchanged
		if ownershipUnchanged > 0 {
			slog.Default().InfoContext(ctx, "atlassian_teams_ownership_unchanged",
				"org_id", orgID, "fresh", len(rows.Ownership), "unchanged", ownershipUnchanged, "written", len(ownership))
		}
	}
	if selections.Structure && (len(teamsToWrite) > 0 || len(deactivate) > 0) {
		// The catalog row's project keys follow the links: a run that closes
		// no link of a team (the snapshot is not complete, or a link of the
		// team got no row) keeps the keys the team had next to the ones it
		// read now.
		keepKeysOf := map[string]bool{}
		if selections.Projects && !rows.ProjectLinksComplete {
			for _, team := range teamsToWrite {
				keepKeysOf[team.ID] = true
			}
		}
		for _, id := range rows.UnreadableProjectLinkTeams {
			keepKeysOf[id] = true
		}
		if err := writeTeams(ctx, conn, orgID, teamsToWrite, deactivate, now, !selections.Projects, keepKeysOf); err != nil {
			return fail("write teams", err)
		}
		result.TeamsWritten = len(teamsToWrite)
		result.TeamKeys = make([]string, 0, len(teamsToWrite))
		for _, team := range teamsToWrite {
			result.TeamKeys = append(result.TeamKeys, team.NativeTeamKey)
		}
		result.DeactivatedTeams = len(deactivate)
	}
	return result, nil
}

// The reasons of the Atlassian Teams snapshot terms.
const (
	snapshotTeamSearchNotEnded  = "team_search_not_read_to_the_end"
	snapshotMemberReadsNotEnded = "member_reads_not_read_to_the_end"
	snapshotLinkReadsNotEnded   = "project_links_not_read_to_the_end"
)

// teamsInScope is every Atlassian team this run answers for: the ones the
// snapshot returned, and the ones already in the catalog that the snapshot
// rule says were deleted upstream (missing). A catalog team outside the answer
// is deleted upstream only when the team search reached its end and returned
// at least one team (providersync.AtlassianTeamCatalogKind): a search that
// answers no team is far more often an access change than an organization
// that deleted every team, so it puts no other team in scope, and nothing of
// those teams is closed or deactivated. The same holds when the scope gate
// did not prove the run the only Jira integration of the organization: a
// catalog team outside the answer may be a team of another site.
func teamsInScope(
	ctx context.Context, conn driver.Conn, orgID string, rows Rows, scope providersync.ScopeProof, prover AbsenceProver,
) (ids, missing []string, plan providersync.SnapshotPlan, err error) {
	seen := map[string]bool{}
	var fresh []providersync.TeamSnapshotRow
	for _, team := range rows.Teams {
		if !seen[team.ID] {
			seen[team.ID] = true
			ids = append(ids, team.ID)
			fresh = append(fresh, providersync.TeamSnapshotRow{TeamID: team.ID})
		}
	}
	result, err := conn.Query(ctx, knownTeamsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", Provider), clickhouse.Named("ari_prefix", atlassianTeamARIPrefix))
	if err != nil {
		return nil, nil, plan, err
	}
	defer result.Close()
	var known []providersync.TeamSnapshotRow
	knownSeen := map[string]bool{}
	for result.Next() {
		var id string
		if err := result.Scan(&id); err != nil {
			return nil, nil, plan, err
		}
		if !knownSeen[id] {
			knownSeen[id] = true
			known = append(known, providersync.TeamSnapshotRow{TeamID: id})
		}
	}
	if err := result.Err(); err != nil {
		return nil, nil, plan, err
	}
	plan = providersync.PlanSnapshot(fresh, known, providersync.TeamSnapshotKey,
		func(providersync.TeamSnapshotRow) time.Time { return time.Time{} }, time.Time{},
		providersync.AtlassianTeamCatalogKind().Snapshot(scope, providersync.ProveSnapshot(
			providersync.SnapshotTerm{Holds: rows.TeamSearchComplete, Reason: snapshotTeamSearchNotEnded}),
			providersync.AbsenceByListing[providersync.TeamSnapshotRow](nil, teamLookups(ctx, prover).Answer)))
	for _, retraction := range plan.Retract {
		id := known[retraction.Open].TeamID
		ids = append(ids, id)
		missing = append(missing, id)
	}
	return ids, missing, plan, nil
}

type inactiveTeam struct {
	id                                       string
	teamUUID                                 uuid.UUID
	name                                     string
	description                              *string
	manualMembers, projectKeys, repoPatterns []string
	nativeTeamKey, parentTeamID              *string
}

// planDeactivations reads the still-active catalog rows of the Atlassian teams
// the snapshot no longer returns, to rewrite them inactive.
func planDeactivations(ctx context.Context, conn driver.Conn, orgID string, missing []string) ([]inactiveTeam, error) {
	if len(missing) == 0 {
		return nil, nil
	}
	result, err := conn.Query(ctx, activeMissingTeamsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", Provider), clickhouse.Named("team_ids", missing))
	if err != nil {
		return nil, err
	}
	defer result.Close()
	var out []inactiveTeam
	for result.Next() {
		var row inactiveTeam
		if err := result.Scan(&row.id, &row.teamUUID, &row.name, &row.description, &row.manualMembers,
			&row.projectKeys, &row.repoPatterns, &row.nativeTeamKey, &row.parentTeamID); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, result.Err()
}

type openMembership struct {
	teamID, memberID  string
	providerUserID    *string
	rawProviderUserID *string
	rawEmail          *string
	identityFacets    []string
	source            string
	isPrimary         uint8
	specificity       uint16
	priority          int32
	validFrom         time.Time
	// updatedAt is when the row was written; a row newer than the run's read is never closed (CHAOS-9204).
	updatedAt time.Time
	// closedAt is the valid_to the snapshot rule closes this row with.
	closedAt time.Time
}

type openOwnership struct {
	teamID      string
	projectID   providersync.ProjectID
	projectKey  *string
	source      string
	isPrimary   uint8
	specificity uint16
	priority    int32
	validFrom   time.Time
	// closedAt is the valid_to the snapshot rule closes this row with.
	closedAt time.Time
}

// planMemberships reads the open memberships of the teams in scope and
// applies the shared snapshot rule (providersync.PlanSnapshot): a fresh row
// keeps the valid_from it was first seen with, and every open membership the
// snapshot no longer has is returned to be closed. A team in scope with no
// fresh member loses all of its members: scope holds the teams deleted
// upstream.
//
// snapshot is the membership kind with the proof of the member reads
// (Rows.MembershipsComplete): only a collection that read every active team's
// members to the end closes a membership.
func planMemberships(
	ctx context.Context, conn driver.Conn, orgID string, scope []string, fresh []MembershipRow, now time.Time,
	snapshot providersync.KindSnapshot[providersync.MembershipSnapshotRow],
) ([]MembershipRow, []openMembership, providersync.SnapshotPlan, int, error) {
	if len(scope) == 0 {
		return fresh, nil, providersync.SnapshotPlan{}, 0, nil
	}
	result, err := conn.Query(ctx, openMembershipsQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", Provider), clickhouse.Named("team_ids", scope))
	if err != nil {
		return nil, nil, providersync.SnapshotPlan{}, 0, err
	}
	defer result.Close()
	var open []openMembership
	for result.Next() {
		var row openMembership
		if err := result.Scan(&row.teamID, &row.memberID, &row.rawProviderUserID, &row.rawEmail, &row.identityFacets, &row.source,
			&row.isPrimary, &row.specificity, &row.priority, &row.validFrom, &row.providerUserID, &row.updatedAt); err != nil {
			return nil, nil, providersync.SnapshotPlan{}, 0, err
		}
		open = append(open, row)
	}
	if err := result.Err(); err != nil {
		return nil, nil, providersync.SnapshotPlan{}, 0, err
	}
	freshFacts := make([]providersync.MembershipSnapshotRow, len(fresh))
	for i, row := range fresh {
		freshFacts[i] = providersync.MembershipSnapshotRow{TeamID: row.TeamID, MemberID: row.MemberID, ValidFrom: row.ValidFrom}
	}
	openFacts := make([]providersync.MembershipSnapshotRow, len(open))
	for i, row := range open {
		openFacts[i] = providersync.MembershipSnapshotRow{TeamID: row.teamID, MemberID: row.memberID, ValidFrom: row.validFrom}
	}
	plan := providersync.PlanSnapshot(freshFacts, openFacts, providersync.MembershipSnapshotKey,
		func(row providersync.MembershipSnapshotRow) time.Time { return row.ValidFrom }, now, snapshot)
	out := make([]MembershipRow, len(fresh))
	for i, row := range fresh {
		row.ValidFrom = plan.ValidFrom[i]
		out[i] = row
	}
	// A fact (team and member) with ANY open row written after this run read the provider is left
	// open whole: a run that read the provider later wrote it, so this run's snapshot is older than
	// the row and cannot say it is a surplus or a departure (CHAOS-9204; the same rule as the GitHub,
	// GitLab and Linear writers, providersync.MembershipSnapshotWriter).
	newerFacts := map[string]bool{}
	for _, row := range open {
		if row.updatedAt.After(now) {
			newerFacts[providersync.MembershipSnapshotKey(providersync.MembershipSnapshotRow{TeamID: row.teamID, MemberID: row.memberID})] = true
		}
	}
	skippedFacts := map[string]bool{}
	var expired []openMembership
	for _, retraction := range plan.Retract {
		row := open[retraction.Open]
		factKey := providersync.MembershipSnapshotKey(providersync.MembershipSnapshotRow{TeamID: row.teamID, MemberID: row.memberID})
		if newerFacts[factKey] {
			if !skippedFacts[factKey] {
				skippedFacts[factKey] = true
			}
			continue
		}
		row.closedAt = retraction.ClosedAt
		expired = append(expired, row)
	}
	return out, expired, plan, len(skippedFacts), nil
}

// planOwnership reads the open project links of the teams in scope and
// applies the shared snapshot rule (providersync.PlanOwnershipSnapshot): a
// fresh link keeps the valid_from it was first seen with, and every open link
// the snapshot no longer has is returned to be closed. A team in scope with no
// fresh link loses all of its links: scope holds the teams deleted upstream.
//
// snapshot is the link kind with the proof of the link reads
// (Rows.ProjectLinksComplete): only a collection that read every team's
// project links to the end closes a link. The kind leaves out the teams of
// Rows.UnreadableProjectLinkTeams: no open link of such a team is closed. Its
// open links still go into the plan, because the team's fresh links take
// their first-seen valid_from from them.
func planOwnership(
	ctx context.Context, conn driver.Conn, orgID string, scope []string, fresh []OwnershipRow, now time.Time,
	snapshot providersync.KindSnapshot[providersync.OwnershipSnapshotRow],
) ([]OwnershipRow, []openOwnership, providersync.SnapshotPlan, error) {
	if len(scope) == 0 {
		return fresh, nil, providersync.SnapshotPlan{}, nil
	}
	result, err := conn.Query(ctx, openOwnershipQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", Provider), clickhouse.Named("team_ids", scope))
	if err != nil {
		return nil, nil, providersync.SnapshotPlan{}, err
	}
	defer result.Close()
	var open []openOwnership
	for result.Next() {
		var row openOwnership
		if err := result.Scan(&row.teamID, &row.projectID, &row.projectKey, &row.source, &row.isPrimary, &row.specificity, &row.priority, &row.validFrom); err != nil {
			return nil, nil, providersync.SnapshotPlan{}, err
		}
		open = append(open, row)
	}
	if err := result.Err(); err != nil {
		return nil, nil, providersync.SnapshotPlan{}, err
	}
	freshFacts := make([]providersync.OwnershipSnapshotRow, len(fresh))
	for i, row := range fresh {
		key := row.ProjectKey
		freshFacts[i] = providersync.OwnershipSnapshotRow{TeamID: row.TeamID, ProjectID: row.ProjectID, Source: row.Source, ValidFrom: row.ValidFrom,
			Shape: providersync.ProjectOwnershipShape(&key, row.IsPrimary, row.Specificity, row.Priority)}
	}
	openFacts := make([]providersync.OwnershipSnapshotRow, len(open))
	for i, row := range open {
		openFacts[i] = providersync.OwnershipSnapshotRow{TeamID: row.teamID, ProjectID: row.projectID, Source: row.source, ValidFrom: row.validFrom,
			Shape: providersync.ProjectOwnershipShape(row.projectKey, row.isPrimary, row.specificity, row.priority)}
	}
	plan := providersync.PlanOwnershipSnapshot(freshFacts, openFacts, now, snapshot)
	out := make([]OwnershipRow, 0, len(fresh))
	for i, row := range fresh {
		if plan.Unchanged[i] {
			continue
		}
		row.ValidFrom = plan.ValidFrom[i]
		out = append(out, row)
	}
	var expired []openOwnership
	for _, retraction := range plan.Retract {
		row := open[retraction.Open]
		row.closedAt = retraction.ClosedAt
		expired = append(expired, row)
	}
	return out, expired, plan, nil
}

// writeTeams writes the catalog rows. keepProjectKeys keeps every team's
// stored project keys (the links were not selected); keepKeysOf names the
// teams that keep their stored keys next to the ones this run read.
func writeTeams(ctx context.Context, conn driver.Conn, orgID string, teams []TeamRow, deactivate []inactiveTeam, now time.Time, keepProjectKeys bool, keepKeysOf map[string]bool) error {
	ids := make([]string, len(teams))
	for i, team := range teams {
		ids[i] = team.ID
	}
	manual, err := providersync.PreserveExistingTeamManualMembers(ctx, conn, orgID, ids)
	if err != nil {
		return err
	}
	var existingKeys map[string][]string
	if keepProjectKeys || len(keepKeysOf) > 0 {
		if existingKeys, err = readProjectKeys(ctx, conn, orgID, ids); err != nil {
			return err
		}
	}
	carryIDs := append([]string{}, ids...)
	for _, team := range deactivate {
		carryIDs = append(carryIDs, team.id)
	}
	createdAt, err := teamcreated.Carry(ctx, conn, orgID, carryIDs)
	if err != nil {
		return err
	}
	stored, err := teamprovider.Carry(ctx, conn, orgID, carryIDs)
	if err != nil {
		return err
	}
	batch, err := conn.PrepareBatch(ctx, teamsInsert)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	for _, team := range teams {
		manualMembers := manual[team.ID]
		if manualMembers == nil {
			manualMembers = []string{}
		}
		keys := team.ProjectKeys
		switch {
		case keepProjectKeys:
			keys = existingKeys[team.ID]
		case keepKeysOf[team.ID]:
			keys = append(append([]string{}, existingKeys[team.ID]...), team.ProjectKeys...)
			slices.Sort(keys)
			keys = slices.Compact(keys)
		}
		if keys == nil {
			keys = []string{}
		}
		nativeKey := team.NativeTeamKey
		identity := teamprovider.Merge(stored, team.ID, providerTeamUUID(team.NativeTeamKey), team.Name)
		if err := batch.Append(
			team.ID, team.TeamUUID, team.Name, team.Description, manualMembers, keys, []string{},
			team.IsActive, team.UpdatedAt, team.OrgID, team.Provider, &nativeKey, (*string)(nil),
			teamcreated.For(createdAt, team.ID, team.UpdatedAt),
			identity.ProviderTeamID, identity.ProviderName, identity.GoneAt,
		); err != nil {
			return err
		}
	}
	for _, team := range deactivate {
		if err := batch.Append(
			team.id, team.teamUUID, team.name, team.description, team.manualMembers, team.projectKeys, team.repoPatterns,
			uint8(0), now, orgID, Provider, team.nativeTeamKey, team.parentTeamID,
			teamcreated.For(createdAt, team.id, now),
			stored[team.id].ProviderTeamID, stored[team.id].ProviderName, stored[team.id].GoneAt,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func readProjectKeys(ctx context.Context, conn driver.Conn, orgID string, ids []string) (map[string][]string, error) {
	result, err := conn.Query(ctx, existingProjectKeysQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("provider", Provider), clickhouse.Named("team_ids", ids))
	if err != nil {
		return nil, err
	}
	defer result.Close()
	keys := map[string][]string{}
	for result.Next() {
		var id string
		var projectKeys []string
		if err := result.Scan(&id, &projectKeys); err != nil {
			return nil, err
		}
		keys[id] = projectKeys
	}
	return keys, result.Err()
}

func writeMemberships(ctx context.Context, conn driver.Conn, orgID string, rows []MembershipRow, expired []openMembership, now time.Time, teamsWritten []string) error {
	rows, err := providersync.KeepLinksOfImportedTeams(ctx, conn, orgID, Provider, "team_memberships", rows,
		func(r MembershipRow) string { return r.TeamID }, func(MembershipRow) bool { return true }, teamsWritten)
	if err != nil {
		return err
	}
	batch, err := conn.PrepareBatch(ctx, membershipsInsert)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	for _, row := range rows {
		raw := row.RawProviderUserID
		providerUserID := optionalText(row.ProviderUserID)
		if err := batch.Append(
			row.OrgID, row.Provider, row.TeamID, row.MemberID, &raw, (*string)(nil), row.IdentityFacets, row.Source,
			row.IsPrimary, row.Specificity, row.Priority, row.ValidFrom, (*time.Time)(nil), row.UpdatedAt, providerUserID,
		); err != nil {
			return err
		}
	}
	// A closed replacement has the sort key of the open row it retracts.
	for _, row := range expired {
		closedAt := row.closedAt
		if err := batch.Append(
			orgID, Provider, row.teamID, row.memberID, row.rawProviderUserID, row.rawEmail, row.identityFacets, row.source,
			row.isPrimary, row.specificity, row.priority, row.validFrom, &closedAt, now, row.providerUserID,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// optionalText is the text, or nil when it is blank.
func optionalText(value string) *string {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return &trimmed
	}
	return nil
}

func writeOwnership(ctx context.Context, conn driver.Conn, orgID string, rows []OwnershipRow, expired []openOwnership, now time.Time, teamsWritten []string) error {
	rows, err := providersync.KeepLinksOfImportedTeams(ctx, conn, orgID, Provider, "team_project_ownership", rows,
		func(r OwnershipRow) string { return r.TeamID }, func(OwnershipRow) bool { return true }, teamsWritten)
	if err != nil {
		return err
	}
	batch, err := conn.PrepareBatch(ctx, ownershipInsert)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	for _, row := range rows {
		key := row.ProjectKey
		if err := batch.Append(
			row.OrgID, row.Provider, row.TeamID, row.ProjectID, &key, row.Source, row.IsPrimary, row.Specificity,
			row.Priority, row.ValidFrom, (*time.Time)(nil), row.UpdatedAt,
		); err != nil {
			return err
		}
	}
	for _, row := range expired {
		closedAt := row.closedAt
		if err := batch.Append(
			orgID, Provider, row.teamID, row.projectID, row.projectKey, row.source, row.isPrimary, row.specificity,
			row.priority, row.validFrom, &closedAt, now,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// checkTeamIDs refuses, before any read or write, a row whose team id does
// not carry the "jira:" prefix.
func checkTeamIDs(rows Rows) error {
	for _, team := range rows.Teams {
		if err := teamid.Check(Provider, team.ID); err != nil {
			return errors.Join(ErrConfiguration, err)
		}
	}
	for _, membership := range rows.Memberships {
		if err := teamid.Check(Provider, membership.TeamID); err != nil {
			return errors.Join(ErrConfiguration, err)
		}
	}
	for _, link := range rows.Ownership {
		if err := teamid.Check(Provider, link.TeamID); err != nil {
			return errors.Join(ErrConfiguration, err)
		}
	}
	return nil
}

// The lookups of one run, one for each cursor kind: each candidate is asked once,
// inside the budget of the kind (providersync.AbsenceLookupBudget).

func teamLookups(ctx context.Context, prover AbsenceProver) *providersync.AbsenceLookups[providersync.TeamSnapshotRow] {
	if prover == nil {
		return providersync.NewAbsenceLookups[providersync.TeamSnapshotRow](ctx, providersync.TeamSnapshotKey, nil)
	}
	return providersync.NewAbsenceLookups(ctx, providersync.TeamSnapshotKey,
		func(ctx context.Context, row providersync.TeamSnapshotRow) providersync.SnapshotAbsence {
			return prover.TeamAbsence(ctx, row.TeamID)
		})
}

func membershipLookups(ctx context.Context, prover AbsenceProver) *providersync.AbsenceLookups[providersync.MembershipSnapshotRow] {
	if prover == nil {
		return providersync.NewAbsenceLookups[providersync.MembershipSnapshotRow](ctx, providersync.MembershipSnapshotKey, nil)
	}
	return providersync.NewAbsenceLookups(ctx, providersync.MembershipSnapshotKey,
		func(ctx context.Context, row providersync.MembershipSnapshotRow) providersync.SnapshotAbsence {
			return prover.MembershipAbsence(ctx, row.TeamID, row.MemberID)
		})
}

type linkProver struct{ AbsenceProver }

func (prover linkProver) OwnershipAbsence(ctx context.Context, row providersync.OwnershipSnapshotRow) providersync.SnapshotAbsence {
	return prover.LinkAbsence(ctx, row.TeamID, row.ProjectID)
}

func linkLookups(ctx context.Context, prover AbsenceProver) *providersync.AbsenceLookups[providersync.OwnershipSnapshotRow] {
	if prover == nil {
		return providersync.NewOwnershipAbsenceLookups(ctx, nil)
	}
	return providersync.NewOwnershipAbsenceLookups(ctx, linkProver{prover})
}

// providerTeamUUID is the provider's immutable team id: the uuid of the team
// ARI, lower-cased as teamID does. Anything that is not a team ARI is kept as
// written so a writer never drops what the run read.
func providerTeamUUID(ari string) string {
	ari = strings.TrimSpace(ari)
	i := strings.LastIndex(ari, teamARIPrefix)
	if i < 0 || !strings.HasPrefix(ari, "ari:") {
		return ari
	}
	return strings.ToLower(strings.TrimSpace(ari[i+len(teamARIPrefix):]))
}
