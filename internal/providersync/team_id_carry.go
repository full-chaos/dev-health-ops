package providersync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// A provider team id carries its provider's prefix (internal/teamid). The
// rows a store wrote before that rule hold the bare id. CarryTeamIDs moves
// one organization's bare team ids to the prefixed form: the team row is
// written again under the new id and the old row goes inactive; the rows
// that name the team move with it. Nothing is deleted or updated in place:
// every change is a new row version. Computed attribution and metric rows
// are not rewritten here: a recompute writes them under the new id.
// See docs/contribute/architecture/team-attribution.md "Team ids".

// teamIDCarryAdminTeamNamespace is the namespace of an admin team's
// team_uuid (internal/api/teamsidentity teamUUID).
var teamIDCarryAdminTeamNamespace = uuid.MustParse("6ba7b811-9dad-11d1-80b4-00c04fd430c8")

// teamIDCarryBare is a SQL condition: the column holds a non-empty id with
// no known provider key that is not only a provider prefix, the SQL form of
// !teamid.HasKey && !teamid.Malformed.
func teamIDCarryBare(column string) string {
	trimmed := "trimBoth(" + column + ")"
	clauses := make([]string, 0, len(teamid.KnownKeys()))
	for _, key := range teamid.KnownKeys() {
		clauses = append(clauses, fmt.Sprintf("(startsWith(%s, '%s') AND trimBoth(substring(%s, %d)) != '')", trimmed, key, trimmed, len(key)+1))
	}
	return trimmed + " != '' AND NOT (" + strings.Join(clauses, " OR ") + ") AND NOT " + teamIDCarryPrefixOnly(column)
}

// teamIDCarryPrefixOnly is a SQL condition: the column holds only a
// provider prefix (teamid.PrefixOnlyForms). Such an id is no team of any
// provider; teamid.Of would give it a second prefix, so the carry leaves it.
func teamIDCarryPrefixOnly(column string) string {
	forms := make([]string, 0, len(teamid.PrefixOnlyForms()))
	for _, form := range teamid.PrefixOnlyForms() {
		forms = append(forms, "'"+form+"'")
	}
	return "trimBoth(" + column + ") IN (" + strings.Join(forms, ", ") + ")"
}

// teamIDCarryAdminProvider is the prefix system of an admin's own team: an
// admin team that no single provider's observation names.
const teamIDCarryAdminProvider = "custom"

// teamIDCarryActiveAdminIDs is the bare ids whose current team row (FINAL:
// teams is keyed by (org_id, id)) is an active admin row. An admin row
// counts only through it: a raw admin row can stay active beside the newer
// inactive row of another provider's team of that id, because one insert
// block that holds rows of both collapses to one of them (the carry writes
// the old rows of a moved team and of its admin edit in one block).
var teamIDCarryActiveAdminIDs = `SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND provider = '' AND is_active = 1 AND ` + teamIDCarryBare("id")

// teamIDCarryCountQuery counts what a carry would move: the (provider, id)
// groups whose newest raw row is active with a bare id (a provider's team,
// or an admin team that one provider's observation names; a Jira
// project-as-team row is retired by RetireJiraProjectAsTeamRows, not
// carried), and the observations that hold a bare team id. The raw rows are
// read, not FINAL: teams is keyed by (org_id, id), so two providers' rows
// of one id are one row after a merge, and the newest raw row of each
// provider is what tells them apart.
var teamIDCarryCountQuery = `SELECT ` +
	`(SELECT count() FROM (SELECT provider, id, ` +
	`argMax(is_active, (updated_at, last_synced)) AS active, ` +
	`argMax(ifNull(native_team_key, ''), (updated_at, last_synced)) AS native_key ` +
	`FROM teams WHERE org_id = {org_id:String} AND ` + teamIDCarryBare("id") + ` GROUP BY provider, id) ` +
	`WHERE active = 1 ` +
	`AND NOT (provider = 'jira' AND native_key = id AND NOT startsWith(native_key, '` + jiraAtlassianTeamARIPrefix + `')) ` +
	`AND (provider != '' OR id IN (` + teamIDCarryActiveAdminIDs + `))), ` +
	`(SELECT count() FROM team_provider_observations FINAL WHERE org_id = {org_id:String} AND provider != '' AND ` + teamIDCarryBare("team_id") + `), ` +
	`(SELECT count() FROM (SELECT provider, id, argMax(is_active, (updated_at, last_synced)) AS active ` +
	`FROM teams WHERE org_id = {org_id:String} AND ` + teamIDCarryPrefixOnly("id") + ` GROUP BY provider, id) WHERE active = 1) + ` +
	`(SELECT count() FROM team_provider_observations FINAL WHERE org_id = {org_id:String} AND provider != '' AND ` + teamIDCarryPrefixOnly("team_id") + `)`

var teamIDCarryTeamsQuery = `SELECT id, team_uuid, name, description, members, manual_members, updated_at, last_synced, ` +
	`org_id, provider, native_team_key, parent_team_id, project_keys, repo_patterns, is_active, source_id ` +
	`FROM teams WHERE org_id = {org_id:String} AND ` + teamIDCarryBare("id") + ` ` +
	`ORDER BY updated_at DESC, last_synced DESC LIMIT 1 BY provider, id`

var teamIDCarryObservationsQuery = `SELECT org_id, provider, native_team_key, team_id, name, description, members_json, ` +
	`project_keys_json, repo_patterns_json, is_active, parent_team_id, discovered_at, updated_at ` +
	`FROM team_provider_observations FINAL WHERE org_id = {org_id:String} AND provider != '' AND ` + teamIDCarryBare("team_id")

// teamIDCarryLink is one table that links a team to something with a
// validity window. natural is the column that names the other side.
type teamIDCarryLink struct {
	table   string
	columns string
	natural string
}

var teamIDCarryLinks = []teamIDCarryLink{
	{table: "team_memberships", natural: "member_id",
		columns: "org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at"},
	{table: "team_project_ownership", natural: "project_id",
		columns: "org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at"},
	{table: "team_repo_ownership", natural: "repo_full_name",
		columns: "org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at"},
}

const (
	teamIDCarryPoliciesColumns  = "org_id, team_id, sync_policy, managed_fields, updated_by, updated_at"
	teamIDCarryDriftColumns     = "org_id, change_id, entity_type, entity_id, provider, native_team_key, change_type, field, old_value_json, new_value_json, status, first_seen_at, last_seen_at, decided_at, decided_by, updated_at"
	teamIDCarryIdentityColumns  = "org_id, canonical_id, identity_uuid, display_name, email, provider_identities, team_ids, is_active, updated_at, source_id"
	teamIDCarryFallbackColumns  = "org_id, provider, scope_type, scope_id, team_id, team_name, reason, priority, valid_from, valid_to, created_by, created_at, updated_at"
	teamIDCarryTeamsColumns     = "id, team_uuid, name, description, members, manual_members, updated_at, last_synced, org_id, provider, native_team_key, parent_team_id, project_keys, repo_patterns, is_active, source_id"
	teamIDCarryObservationsCols = "org_id, provider, native_team_key, team_id, name, description, members_json, project_keys_json, repo_patterns_json, is_active, parent_team_id, discovered_at, updated_at"
)

// TeamIDCarryOutcome is counts only: no id, key or name of a team, a member
// or an organization leaves the store through it.
type TeamIDCarryOutcome struct {
	DryRun bool `json:"dry_run"`
	// Teams is the bare team ids moved to the prefixed form; AdminTeams is
	// the part of them that are admin teams (provider ""), and
	// AdminTeamsToCustom the part of those that no single provider's
	// observation names: the admin's own teams, moved to custom:<id>.
	Teams              uint64 `json:"teams"`
	AdminTeams         uint64 `json:"admin_teams"`
	AdminTeamsToCustom uint64 `json:"admin_teams_to_custom"`
	// AdminTeamsNotCarried is the active admin teams with a bare id that a
	// Jira project-as-team row of the same id holds (an admin edit of that
	// row, which RetireJiraProjectAsTeamRows owns).
	AdminTeamsNotCarried uint64 `json:"admin_teams_not_carried"`
	// AmbiguousTeams is the bare ids that more than one team holds (two
	// providers, or a provider and a Jira project-as-team row). Each
	// provider's rows move to that provider's id; the rows that name the id
	// without a provider stay.
	AmbiguousTeams uint64 `json:"ambiguous_teams"`
	// TeamsAlreadyKeyed is the new ids that already had a row. That row is
	// not written again; the old row still goes inactive.
	TeamsAlreadyKeyed uint64 `json:"teams_already_keyed"`
	// The open link rows moved, and the ones whose prefixed twin was already
	// open (the old row is closed, no second open row is written).
	Memberships          uint64 `json:"memberships"`
	ProjectOwnership     uint64 `json:"project_ownership"`
	RepoOwnership        uint64 `json:"repo_ownership"`
	LinkRowsAlreadyKeyed uint64 `json:"link_rows_already_keyed"`
	Observations         uint64 `json:"observations"`
	SyncPolicies         uint64 `json:"sync_policies"`
	DriftChanges         uint64 `json:"drift_changes"`
	// IdentityDriftChanges is the pending identity membership changes of a
	// moved team that were superseded.
	IdentityDriftChanges uint64 `json:"identity_drift_changes"`
	Identities           uint64 `json:"identities"`
	Fallbacks            uint64 `json:"fallbacks"`
	// MalformedTeamIDs is the active team ids and the observations that hold
	// only a provider prefix: no team of any provider, left as they are.
	MalformedTeamIDs uint64 `json:"malformed_team_ids"`
	// RowsWritten is the rows a real run wrote; 0 in a dry run.
	RowsWritten uint64 `json:"rows_written"`
}

// Found says the store holds something to carry.
func (outcome TeamIDCarryOutcome) Found() bool {
	return outcome.Teams > 0 || outcome.Observations > 0
}

// errTeamIDCarryUnkeyed is the guard before any write: a planned new id
// that does not carry a known key.
var errTeamIDCarryUnkeyed = errors.New("team id carry: planned id without a provider key")

// CarryTeamIDs moves one organization's bare team ids to the prefixed form,
// at `at`. Every read runs before the first write, so a failed read writes
// nothing. A dry run reads and counts and writes nothing. With nothing to
// carry it is one count read and no write, so a second run reports zero.
// Every write path of a prefixed team id runs it first: a team catalog
// sync through CarryFirstTeamCatalogCollector, before the collector; the
// stream sink, the admin import and the Atlassian teams CLI verb at their
// entry, through CarryTeamIDsBeforeWrite.
//
// The team rows and then the observations are written last: after a
// failure part way the old team is still found active, so a re-run moves
// what is left.
func CarryTeamIDs(ctx context.Context, conn TeamIDCarryConn, orgID string, at time.Time, dryRun bool) (TeamIDCarryOutcome, error) {
	orgID = strings.TrimSpace(orgID)
	if ctx == nil || conn == nil || orgID == "" || at.IsZero() {
		return TeamIDCarryOutcome{}, ErrInvalidConfiguration
	}
	at = at.UTC().Truncate(time.Millisecond)
	org := clickhouse.Named("org_id", orgID)
	outcome := TeamIDCarryOutcome{DryRun: dryRun}
	teamGroups, observations, malformed, err := teamIDCarryCount(ctx, conn, org)
	if err != nil {
		return TeamIDCarryOutcome{}, fmt.Errorf("team id carry: count: %w", err)
	}
	outcome.MalformedTeamIDs = malformed
	if malformed > 0 {
		slog.Default().WarnContext(ctx, "team_ids_malformed_skipped", "malformed_team_ids", malformed)
	}
	if teamGroups == 0 && observations == 0 {
		return outcome, nil
	}
	carry := teamIDCarryRun{ctx: ctx, conn: conn, org: org, orgID: orgID, at: at, outcome: &outcome}
	if err := carry.plan(); err != nil {
		return TeamIDCarryOutcome{}, err
	}
	if dryRun {
		return outcome, nil
	}
	if err := carry.write(); err != nil {
		return outcome, err
	}
	return outcome, nil
}

// TeamIDCarryConn is the part of a ClickHouse connection the carry uses.
type TeamIDCarryConn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// teamIDCarryCount reads the count query; a missing row is a failed read.
func teamIDCarryCount(ctx context.Context, conn TeamIDCarryConn, org driver.NamedValue) (uint64, uint64, uint64, error) {
	rows, err := conn.Query(ctx, teamIDCarryCountQuery, org)
	if err != nil {
		return 0, 0, 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, 0, 0, err
		}
		return 0, 0, 0, errors.New("no count row")
	}
	var teamGroups, observations, malformed uint64
	if err := rows.Scan(&teamGroups, &observations, &malformed); err != nil {
		return 0, 0, 0, err
	}
	return teamGroups, observations, malformed, rows.Err()
}

type teamIDCarryWrite struct {
	table   string
	columns string
	rows    []chRow
}

type teamIDCarryRun struct {
	ctx     context.Context
	conn    TeamIDCarryConn
	org     driver.NamedValue
	orgID   string
	at      time.Time
	outcome *TeamIDCarryOutcome

	// target: old id -> provider -> new id, for every provider group that
	// moves. groups: old id -> every provider with a bare row of that id.
	// primary: old id -> the one new id, for the rows that name the id
	// without its provider.
	target  map[string]map[string]string
	groups  map[string]map[string]bool
	primary map[string]string

	// writes run in this order; teams and observations are last.
	writes []teamIDCarryWrite
}

func (run *teamIDCarryRun) plan() error {
	teams, err := run.read(teamIDCarryTeamsQuery, run.org)
	if err != nil {
		return fmt.Errorf("team id carry: read teams: %w", err)
	}
	observations, err := run.read(teamIDCarryObservationsQuery, run.org)
	if err != nil {
		return fmt.Errorf("team id carry: read observations: %w", err)
	}
	activeAdmins, err := run.read(teamIDCarryActiveAdminIDs, run.org)
	if err != nil {
		return fmt.Errorf("team id carry: read admin teams: %w", err)
	}
	adminIDs := make(map[string]bool, len(activeAdmins))
	for _, row := range activeAdmins {
		adminIDs[row.str("id")] = true
	}
	teamRows := run.planTeams(teams, observations, adminIDs)
	if err := run.dropKeyedTeams(&teamRows); err != nil {
		return err
	}
	for _, link := range teamIDCarryLinks {
		if err := run.planLink(link); err != nil {
			return err
		}
	}
	for _, step := range []func() error{run.planPolicies, run.planDrift, run.planIdentityDrift, run.planIdentities, run.planFallbacks} {
		if err := step(); err != nil {
			return err
		}
	}
	run.writes = append(run.writes, teamIDCarryWrite{table: "teams", columns: teamIDCarryTeamsColumns, rows: teamRows})
	run.writes = append(run.writes, run.planObservations(observations))
	for _, write := range run.writes {
		if err := teamIDCarryGuard(write); err != nil {
			return err
		}
	}
	return nil
}

// planTeams decides, per bare id, which provider groups move and to which
// id, and builds the new and the inactive team rows.
func (run *teamIDCarryRun) planTeams(teams, observations []chRow, activeAdmins map[string]bool) []chRow {
	run.target = map[string]map[string]string{}
	run.groups = map[string]map[string]bool{}
	run.primary = map[string]string{}
	observedBy := map[string]map[string]bool{}
	for _, row := range observations {
		id := row.str("team_id")
		if observedBy[id] == nil {
			observedBy[id] = map[string]bool{}
		}
		observedBy[id][row.str("provider")] = true
	}
	byID := map[string][]chRow{}
	for _, row := range teams {
		id := row.str("id")
		if teamid.HasKey(id) || strings.TrimSpace(id) == "" {
			continue
		}
		byID[id] = append(byID[id], row)
		if run.groups[id] == nil {
			run.groups[id] = map[string]bool{}
		}
		run.groups[id][row.str("provider")] = true
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	type newTeam struct {
		content chRow
		id      string
	}
	var created []newTeam
	var retired []chRow
	for _, id := range ids {
		var carried []chRow
		var admin *chRow
		blocked := false
		for _, row := range byID[id] {
			if row.u8("is_active") != 1 {
				continue
			}
			switch {
			case row.str("provider") == "":
				if activeAdmins[id] {
					admin = &row
				}
			case teamIDCarryJiraProjectAsTeam(row):
				blocked = true
			default:
				carried = append(carried, row)
			}
		}
		targets := map[string]string{}
		switch {
		case len(carried) == 0 && admin == nil:
			continue
		case len(carried) == 0:
			// An admin edit of a Jira project-as-team row is that row, and
			// RetireJiraProjectAsTeamRows owns it: it does not become a
			// Jira team.
			if blocked {
				run.outcome.AdminTeamsNotCarried++
				continue
			}
			// An admin team that one provider's observation names came from
			// that provider's import; any other is the admin's own team and
			// takes the custom prefix, the id the write seam gives a plain
			// admin id.
			provider := teamIDCarryAdminProvider
			if len(observedBy[id]) == 1 {
				for observed := range observedBy[id] {
					provider = observed
				}
			} else {
				run.outcome.AdminTeamsToCustom++
			}
			newID := teamid.Of(provider, id)
			targets[""] = newID
			created = append(created, newTeam{content: *admin, id: newID})
			retired = append(retired, *admin)
			run.outcome.AdminTeams++
		default:
			for _, row := range carried {
				newID := teamid.Of(row.str("provider"), id)
				targets[row.str("provider")] = newID
				content := row
				if len(carried) == 1 && admin != nil {
					// An admin edit of a provider team writes the same id with
					// no provider; it is that team, so it moves with it.
					targets[""] = newID
					if admin.time("updated_at").After(row.time("updated_at")) {
						content = *admin
					}
					retired = append(retired, *admin)
				}
				created = append(created, newTeam{content: content, id: newID})
				retired = append(retired, row)
			}
		}
		run.outcome.Teams++
		run.target[id] = targets
		distinct := map[string]bool{}
		for _, newID := range targets {
			distinct[newID] = true
		}
		if len(distinct) == 1 && !blocked {
			for newID := range distinct {
				run.primary[id] = newID
			}
		} else {
			run.outcome.AmbiguousTeams++
		}
	}

	rows := make([]chRow, 0, len(created)+len(retired))
	for _, team := range created {
		rows = append(rows, run.newTeamRow(team.content, team.content.str("id"), team.id))
	}
	for _, row := range retired {
		rows = append(rows, row.with("is_active", uint8(0)).with("updated_at", teamIDCarryBump(run.at, row.time("updated_at"), time.Microsecond)))
	}
	return rows
}

func (run *teamIDCarryRun) newTeamRow(content chRow, oldID, newID string) chRow {
	provider := content.str("provider")
	teamUUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+newID))
	if provider == "" {
		teamUUID = uuid.NewSHA1(teamIDCarryAdminTeamNamespace, []byte("team:"+run.orgID+":"+newID))
	}
	row := content.with("id", newID).with("team_uuid", teamUUID).with("is_active", uint8(1))
	if provider != "" {
		if native := content.strPtr("native_team_key"); native == nil || strings.TrimSpace(*native) == "" {
			key := oldID
			row = row.with("native_team_key", &key)
		}
	}
	if parent := content.strPtr("parent_team_id"); parent != nil {
		if mapped, ok := run.primary[*parent]; ok {
			row = row.with("parent_team_id", &mapped)
		}
	}
	return row
}

// teamIDCarryJiraProjectAsTeam is the shape RetireJiraProjectAsTeamRows
// retires: provider jira and a native_team_key equal to the id that is not
// a team ARI.
func teamIDCarryJiraProjectAsTeam(row chRow) bool {
	native := ""
	if value := row.strPtr("native_team_key"); value != nil {
		native = *value
	}
	return row.str("provider") == jiraTeamCatalogProvider && native == row.str("id") && !strings.HasPrefix(native, jiraAtlassianTeamARIPrefix)
}

// dropKeyedTeams keeps a team row that a prefixed writer already wrote: the
// new row of such an id is not written.
func (run *teamIDCarryRun) dropKeyedTeams(rows *[]chRow) error {
	var newIDs []string
	for _, row := range *rows {
		if row.u8("is_active") == 1 {
			newIDs = append(newIDs, row.str("id"))
		}
	}
	if len(newIDs) == 0 {
		return nil
	}
	existing, err := run.readSet(`SELECT DISTINCT id FROM teams WHERE org_id = {org_id:String} AND id IN {ids:Array(String)}`, newIDs)
	if err != nil {
		return fmt.Errorf("team id carry: read prefixed teams: %w", err)
	}
	kept := (*rows)[:0]
	for _, row := range *rows {
		if row.u8("is_active") == 1 && existing[row.str("id")] {
			run.outcome.TeamsAlreadyKeyed++
			continue
		}
		kept = append(kept, row)
	}
	*rows = kept
	return nil
}

// linkTarget is the new id of a row that names a team with its provider.
// A row of a provider that has its own bare team of that id that does not
// move (a Jira project-as-team row, an inactive team) stays.
func (run *teamIDCarryRun) linkTarget(provider, oldID string) (string, bool) {
	if newID, ok := run.target[oldID][provider]; ok {
		return newID, true
	}
	if run.groups[oldID][provider] {
		return "", false
	}
	newID, ok := run.primary[oldID]
	return newID, ok
}

func (run *teamIDCarryRun) linkIDs() (oldIDs, newIDs []string) {
	seenNew := map[string]bool{}
	for oldID, targets := range run.target {
		oldIDs = append(oldIDs, oldID)
		for _, newID := range targets {
			if !seenNew[newID] {
				seenNew[newID] = true
				newIDs = append(newIDs, newID)
			}
		}
	}
	sort.Strings(oldIDs)
	sort.Strings(newIDs)
	return oldIDs, newIDs
}

// planLink moves the open rows of one link table: the same row under the
// new id with the first valid_from the old id ever had for that link
// (closed rows included), and the old row closed at `at`.
func (run *teamIDCarryRun) planLink(link teamIDCarryLink) error {
	oldIDs, newIDs := run.linkIDs()
	if len(oldIDs) == 0 {
		return nil
	}
	open, err := run.read(`SELECT `+link.columns+` FROM `+link.table+` FINAL WHERE org_id = {org_id:String} `+
		`AND valid_to IS NULL AND team_id IN {ids:Array(String)}`, run.org, clickhouse.Named("ids", oldIDs))
	if err != nil {
		return fmt.Errorf("team id carry: read %s: %w", link.table, err)
	}
	if len(open) == 0 {
		return nil
	}
	firstSeen := map[string]time.Time{}
	rows, err := run.conn.Query(run.ctx, `SELECT provider, team_id, `+link.natural+`, toString(source), min(valid_from) FROM `+link.table+
		` WHERE org_id = {org_id:String} AND team_id IN {ids:Array(String)} GROUP BY provider, team_id, `+link.natural+`, source`,
		run.org, clickhouse.Named("ids", oldIDs))
	if err != nil {
		return fmt.Errorf("team id carry: read %s first valid_from: %w", link.table, err)
	}
	for rows.Next() {
		var provider, teamID, natural, source string
		var validFrom time.Time
		if err := rows.Scan(&provider, &teamID, &natural, &source, &validFrom); err != nil {
			rows.Close()
			return fmt.Errorf("team id carry: read %s first valid_from: %w", link.table, err)
		}
		firstSeen[teamIDCarryLinkKey(provider, teamID, natural, source)] = validFrom
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("team id carry: read %s first valid_from: %w", link.table, err)
	}
	keyed := map[string]bool{}
	rows, err = run.conn.Query(run.ctx, `SELECT provider, team_id, `+link.natural+`, toString(source) FROM `+link.table+
		` FINAL WHERE org_id = {org_id:String} AND valid_to IS NULL AND team_id IN {ids:Array(String)}`,
		run.org, clickhouse.Named("ids", newIDs))
	if err != nil {
		return fmt.Errorf("team id carry: read %s prefixed rows: %w", link.table, err)
	}
	for rows.Next() {
		var provider, teamID, natural, source string
		if err := rows.Scan(&provider, &teamID, &natural, &source); err != nil {
			rows.Close()
			return fmt.Errorf("team id carry: read %s prefixed rows: %w", link.table, err)
		}
		keyed[teamIDCarryLinkKey(provider, teamID, natural, source)] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("team id carry: read %s prefixed rows: %w", link.table, err)
	}

	var out []chRow
	var moved uint64
	for _, row := range open {
		provider, oldID := row.str("provider"), row.str("team_id")
		newID, ok := run.linkTarget(provider, oldID)
		if !ok {
			continue
		}
		natural, source := row.str(link.natural), row.str("source")
		updated := teamIDCarryBump(run.at, row.time("updated_at"), time.Millisecond)
		if keyed[teamIDCarryLinkKey(provider, newID, natural, source)] {
			run.outcome.LinkRowsAlreadyKeyed++
		} else {
			validFrom := row.time("valid_from")
			if first, ok := firstSeen[teamIDCarryLinkKey(provider, oldID, natural, source)]; ok && first.Before(validFrom) {
				validFrom = first
			}
			out = append(out, row.with("team_id", newID).with("valid_from", validFrom).with("updated_at", updated))
			moved++
		}
		// Closed at the carry's second, not its millisecond: a reader that
		// keeps a row while valid_to > now() (ClickHouse now() has second
		// precision) would otherwise see the old row active for up to a
		// second, and a collector's guard that runs right after the carry
		// would stage the moved manual member as a conflict.
		closedAt := run.at.Truncate(time.Second)
		if validFrom := row.time("valid_from"); validFrom.After(closedAt) {
			closedAt = validFrom
		}
		out = append(out, row.with("valid_to", &closedAt).with("updated_at", updated))
	}
	switch link.table {
	case "team_memberships":
		run.outcome.Memberships = moved
	case "team_project_ownership":
		run.outcome.ProjectOwnership = moved
	case "team_repo_ownership":
		run.outcome.RepoOwnership = moved
	}
	if len(out) > 0 {
		run.writes = append(run.writes, teamIDCarryWrite{table: link.table, columns: link.columns, rows: out})
	}
	return nil
}

func teamIDCarryLinkKey(provider, teamID, natural, source string) string {
	return strings.Join([]string{provider, teamID, natural, source}, "\x00")
}

func (run *teamIDCarryRun) primaryIDs() (oldIDs, newIDs []string) {
	for oldID, newID := range run.primary {
		oldIDs = append(oldIDs, oldID)
		newIDs = append(newIDs, newID)
	}
	sort.Strings(oldIDs)
	sort.Strings(newIDs)
	return oldIDs, newIDs
}

// planPolicies copies a sync policy to the new id, unless the new id has
// one. The old row stays; no team holds the old id any more.
func (run *teamIDCarryRun) planPolicies() error {
	oldIDs, newIDs := run.primaryIDs()
	if len(oldIDs) == 0 {
		return nil
	}
	policies, err := run.read(`SELECT `+teamIDCarryPoliciesColumns+` FROM team_sync_policies FINAL WHERE org_id = {org_id:String} AND team_id IN {ids:Array(String)}`,
		run.org, clickhouse.Named("ids", oldIDs))
	if err != nil {
		return fmt.Errorf("team id carry: read team_sync_policies: %w", err)
	}
	if len(policies) == 0 {
		return nil
	}
	existing, err := run.readSet(`SELECT DISTINCT team_id FROM team_sync_policies WHERE org_id = {org_id:String} AND team_id IN {ids:Array(String)}`, newIDs)
	if err != nil {
		return fmt.Errorf("team id carry: read prefixed team_sync_policies: %w", err)
	}
	var out []chRow
	for _, row := range policies {
		newID := run.primary[row.str("team_id")]
		if existing[newID] {
			continue
		}
		out = append(out, row.with("team_id", newID).with("updated_at", teamIDCarryBump(run.at, row.time("updated_at"), time.Microsecond)))
	}
	run.outcome.SyncPolicies = uint64(len(out))
	if len(out) > 0 {
		run.writes = append(run.writes, teamIDCarryWrite{table: "team_sync_policies", columns: teamIDCarryPoliciesColumns, rows: out})
	}
	return nil
}

// planDrift writes every field change of an old id again under the new id,
// with the change id the drift review computes for it, so a pending change
// stays pending and a dismissed one stays dismissed. A pending change of
// the old id is superseded.
func (run *teamIDCarryRun) planDrift() error {
	oldIDs, _ := run.primaryIDs()
	if len(oldIDs) == 0 {
		return nil
	}
	changes, err := run.read(`SELECT `+teamIDCarryDriftColumns+` FROM team_drift_changes FINAL WHERE org_id = {org_id:String} `+
		`AND entity_type = '`+teamDriftEntityTypeTeam+`' AND change_type = '`+teamDriftFieldChangedType+`' AND field IS NOT NULL `+
		`AND entity_id IN {ids:Array(String)}`, run.org, clickhouse.Named("ids", oldIDs))
	if err != nil {
		return fmt.Errorf("team id carry: read team_drift_changes: %w", err)
	}
	if len(changes) == 0 {
		return nil
	}
	newChangeIDs := make([]string, 0, len(changes))
	moved := make([]chRow, 0, len(changes))
	for _, row := range changes {
		newID := run.primary[row.str("entity_id")]
		changeID := changeIDForTeamField(run.orgID, newID, *row.strPtr("field"), row.str("old_value_json"), row.str("new_value_json"))
		newChangeIDs = append(newChangeIDs, changeID)
		moved = append(moved, row.with("entity_id", newID).with("change_id", changeID).
			with("updated_at", teamIDCarryBump(run.at, row.time("updated_at"), time.Microsecond)))
	}
	existing, err := run.readSet(`SELECT DISTINCT change_id FROM team_drift_changes WHERE org_id = {org_id:String} AND change_id IN {ids:Array(String)}`, newChangeIDs)
	if err != nil {
		return fmt.Errorf("team id carry: read prefixed team_drift_changes: %w", err)
	}
	var out []chRow
	for i, row := range changes {
		if !existing[moved[i].str("change_id")] {
			out = append(out, moved[i])
		}
		if row.str("status") == teamDriftStatusPending {
			out = append(out, row.with("status", teamDriftStatusSuperseded).with("updated_at", teamIDCarryBump(run.at, row.time("updated_at"), time.Microsecond)))
		}
	}
	run.outcome.DriftChanges = uint64(len(changes))
	if len(out) > 0 {
		run.writes = append(run.writes, teamIDCarryWrite{table: "team_drift_changes", columns: teamIDCarryDriftColumns, rows: out})
	}
	return nil
}

// planIdentityDrift supersedes a pending identity membership change of a
// team that moves. The review only resolves a pending change of a team it
// observes, and it observes the prefixed id, so a change of the bare id
// would stay pending, and an approval would write a membership of the
// inactive bare team. It is not written again under the new id: its change
// id holds the membership row with its updated_at, so no later review can
// match a copy; the next review of the provider stages the conflict again
// under the prefixed id if it is still there (in a catalog sync, the same
// run, as the carry runs before the collector).
func (run *teamIDCarryRun) planIdentityDrift() error {
	oldIDs, _ := run.linkIDs()
	if len(oldIDs) == 0 {
		return nil
	}
	changes, err := run.read(`SELECT `+teamIDCarryDriftColumns+` FROM team_drift_changes FINAL WHERE org_id = {org_id:String} `+
		`AND entity_type = '`+identityDriftEntityType+`' AND change_type = '`+identityDriftMembershipChangedT+`' `+
		`AND status = '`+teamDriftStatusPending+`' AND entity_id IN {ids:Array(String)}`, run.org, clickhouse.Named("ids", oldIDs))
	if err != nil {
		return fmt.Errorf("team id carry: read identity team_drift_changes: %w", err)
	}
	var out []chRow
	for _, row := range changes {
		if _, ok := run.linkTarget(row.str("provider"), row.str("entity_id")); !ok {
			continue
		}
		out = append(out, row.with("status", teamDriftStatusSuperseded).with("updated_at", teamIDCarryBump(run.at, row.time("updated_at"), time.Microsecond)))
	}
	run.outcome.IdentityDriftChanges = uint64(len(out))
	if len(out) > 0 {
		run.writes = append(run.writes, teamIDCarryWrite{table: "team_drift_changes", columns: teamIDCarryDriftColumns, rows: out})
	}
	return nil
}

// planIdentities rewrites the old ids in identities.team_ids.
func (run *teamIDCarryRun) planIdentities() error {
	oldIDs, _ := run.primaryIDs()
	if len(oldIDs) == 0 {
		return nil
	}
	identities, err := run.read(`SELECT `+teamIDCarryIdentityColumns+` FROM identities FINAL WHERE org_id = {org_id:String} AND hasAny(team_ids, {ids:Array(String)})`,
		run.org, clickhouse.Named("ids", oldIDs))
	if err != nil {
		return fmt.Errorf("team id carry: read identities: %w", err)
	}
	var out []chRow
	for _, row := range identities {
		seen := map[string]bool{}
		teamIDs := []string{}
		for _, teamID := range row.strs("team_ids") {
			if newID, ok := run.primary[teamID]; ok {
				teamID = newID
			}
			if !seen[teamID] {
				seen[teamID] = true
				teamIDs = append(teamIDs, teamID)
			}
		}
		out = append(out, row.with("team_ids", teamIDs).with("updated_at", teamIDCarryBump(run.at, row.time("updated_at"), time.Microsecond)))
	}
	run.outcome.Identities = uint64(len(out))
	if len(out) > 0 {
		run.writes = append(run.writes, teamIDCarryWrite{table: "identities", columns: teamIDCarryIdentityColumns, rows: out})
	}
	return nil
}

// planFallbacks points a manual attribution fallback at the new id (same
// key, so the row is replaced).
func (run *teamIDCarryRun) planFallbacks() error {
	oldIDs, _ := run.primaryIDs()
	if len(oldIDs) == 0 {
		return nil
	}
	fallbacks, err := run.read(`SELECT `+teamIDCarryFallbackColumns+` FROM manual_attribution_fallbacks FINAL WHERE org_id = {org_id:String} AND team_id IN {ids:Array(String)}`,
		run.org, clickhouse.Named("ids", oldIDs))
	if err != nil {
		return fmt.Errorf("team id carry: read manual_attribution_fallbacks: %w", err)
	}
	var out []chRow
	for _, row := range fallbacks {
		out = append(out, row.with("team_id", run.primary[row.str("team_id")]).with("updated_at", teamIDCarryBump(run.at, row.time("updated_at"), time.Millisecond)))
	}
	run.outcome.Fallbacks = uint64(len(out))
	if len(out) > 0 {
		run.writes = append(run.writes, teamIDCarryWrite{table: "manual_attribution_fallbacks", columns: teamIDCarryFallbackColumns, rows: out})
	}
	return nil
}

// planObservations writes every observation that holds a bare team id again
// with its provider's id (same key, so the row is replaced). The
// observation writers build the id with teamid.Of, so this is the id the
// next observation of the team writes.
func (run *teamIDCarryRun) planObservations(observations []chRow) teamIDCarryWrite {
	out := make([]chRow, 0, len(observations))
	for _, row := range observations {
		row = row.with("team_id", teamid.Of(row.str("provider"), row.str("team_id")))
		if parent := row.strPtr("parent_team_id"); parent != nil {
			if mapped, ok := run.primary[*parent]; ok {
				row = row.with("parent_team_id", &mapped)
			}
		}
		out = append(out, row.with("updated_at", teamIDCarryBump(run.at, row.time("updated_at"), time.Microsecond)))
	}
	run.outcome.Observations = uint64(len(out))
	return teamIDCarryWrite{table: "team_provider_observations", columns: teamIDCarryObservationsCols, rows: out}
}

// teamIDCarryGuard refuses a planned row that would write a team id with no
// provider key, or a malformed one (teamid.Malformed): an active team row, an open link row, a policy, a drift
// change or an observation of the new id.
func teamIDCarryGuard(write teamIDCarryWrite) error {
	for _, row := range write.rows {
		var ids []string
		switch write.table {
		case "teams":
			if row.u8("is_active") == 1 {
				ids = append(ids, row.str("id"))
			}
		case "team_memberships", "team_project_ownership", "team_repo_ownership":
			if row.timePtr("valid_to") == nil {
				ids = append(ids, row.str("team_id"))
			}
		case "team_sync_policies", "team_provider_observations":
			ids = append(ids, row.str("team_id"))
		case "team_drift_changes":
			if row.str("status") != teamDriftStatusSuperseded {
				ids = append(ids, row.str("entity_id"))
			}
		}
		for _, value := range ids {
			if !teamid.HasKey(value) || teamid.Malformed(value) {
				return fmt.Errorf("%w: %s", errTeamIDCarryUnkeyed, write.table)
			}
		}
	}
	return nil
}

// teamIDCarryInsertSettings keeps every row of a carry insert: with
// optimize_on_insert a block that holds two rows of one sorting key (the old
// rows of two providers' teams of one bare id, or of a team and its admin
// edit) is collapsed to one before it is written, and the other provider's
// older active row then stays the newest raw row of its group, so every
// later carry would move it again.
var teamIDCarryInsertSettings = clickhouse.Settings{"optimize_on_insert": 0}

func (run *teamIDCarryRun) write() error {
	ctx := clickhouse.Context(run.ctx, clickhouse.WithSettings(teamIDCarryInsertSettings))
	for _, write := range run.writes {
		if len(write.rows) == 0 {
			continue
		}
		batch, err := run.conn.PrepareBatch(ctx, "INSERT INTO "+write.table+" ("+write.columns+")")
		if err != nil {
			return fmt.Errorf("team id carry: write %s: %w", write.table, err)
		}
		for _, row := range write.rows {
			if err := batch.Append(row.vals...); err != nil {
				_ = batch.Abort()
				return fmt.Errorf("team id carry: write %s: %w", write.table, err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("team id carry: write %s: %w", write.table, err)
		}
		run.outcome.RowsWritten += uint64(len(write.rows))
	}
	return nil
}

// teamIDCarryBump is a version stamp past the stored one: `at`, or the
// stored stamp plus one tick when that is later.
func teamIDCarryBump(at, stored time.Time, tick time.Duration) time.Time {
	if next := stored.Add(tick); next.After(at) {
		return next
	}
	return at
}

func (run *teamIDCarryRun) read(query string, args ...any) ([]chRow, error) {
	rows, err := run.conn.Query(run.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	types := rows.ColumnTypes()
	index := make(map[string]int, len(types))
	for i, columnType := range types {
		index[columnType.Name()] = i
	}
	var out []chRow
	for rows.Next() {
		pointers := make([]any, len(types))
		for i, columnType := range types {
			pointers[i] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		values := make([]any, len(types))
		for i, pointer := range pointers {
			values[i] = reflect.ValueOf(pointer).Elem().Interface()
		}
		out = append(out, chRow{index: index, vals: values})
	}
	return out, rows.Err()
}

func (run *teamIDCarryRun) readSet(query string, ids []string) (map[string]bool, error) {
	set := map[string]bool{}
	rows, err := run.conn.Query(run.ctx, query, run.org, clickhouse.Named("ids", ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		set[value] = true
	}
	return set, rows.Err()
}

// chRow is one row read with the driver's own scan types, so it is written
// back with the same types.
type chRow struct {
	index map[string]int
	vals  []any
}

func (row chRow) value(column string) any {
	i, ok := row.index[column]
	if !ok {
		panic("team id carry: no column " + column)
	}
	return row.vals[i]
}

func (row chRow) str(column string) string {
	value, _ := row.value(column).(string)
	return value
}

func (row chRow) strPtr(column string) *string {
	value, _ := row.value(column).(*string)
	return value
}

func (row chRow) strs(column string) []string {
	value, _ := row.value(column).([]string)
	return value
}

func (row chRow) time(column string) time.Time {
	value, _ := row.value(column).(time.Time)
	return value
}

func (row chRow) timePtr(column string) *time.Time {
	value, _ := row.value(column).(*time.Time)
	return value
}

func (row chRow) u8(column string) uint8 {
	value, _ := row.value(column).(uint8)
	return value
}

// with returns a copy of the row with one column set.
func (row chRow) with(column string, value any) chRow {
	values := append([]any(nil), row.vals...)
	i, ok := row.index[column]
	if !ok {
		panic("team id carry: no column " + column)
	}
	values[i] = value
	return chRow{index: row.index, vals: values}
}

// CarryTeamIDsBeforeWrite runs CarryTeamIDs for the organization now and
// logs what moved; a write path calls it before it reads or writes a team
// id (see CarryTeamIDs). An error stops the path before it writes.
func CarryTeamIDsBeforeWrite(ctx context.Context, conn TeamIDCarryConn, orgID, writer string) error {
	outcome, err := CarryTeamIDs(ctx, conn, orgID, time.Now().UTC(), false)
	if err != nil {
		return err
	}
	if outcome.Found() || outcome.AdminTeamsNotCarried > 0 {
		slog.Default().InfoContext(ctx, "team_ids_carried", "writer", writer,
			"teams", outcome.Teams, "admin_teams", outcome.AdminTeams, "admin_teams_to_custom", outcome.AdminTeamsToCustom, "admin_teams_not_carried", outcome.AdminTeamsNotCarried,
			"ambiguous_teams", outcome.AmbiguousTeams, "teams_already_keyed", outcome.TeamsAlreadyKeyed,
			"memberships", outcome.Memberships, "project_ownership", outcome.ProjectOwnership, "repo_ownership", outcome.RepoOwnership,
			"link_rows_already_keyed", outcome.LinkRowsAlreadyKeyed, "observations", outcome.Observations,
			"sync_policies", outcome.SyncPolicies, "drift_changes", outcome.DriftChanges, "identity_drift_changes", outcome.IdentityDriftChanges, "identities", outcome.Identities,
			"fallbacks", outcome.Fallbacks, "rows_written", outcome.RowsWritten)
	}
	return nil
}
