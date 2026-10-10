package providersync

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The roster column `members` of the teams table is dropped (CHAOS-9087): a
// person's team comes from team_memberships. Before the column goes, the
// members of an admin-made team that exist ONLY in the column move into
// team_memberships, so none is lost. A team an integration made (a provider
// team) is not moved: its roster is a copy of the memberships of the same sync
// run, and a roster facet with no open membership row is a person the
// provider no longer lists, not a member to keep.
//
// The step is one function with run-time guards: it refuses a column that is
// gone (nothing to move), a count above the bound, and it proves the result
// (the open membership rows grew by exactly the rows it wrote, and no admin
// facet is left uncovered).

// ErrTeamRosterMoveTooMany is the refusal of a move above the bound: the
// rows are not written.
var ErrTeamRosterMoveTooMany = errors.New("providersync: the admin team roster to move is above the bound")

// ErrTeamRosterMoveNotProven is the failure of the proof after the write: the
// open membership rows did not grow by the rows written, or an admin facet
// is still uncovered. The rows already written stay (a re-run moves the rest).
var ErrTeamRosterMoveNotProven = errors.New("providersync: the admin team roster move is not proven")

// TeamRosterMoveBound is the most facets one run moves.
const TeamRosterMoveBound = 100000

// A moved entry carries a provenance that names its origin and never outranks a
// native row: provider `teams_roster` (migrated from the roster column of the
// teams table: no work item has this provider, so it attributes no work item),
// source `inferred` (derived, not stated by a person or by a provider; the
// membership conflict guard pins only `manual` rows, so a moved entry pins
// nothing), not primary, specificity 0 and priority 1000 (candidates rank
// primary first, then the more specific, then the lower priority number). A
// person who already holds an open native row (any team) gets no second row.
// The step reads the roster of ADMIN-made teams only.
const (
	teamRosterMoveSource   = "inferred"
	teamRosterMoveProvider = "teams_roster"
)

const teamRosterColumnQuery = `SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'teams' AND name = 'members'`

// teamRosterOpenKeys are the identities with an OPEN membership row in each
// team at {at}: the member id, the raw e-mail, the raw provider user id and
// every identity facet of the row, trimmed and lower-cased.
const teamRosterOpenKeys = `
open_keys AS (
  SELECT team_id, src, lower(trimBoth(k)) AS k FROM (
    SELECT team_id, toString(source) AS src, arrayJoin(arrayConcat([member_id, ifNull(raw_email, ''), ifNull(raw_provider_user_id, '')], identity_facets)) AS k
    FROM team_memberships FINAL
    WHERE org_id = {org_id:String} AND valid_from <= {at:DateTime64(3, 'UTC')}
      AND (valid_to IS NULL OR valid_to > {at:DateTime64(3, 'UTC')})
  ) WHERE k != ''
),
native_keys AS (
  SELECT DISTINCT k FROM open_keys WHERE src IN ('native', 'jira_legacy', 'provider_access')
)`

// teamRosterFacets are the distinct roster facets of the ACTIVE teams, one row
// per (team, lower-cased facet), with the team's provider and whether an open
// membership row of the same team covers the facet.
const teamRosterFacets = `
roster AS (
  SELECT id AS team_id, any(provider) AS provider, lower(trimBoth(f)) AS norm, any(trimBoth(f)) AS facet
  FROM teams FINAL ARRAY JOIN members AS f
  WHERE org_id = {org_id:String} AND is_active = 1 AND trimBoth(f) != ''
  GROUP BY id, norm
)`

const teamRosterCountQuery = `WITH` + teamRosterOpenKeys + `,` + teamRosterFacets + `
SELECT
  count() AS facets,
  countIf((team_id, norm) IN (SELECT team_id, k FROM open_keys)) AS covered,
  countIf((team_id, norm) NOT IN (SELECT team_id, k FROM open_keys) AND provider = '' AND norm IN (SELECT k FROM native_keys)) AS native_elsewhere,
  countIf((team_id, norm) NOT IN (SELECT team_id, k FROM open_keys) AND provider = '' AND norm NOT IN (SELECT k FROM native_keys)) AS admin_to_move,
  countIf((team_id, norm) NOT IN (SELECT team_id, k FROM open_keys) AND provider != '') AS provider_only,
  uniqExactIf(team_id, (team_id, norm) NOT IN (SELECT team_id, k FROM open_keys) AND provider = '' AND norm NOT IN (SELECT k FROM native_keys)) AS teams_to_move
FROM roster`

const teamRosterOpenCountQuery = `
SELECT count() FROM team_memberships FINAL
WHERE org_id = {org_id:String} AND valid_from <= {at:DateTime64(3, 'UTC')}
  AND (valid_to IS NULL OR valid_to > {at:DateTime64(3, 'UTC')})`

const teamRosterMoveInsert = `INSERT INTO team_memberships
  (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
WITH` + teamRosterOpenKeys + `,` + teamRosterFacets + `
SELECT {org_id:String}, '` + teamRosterMoveProvider + `', team_id, facet, CAST(NULL AS Nullable(String)),
       if(position(facet, '@') > 0, facet, CAST(NULL AS Nullable(String))), [facet],
       '` + teamRosterMoveSource + `', 0, 0, 1000,
       {at:DateTime64(3, 'UTC')}, CAST(NULL AS Nullable(DateTime64(3, 'UTC'))), {at:DateTime64(3, 'UTC')}
FROM roster
WHERE (team_id, norm) NOT IN (SELECT team_id, k FROM open_keys) AND provider = ''
  AND norm NOT IN (SELECT k FROM native_keys)`

// TeamRosterMoveOutcome is counts only: no id, e-mail or name of a team or a
// person leaves the store through it.
type TeamRosterMoveOutcome struct {
	DryRun bool `json:"dry_run"`
	// ColumnPresent is false once the roster column is dropped: there is
	// nothing to move and nothing is written.
	ColumnPresent bool `json:"column_present"`
	// RosterFacets is the distinct (team, person) roster entries of the active
	// teams; Covered are those with an open membership row of the same team.
	RosterFacets uint64 `json:"roster_facets"`
	Covered      uint64 `json:"covered"`
	// NativeElsewhere are the uncovered entries of admin-made teams whose person
	// holds an open native membership in some team: not moved (no second row).
	NativeElsewhere uint64 `json:"native_elsewhere"`
	// AdminToMove are the other uncovered entries of admin-made teams (written
	// as inferred memberships); TeamsToMove the teams they belong to.
	AdminToMove uint64 `json:"admin_to_move"`
	TeamsToMove uint64 `json:"teams_to_move"`
	// ProviderRosterOnly are the uncovered entries of provider teams: a person
	// the provider no longer lists. They are not moved.
	ProviderRosterOnly uint64 `json:"provider_roster_only"`
	// OpenBefore/OpenAfter are the open membership rows of the organization
	// before and after the write (equal in a dry run).
	OpenBefore uint64 `json:"open_memberships_before"`
	OpenAfter  uint64 `json:"open_memberships_after"`
	// Moved is what a real run wrote; 0 in a dry run.
	Moved uint64 `json:"moved"`
}

// MoveAdminTeamRosterToMemberships moves the roster facets of the admin-made
// teams of one organization that no open membership row covers into
// team_memberships (source inferred, valid from `at`). See the file comment for
// the guards. A dry run counts and writes nothing; with nothing to move a real
// run is the count reads and no write, so a second run reports zero.
func MoveAdminTeamRosterToMemberships(
	ctx context.Context, conn driver.Conn, orgID string, at time.Time, dryRun bool,
) (TeamRosterMoveOutcome, error) {
	orgID = strings.TrimSpace(orgID)
	if ctx == nil || conn == nil || orgID == "" || at.IsZero() {
		return TeamRosterMoveOutcome{}, ErrInvalidConfiguration
	}
	org := clickhouse.Named("org_id", orgID)
	stamp := clickhouse.Named("at", at.UTC().Truncate(time.Millisecond).Format("2006-01-02 15:04:05.000"))
	outcome := TeamRosterMoveOutcome{DryRun: dryRun}

	var column uint64
	if err := conn.QueryRow(ctx, teamRosterColumnQuery).Scan(&column); err != nil {
		return TeamRosterMoveOutcome{}, err
	}
	if column == 0 {
		return outcome, nil
	}
	outcome.ColumnPresent = true

	if err := conn.QueryRow(ctx, teamRosterCountQuery, org, stamp).Scan(
		&outcome.RosterFacets, &outcome.Covered, &outcome.NativeElsewhere, &outcome.AdminToMove, &outcome.ProviderRosterOnly, &outcome.TeamsToMove,
	); err != nil {
		return TeamRosterMoveOutcome{}, err
	}
	if err := conn.QueryRow(ctx, teamRosterOpenCountQuery, org, stamp).Scan(&outcome.OpenBefore); err != nil {
		return TeamRosterMoveOutcome{}, err
	}
	outcome.OpenAfter = outcome.OpenBefore
	if dryRun || outcome.AdminToMove == 0 {
		return outcome, nil
	}
	if outcome.AdminToMove > TeamRosterMoveBound {
		return outcome, ErrTeamRosterMoveTooMany
	}

	if err := conn.Exec(ctx, teamRosterMoveInsert, org, stamp); err != nil {
		return outcome, err
	}
	outcome.Moved = outcome.AdminToMove

	// The proof is a fresh read, never the insert's own count: the open rows
	// grew by exactly the rows written, and no admin facet is uncovered.
	if err := conn.QueryRow(ctx, teamRosterOpenCountQuery, org, stamp).Scan(&outcome.OpenAfter); err != nil {
		return outcome, err
	}
	var facets, covered, nativeElsewhere, left, providerOnly, teams uint64
	if err := conn.QueryRow(ctx, teamRosterCountQuery, org, stamp).Scan(&facets, &covered, &nativeElsewhere, &left, &providerOnly, &teams); err != nil {
		return outcome, err
	}
	if outcome.OpenAfter != outcome.OpenBefore+outcome.Moved || left != 0 {
		return outcome, ErrTeamRosterMoveNotProven
	}
	return outcome, nil
}
