package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TableGrant declares one table's required ClickHouse privileges under a
// role's posture -- the ClickHouse analog of postgres.TablePrivilege.
type TableGrant struct {
	Database    string
	Table       string
	AllowInsert bool
	AllowSelect bool
	// AllowDelete is ALTER DELETE: there is no separate "DELETE" grant in
	// ClickHouse, a plain `DELETE FROM ... WHERE ...` (lightweight delete)
	// compiles onto the same mutation machinery ALTER TABLE ... DELETE
	// uses, and ALTER DELETE is the privilege it needs -- confirmed live
	// against a real server, not assumed.
	AllowDelete bool
}

// Posture is one role's declared privilege manifest: the ClickHouse analog
// of postgres.RolePosture.
type Posture struct {
	RequiredTables []TableGrant
}

// APIPosture is dho api's ClickHouse posture (CHAOS-6310): the dedicated
// write login internal/api/teamsidentity uses for the team + identity
// admin CRUD routes. Insert for create/update, select for every read
// (including the CHAOS-4321 manual_members-preserving read-before-write),
// alter delete for the lightweight DELETE FROM ... WHERE ... calls. No
// other table is in this manifest -- CheckAPIClickHouseAuthorization fails
// on any grant outside it, the same "no more, no less" discipline
// postgres.CheckAPIAuthorization already holds the Postgres api role to.
// database is the ClickHouse database the posture applies to ("default" in
// every real deployment; a venue oracle's own per-run database in tests).
func APIPosture(database string) Posture {
	return Posture{RequiredTables: []TableGrant{
		{Database: database, Table: "teams", AllowInsert: true, AllowSelect: true, AllowDelete: true},
		{Database: database, Table: "identities", AllowInsert: true, AllowSelect: true, AllowDelete: true},
		// POST /teams/import (CHAOS-6311) runs import_teams' drift-projector
		// write path: it reads a team's sync policy and its pending drift
		// changes, and writes provider observations and drift changes.
		// (Team drift review, CHAOS-6312, also reads the observations.)
		// Nothing else touches these tables through this login, and none
		// of them is ever deleted from here (status moves by inserting a
		// newer ReplacingMergeTree row).
		{Database: database, Table: "team_sync_policies", AllowSelect: true},
		{Database: database, Table: "team_provider_observations", AllowInsert: true, AllowSelect: true},
		{Database: database, Table: "team_drift_changes", AllowInsert: true, AllowSelect: true},
		// Team drift review (CHAOS-6312): approving an identity membership
		// change inserts the membership and expires the manual membership /
		// member fallback it conflicted with (a newer ReplacingMergeTree row),
		// and approving a team change reads the provider observation it
		// applies. Insert only: nothing here reads or deletes these tables.
		{Database: database, Table: "team_memberships", AllowInsert: true},
		{Database: database, Table: "manual_attribution_fallbacks", AllowInsert: true},
		// Organization activity for the login route's active-org choice
		// and GET /api/v1/auth/me/organizations (_load_org_activity): a
		// row count and the newest computed_at per org, read only.
		{Database: database, Table: "repo_metrics_daily", AllowSelect: true},
		{Database: database, Table: "user_metrics_daily", AllowSelect: true},
		{Database: database, Table: "team_metrics_daily", AllowSelect: true},
		{Database: database, Table: "work_item_metrics_daily", AllowSelect: true},
		// GET /backfill-jobs/{job_id} (CHAOS-6439) reads the backfill
		// window's metrics diagnostics: repos with data per day (from
		// repo_metrics_daily, granted above) and the latest repo-scope
		// complexity and compounding-risk rows. Read-only.
		{Database: database, Table: "repo_complexity_daily", AllowSelect: true},
		{Database: database, Table: "compounding_risk_daily", AllowSelect: true},
	}}
}

// ErrPostureMismatch is CheckPosture's refusal: the connected login's
// grants do not exactly equal the declared posture (missing, or extra).
var ErrPostureMismatch = errors.New("clickhouse: role posture does not match the declared manifest")

// showGrantsRowPattern parses one line of `SHOW GRANTS FOR CURRENT_USER`'s
// output: "GRANT <priv>[, <priv>...] ON <database>.<table> TO <user>".
// Confirmed live against a real server -- this is the server's own
// rendering, not a guessed shape.
var showGrantsRowPattern = regexp.MustCompile(`(?i)^GRANT\s+(.+?)\s+ON\s+([^\s.]+)\.([^\s]+)\s+TO\s+`)

// CheckAPIClickHouseAuthorization proves conn's connected user holds
// EXACTLY APIPosture's privilege manifest, scoped to whatever database the
// connection ACTUALLY names (queried via currentDatabase(), never
// hardcoded to "default" -- a deployment's API_CLICKHOUSE_URI names
// "default" today, but a test venue (internal/testsupport/venueoracle)
// legitimately names its own per-run database, and this check must be
// correct for both without special-casing either).
func CheckAPIClickHouseAuthorization(ctx context.Context, conn driver.Conn) error {
	if conn == nil {
		return ErrUnavailable
	}
	var database string
	if err := conn.QueryRow(ctx, "SELECT currentDatabase()").Scan(&database); err != nil {
		return fmt.Errorf("clickhouse: read current database: %w", err)
	}
	return CheckPosture(ctx, conn, APIPosture(database))
}

// GrantStatements renders posture as the literal `GRANT ... TO <role>` SQL
// text an operator runs once against the target ClickHouse server -- the
// single source both a deploy's grant recipe and this package's own
// provisioning in tests (internal/testsupport/venueoracle) render from, so
// the two can never drift apart. One statement per table (ClickHouse
// accepts a comma-separated privilege list in one GRANT).
func GrantStatements(role string, posture Posture) []string {
	statements := make([]string, 0, len(posture.RequiredTables))
	for _, table := range posture.RequiredTables {
		var privileges []string
		if table.AllowSelect {
			privileges = append(privileges, "SELECT")
		}
		if table.AllowInsert {
			privileges = append(privileges, "INSERT")
		}
		if table.AllowDelete {
			privileges = append(privileges, "ALTER DELETE")
		}
		if len(privileges) == 0 {
			continue
		}
		statements = append(statements, fmt.Sprintf("GRANT %s ON %s.%s TO %s",
			strings.Join(privileges, ", "), table.Database, table.Table, role))
	}
	return statements
}

// CheckPosture reads the connected user's own grants via
// `SHOW GRANTS FOR CURRENT_USER` (self-inspection needs no extra
// privilege -- unlike querying system.grants directly, which 403s without
// its own SELECT grant, confirmed live) and proves them EXACTLY equal to
// posture: every declared table carries every declared privilege and no
// other, and no table outside the manifest carries any grant at all. A
// grant line this parser cannot recognize (a database-wide or global
// grant, a role grant) is treated as an unexpected over-grant rather than
// silently ignored, so a manifest can never be satisfied by a privilege
// posture broader than what it declares.
func CheckPosture(ctx context.Context, conn driver.Conn, posture Posture) error {
	if conn == nil {
		return ErrUnavailable
	}
	rows, err := conn.Query(ctx, "SHOW GRANTS FOR CURRENT_USER")
	if err != nil {
		return fmt.Errorf("clickhouse: read current grants: %w", err)
	}
	defer rows.Close()

	held := map[string]map[string]bool{}
	var unrecognized []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return fmt.Errorf("clickhouse: scan grant row: %w", err)
		}
		// A grant carrying "WITH GRANT OPTION" is a broader privilege than
		// any declared posture asks for -- it lets the connected role grant
		// its own privileges on to others, which no TableGrant field
		// expresses. Treat it as unrecognized (an over-grant outside the
		// manifest) rather than letting showGrantsRowPattern's non-anchored
		// match silently accept the line and drop the suffix, which would
		// make an over-privileged grant indistinguishable from an exact
		// one -- confirmed live: `GRANT SELECT ON default.teams TO role
		// WITH GRANT OPTION` previously matched and passed unnoticed.
		if strings.Contains(strings.ToUpper(line), "WITH GRANT OPTION") {
			unrecognized = append(unrecognized, line)
			continue
		}
		match := showGrantsRowPattern.FindStringSubmatch(line)
		if match == nil {
			unrecognized = append(unrecognized, line)
			continue
		}
		privilegesText, database, table := match[1], unquoteIdent(match[2]), unquoteIdent(match[3])
		key := database + "." + table
		set, ok := held[key]
		if !ok {
			set = map[string]bool{}
			held[key] = set
		}
		for _, privilege := range strings.Split(privilegesText, ",") {
			set[strings.ToUpper(strings.TrimSpace(privilege))] = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("clickhouse: read grants: %w", err)
	}
	if len(unrecognized) > 0 {
		return fmt.Errorf("%w: unrecognized grant(s) outside the declared manifest: %s",
			ErrPostureMismatch, strings.Join(unrecognized, "; "))
	}

	expected := map[string]map[string]bool{}
	for _, table := range posture.RequiredTables {
		key := table.Database + "." + table.Table
		privileges := map[string]bool{}
		if table.AllowInsert {
			privileges["INSERT"] = true
		}
		if table.AllowSelect {
			privileges["SELECT"] = true
		}
		if table.AllowDelete {
			privileges["ALTER DELETE"] = true
		}
		expected[key] = privileges
	}

	var problems []string
	for key, wantPrivileges := range expected {
		gotPrivileges := held[key]
		for privilege := range wantPrivileges {
			if !gotPrivileges[privilege] {
				problems = append(problems, fmt.Sprintf("%s missing %s", key, privilege))
			}
		}
		for privilege := range gotPrivileges {
			if !wantPrivileges[privilege] {
				problems = append(problems, fmt.Sprintf("%s has unexpected %s", key, privilege))
			}
		}
	}
	for key := range held {
		if _, declared := expected[key]; !declared {
			problems = append(problems, fmt.Sprintf("%s has grants but is not in the declared manifest", key))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrPostureMismatch, strings.Join(problems, "; "))
	}
	return nil
}

// unquoteIdent strips ClickHouse's backtick identifier quoting
// (SHOW GRANTS backtick-quotes a database/table name only when it needs
// to -- a reserved word or one containing special characters; neither
// "default", "teams" nor "identities" ever does, confirmed live, but a
// future manifest entry might).
func unquoteIdent(identifier string) string {
	return strings.Trim(identifier, "`")
}
