package rivermigrate

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	apiExcessFailure = "migration failed: executed grant posture check found excess privileges for the api role after " +
		"go-river-migrate; see the preceding structured log line for each privilege and its grantor"
	apiExcessUnavailable = "migration failed: executed grant posture check could not read the api role's privileges after " +
		"go-river-migrate"
)

// checkAPIRoleExcess fails the run when the api role still holds a privilege
// beyond its posture after the apply: a grant by another grantor survives the
// leg's REVOKE, and go-api's readiness would refuse the role mid-roll. The
// decision is DiagnoseRolePosture's excess, the slice of go-api's readiness
// refusal an admin connection can read by role name; ownership, database and
// schema privileges, the River schema, undeclared sequences and functions,
// and column grants on undeclared relations stay readiness-only. Missing
// grants never fail here: the api role had no post-run check before, and
// readiness alone guards them.
func checkAPIRoleExcess(
	ctx context.Context,
	pool *pgxpool.Pool,
	role string,
	posture postgresstore.RolePosture,
	logger *slog.Logger,
	stderr io.Writer,
) bool {
	gaps, err := postgresstore.DiagnoseRolePosture(ctx, pool, role, posture)
	names := func(excess []postgresstore.PostureGap) ([]aclEntry, error) {
		return readExcessGrantors(ctx, pool, role, excess)
	}
	outcome := apiExcessOutcome(gaps, err, names)
	if outcome.ok {
		return true
	}
	if outcome.unavailable {
		logger.Error("api role posture excess check failed", "role", role, "reason", "diagnostic_unavailable")
		fmt.Fprintln(stderr, apiExcessUnavailable)
		return false
	}
	logger.Error("api role posture excess after apply", "role", role, "excess", outcome.lines)
	fmt.Fprintln(stderr, apiExcessFailure)
	return false
}

type apiExcessResult struct {
	ok          bool
	unavailable bool
	lines       []string
}

// apiExcessOutcome decides from DiagnoseRolePosture's answer alone; names
// only labels the excess and never changes the decision.
func apiExcessOutcome(
	gaps []postgresstore.PostureGap,
	err error,
	names func([]postgresstore.PostureGap) ([]aclEntry, error),
) apiExcessResult {
	if err != nil {
		return apiExcessResult{unavailable: true}
	}
	var excess []postgresstore.PostureGap
	for _, gap := range gaps {
		if len(gap.Excess) > 0 {
			excess = append(excess, gap)
		}
	}
	if len(excess) == 0 {
		return apiExcessResult{ok: true}
	}
	entries, namesErr := names(excess)
	return apiExcessResult{lines: apiExcessLines(excess, entries, namesErr)}
}

// aclEntry is one ACL entry on a public relation (Column "" = table level)
// that reaches the api role: granted to it, to a role it is a member of, or
// to PUBLIC.
type aclEntry struct {
	Table, Column    string
	Grantee, Grantor string
	Privilege        string
	Grantable        bool
}

func apiExcessLines(excess []postgresstore.PostureGap, entries []aclEntry, namesErr error) []string {
	lines := make([]string, 0, len(excess))
	for _, gap := range excess {
		if namesErr != nil {
			lines = append(lines, gap.String()+" (grantor read unavailable)")
			continue
		}
		var via []string
		for _, entry := range entries {
			if entry.Table != gap.TableName || (gap.ColumnName != "" && entry.Column != "" && entry.Column != gap.ColumnName) {
				continue
			}
			if !gapNamesPrivilege(gap, entry) {
				continue
			}
			where := ""
			if entry.Column != "" && gap.ColumnName == "" {
				where = " on column " + entry.Column
			}
			grantable := ""
			if entry.Grantable {
				grantable = " with grant option"
			}
			via = append(via, fmt.Sprintf("%s%s via %s granted by %s%s", entry.Privilege, where, entry.Grantee, entry.Grantor, grantable))
		}
		if len(via) == 0 {
			lines = append(lines, gap.String()+" (no ACL entry on the object reaches the role)")
			continue
		}
		lines = append(lines, gap.String()+" ("+strings.Join(via, "; ")+")")
	}
	return lines
}

// gapNamesPrivilege reports whether entry carries a privilege the gap names
// ("INSERT", "INSERT (column)", "SELECT WITH GRANT OPTION"); a grant-option
// gap matches only grantable entries.
func gapNamesPrivilege(gap postgresstore.PostureGap, entry aclEntry) bool {
	for _, named := range gap.Excess {
		base, _, _ := strings.Cut(named, " ")
		if base != entry.Privilege {
			continue
		}
		if strings.Contains(named, "WITH GRANT OPTION") && !entry.Grantable {
			continue
		}
		return true
	}
	return false
}

// excessGrantorsQuery lists, per named public relation (and column), the
// table-level and column-level ACL entries that reach role $1.
const excessGrantorsQuery = `
WITH wanted(table_name, column_name) AS (
	SELECT DISTINCT * FROM unnest($2::text[], $3::text[])
), objects AS (
	SELECT wanted.table_name, wanted.column_name, to_regclass(format('public.%I', wanted.table_name)) AS relation
	FROM wanted
), entries AS (
	SELECT objects.table_name, ''::text AS column_name, a.grantee, a.grantor, a.privilege_type, a.is_grantable
	FROM objects
	JOIN pg_catalog.pg_class AS c ON c.oid = objects.relation
	CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(c.relacl, pg_catalog.acldefault('r'::"char", c.relowner))) AS a
	UNION
	SELECT objects.table_name, att.attname::text, a.grantee, a.grantor, a.privilege_type, a.is_grantable
	FROM objects
	JOIN pg_catalog.pg_attribute AS att ON att.attrelid = objects.relation
		AND att.attnum > 0 AND NOT att.attisdropped AND att.attacl IS NOT NULL
		AND (objects.column_name = '' OR att.attname = objects.column_name)
	CROSS JOIN LATERAL pg_catalog.aclexplode(att.attacl) AS a
)
SELECT table_name, column_name,
	CASE WHEN grantee = 0 THEN 'PUBLIC' ELSE pg_catalog.pg_get_userbyid(grantee)::text END,
	pg_catalog.pg_get_userbyid(grantor)::text, privilege_type, is_grantable
FROM entries
WHERE CASE WHEN grantee = 0 THEN true ELSE pg_catalog.pg_has_role($1, grantee, 'MEMBER') END
ORDER BY 1, 2, 3, 4, 5
`

func readExcessGrantors(ctx context.Context, pool *pgxpool.Pool, role string, excess []postgresstore.PostureGap) ([]aclEntry, error) {
	tables := make([]string, len(excess))
	columns := make([]string, len(excess))
	for i, gap := range excess {
		tables[i], columns[i] = gap.TableName, gap.ColumnName
	}
	rows, err := pool.Query(ctx, excessGrantorsQuery, role, tables, columns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []aclEntry
	for rows.Next() {
		var entry aclEntry
		if err := rows.Scan(&entry.Table, &entry.Column, &entry.Grantee, &entry.Grantor, &entry.Privilege, &entry.Grantable); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}
