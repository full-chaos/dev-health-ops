package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostureGap is one requirement of a RolePosture the connected login does
// not currently satisfy. It is built entirely from checked-in, non-secret
// schema identifiers (table and column names this repository already
// declares in domainPosture/coordinatorPosture) and standard SQL privilege
// keywords -- never a DSN, host, credential, or any catalog detail beyond
// what the declared posture itself already names. Safe to log.
type PostureGap struct {
	TableName    string
	ColumnName   string // empty for a table-wide requirement
	TableMissing bool
	Missing      []string // e.g. ["INSERT", "UPDATE"]; ["SELECT"] for a column-scoped gap
}

// String renders one gap as a single redacted line, safe to place in a log
// message.
func (gap PostureGap) String() string {
	if gap.TableMissing {
		if gap.ColumnName != "" {
			return fmt.Sprintf("%s.%s: table does not exist", gap.TableName, gap.ColumnName)
		}
		return fmt.Sprintf("%s: table does not exist", gap.TableName)
	}
	if gap.ColumnName != "" {
		return fmt.Sprintf("%s.%s: missing %v", gap.TableName, gap.ColumnName, gap.Missing)
	}
	return fmt.Sprintf("%s: missing %v", gap.TableName, gap.Missing)
}

// diagnoseTablePostureQuery re-derives, per required table, whether it
// exists and which of its required table-wide privileges the given role
// currently lacks. It deliberately duplicates a small slice of
// rolePostureQuery's logic rather than generalizing that hardened,
// security-critical query to also return row-level diagnostics:
// rolePostureQuery's job is a single trustworthy boolean that readiness
// gates depend on, and this query's job is a human-readable explanation for
// when that boolean is false. Keeping them separate means a bug in this
// diagnostic-only path can never weaken the boolean the readiness gate
// itself checks.
const diagnoseTablePostureQuery = `
WITH required(table_name, allow_insert, allow_update, allow_delete) AS (
	SELECT * FROM unnest($2::text[], $3::boolean[], $4::boolean[], $5::boolean[])
), resolved AS (
	SELECT
		required.table_name,
		required.allow_insert,
		required.allow_update,
		required.allow_delete,
		to_regclass('public.' || required.table_name) AS relation
	FROM required
)
SELECT
	table_name,
	relation IS NULL AS table_missing,
	relation IS NOT NULL AND NOT has_table_privilege($1, relation, 'SELECT') AS missing_select,
	relation IS NOT NULL AND allow_insert AND NOT has_table_privilege($1, relation, 'INSERT') AS missing_insert,
	relation IS NOT NULL AND allow_update AND NOT has_table_privilege($1, relation, 'UPDATE') AS missing_update,
	relation IS NOT NULL AND allow_delete AND NOT has_table_privilege($1, relation, 'DELETE') AS missing_delete
FROM resolved
ORDER BY table_name
`

// diagnoseColumnPostureQuery is diagnoseTablePostureQuery's column-scoped
// counterpart, for the same reason and with the same separation from
// rolePostureQuery.
const diagnoseColumnPostureQuery = `
WITH required(table_name, column_name, privilege) AS (
	SELECT * FROM unnest($2::text[], $3::text[], $4::text[])
), resolved AS (
	SELECT
		required.table_name,
		required.column_name,
		required.privilege,
		to_regclass('public.' || required.table_name) AS relation
	FROM required
)
SELECT
	table_name,
	column_name,
	privilege,
	relation IS NULL AS table_missing,
	relation IS NOT NULL AND NOT has_column_privilege($1, relation, column_name, privilege) AS missing
FROM resolved
ORDER BY table_name, column_name, privilege
`

// DiagnoseRolePosture re-checks a RolePosture's requirements individually
// and returns the subset the connected login does not currently satisfy. It
// is deliberately NOT part of CheckRolePosture's hot readiness path: it
// issues two additional queries re-deriving what CheckRolePosture's single
// hardened boolean already computed, at higher cost and with a smaller
// trusted surface, so callers should use it only after CheckRolePosture has
// already reported failure -- purely to explain why, in a form safe to log.
//
// It does NOT prove the inverse of CheckRolePosture (that the role holds
// nothing beyond its posture): an empty result here means every declared
// requirement is individually satisfied, which usually but not always means
// CheckRolePosture would now pass. If CheckRolePosture still fails against
// an empty diagnostic here, the role holds something it must not (an excess
// grant), which this function does not attempt to detect -- identifying
// that would mean walking effective privileges on every relation in the
// schema against the manifest, the same work rolePostureQuery's catch-all
// predicates already do inside its single trustworthy boolean, and
// duplicating that here would be exactly the drift risk CheckRolePosture's
// own package doc warns against.
func DiagnoseRolePosture(
	ctx context.Context,
	pool *pgxpool.Pool,
	expectedRole string,
	posture RolePosture,
) ([]PostureGap, error) {
	if pool == nil || !validRuntimeIdentifier(expectedRole) {
		return nil, ErrUnavailable
	}
	var gaps []PostureGap

	if len(posture.RequiredTables) > 0 {
		tableGaps, err := diagnoseTablePosture(ctx, pool, expectedRole, posture.RequiredTables)
		if err != nil {
			return nil, err
		}
		gaps = append(gaps, tableGaps...)
	}

	if len(posture.ColumnScoped) > 0 {
		columnGaps, err := diagnoseColumnPosture(ctx, pool, expectedRole, posture.ColumnScoped)
		if err != nil {
			return nil, err
		}
		gaps = append(gaps, columnGaps...)
	}

	return gaps, nil
}

func diagnoseTablePosture(
	ctx context.Context,
	pool *pgxpool.Pool,
	expectedRole string,
	tables []TablePrivilege,
) ([]PostureGap, error) {
	tableNames := make([]string, len(tables))
	allowInserts := make([]bool, len(tables))
	allowUpdates := make([]bool, len(tables))
	allowDeletes := make([]bool, len(tables))
	for i, table := range tables {
		tableNames[i] = table.TableName
		allowInserts[i] = table.AllowInsert
		allowUpdates[i] = table.AllowUpdate
		allowDeletes[i] = table.AllowDelete
	}
	rows, err := pool.Query(
		ctx, diagnoseTablePostureQuery, expectedRole, tableNames, allowInserts, allowUpdates, allowDeletes,
	)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()

	var gaps []PostureGap
	for rows.Next() {
		var (
			tableName                                                                string
			tableMissing, missingSelect, missingInsert, missingUpdate, missingDelete bool
		)
		if err := rows.Scan(
			&tableName, &tableMissing, &missingSelect, &missingInsert, &missingUpdate, &missingDelete,
		); err != nil {
			return nil, ErrUnavailable
		}
		if tableMissing {
			gaps = append(gaps, PostureGap{TableName: tableName, TableMissing: true})
			continue
		}
		var missing []string
		if missingSelect {
			missing = append(missing, "SELECT")
		}
		if missingInsert {
			missing = append(missing, "INSERT")
		}
		if missingUpdate {
			missing = append(missing, "UPDATE")
		}
		if missingDelete {
			missing = append(missing, "DELETE")
		}
		if len(missing) > 0 {
			gaps = append(gaps, PostureGap{TableName: tableName, Missing: missing})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, ErrUnavailable
	}
	return gaps, nil
}

func diagnoseColumnPosture(
	ctx context.Context,
	pool *pgxpool.Pool,
	expectedRole string,
	columns []ColumnPrivilege,
) ([]PostureGap, error) {
	tableNames := make([]string, len(columns))
	columnNames := make([]string, len(columns))
	privileges := make([]string, len(columns))
	for i, column := range columns {
		tableNames[i] = column.TableName
		columnNames[i] = column.ColumnName
		privileges[i] = column.Privilege
	}
	rows, err := pool.Query(ctx, diagnoseColumnPostureQuery, expectedRole, tableNames, columnNames, privileges)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()

	var gaps []PostureGap
	for rows.Next() {
		var (
			tableName, columnName, privilege string
			tableMissing, missing            bool
		)
		if err := rows.Scan(&tableName, &columnName, &privilege, &tableMissing, &missing); err != nil {
			return nil, ErrUnavailable
		}
		if tableMissing {
			gaps = append(gaps, PostureGap{TableName: tableName, ColumnName: columnName, TableMissing: true})
			continue
		}
		if missing {
			gaps = append(gaps, PostureGap{TableName: tableName, ColumnName: columnName, Missing: []string{privilege}})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, ErrUnavailable
	}
	return gaps, nil
}
