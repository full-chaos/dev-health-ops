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
	// Excess names table-wide privileges the role holds on a table this
	// posture declares column-scoped only (CHAOS-4675). Mutually exclusive
	// with Missing/TableMissing on any one gap: a table cannot both not
	// exist and carry an excess privilege on it.
	Excess []string
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
	if len(gap.Excess) > 0 {
		return fmt.Sprintf("%s: excess table-wide privileges %v (declared column-scoped only)", gap.TableName, gap.Excess)
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

// diagnoseSequencePostureQuery is diagnoseTablePostureQuery's sequence-scoped
// counterpart. Every RolePosture.RequiredSequences entry this repository
// declares is granted USAGE only (coordinatorGrantStatements'
// `GRANT USAGE ON SEQUENCE ...`), matching rolePostureQuery's own
// satisfaction predicate (`NOT has_sequence_privilege(current_user, oid,
// 'USAGE')` is what makes a sequence requirement unmet) -- so USAGE is the
// only privilege this diagnostic checks.
const diagnoseSequencePostureQuery = `
WITH required(sequence_name) AS (
	SELECT * FROM unnest($2::text[])
), resolved AS (
	SELECT
		required.sequence_name,
		to_regclass('public.' || required.sequence_name) AS relation
	FROM required
)
SELECT
	sequence_name,
	relation IS NULL AS sequence_missing,
	relation IS NOT NULL AND NOT has_sequence_privilege($1, relation, 'USAGE') AS missing
FROM resolved
ORDER BY sequence_name
`

// diagnoseColumnScopedExcessQuery is the admin-callable, role-NAME-parameterized
// counterpart of rolePostureQuery's "no table-wide privilege leakage on a
// column-scoped table" predicate (domain_authorization.go's
// column_scoped_relations clause). It exists for CHAOS-4675: go-river-migrate's
// executed-proof gate (checkExecutedGrantPosture, cmd/dev-health-worker-migrate)
// calls DiagnoseRolePosture from an admin connection that cannot open a session
// AS the runtime role, so it cannot call CheckRolePosture/rolePostureQuery
// itself (that query is deliberately current_user-bound). Before this query
// existed, DiagnoseRolePosture only ever asked "is anything declared missing",
// so a coordinator role that also held a stray TABLE-WIDE grant on a table
// this posture declares column-scoped (e.g. a leftover from before the table
// was split to column-scoped, or a hand-run admin GRANT) read as zero gaps --
// "posture confirmed" -- from this gate, while CheckCoordinatorAuthorization
// still correctly rejected the same role at every runtime binary's own
// startup. That silent disagreement, with both sides individually reporting
// success, is CHAOS-4675: the granter/checker pair can drift on privilege
// GRANULARITY (table-wide vs column-scoped) for the same table without the
// migrate-time telemetry ever naming it.
//
// has_table_privilege accepts an explicit role name (it is not restricted to
// current_user the way the ownership/ambient-privilege checks in
// rolePostureQuery are), so this reuses the identical seven-privilege sweep
// (MAINTAIN gated the same way, by server_version_num) against an admin
// connection bound to the role BY NAME rather than by session identity.
const diagnoseColumnScopedExcessQuery = `
WITH required(table_name) AS (
	SELECT DISTINCT * FROM unnest($2::text[])
), resolved AS (
	SELECT
		required.table_name,
		to_regclass('public.' || required.table_name) AS relation
	FROM required
)
SELECT
	table_name,
	relation IS NOT NULL AND has_table_privilege($1, relation, 'SELECT') AS excess_select,
	relation IS NOT NULL AND has_table_privilege($1, relation, 'INSERT') AS excess_insert,
	relation IS NOT NULL AND has_table_privilege($1, relation, 'UPDATE') AS excess_update,
	relation IS NOT NULL AND has_table_privilege($1, relation, 'DELETE') AS excess_delete,
	relation IS NOT NULL AND has_table_privilege($1, relation, 'TRUNCATE') AS excess_truncate,
	relation IS NOT NULL AND has_table_privilege($1, relation, 'REFERENCES') AS excess_references,
	relation IS NOT NULL AND has_table_privilege($1, relation, 'TRIGGER') AS excess_trigger,
	relation IS NOT NULL
		AND current_setting('server_version_num')::integer >= 170000
		AND has_table_privilege($1, relation, 'MAINTAIN') AS excess_maintain
FROM resolved
ORDER BY table_name
`

// DiagnoseRolePosture re-checks a RolePosture's requirements individually
// (tables, column-scoped privileges, AND required sequences) and returns
// the subset the connected login does not currently satisfy, PLUS
// (CHAOS-4675) any table-wide privilege excess on a table this posture
// declares column-scoped only. It is deliberately NOT part of
// CheckRolePosture's hot readiness path: it issues additional queries
// re-deriving what CheckRolePosture's single hardened boolean already
// computed, at higher cost and with a smaller trusted surface, so callers
// should use it only after CheckRolePosture has already reported failure --
// purely to explain why, in a form safe to log.
//
// It still does NOT prove the FULL inverse of CheckRolePosture (that the
// role holds nothing beyond its posture, in every sense rolePostureQuery's
// catch-all predicates check: ownership, database/schema-level ambient
// privileges, River-schema leakage, or table-wide excess on a table this
// posture does not mention at all). Column-scoped-table excess is the one
// slice of that property this function now covers, because it is exactly
// the slice CHAOS-4675 proved can drift silently: a granter/checker pair
// that agrees a table is column-scoped can still disagree, over time, about
// whether the role also holds a table-wide grant on it, and unlike a
// missing grant (which every runtime binary's own startup check catches
// loudly) an excess grant here only fails LATER, in a way this diagnostic
// used to read as "posture confirmed". Every other excess-privilege route
// remains something only CheckRolePosture itself proves, from a real
// session as that role -- identifying those here would mean walking
// effective privileges on every relation in the schema against the
// manifest, the same work rolePostureQuery's catch-all predicates already
// do inside its single trustworthy boolean, and duplicating that here would
// be exactly the drift risk CheckRolePosture's own package doc warns
// against.
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

		excessGaps, err := diagnoseColumnScopedExcess(ctx, pool, expectedRole, posture.ColumnScoped)
		if err != nil {
			return nil, err
		}
		gaps = append(gaps, excessGaps...)
	}

	if len(posture.RequiredSequences) > 0 {
		sequenceGaps, err := diagnoseSequencePosture(ctx, pool, expectedRole, posture.RequiredSequences)
		if err != nil {
			return nil, err
		}
		gaps = append(gaps, sequenceGaps...)
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

// diagnoseSequencePosture re-derives, per required sequence, whether it
// exists and whether the given role currently holds USAGE on it. Added for
// CHAOS-4261: the executed-proof gate in cmd/dev-health-worker-migrate
// (checkExecutedGrantPosture) called DiagnoseRolePosture expecting it to
// prove the FULL declared posture, but RequiredSequences (coordinatorPosture's
// worker_operator_audits_id_seq) was silently never checked -- a database
// missing that sequence's grant, with every table and column requirement
// otherwise satisfied, reported zero gaps here while
// CheckCoordinatorAuthorization (the real, current_user-bound readiness
// check every coordinator-role process gates on at startup) still failed.
func diagnoseSequencePosture(
	ctx context.Context,
	pool *pgxpool.Pool,
	expectedRole string,
	sequences []string,
) ([]PostureGap, error) {
	rows, err := pool.Query(ctx, diagnoseSequencePostureQuery, expectedRole, sequences)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()

	var gaps []PostureGap
	for rows.Next() {
		var (
			sequenceName             string
			sequenceMissing, missing bool
		)
		if err := rows.Scan(&sequenceName, &sequenceMissing, &missing); err != nil {
			return nil, ErrUnavailable
		}
		if sequenceMissing {
			gaps = append(gaps, PostureGap{TableName: sequenceName, TableMissing: true})
			continue
		}
		if missing {
			gaps = append(gaps, PostureGap{TableName: sequenceName, Missing: []string{"USAGE"}})
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

// diagnoseColumnScopedExcess is diagnoseColumnScopedExcessQuery's Go side: for
// every distinct table named in a posture's ColumnScoped entries, it reports
// any table-wide privilege the role holds on that table as an excess gap
// (CHAOS-4675). A table declared column-scoped is never also supposed to
// carry a table-wide grant -- coordinatorGrantStatements enforces that by
// construction going forward (a table cannot appear in both
// CoordinatorGrants and CoordinatorColumnGrants, see ValidateMigrationOptions),
// but this diagnostic is what notices the database has drifted from that
// invariant regardless of how -- a leftover grant from before a table was
// split to column-scoped, or one applied by hand outside the migration.
func diagnoseColumnScopedExcess(
	ctx context.Context,
	pool *pgxpool.Pool,
	expectedRole string,
	columns []ColumnPrivilege,
) ([]PostureGap, error) {
	seen := make(map[string]struct{}, len(columns))
	tableNames := make([]string, 0, len(columns))
	for _, column := range columns {
		if _, ok := seen[column.TableName]; ok {
			continue
		}
		seen[column.TableName] = struct{}{}
		tableNames = append(tableNames, column.TableName)
	}
	rows, err := pool.Query(ctx, diagnoseColumnScopedExcessQuery, expectedRole, tableNames)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()

	var gaps []PostureGap
	for rows.Next() {
		var (
			tableName                                                       string
			excessSelect, excessInsert, excessUpdate, excessDelete          bool
			excessTruncate, excessReferences, excessTrigger, excessMaintain bool
		)
		if err := rows.Scan(
			&tableName, &excessSelect, &excessInsert, &excessUpdate, &excessDelete,
			&excessTruncate, &excessReferences, &excessTrigger, &excessMaintain,
		); err != nil {
			return nil, ErrUnavailable
		}
		var excess []string
		if excessSelect {
			excess = append(excess, "SELECT")
		}
		if excessInsert {
			excess = append(excess, "INSERT")
		}
		if excessUpdate {
			excess = append(excess, "UPDATE")
		}
		if excessDelete {
			excess = append(excess, "DELETE")
		}
		if excessTruncate {
			excess = append(excess, "TRUNCATE")
		}
		if excessReferences {
			excess = append(excess, "REFERENCES")
		}
		if excessTrigger {
			excess = append(excess, "TRIGGER")
		}
		if excessMaintain {
			excess = append(excess, "MAINTAIN")
		}
		if len(excess) > 0 {
			gaps = append(gaps, PostureGap{TableName: tableName, Excess: excess})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, ErrUnavailable
	}
	return gaps, nil
}
