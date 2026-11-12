package authschema

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// MigratorPublicRowsSQL lists every privilege row the role $1 holds on a table of the public schema, table-level and
// column-level: tbl, col (” for a table-level row), priv, grantable, the grantor, the table owner, and whether the
// owner is the grantor. The owner-move revoke and the production read-back both run it.
const MigratorPublicRowsSQL = `
SELECT tbl, col, priv, grantable, grantor, owner, qtbl, qcol, qowner FROM (
  SELECT c.relname::text AS tbl, ''::text AS col, a.privilege_type AS priv, a.is_grantable AS grantable,
         gr.rolname::text AS grantor, ow.rolname::text AS owner,
         quote_ident(c.relname) AS qtbl, ''::text AS qcol, quote_ident(ow.rolname) AS qowner
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_roles ow ON ow.oid = c.relowner
  CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a JOIN pg_roles gr ON gr.oid = a.grantor
  WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND a.grantee = (SELECT oid FROM pg_roles WHERE rolname = $1)
  UNION ALL
  SELECT c.relname::text, att.attname::text, a.privilege_type, a.is_grantable, gr.rolname::text, ow.rolname::text,
         quote_ident(c.relname), quote_ident(att.attname), quote_ident(ow.rolname)
  FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_roles ow ON ow.oid = c.relowner
  CROSS JOIN LATERAL aclexplode(att.attacl) a JOIN pg_roles gr ON gr.oid = a.grantor
  WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND att.attnum > 0 AND NOT att.attisdropped
    AND a.grantee = (SELECT oid FROM pg_roles WHERE rolname = $1)
) s ORDER BY tbl, col, priv`

// migratorDependentRowsSQL lists the privilege rows (public table, privilege, quoted column or ”) the role $1 granted
// to another role: a REVOKE of the role's own matching privilege is refused (RESTRICT) or takes those rows away
// (CASCADE).
const migratorDependentRowsSQL = `
SELECT tbl, priv, qcol FROM (
  SELECT c.relname::text AS tbl, a.privilege_type AS priv, ''::text AS qcol
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
  WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND a.grantor = (SELECT oid FROM pg_roles WHERE rolname = $1) AND a.grantee <> a.grantor
  UNION ALL
  SELECT c.relname::text, a.privilege_type, quote_ident(att.attname)
  FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(att.attacl) a
  WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND NOT att.attisdropped
    AND a.grantor = (SELECT oid FROM pg_roles WHERE rolname = $1) AND a.grantee <> a.grantor
) s`

// dependentRow is a privilege another role holds from the migrator, or one this run gives it.
type dependentRow struct{ table, privilege, quotedColumn string }

// blocks reports whether a revoke of the migrator's row r leaves this row without its grantor: the same table and
// privilege, and the same column or either side table-level.
func (d dependentRow) blocks(r MigratorPublicRow) bool {
	return d.table == r.Table && d.privilege == r.Privilege && (d.quotedColumn == "" || r.Column == "" || d.quotedColumn == r.quotedCol)
}

// ReadbackQueryText is MigratorPublicRowsSQL for the named role as a literal, the text a read-only session runs on a
// database to list what the migration login holds in the public schema.
func ReadbackQueryText(role string) string {
	return strings.ReplaceAll(MigratorPublicRowsSQL, "$1", "'"+strings.ReplaceAll(role, "'", "''")+"'")
}

// MigratorPublicRow is one privilege row of the migration login on a public table.
type MigratorPublicRow struct {
	Table, Column, Privilege string
	Grantable                bool
	Grantor, Owner           string
	quotedTable, quotedCol   string
	quotedOwner              string
}

// Key is table|privilege|column, the form of the privilege set's keys.
func (r MigratorPublicRow) Key() string { return r.Table + "|" + r.Privilege + "|" + r.Column }

// MigratorRowsOutsideTheSet returns the rows that are neither a privilege of PublicPrivilegeSet nor the anchor
// privilege on a table of the set.
func MigratorRowsOutsideTheSet(rows []MigratorPublicRow) []MigratorPublicRow {
	allowed := map[string]bool{}
	for _, entry := range PublicPrivilegeSet() {
		if entry.Columns == nil {
			allowed[entry.Table+"|"+entry.Privilege+"|"] = true
			continue
		}
		for _, column := range entry.Columns {
			allowed[entry.Table+"|"+entry.Privilege+"|"+column] = true
		}
	}
	for _, table := range PublicAnchorTables() {
		allowed[table+"|"+PublicAnchorPrivilege+"|"] = true
	}
	var outside []MigratorPublicRow
	for _, row := range rows {
		if !allowed[row.Key()] {
			outside = append(outside, row)
		}
	}
	return outside
}

// migratorExtraStatements returns the statements that take from the migrator every public privilege outside the
// privilege set and the anchor, run as the table owner that granted it, and counts them. A privilege is kept (and
// counted) while it cannot go without side effects: another role holds a row the migrator granted on that table (a
// RESTRICT revoke is refused and CASCADE would narrow that role without its acknowledgement), the runtime role still
// holds a direct privilege on a table outside the set (the grants pass revokes it as the migrator, which needs a
// table-level privilege there), the grantor is not the table owner, or the privilege is one this move does not know.
// regranted lists the runtime rows this run moves: the migrator becomes their grantor in the same transaction.
func migratorExtraStatements(ctx context.Context, tx pgx.Tx, migrator, runtimeRole string, regranted []dependentRow, report *OwnerMoveReport) ([]string, error) {
	rows, err := tx.Query(ctx, MigratorPublicRowsSQL, migrator)
	if err != nil {
		return nil, fmt.Errorf("%w: reading the migrator's public privileges", ErrOwnerMove)
	}
	var held []MigratorPublicRow
	for rows.Next() {
		var r MigratorPublicRow
		if err := rows.Scan(&r.Table, &r.Column, &r.Privilege, &r.Grantable, &r.Grantor, &r.Owner, &r.quotedTable, &r.quotedCol, &r.quotedOwner); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: reading a privilege row", ErrOwnerMove)
		}
		held = append(held, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading the migrator's public privileges", ErrOwnerMove)
	}
	extras := MigratorRowsOutsideTheSet(held)
	if len(extras) == 0 {
		return nil, nil
	}
	dependents := append([]dependentRow(nil), regranted...)
	depRows, err := tx.Query(ctx, migratorDependentRowsSQL, migrator)
	if err != nil {
		return nil, fmt.Errorf("%w: reading the privileges the migrator granted", ErrOwnerMove)
	}
	for depRows.Next() {
		var d dependentRow
		if err := depRows.Scan(&d.table, &d.privilege, &d.quotedColumn); err != nil {
			depRows.Close()
			return nil, fmt.Errorf("%w: reading a privilege row", ErrOwnerMove)
		}
		dependents = append(dependents, d)
	}
	depRows.Close()
	if err := depRows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading the privileges the migrator granted", ErrOwnerMove)
	}
	inSet := map[string]bool{}
	for _, table := range PublicAnchorTables() {
		inSet[table] = true
	}
	runtimeHolds := map[string]bool{}
	for _, row := range extras {
		if inSet[row.Table] {
			continue
		}
		if _, known := runtimeHolds[row.Table]; !known {
			held, err := runtimeHoldsPublicGrant(ctx, tx, row.Table, runtimeRole)
			if err != nil {
				return nil, err
			}
			runtimeHolds[row.Table] = held
		}
	}

	byOwner := map[string][]string{}
	var owners []string
	for _, row := range extras {
		keyword, known := privilegeKeyword(row.Privilege)
		if !known || row.Grantor != row.Owner || row.Owner == migrator || hasDependent(dependents, row) || runtimeHolds[row.Table] {
			report.ExtrasKept++
			continue
		}
		target := "TABLE " + quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + row.quotedTable
		statement := "REVOKE " + keyword
		if row.Column != "" {
			statement += " (" + row.quotedCol + ")"
		}
		statement += " ON " + target + " FROM " + quoteIdentifier(mustValidatedIdentifier(migrator))
		if _, seen := byOwner[row.quotedOwner]; !seen {
			owners = append(owners, row.quotedOwner)
		}
		byOwner[row.quotedOwner] = append(byOwner[row.quotedOwner], statement)
		report.ExtrasRevoked++
	}
	sort.Strings(owners)
	var statements []string
	for _, owner := range owners {
		statements = append(statements, "SET LOCAL ROLE "+owner)
		statements = append(statements, byOwner[owner]...)
		statements = append(statements, "RESET ROLE")
	}
	return statements, nil
}

func hasDependent(dependents []dependentRow, row MigratorPublicRow) bool {
	for _, d := range dependents {
		if d.blocks(row) {
			return true
		}
	}
	return false
}
