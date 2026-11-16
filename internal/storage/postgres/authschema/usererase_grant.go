package authschema

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// While auth-service owns users (AUTH_USERS_OWNED), an admin force delete erases the user (identity.AdminForceDeleteUser)
// as go-api's own force delete does (apiservice/admin userdelete.go forceDeleteUser): each RESTRICT or NO ACTION foreign
// key into public.users has its column set to NULL, so the record stays, and then the public.users row goes in the
// commit that deletes the principal. The other keys delete or null their rows themselves, run by the database as the
// table owner.
// Off, the role holds none of it: users and impersonation_sessions are cleared by the public read pass, and the two
// erase-only tables by userEraseStatements.

// UserEraseNullColumn is one RESTRICT or NO ACTION foreign key column into public.users: the erase sets it to NULL.
// TestUserEraseNullColumnsAreEveryRestrictKeyIntoPublicUsers holds the list equal to the baseline schema.
type UserEraseNullColumn struct {
	Table, Column string
}

// UserEraseNullColumns are the columns the erase sets to NULL, in the order it does it.
func UserEraseNullColumns() []UserEraseNullColumn {
	return []UserEraseNullColumn{
		{Table: "billing_audit_log", Column: "actor_id"},
		{Table: "impersonation_sessions", Column: "admin_user_id"},
		{Table: "impersonation_sessions", Column: "target_user_id"},
		{Table: "refunds", Column: "initiated_by"},
	}
}

// userEraseTable is go-api's user table: the erase holds the table-level DELETE on it.
const userEraseTable = "users"

// userEraseOnlyTables are the tables the erase reaches that no other grant of this package names: the role holds a
// privilege on them only while users are owned.
var userEraseOnlyTables = []string{"billing_audit_log", "refunds"}

// userEraseColumns is the column privileges of the erase on table: the UPDATE that sets the column to NULL and the
// SELECT its WHERE reads. The SELECT on impersonation_sessions' two columns comes from the public read pass and the
// delegated Start's read (PublicReadColumns, ImpersonationLegacyReadColumns), so it is not repeated here.
func userEraseColumns(table, privilege string) []string {
	var columns []string
	for _, c := range UserEraseNullColumns() {
		if c.Table != table {
			continue
		}
		if privilege == "UPDATE" || privilege == "SELECT" && table != impersonationTable {
			columns = append(columns, c.Column)
		}
	}
	return columns
}

// userEraseTables are the tables of UserEraseNullColumns, in order, once each.
func userEraseTables() []string {
	var tables []string
	for _, c := range UserEraseNullColumns() {
		if !slices.Contains(tables, c.Table) {
			tables = append(tables, c.Table)
		}
	}
	return tables
}

// userEraseStatements grants the erase while the switch is on. Off, it clears every privilege the role still holds on
// the erase-only tables (the other two are cleared by the public read pass).
func userEraseStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return nil, err
	}
	role := quoteIdentifier(roleID)
	var statements []string
	if !options.UsersOwned {
		for _, table := range userEraseOnlyTables {
			exists, err := publicTableState(ctx, conn, table)
			if err != nil {
				return nil, err
			}
			if !exists {
				continue
			}
			held, err := runtimeHoldsPublicGrant(ctx, conn, table, options.RuntimeRole)
			if err != nil {
				return nil, err
			}
			if held {
				qualified := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(mustValidatedIdentifier(table))
				statements = append(statements, fmt.Sprintf(`REVOKE ALL ON TABLE %s FROM %s`, qualified, role))
			}
		}
		return statements, nil
	}
	for _, table := range append([]string{userEraseTable}, userEraseTables()...) {
		exists, err := publicTableState(ctx, conn, table)
		if err != nil {
			return nil, err
		}
		if !exists {
			if options.RequirePublicTables {
				return nil, fmt.Errorf("%w: %s.%s", ErrPublicTableMissing, publicSchema, table)
			}
			logger.WarnContext(ctx, "skipped the user erase grant: the table does not exist", "schema", publicSchema, "table", table)
			continue
		}
		qualified := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(mustValidatedIdentifier(table))
		if table == userEraseTable {
			statements = append(statements, fmt.Sprintf(`GRANT DELETE ON TABLE %s TO %s`, qualified, role))
			continue
		}
		if slices.Contains(userEraseOnlyTables, table) {
			statements = append(statements, fmt.Sprintf(`REVOKE ALL ON TABLE %s FROM %s`, qualified, role))
		}
		update, err := quotedColumns(userEraseColumns(table, "UPDATE"))
		if err != nil {
			return nil, err
		}
		statements = append(statements, fmt.Sprintf(`GRANT UPDATE (%s) ON TABLE %s TO %s`, strings.Join(update, ", "), qualified, role))
		if read := userEraseColumns(table, "SELECT"); len(read) > 0 {
			selectColumns, err := quotedColumns(read)
			if err != nil {
				return nil, err
			}
			statements = append(statements, fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s TO %s`, strings.Join(selectColumns, ", "), qualified, role))
		}
	}
	logger.InfoContext(ctx, "applying the user erase grant", "schema", publicSchema, "tables", len(userEraseTables())+1)
	return statements, nil
}

// isUserEraseDelete reports whether a public table DELETE is the erase's.
func isUserEraseDelete(table string) bool { return table == userEraseTable }

// isUserEraseColumn reports whether a public column privilege is one the erase declares.
func isUserEraseColumn(table, privilege, column string) bool {
	return slices.Contains(userEraseColumns(table, privilege), column)
}

// userEraseMissing reports each privilege of the erase the runtime role does not hold while the switch is on.
func userEraseMissing(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	if !options.UsersOwned {
		return nil, nil
	}
	const detail = "declared by the user erase grant (the switch is on) but the runtime role does not hold it (missing grant)"
	var violations []Violation
	exists, err := publicTableState(ctx, conn, userEraseTable)
	if err != nil {
		return nil, err
	}
	if exists {
		var held bool
		if err := conn.QueryRow(ctx, `SELECT has_table_privilege($1, format('%I.%I', $2::text, $3::text), 'DELETE')`,
			options.RuntimeRole, publicSchema, userEraseTable).Scan(&held); err != nil {
			return nil, fmt.Errorf("%w: table %s.%s is unreadable to the posture check", ErrMigrationFailed, publicSchema, userEraseTable)
		}
		if !held {
			violations = append(violations, Violation{Kind: "relation", Schema: publicSchema, Object: userEraseTable, Privilege: "DELETE", Detail: detail})
		}
	}
	for _, table := range userEraseTables() {
		exists, err := publicTableState(ctx, conn, table)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		for _, privilege := range []string{"UPDATE", "SELECT"} {
			for _, column := range userEraseColumns(table, privilege) {
				var held bool
				if err := conn.QueryRow(ctx, `SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, $5)`,
					options.RuntimeRole, publicSchema, table, column, privilege).Scan(&held); err != nil {
					return nil, fmt.Errorf("%w: column %s.%s.%s is missing or unreadable to the posture check",
						ErrMigrationFailed, publicSchema, table, column)
				}
				if !held {
					violations = append(violations, Violation{Kind: "column", Schema: publicSchema, Object: table + "." + column,
						Privilege: privilege, Detail: detail})
				}
			}
		}
	}
	return violations, nil
}
