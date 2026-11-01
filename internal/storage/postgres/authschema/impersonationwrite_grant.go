package authschema

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// impersonationTable is go-api's delegation table.
const impersonationTable = "impersonation_sessions"

// ImpersonationEndColumns are the columns of go-api's delegation row the auth
// runtime role updates when a delegated session ends. UPDATE only, whatever the
// users-owned switch says: a security end must not depend on operator order or a
// skip branch. The row is found by id, which the public read pass grants.
func ImpersonationEndColumns() []string {
	return []string{"ended_at"}
}

// ImpersonationStartColumns are the columns a delegated Start inserts. They
// follow the users-owned switch: Start refuses with the switch off.
func ImpersonationStartColumns() []string {
	return []string{"id", "admin_user_id", "target_user_id", "target_org_id", "target_role", "created_at", "expires_at"}
}

// ImpersonationLegacyReadColumns are what a Start reads of a go-api row it ends,
// for that row's impersonation_stop audit row. They follow the switch as the
// start does.
func ImpersonationLegacyReadColumns() []string {
	return []string{"target_user_id", "target_org_id"}
}

// isImpersonationStartColumn reports whether a public column privilege is one
// the switch gives on the delegation table.
func isImpersonationStartColumn(table, privilege, column string) bool {
	if table != impersonationTable {
		return false
	}
	switch privilege {
	case "INSERT":
		return slices.Contains(ImpersonationStartColumns(), column)
	case "SELECT":
		return slices.Contains(ImpersonationLegacyReadColumns(), column)
	}
	return false
}

// isImpersonationWriteColumn reports whether a public column privilege is one
// this file declares, with the switch as given.
func isImpersonationWriteColumn(usersOwned bool, table, privilege, column string) bool {
	if table == impersonationTable && privilege == "UPDATE" && slices.Contains(ImpersonationEndColumns(), column) {
		return true
	}
	return usersOwned && isImpersonationStartColumn(table, privilege, column)
}

// impersonationWriteStatements grants the end, and the start with the switch on.
// It runs after the public read pass, which first takes away whatever the role
// held on the table, so a switch turned off leaves the end and the reads. A
// missing table is skipped unless RequirePublicTables.
func impersonationWriteStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return nil, err
	}
	exists, err := publicTableState(ctx, conn, impersonationTable)
	if err != nil {
		return nil, err
	}
	if !exists {
		if options.RequirePublicTables {
			return nil, fmt.Errorf("%w: %s.%s", ErrPublicTableMissing, publicSchema, impersonationTable)
		}
		logger.WarnContext(ctx, "skipped the impersonation write grant: the table does not exist",
			"schema", publicSchema, "table", impersonationTable)
		return nil, nil
	}
	table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(mustValidatedIdentifier(impersonationTable))
	role := quoteIdentifier(roleID)
	end, err := quotedColumns(ImpersonationEndColumns())
	if err != nil {
		return nil, err
	}
	statements := []string{fmt.Sprintf(`GRANT UPDATE (%s) ON TABLE %s TO %s`, strings.Join(end, ", "), table, role)}
	if options.UsersOwned {
		start, err := quotedColumns(ImpersonationStartColumns())
		if err != nil {
			return nil, err
		}
		read, err := quotedColumns(ImpersonationLegacyReadColumns())
		if err != nil {
			return nil, err
		}
		statements = append(statements,
			fmt.Sprintf(`GRANT INSERT (%s) ON TABLE %s TO %s`, strings.Join(start, ", "), table, role),
			fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s TO %s`, strings.Join(read, ", "), table, role))
	}
	logger.InfoContext(ctx, "applying the impersonation write grant", "schema", publicSchema, "table", impersonationTable)
	return statements, nil
}

// impersonationWriteMissing reports each privilege of this file the runtime role
// does not hold on an existing delegation table. The end is reported whatever
// the switch says.
func impersonationWriteMissing(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	exists, err := publicTableState(ctx, conn, impersonationTable)
	if err != nil || !exists {
		return nil, err
	}
	wanted := []struct {
		privilege, detail string
		columns           []string
	}{{"UPDATE", "declared in ImpersonationEndColumns, which no switch gates: a security end cannot fail on a grant (missing grant)", ImpersonationEndColumns()}}
	if options.UsersOwned {
		const detail = "declared for a delegated Start (the switch is on) but the runtime role does not hold it (missing grant)"
		wanted = append(wanted, []struct {
			privilege, detail string
			columns           []string
		}{{"INSERT", detail, ImpersonationStartColumns()}, {"SELECT", detail, ImpersonationLegacyReadColumns()}}...)
	}
	var violations []Violation
	for _, w := range wanted {
		for _, column := range w.columns {
			var held bool
			if err := conn.QueryRow(ctx, `SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, $5)`,
				options.RuntimeRole, publicSchema, impersonationTable, column, w.privilege).Scan(&held); err != nil {
				return nil, fmt.Errorf("%w: column %s.%s.%s is missing or unreadable to the posture check",
					ErrMigrationFailed, publicSchema, impersonationTable, column)
			}
			if !held {
				violations = append(violations, Violation{Kind: "column", Schema: publicSchema,
					Object: impersonationTable + "." + column, Privilege: w.privilege, Detail: w.detail})
			}
		}
	}
	return violations, nil
}
