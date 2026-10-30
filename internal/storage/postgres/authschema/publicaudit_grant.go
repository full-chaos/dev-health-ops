package authschema

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// publicAuditTable is go-api's audit log table.
const publicAuditTable = "audit_logs"

// PublicAuditWriteColumns are the columns of go-api's audit_logs row the auth
// runtime role inserts (internal/api/audit.PGWriter's column set): the invite
// aggregate's member_invited and member_joined rows, and the sso_login
// failure row of a refused OAuth sign-in. INSERT only, whatever the
// users-owned switch says, since the sign-in routes are served without it.
// No SELECT, UPDATE, DELETE or TRUNCATE.
func PublicAuditWriteColumns() []string {
	return []string{"id", "org_id", "user_id", "action", "resource_type", "resource_id", "description",
		"changes", "request_metadata", "status", "error_message", "created_at"}
}

// isPublicAuditWriteColumn reports whether a public column privilege is one
// PublicAuditWriteColumns declares.
func isPublicAuditWriteColumn(table, privilege, column string) bool {
	return table == publicAuditTable && privilege == "INSERT" && slices.Contains(PublicAuditWriteColumns(), column)
}

// publicAuditWriteStatements replaces every privilege the role holds on
// audit_logs with the column INSERT of PublicAuditWriteColumns. A missing table
// is skipped unless RequirePublicTables.
func publicAuditWriteStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return nil, err
	}
	exists, err := publicTableState(ctx, conn, publicAuditTable)
	if err != nil {
		return nil, err
	}
	if !exists {
		if options.RequirePublicTables {
			return nil, fmt.Errorf("%w: %s.%s", ErrPublicTableMissing, publicSchema, publicAuditTable)
		}
		logger.WarnContext(ctx, "skipped the public audit write grant: the table does not exist",
			"schema", publicSchema, "table", publicAuditTable)
		return nil, nil
	}
	table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(mustValidatedIdentifier(publicAuditTable))
	role := quoteIdentifier(roleID)
	columns, err := quotedColumns(PublicAuditWriteColumns())
	if err != nil {
		return nil, err
	}
	logger.InfoContext(ctx, "applying the public audit write grant", "schema", publicSchema, "table", publicAuditTable)
	return []string{
		fmt.Sprintf(`REVOKE ALL ON TABLE %s FROM %s`, table, role),
		fmt.Sprintf(`GRANT INSERT (%s) ON TABLE %s TO %s`, strings.Join(columns, ", "), table, role),
	}, nil
}

// publicAuditWriteMissing reports each column INSERT of PublicAuditWriteColumns
// the runtime role does not hold on an existing audit_logs.
func publicAuditWriteMissing(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	exists, err := publicTableState(ctx, conn, publicAuditTable)
	if err != nil || !exists {
		return nil, err
	}
	const detail = "declared in PublicAuditWriteColumns but the runtime role does not hold it (missing grant)"
	var violations []Violation
	for _, column := range PublicAuditWriteColumns() {
		var held bool
		if err := conn.QueryRow(ctx, `SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, 'INSERT')`,
			options.RuntimeRole, publicSchema, publicAuditTable, column).Scan(&held); err != nil {
			return nil, fmt.Errorf("%w: column %s.%s.%s is missing or unreadable to the posture check",
				ErrMigrationFailed, publicSchema, publicAuditTable, column)
		}
		if !held {
			violations = append(violations, Violation{Kind: "column", Schema: publicSchema,
				Object: publicAuditTable + "." + column, Privilege: "INSERT", Detail: detail})
		}
	}
	return violations, nil
}
