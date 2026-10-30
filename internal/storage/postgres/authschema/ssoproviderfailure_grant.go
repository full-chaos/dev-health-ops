package authschema

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ssoProvidersTable is go-api's SSO provider table.
const ssoProvidersTable = "sso_providers"

// SSOProviderFailureColumns are the columns of go-api's sso_providers row the auth runtime role updates to record a
// provider failure of a sign-in (internal/api/sso recordSSOFailure; acp ruling D6515): the failure text, its time and
// updated_at. Never the status. UPDATE only, whatever the users-owned switch says, since the sign-in routes are served without it; the
// row is found by id, which the public read pass grants.
func SSOProviderFailureColumns() []string {
	return []string{"last_error", "last_error_at", "updated_at"}
}

// isSSOProviderFailureColumn reports whether a public column privilege is one SSOProviderFailureColumns declares.
func isSSOProviderFailureColumn(table, privilege, column string) bool {
	return table == ssoProvidersTable && privilege == "UPDATE" && slices.Contains(SSOProviderFailureColumns(), column)
}

// ssoProviderFailureStatements grants the column UPDATE of SSOProviderFailureColumns. It runs after the public read
// pass, which first takes away whatever the role held on the table. A missing table is skipped unless
// RequirePublicTables.
func ssoProviderFailureStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return nil, err
	}
	exists, err := publicTableState(ctx, conn, ssoProvidersTable)
	if err != nil {
		return nil, err
	}
	if !exists {
		if options.RequirePublicTables {
			return nil, fmt.Errorf("%w: %s.%s", ErrPublicTableMissing, publicSchema, ssoProvidersTable)
		}
		logger.WarnContext(ctx, "skipped the SSO provider failure grant: the table does not exist",
			"schema", publicSchema, "table", ssoProvidersTable)
		return nil, nil
	}
	table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(mustValidatedIdentifier(ssoProvidersTable))
	columns, err := quotedColumns(SSOProviderFailureColumns())
	if err != nil {
		return nil, err
	}
	logger.InfoContext(ctx, "applying the SSO provider failure grant", "schema", publicSchema, "table", ssoProvidersTable)
	return []string{
		fmt.Sprintf(`GRANT UPDATE (%s) ON TABLE %s TO %s`, strings.Join(columns, ", "), table, quoteIdentifier(roleID)),
	}, nil
}

// ssoProviderFailureMissing reports each column UPDATE of SSOProviderFailureColumns the runtime role does not hold on
// an existing sso_providers.
func ssoProviderFailureMissing(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	exists, err := publicTableState(ctx, conn, ssoProvidersTable)
	if err != nil || !exists {
		return nil, err
	}
	const detail = "declared in SSOProviderFailureColumns but the runtime role does not hold it (missing grant)"
	var violations []Violation
	for _, column := range SSOProviderFailureColumns() {
		var held bool
		if err := conn.QueryRow(ctx, `SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, 'UPDATE')`,
			options.RuntimeRole, publicSchema, ssoProvidersTable, column).Scan(&held); err != nil {
			return nil, fmt.Errorf("%w: column %s.%s.%s is missing or unreadable to the posture check",
				ErrMigrationFailed, publicSchema, ssoProvidersTable, column)
		}
		if !held {
			violations = append(violations, Violation{Kind: "column", Schema: publicSchema,
				Object: ssoProvidersTable + "." + column, Privilege: "UPDATE", Detail: detail})
		}
	}
	return violations, nil
}
