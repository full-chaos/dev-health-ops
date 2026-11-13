package authschema

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// SSOProviderOwnGrants are the privileges the auth runtime role holds on go-api's sso_providers row while auth-service
// owns the provider routes (the SSOProvidersOwned switch, AUTH_SSO_PROVIDERS_OWNED). Each privilege names its columns:
// no table-wide UPDATE, no UPDATE of id, org_id, protocol or created_at (set by the one INSERT), and a SELECT of only the
// columns the provider answer adds to the public read pass. DELETE is table-level (a delete takes the row).
type SSOProviderOwnGrants struct {
	Insert, Update, Select []string
	Delete                 bool
}

// SSOProviderOwn returns the grants the switch gives.
func SSOProviderOwn() SSOProviderOwnGrants {
	return SSOProviderOwnGrants{
		Insert: []string{"id", "org_id", "name", "protocol", "status", "default_role", "is_default", "allow_idp_initiated",
			"auto_provision_users", "config", "encrypted_secrets", "allowed_domains", "created_at", "updated_at"},
		Update: []string{"name", "status", "default_role", "is_default", "allow_idp_initiated", "auto_provision_users", "config",
			"encrypted_secrets", "allowed_domains", "updated_at"},
		Select: []string{"name", "last_metadata_sync_at", "last_login_at", "last_error", "last_error_at", "updated_at"},
		Delete: true,
	}
}

// isSSOProviderOwnColumn reports whether a public column privilege is one SSOProviderOwn declares.
func isSSOProviderOwnColumn(table, privilege, column string) bool {
	if table != ssoProvidersTable {
		return false
	}
	g := SSOProviderOwn()
	switch privilege {
	case "INSERT":
		return slices.Contains(g.Insert, column)
	case "UPDATE":
		return slices.Contains(g.Update, column)
	case "SELECT":
		return slices.Contains(g.Select, column)
	}
	return false
}

// isSSOProviderOwnDelete reports whether a public table DELETE is the one SSOProviderOwn declares: exactly sso_providers.
func isSSOProviderOwnDelete(table string) bool {
	return SSOProviderOwn().Delete && table == ssoProvidersTable
}

// ssoProviderOwnStatements grants SSOProviderOwn when the switch is on, and nothing when it is off (the public read pass already
// took away whatever the role held). It runs after the public read pass. A missing table is
// skipped unless RequirePublicTables.
func ssoProviderOwnStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	if !options.SSOProvidersOwned {
		return nil, nil
	}
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
		logger.WarnContext(ctx, "skipped the SSO provider own grant: the table does not exist",
			"schema", publicSchema, "table", ssoProvidersTable)
		return nil, nil
	}
	table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(mustValidatedIdentifier(ssoProvidersTable))
	role := quoteIdentifier(roleID)
	g := SSOProviderOwn()
	insert, err := quotedColumns(g.Insert)
	if err != nil {
		return nil, err
	}
	update, err := quotedColumns(g.Update)
	if err != nil {
		return nil, err
	}
	read, err := quotedColumns(g.Select)
	if err != nil {
		return nil, err
	}
	statements := []string{
		fmt.Sprintf(`GRANT INSERT (%s) ON TABLE %s TO %s`, strings.Join(insert, ", "), table, role),
		fmt.Sprintf(`GRANT UPDATE (%s) ON TABLE %s TO %s`, strings.Join(update, ", "), table, role),
		fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s TO %s`, strings.Join(read, ", "), table, role),
	}
	if g.Delete {
		statements = append(statements, fmt.Sprintf(`GRANT DELETE ON TABLE %s TO %s`, table, role))
	}
	logger.InfoContext(ctx, "applying the SSO provider own grant", "schema", publicSchema, "table", ssoProvidersTable)
	return statements, nil
}

// ssoProviderOwnMissing reports each privilege of SSOProviderOwn the runtime role does not hold while the switch is on.
func ssoProviderOwnMissing(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	if !options.SSOProvidersOwned {
		return nil, nil
	}
	exists, err := publicTableState(ctx, conn, ssoProvidersTable)
	if err != nil || !exists {
		return nil, err
	}
	const detail = "declared in SSOProviderOwn (the switch is on) but the runtime role does not hold it (missing grant)"
	g := SSOProviderOwn()
	var violations []Violation
	for _, part := range []struct {
		privilege string
		names     []string
	}{{"INSERT", g.Insert}, {"UPDATE", g.Update}, {"SELECT", g.Select}} {
		for _, column := range part.names {
			var held bool
			if err := conn.QueryRow(ctx, `SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, $5)`,
				options.RuntimeRole, publicSchema, ssoProvidersTable, column, part.privilege).Scan(&held); err != nil {
				return nil, fmt.Errorf("%w: column %s.%s.%s is missing or unreadable to the posture check",
					ErrMigrationFailed, publicSchema, ssoProvidersTable, column)
			}
			if !held {
				violations = append(violations, Violation{Kind: "column", Schema: publicSchema,
					Object: ssoProvidersTable + "." + column, Privilege: part.privilege, Detail: detail})
			}
		}
	}
	var held bool
	if err := conn.QueryRow(ctx, `SELECT has_table_privilege($1, format('%I.%I', $2::text, $3::text), 'DELETE')`,
		options.RuntimeRole, publicSchema, ssoProvidersTable).Scan(&held); err != nil {
		return nil, fmt.Errorf("%w: table %s.%s is unreadable to the posture check", ErrMigrationFailed, publicSchema, ssoProvidersTable)
	}
	if !held {
		violations = append(violations, Violation{Kind: "table", Schema: publicSchema, Object: ssoProvidersTable, Privilege: "DELETE", Detail: detail})
	}
	return violations, nil
}
