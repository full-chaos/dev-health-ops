package authschema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ColumnGrant is one table's column-level SELECT entry in the runtime role's
// read-only access to the ops (public) schema.
type ColumnGrant struct {
	Table   string
	Columns []string
}

// publicSchema is the ops-owned schema the auth runtime reads until the users
// cutover. It is fixed: the auth schema name is configurable, this one is not.
const publicSchema = "public"

// PublicReadColumns is the complete, closed list of ops-schema columns the
// auth runtime role may SELECT, and nothing else in that schema, until the
// users cutover revokes it. It is column-level so a table's other columns stay
// unreadable; no INSERT, UPDATE, DELETE or REFERENCES is ever granted.
//
// password_hash and encrypted_secrets are readable by decision of the
// identity-store plan: the store verifies passwords and decrypts provider
// secrets itself while the ops tables remain the system of record.
func PublicReadColumns() []ColumnGrant {
	return []ColumnGrant{
		{Table: "users", Columns: []string{
			"id", "email", "password_hash", "full_name", "username", "auth_provider", "auth_provider_id",
			"is_active", "is_verified", "is_superuser", "token_version", "last_login_at", "sso_provider_id", "saml_nameid_format", "saml_sp_name_qualifier",
		}},
		{Table: "memberships", Columns: []string{"user_id", "org_id", "role", "joined_at", "created_at"}},
		{Table: "sso_providers", Columns: []string{
			"id", "org_id", "protocol", "status", "allow_idp_initiated", "auto_provision_users",
			"config", "allowed_domains", "encrypted_secrets", "is_default", "created_at",
		}},
		{Table: "organizations", Columns: []string{"id", "slug", "name", "is_active", "tier"}},
		{Table: "saml_assertion_replays", Columns: []string{"provider_id", "assertion_id", "expires_at"}},
		{Table: "org_licenses", Columns: []string{"org_id", "tier", "features_override"}},
		// The login organization choice ranks a user's organizations by their metrics
		// activity; the metrics finalize job projects it here (computed_at is its
		// freshness), so auth-service never reads ClickHouse.
		{Table: "org_activity", Columns: []string{"org_id", "has_data", "computed_at"}},
		// The session issue route checks go-api's auth-session-issuer
		// service credential by its hash (last_used_at is not read or
		// stamped).
		{Table: "internal_service_credentials", Columns: []string{"id", "service_name", "token_hash", "scopes", "expires_at", "revoked_at"}},
	}
}

// SessionReaderColumns is what a session reader role may SELECT in the auth
// schema: column-level, on the sessions table only.
func SessionReaderColumns() ColumnGrant {
	return ColumnGrant{Table: "sessions", Columns: []string{"id", "principal_id", "revoked_at", "legacy_token_version"}}
}

// TableWriteGrant is a column-level SELECT and UPDATE grant on one ops-schema
// table, outside PublicReadColumns' read-only rule.
type TableWriteGrant struct {
	Table  string
	Select []string
	Update []string
	// RemovedBy names the ticket that removes the grant.
	RemovedBy string
}

// LegacyRefreshTokenGrant is TEMPORARY: the auth runtime role reads go-api's
// refresh tokens and revokes them (revoked_at only) while a go-api refresh
// token can still be exchanged once for an auth-service session
// (session.Store.ExchangeLegacy). CHAOS-10408 removes it when no live go-api
// refresh token is left.
func LegacyRefreshTokenGrant() TableWriteGrant {
	return TableWriteGrant{
		Table:     "refresh_tokens",
		Select:    []string{"token_hash", "user_id", "org_id", "family_id", "expires_at", "revoked_at"},
		Update:    []string{"revoked_at"},
		RemovedBy: "CHAOS-10408",
	}
}

// UsersWriteGrant is the switch-gated write access to public.users: the auth
// runtime role projects each user it creates or changes to go-api's table in
// the same transaction (AUTH_USERS_OWNED). INSERT and UPDATE on exactly these
// columns, no DELETE, no TRUNCATE, no other table. token_version is inserted
// as 0 and updated only by a password change, which adds 1 in the same
// transaction (go-api's own reset does), so go-api tokens minted before it end.
// last_login_at is updated only by a sign-in, with updated_at, as go-api's does.
// sso_provider_id (ops revision 0155) is inserted only: the provider row an OIDC or
// SAML account belongs to, never rewritten. Before 0155 its grant is DEFERRED.
func UsersWriteGrant() (insert, update []string) {
	return []string{
			"id", "email", "username", "password_hash", "full_name", "auth_provider", "auth_provider_id",
			"is_active", "is_verified", "is_superuser", "token_version", "created_at", "updated_at", "sso_provider_id",
		},
		[]string{"password_hash", "is_verified", "token_version", "last_login_at", "updated_at"}
}

// InsertGrant is a column-level INSERT grant on one ops-schema table.
type InsertGrant struct {
	Table   string
	Columns []string
}

// RegisterOrganizationGrants is the switch-gated INSERT access the auth
// runtime role needs so a sign-up writes go-api's organization and owner
// membership in the transaction of the user it creates, and a join writes
// go-api's membership row in the transaction of its auth membership
// (AUTH_USERS_OWNED).
// INSERT on exactly these columns of these two tables: no UPDATE, no DELETE,
// no TRUNCATE, and no SELECT beyond PublicReadColumns. go-api keeps every
// update of an organization or a membership.
func RegisterOrganizationGrants() []InsertGrant {
	return []InsertGrant{
		{Table: "organizations", Columns: []string{"id", "slug", "name", "settings", "tier", "managed_by", "is_active", "created_at", "updated_at"}},
		{Table: "memberships", Columns: []string{"id", "user_id", "org_id", "role", "joined_at", "created_at", "updated_at"}},
	}
}

// registerOrganizationStatements grants RegisterOrganizationGrants when the
// switch is on. Off, the REVOKE ALL that publicReadStatements issues on both
// tables already removed any write privilege.
func registerOrganizationStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	if !options.UsersOwned {
		return nil, nil
	}
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return nil, err
	}
	role := quoteIdentifier(roleID)
	var statements []string
	for _, grant := range RegisterOrganizationGrants() {
		exists, err := publicTableState(ctx, conn, grant.Table)
		if err != nil {
			return nil, err
		}
		if !exists {
			if options.RequirePublicTables {
				return nil, fmt.Errorf("%w: %s.%s", ErrPublicTableMissing, publicSchema, grant.Table)
			}
			continue
		}
		tableID, err := NewValidatedIdentifier(grant.Table)
		if err != nil {
			return nil, err
		}
		columns, err := quotedColumns(grant.Columns)
		if err != nil {
			return nil, err
		}
		table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(tableID)
		logger.InfoContext(ctx, "applying the register organization insert grant", "schema", publicSchema, "table", grant.Table)
		statements = append(statements, fmt.Sprintf(`GRANT INSERT (%s) ON TABLE %s TO %s`, strings.Join(columns, ", "), table, role))
	}
	return statements, nil
}

// usersWriteStatements grants UsersWriteGrant when the switch is on. Off, the
// REVOKE ALL that publicReadStatements issues on the table already removed any
// write privilege.
func usersWriteStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	if !options.UsersOwned {
		return nil, nil
	}
	exists, err := publicTableState(ctx, conn, "users")
	if err != nil {
		return nil, err
	}
	if !exists {
		if options.RequirePublicTables {
			return nil, fmt.Errorf("%w: %s.users", ErrPublicTableMissing, publicSchema)
		}
		return nil, nil
	}
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return nil, err
	}
	role := quoteIdentifier(roleID)
	table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(mustValidatedIdentifier("users"))
	insert, update := UsersWriteGrant()
	insert, err = presentUsersWriteColumns(ctx, conn, options, logger, "INSERT", insert)
	if err != nil {
		return nil, err
	}
	update, err = presentUsersWriteColumns(ctx, conn, options, logger, "UPDATE", update)
	if err != nil {
		return nil, err
	}
	insertColumns, err := quotedColumns(insert)
	if err != nil {
		return nil, err
	}
	updateColumns, err := quotedColumns(update)
	if err != nil {
		return nil, err
	}
	logger.InfoContext(ctx, "applying the users-owned write grant", "schema", publicSchema, "table", "users")
	return []string{
		fmt.Sprintf(`GRANT INSERT (%s) ON TABLE %s TO %s`, strings.Join(insertColumns, ", "), table, role),
		fmt.Sprintf(`GRANT UPDATE (%s) ON TABLE %s TO %s`, strings.Join(updateColumns, ", "), table, role),
	}, nil
}

// presentUsersWriteColumns is the UsersWriteGrant columns of one privilege that
// public.users has. An absent column (a schema before the ops revision that adds
// it) is refused by the grants pass (RequirePublicTables), naming it; Apply skips
// it and logs it DEFERRED, and the posture check holds it PENDING.
func presentUsersWriteColumns(ctx context.Context, conn querier, options Options, logger *slog.Logger, privilege string, columns []string) ([]string, error) {
	present := make([]string, 0, len(columns))
	for _, column := range columns {
		exists, err := publicColumnExists(ctx, conn, "users", column)
		if err != nil {
			return nil, err
		}
		if !exists && options.RequirePublicTables {
			return nil, fmt.Errorf("%w: %s.users.%s", ErrPublicColumnMissing, publicSchema, column)
		}
		if !exists {
			logger.WarnContext(ctx, "users-owned write grant DEFERRED: the column does not exist yet",
				"schema", publicSchema, "table", "users", "column", column, "privilege", privilege, "state", "DEFERRED")
			continue
		}
		present = append(present, column)
	}
	return present, nil
}

// DeleteGrant is the switch-gated purge access to one ops-schema table:
// table-level DELETE and column-level SELECT of the named columns.
type DeleteGrant struct {
	Table  string
	Select []string
}

// LegacyLinkPurgeGrants is the switch-gated access the auth runtime role needs
// to purge go-api's link-token rows once users are owned (AUTH_USERS_OWNED):
// DELETE on the two legacy link-token tables and SELECT of their id column
// (the purge counts the rows it deletes). No INSERT, UPDATE, TRUNCATE or other
// column, and no other table. Off, it is revoked (acp ruling 2026-10-26, CHAOS-11506).
func LegacyLinkPurgeGrants() []DeleteGrant {
	return []DeleteGrant{
		{Table: "email_verification_tokens", Select: []string{"id"}},
		{Table: "password_reset_tokens", Select: []string{"id"}},
	}
}

// legacyLinkPurgeStatements grants LegacyLinkPurgeGrants when the switch is on
// and revokes anything the role holds on those tables when it is off.
func legacyLinkPurgeStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return nil, err
	}
	role := quoteIdentifier(roleID)
	var statements []string
	for _, grant := range LegacyLinkPurgeGrants() {
		exists, err := publicTableState(ctx, conn, grant.Table)
		if err != nil {
			return nil, err
		}
		if !exists {
			if options.RequirePublicTables && options.UsersOwned {
				return nil, fmt.Errorf("%w: %s.%s", ErrPublicTableMissing, publicSchema, grant.Table)
			}
			continue
		}
		tableID, err := NewValidatedIdentifier(grant.Table)
		if err != nil {
			return nil, err
		}
		table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(tableID)
		if !options.UsersOwned {
			var held bool
			if err := conn.QueryRow(ctx,
				`SELECT has_table_privilege($1, format('%I.%I', $2::text, $3::text), 'DELETE')
				     OR has_column_privilege($1, format('%I.%I', $2::text, $3::text), 'id', 'SELECT')`,
				options.RuntimeRole, publicSchema, grant.Table,
			).Scan(&held); err != nil {
				return nil, fmt.Errorf("%w: reading the legacy link purge grant of %s.%s", ErrMigrationFailed, publicSchema, grant.Table)
			}
			if held {
				statements = append(statements, fmt.Sprintf(`REVOKE ALL ON TABLE %s FROM %s`, table, role))
			}
			continue
		}
		columns, err := quotedColumns(grant.Select)
		if err != nil {
			return nil, err
		}
		logger.InfoContext(ctx, "applying the legacy link purge grant", "schema", publicSchema, "table", grant.Table)
		statements = append(statements,
			fmt.Sprintf(`REVOKE ALL ON TABLE %s FROM %s`, table, role),
			fmt.Sprintf(`GRANT DELETE ON TABLE %s TO %s`, table, role),
			fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s TO %s`, strings.Join(columns, ", "), table, role),
		)
	}
	return statements, nil
}

func isLegacyLinkPurgeTable(table string) bool {
	for _, grant := range LegacyLinkPurgeGrants() {
		if grant.Table == table {
			return true
		}
	}
	return false
}

// quotedColumns validates and quotes each name.
func quotedColumns(names []string) ([]string, error) {
	columns := make([]string, 0, len(names))
	for _, name := range names {
		columnID, err := NewValidatedIdentifier(name)
		if err != nil {
			return nil, err
		}
		columns = append(columns, quoteIdentifier(columnID))
	}
	return columns, nil
}

// rowQuerier1 is the slice of a connection the relation lookup needs.
type rowQuerier1 interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// publicRelationKind reports the relkind of public.<table> ("" when no such
// relation exists). A view or foreign table of the same name is a different
// object from the ops table the manifest describes.
func publicRelationKind(ctx context.Context, conn rowQuerier1, table string) (string, error) {
	var kind string
	err := conn.QueryRow(ctx,
		`SELECT c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relname = $2`, publicSchema, table,
	).Scan(&kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("%w: looking for %s.%s", ErrMigrationFailed, publicSchema, table)
	}
	return kind, nil
}

// publicColumnExists reports whether public.<table> has a live column named
// column. A manifest column a later ops revision adds (users.sso_provider_id,
// revision 0155) is absent on a schema before it.
func publicColumnExists(ctx context.Context, conn rowQuerier1, table, column string) (bool, error) {
	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3 AND a.attnum > 0 AND NOT a.attisdropped)`,
		publicSchema, table, column,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("%w: looking for %s.%s.%s", ErrMigrationFailed, publicSchema, table, column)
	}
	return exists, nil
}

// publicTableState classifies public.<table> for the manifest: absent, a table
// ('r' or 'p'), or something else, which is an error rather than a skip.
func publicTableState(ctx context.Context, conn rowQuerier1, table string) (exists bool, err error) {
	kind, err := publicRelationKind(ctx, conn, table)
	if err != nil {
		return false, err
	}
	switch kind {
	case "":
		return false, nil
	case "r", "p":
		return true, nil
	}
	return false, fmt.Errorf("%w: %s.%s is not a table (relkind %q) but PublicReadColumns names a table",
		ErrMigrationFailed, publicSchema, table, kind)
}

// publicReadStatements returns the statements that make the runtime role's
// access to the ops schema equal PublicReadColumns: for each manifest table that
// EXISTS, revoke every privilege the role holds on it (table and column level)
// and grant SELECT on exactly the manifest columns. A manifest table that does
// not exist (an auth-only database) is skipped and logged by name;
// VerifyRuntimePosture requires the grants as soon as it does exist. A manifest
// column that does not exist yet (a schema before the ops revision that adds
// it) is skipped and logged as DEFERRED by Apply: the grants pass after ops
// migrate grants it, and VerifyRuntimePosture holds it PENDING until the column
// exists and FAILS once it exists without the grant. The grants pass itself
// (RequirePublicTables) refuses an absent column, naming it. They run
// inside applyRuntimeGrants' transaction, so a privilege they would remove is a
// narrowing that needs the same acknowledgement as any other.
//
// The migration login must own those tables or hold GRANT OPTION on them; if it
// does not, the statement fails with PostgreSQL's permission error rather than
// widening anything.
func publicReadStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return nil, err
	}
	role := quoteIdentifier(roleID)
	var statements []string
	for _, entry := range PublicReadColumns() {
		exists, err := publicTableState(ctx, conn, entry.Table)
		if err != nil {
			return nil, err
		}
		if !exists && options.RequirePublicTables {
			return nil, fmt.Errorf("%w: %s.%s", ErrPublicTableMissing, publicSchema, entry.Table)
		}
		if !exists {
			logger.WarnContext(ctx, "skipped public read grants: the table does not exist",
				"schema", publicSchema, "table", entry.Table)
			continue
		}
		tableID, err := NewValidatedIdentifier(entry.Table)
		if err != nil {
			return nil, err
		}
		columns := make([]string, 0, len(entry.Columns))
		for _, name := range entry.Columns {
			columnID, err := NewValidatedIdentifier(name)
			if err != nil {
				return nil, err
			}
			present, err := publicColumnExists(ctx, conn, entry.Table, name)
			if err != nil {
				return nil, err
			}
			if !present && options.RequirePublicTables {
				return nil, fmt.Errorf("%w: %s.%s.%s", ErrPublicColumnMissing, publicSchema, entry.Table, name)
			}
			if !present {
				logger.WarnContext(ctx, "public read grant DEFERRED: the column does not exist yet",
					"schema", publicSchema, "table", entry.Table, "column", name, "state", "DEFERRED")
				continue
			}
			columns = append(columns, quoteIdentifier(columnID))
		}
		table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(tableID)
		statements = append(statements, fmt.Sprintf(`REVOKE ALL ON TABLE %s FROM %s`, table, role))
		if len(columns) > 0 {
			statements = append(statements, fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s TO %s`, strings.Join(columns, ", "), table, role))
		}
		logger.InfoContext(ctx, "applying public read grants",
			"schema", publicSchema, "table", entry.Table, "columns", len(columns), "deferred", len(entry.Columns)-len(columns))
	}
	write := LegacyRefreshTokenGrant()
	exists, err := publicTableState(ctx, conn, write.Table)
	if err != nil {
		return nil, err
	}
	if !exists && options.RequirePublicTables {
		return nil, fmt.Errorf("%w: %s.%s", ErrPublicTableMissing, publicSchema, write.Table)
	}
	if !exists {
		logger.WarnContext(ctx, "skipped the legacy refresh token grant: the table does not exist",
			"schema", publicSchema, "table", write.Table)
		return statements, nil
	}
	tableID, err := NewValidatedIdentifier(write.Table)
	if err != nil {
		return nil, err
	}
	table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(tableID)
	statements = append(statements, fmt.Sprintf(`REVOKE ALL ON TABLE %s FROM %s`, table, role))
	{
		columns, err := quotedColumns(write.Select)
		if err != nil {
			return nil, err
		}
		statements = append(statements, fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s TO %s`, strings.Join(columns, ", "), table, role))
	}
	{
		columns, err := quotedColumns(write.Update)
		if err != nil {
			return nil, err
		}
		statements = append(statements, fmt.Sprintf(`GRANT UPDATE (%s) ON TABLE %s TO %s`, strings.Join(columns, ", "), table, role))
	}
	logger.InfoContext(ctx, "applying the temporary legacy refresh token grant",
		"schema", publicSchema, "table", write.Table, "removed_by", write.RemovedBy)
	return statements, nil
}

// publicReadViolations compares the role's effective column privileges with
// PublicReadColumns in both directions: a manifest column the role cannot read
// on a table that exists is a violation, and so is any column privilege the
// manifest does not declare anywhere outside the auth schema. Table-level
// privileges are reported by relationViolations, so they are not repeated here.
func publicReadViolations(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	declared := make(map[string]map[string]struct{}, len(PublicReadColumns()))
	for _, entry := range PublicReadColumns() {
		columns := make(map[string]struct{}, len(entry.Columns))
		for _, column := range entry.Columns {
			columns[column] = struct{}{}
		}
		declared[entry.Table] = columns
	}
	write := LegacyRefreshTokenGrant()
	writeDeclared := map[string]map[string]struct{}{"SELECT": {}, "UPDATE": {}}
	for _, column := range write.Select {
		writeDeclared["SELECT"][column] = struct{}{}
	}
	for _, column := range write.Update {
		writeDeclared["UPDATE"][column] = struct{}{}
	}

	usersInsert, usersUpdate := UsersWriteGrant()
	usersDeclared := map[string]map[string]struct{}{"INSERT": {}, "UPDATE": {}}
	for _, column := range usersInsert {
		usersDeclared["INSERT"][column] = struct{}{}
	}
	for _, column := range usersUpdate {
		usersDeclared["UPDATE"][column] = struct{}{}
	}

	registerDeclared := map[string]map[string]struct{}{}
	for _, grant := range RegisterOrganizationGrants() {
		columns := make(map[string]struct{}, len(grant.Columns))
		for _, column := range grant.Columns {
			columns[column] = struct{}{}
		}
		registerDeclared[grant.Table] = columns
	}

	var violations []Violation
	if options.UsersOwned {
		for _, grant := range RegisterOrganizationGrants() {
			exists, err := publicTableState(ctx, conn, grant.Table)
			if err != nil {
				return nil, err
			}
			if !exists {
				continue
			}
			for _, column := range grant.Columns {
				var held bool
				if err := conn.QueryRow(ctx,
					`SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, 'INSERT')`,
					options.RuntimeRole, publicSchema, grant.Table, column,
				).Scan(&held); err != nil {
					return nil, fmt.Errorf("%w: column %s.%s.%s is missing or unreadable to the posture check",
						ErrMigrationFailed, publicSchema, grant.Table, column)
				}
				if !held {
					violations = append(violations, Violation{
						Kind: "column", Schema: publicSchema, Object: grant.Table + "." + column, Privilege: "INSERT",
						Detail: "declared in RegisterOrganizationGrants (the switch is on) but the runtime role does not hold it (missing grant)",
					})
				}
			}
		}
		for _, grant := range LegacyLinkPurgeGrants() {
			exists, err := publicTableState(ctx, conn, grant.Table)
			if err != nil {
				return nil, err
			}
			if !exists {
				continue
			}
			var deleteHeld, selectHeld bool
			if err := conn.QueryRow(ctx,
				`SELECT has_table_privilege($1, format('%I.%I', $2::text, $3::text), 'DELETE'),
				        has_column_privilege($1, format('%I.%I', $2::text, $3::text), 'id', 'SELECT')`,
				options.RuntimeRole, publicSchema, grant.Table,
			).Scan(&deleteHeld, &selectHeld); err != nil {
				return nil, fmt.Errorf("%w: table %s.%s is missing or unreadable to the posture check",
					ErrMigrationFailed, publicSchema, grant.Table)
			}
			for privilege, held := range map[string]bool{"DELETE": deleteHeld, "SELECT": selectHeld} {
				if !held {
					violations = append(violations, Violation{
						Kind: "column", Schema: publicSchema, Object: grant.Table, Privilege: privilege,
						Detail: "declared in LegacyLinkPurgeGrants (the switch is on) but the runtime role does not hold it (missing grant)",
					})
				}
			}
		}
		usersExist, err := publicTableState(ctx, conn, "users")
		if err != nil {
			return nil, err
		}
		if usersExist {
			for _, privilege := range []string{"INSERT", "UPDATE"} {
				for column := range usersDeclared[privilege] {
					present, err := publicColumnExists(ctx, conn, "users", column)
					if err != nil {
						return nil, err
					}
					if !present {
						// PENDING: a DEFERRED write grant of a column a later ops revision adds.
						if options.Logger != nil {
							options.Logger.InfoContext(ctx, "users-owned write grant PENDING: the column does not exist yet",
								"schema", publicSchema, "table", "users", "column", column, "privilege", privilege, "state", "PENDING")
						}
						continue
					}
					var held bool
					if err := conn.QueryRow(ctx,
						`SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, $5)`,
						options.RuntimeRole, publicSchema, "users", column, privilege,
					).Scan(&held); err != nil {
						return nil, fmt.Errorf("%w: column %s.users.%s is missing or unreadable to the posture check",
							ErrMigrationFailed, publicSchema, column)
					}
					if !held {
						violations = append(violations, Violation{
							Kind: "column", Schema: publicSchema, Object: "users." + column, Privilege: privilege,
							Detail: "declared in UsersWriteGrant (the switch is on) but the runtime role does not hold it (missing grant)",
						})
					}
				}
			}
		}
	}
	for _, entry := range PublicReadColumns() {
		exists, err := publicTableState(ctx, conn, entry.Table)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		for _, column := range entry.Columns {
			present, err := publicColumnExists(ctx, conn, entry.Table, column)
			if err != nil {
				return nil, err
			}
			if !present {
				// PENDING: a DEFERRED grant of a column a later ops revision adds.
				if options.Logger != nil {
					options.Logger.InfoContext(ctx, "public read grant PENDING: the column does not exist yet",
						"schema", publicSchema, "table", entry.Table, "column", column, "state", "PENDING")
				}
				continue
			}
			var readable bool
			if err := conn.QueryRow(ctx,
				`SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, 'SELECT')`,
				options.RuntimeRole, publicSchema, entry.Table, column,
			).Scan(&readable); err != nil {
				return nil, fmt.Errorf("%w: column %s.%s.%s is missing or unreadable to the posture check",
					ErrMigrationFailed, publicSchema, entry.Table, column)
			}
			if !readable {
				violations = append(violations, Violation{
					Kind: "column", Schema: publicSchema, Object: entry.Table + "." + column, Privilege: "SELECT",
					Detail: "declared in PublicReadColumns but the runtime role cannot read it (missing grant)",
				})
			}
		}
	}

	writeExists, err := publicTableState(ctx, conn, write.Table)
	if err != nil {
		return nil, err
	}
	if writeExists {
		for _, privilege := range []string{"SELECT", "UPDATE"} {
			for column := range writeDeclared[privilege] {
				var held bool
				if err := conn.QueryRow(ctx,
					`SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, $5)`,
					options.RuntimeRole, publicSchema, write.Table, column, privilege,
				).Scan(&held); err != nil {
					return nil, fmt.Errorf("%w: column %s.%s.%s is missing or unreadable to the posture check",
						ErrMigrationFailed, publicSchema, write.Table, column)
				}
				if !held {
					violations = append(violations, Violation{
						Kind: "column", Schema: publicSchema, Object: write.Table + "." + column, Privilege: privilege,
						Detail: "declared in LegacyRefreshTokenGrant (" + write.RemovedBy + ") but the runtime role does not hold it (missing grant)",
					})
				}
			}
		}
	}

	// One literal statement: the role, the schema and the system-schema filter
	// (systemSchemaNames, systemSchemaPattern) are bound parameters.
	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname, c.relkind::text, a.attname, p.priv
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL unnest(ARRAY['SELECT','INSERT','UPDATE','REFERENCES']) AS p(priv)
		WHERE n.nspname <> ALL($3::text[]) AND n.nspname NOT LIKE $4
		  AND n.nspname <> $2
		  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
		  AND a.attnum > 0 AND NOT a.attisdropped
		  AND NOT has_table_privilege($1, c.oid, p.priv)
		  AND has_column_privilege($1, c.oid, a.attnum, p.priv)`,
		options.RuntimeRole, options.Schema, systemSchemaNames, systemSchemaPattern,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: reading effective column privileges", ErrMigrationFailed)
	}
	defer rows.Close()
	for rows.Next() {
		var schema, table, kind, column, privilege string
		if err := rows.Scan(&schema, &table, &kind, &column, &privilege); err != nil {
			return nil, fmt.Errorf("%w: reading a column privilege", ErrMigrationFailed)
		}
		if schema == publicSchema && privilege == "SELECT" && (kind == "r" || kind == "p") {
			if _, ok := declared[table][column]; ok {
				continue
			}
		}
		if schema == publicSchema && table == write.Table && (kind == "r" || kind == "p") {
			if _, ok := writeDeclared[privilege][column]; ok {
				continue
			}
		}
		if options.UsersOwned && schema == publicSchema && table == "users" && (kind == "r" || kind == "p") {
			if _, ok := usersDeclared[privilege][column]; ok {
				continue
			}
		}
		if options.UsersOwned && schema == publicSchema && privilege == "SELECT" && isLegacyLinkPurgeTable(table) && column == "id" {
			continue
		}
		if options.UsersOwned && schema == publicSchema && privilege == "INSERT" && (kind == "r" || kind == "p") {
			if _, ok := registerDeclared[table][column]; ok {
				continue
			}
		}
		if options.UsersOwned && schema == publicSchema && (kind == "r" || kind == "p") && isMembershipWriteColumn(table, privilege, column) {
			continue
		}
		if options.UsersOwned && schema == publicSchema && (kind == "r" || kind == "p") && isInviteWriteColumn(table, privilege, column) {
			continue
		}
		violations = append(violations, Violation{
			Kind: "column", Schema: schema, Object: table + "." + column, Privilege: privilege,
			Detail: "not declared in PublicReadColumns",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading effective column privileges", ErrMigrationFailed)
	}
	missing, err := membershipWriteMissing(ctx, conn, options)
	if err != nil {
		return nil, err
	}
	inviteMissing, err := inviteWriteMissing(ctx, conn, options)
	if err != nil {
		return nil, err
	}
	return append(append(violations, missing...), inviteMissing...), nil
}
