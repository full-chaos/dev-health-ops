package authschema

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// InviteWriteGrant is one switch-gated column grant set on a go-api table the
// invite aggregate keeps equal to its own rows while auth-service owns users
// (AUTH_USERS_OWNED; internal/auth/invite).
type InviteWriteGrant struct {
	Table  string
	Select []string
	Insert []string
	Update []string
}

// InviteWriteGrants: go-api's org_invites row of each invite (insert, the
// accept and revoke status update, and the read an accept or a take-over of a
// go-api invite needs), and go-api's audit_logs row of a create and an accept
// (one writer per audit row, acp ruling round1062 Q-D1). No DELETE, no
// TRUNCATE, no other column. Off, every privilege on both tables is revoked.
func InviteWriteGrants() []InviteWriteGrant {
	return []InviteWriteGrant{
		{
			Table:  "org_invites",
			Select: []string{"id", "org_id", "email", "role", "token_hash", "invited_by_id", "status", "expires_at", "created_at"},
			Insert: []string{"id", "org_id", "email", "role", "token_hash", "invited_by_id", "status", "expires_at", "created_at", "updated_at"},
			Update: []string{"status", "accepted_at", "updated_at"},
		},
		{
			Table:  "audit_logs",
			Insert: []string{"id", "org_id", "user_id", "action", "resource_type", "resource_id", "description", "changes", "request_metadata", "status", "created_at"},
		},
	}
}

// isInviteWriteColumn reports whether a public column privilege is one
// InviteWriteGrants declares.
func isInviteWriteColumn(table, privilege, column string) bool {
	for _, g := range InviteWriteGrants() {
		if g.Table != table {
			continue
		}
		switch privilege {
		case "SELECT":
			return slices.Contains(g.Select, column)
		case "INSERT":
			return slices.Contains(g.Insert, column)
		case "UPDATE":
			return slices.Contains(g.Update, column)
		}
	}
	return false
}

// inviteWriteStatements grants InviteWriteGrants when the switch is on and
// revokes everything the role holds on those tables when it is off (neither
// table is in PublicReadColumns, so no other pass revokes it).
func inviteWriteStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return nil, err
	}
	role := quoteIdentifier(roleID)
	var statements []string
	for _, g := range InviteWriteGrants() {
		exists, err := publicTableState(ctx, conn, g.Table)
		if err != nil {
			return nil, err
		}
		if !exists {
			if options.RequirePublicTables && options.UsersOwned {
				return nil, fmt.Errorf("%w: %s.%s", ErrPublicTableMissing, publicSchema, g.Table)
			}
			continue
		}
		tableID, err := NewValidatedIdentifier(g.Table)
		if err != nil {
			return nil, err
		}
		table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(tableID)
		if !options.UsersOwned {
			// Off: revoke only what the role holds (the legacyLinkPurgeStatements shape), so a
			// migrator without grant rights on a table the role holds nothing on issues nothing.
			var held bool
			if err := conn.QueryRow(ctx,
				`SELECT has_table_privilege($1, format('%I.%I', $2::text, $3::text), 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
				     OR has_any_column_privilege($1, format('%I.%I', $2::text, $3::text), 'SELECT,INSERT,UPDATE,REFERENCES')`,
				options.RuntimeRole, publicSchema, g.Table,
			).Scan(&held); err != nil {
				return nil, fmt.Errorf("%w: reading the invite write grant of %s.%s", ErrMigrationFailed, publicSchema, g.Table)
			}
			if held {
				statements = append(statements, fmt.Sprintf(`REVOKE ALL ON TABLE %s FROM %s`, table, role))
			}
			continue
		}
		statements = append(statements, fmt.Sprintf(`REVOKE ALL ON TABLE %s FROM %s`, table, role))
		selectColumns, err := quotedColumns(g.Select)
		if err != nil {
			return nil, err
		}
		insertColumns, err := quotedColumns(g.Insert)
		if err != nil {
			return nil, err
		}
		updateColumns, err := quotedColumns(g.Update)
		if err != nil {
			return nil, err
		}
		if len(selectColumns) > 0 {
			statements = append(statements, fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s TO %s`, strings.Join(selectColumns, ", "), table, role))
		}
		if len(insertColumns) > 0 {
			statements = append(statements, fmt.Sprintf(`GRANT INSERT (%s) ON TABLE %s TO %s`, strings.Join(insertColumns, ", "), table, role))
		}
		if len(updateColumns) > 0 {
			statements = append(statements, fmt.Sprintf(`GRANT UPDATE (%s) ON TABLE %s TO %s`, strings.Join(updateColumns, ", "), table, role))
		}
		logger.InfoContext(ctx, "applying the invite write grant", "schema", publicSchema, "table", g.Table)
	}
	return statements, nil
}

// inviteWriteMissing reports each privilege of InviteWriteGrants the runtime
// role does not hold while the switch is on.
func inviteWriteMissing(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	if !options.UsersOwned {
		return nil, nil
	}
	const detail = "declared in InviteWriteGrants (the switch is on) but the runtime role does not hold it (missing grant)"
	var violations []Violation
	for _, g := range InviteWriteGrants() {
		exists, err := publicTableState(ctx, conn, g.Table)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		for _, columnGrant := range []struct {
			privilege string
			columns   []string
		}{{"SELECT", g.Select}, {"INSERT", g.Insert}, {"UPDATE", g.Update}} {
			for _, column := range columnGrant.columns {
				var held bool
				if err := conn.QueryRow(ctx, `SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, $5)`,
					options.RuntimeRole, publicSchema, g.Table, column, columnGrant.privilege).Scan(&held); err != nil {
					return nil, fmt.Errorf("%w: column %s.%s.%s is missing or unreadable to the posture check",
						ErrMigrationFailed, publicSchema, g.Table, column)
				}
				if !held {
					violations = append(violations, Violation{Kind: "column", Schema: publicSchema,
						Object: g.Table + "." + column, Privilege: columnGrant.privilege, Detail: detail})
				}
			}
		}
	}
	return violations, nil
}
