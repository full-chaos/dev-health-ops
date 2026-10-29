package authschema

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// MembershipWriteGrant is the switch-gated access beyond
// RegisterOrganizationGrants that the auth runtime role needs to keep go-api's
// public.memberships equal to auth.memberships while it owns users
// (AUTH_USERS_OWNED; internal/auth/membership): DELETE a row whose membership
// left active, UPDATE its role, and SELECT its id (the verbs read and lock the
// row they change). No TRUNCATE, no other column and no other table. Off, the REVOKE ALL that
// publicReadStatements issues on the table removes it.
type MembershipWriteGrant struct {
	Table  string
	Delete bool
	Select []string
	Update []string
}

func MembershipWriteGrants() MembershipWriteGrant {
	return MembershipWriteGrant{
		Table:  "memberships",
		Delete: true,
		Select: []string{"id"},
		Update: []string{"role", "updated_at"},
	}
}

// membershipWriteStatements grants MembershipWriteGrants when the switch is on.
func membershipWriteStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	if !options.UsersOwned {
		return nil, nil
	}
	g := MembershipWriteGrants()
	exists, err := publicTableState(ctx, conn, g.Table)
	if err != nil {
		return nil, err
	}
	if !exists {
		if options.RequirePublicTables {
			return nil, fmt.Errorf("%w: %s.%s", ErrPublicTableMissing, publicSchema, g.Table)
		}
		return nil, nil
	}
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return nil, err
	}
	role := quoteIdentifier(roleID)
	tableID, err := NewValidatedIdentifier(g.Table)
	if err != nil {
		return nil, err
	}
	table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(tableID)
	selectColumns, err := quotedColumns(g.Select)
	if err != nil {
		return nil, err
	}
	updateColumns, err := quotedColumns(g.Update)
	if err != nil {
		return nil, err
	}
	statements := []string{
		fmt.Sprintf(`GRANT DELETE ON TABLE %s TO %s`, table, role),
		fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s TO %s`, strings.Join(selectColumns, ", "), table, role),
		fmt.Sprintf(`GRANT UPDATE (%s) ON TABLE %s TO %s`, strings.Join(updateColumns, ", "), table, role),
	}
	logger.InfoContext(ctx, "applying the membership write grant", "schema", publicSchema, "table", g.Table)
	return statements, nil
}

// isMembershipWriteDelete reports whether a public table DELETE is the one
// MembershipWriteGrants declares.
func isMembershipWriteDelete(table string) bool {
	g := MembershipWriteGrants()
	return g.Delete && table == g.Table
}

// isMembershipWriteColumn reports whether a public column privilege is one
// MembershipWriteGrants declares.
func isMembershipWriteColumn(table, privilege, column string) bool {
	g := MembershipWriteGrants()
	if table != g.Table {
		return false
	}
	switch privilege {
	case "SELECT":
		return slices.Contains(g.Select, column)
	case "UPDATE":
		return slices.Contains(g.Update, column)
	}
	return false
}

// membershipWriteMissing reports each privilege of MembershipWriteGrants the
// runtime role does not hold while the switch is on.
func membershipWriteMissing(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	if !options.UsersOwned {
		return nil, nil
	}
	g := MembershipWriteGrants()
	exists, err := publicTableState(ctx, conn, g.Table)
	if err != nil || !exists {
		return nil, err
	}
	const detail = "declared in MembershipWriteGrants (the switch is on) but the runtime role does not hold it (missing grant)"
	var violations []Violation
	var deleteHeld bool
	if err := conn.QueryRow(ctx, `SELECT has_table_privilege($1, format('%I.%I', $2::text, $3::text), 'DELETE')`,
		options.RuntimeRole, publicSchema, g.Table).Scan(&deleteHeld); err != nil {
		return nil, fmt.Errorf("%w: table %s.%s is missing or unreadable to the posture check", ErrMigrationFailed, publicSchema, g.Table)
	}
	if !deleteHeld {
		violations = append(violations, Violation{Kind: "relation", Schema: publicSchema, Object: g.Table, Privilege: "DELETE", Detail: detail})
	}
	for _, columnGrant := range []struct {
		privilege string
		columns   []string
	}{{"SELECT", g.Select}, {"UPDATE", g.Update}} {
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
	return violations, nil
}
