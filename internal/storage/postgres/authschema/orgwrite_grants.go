package authschema

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// OrganizationWriteGrant is the switch-gated UPDATE the auth runtime role needs
// to keep go-api's public.organizations name and is_active equal to
// auth.organizations while it owns organizations (AUTH_ORGS_OWNED;
// internal/auth/org): a rename, a suspend, a reactivate and a deprovision write
// them in the verb's commit. go-api keeps every other column (description,
// settings, tier, managed_by, stripe_customer_id, the onboarding stamp) and the
// purge DELETE. No INSERT beyond RegisterOrganizationGrants, no DELETE, no
// TRUNCATE. Off, the REVOKE ALL that publicReadStatements issues on the table
// removes it.
type OrganizationWriteGrant struct {
	Table  string
	Update []string
}

func OrganizationWriteGrants() OrganizationWriteGrant {
	return OrganizationWriteGrant{Table: "organizations", Update: []string{"name", "is_active", "updated_at"}}
}

// organizationWriteStatements grants OrganizationWriteGrants when the switch is on.
func organizationWriteStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	if !options.OrgsOwned {
		return nil, nil
	}
	g := OrganizationWriteGrants()
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
	tableID, err := NewValidatedIdentifier(g.Table)
	if err != nil {
		return nil, err
	}
	columns, err := quotedColumns(g.Update)
	if err != nil {
		return nil, err
	}
	table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(tableID)
	logger.InfoContext(ctx, "applying the organization write grant", "schema", publicSchema, "table", g.Table)
	return []string{fmt.Sprintf(`GRANT UPDATE (%s) ON TABLE %s TO %s`, strings.Join(columns, ", "), table, quoteIdentifier(roleID))}, nil
}

// isOrganizationWriteColumn reports whether a public column privilege is one
// OrganizationWriteGrants declares.
func isOrganizationWriteColumn(table, privilege, column string) bool {
	g := OrganizationWriteGrants()
	return table == g.Table && privilege == "UPDATE" && slices.Contains(g.Update, column)
}

// organizationWriteMissing reports each column of OrganizationWriteGrants the
// runtime role cannot UPDATE while the switch is on.
func organizationWriteMissing(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	if !options.OrgsOwned {
		return nil, nil
	}
	g := OrganizationWriteGrants()
	exists, err := publicTableState(ctx, conn, g.Table)
	if err != nil || !exists {
		return nil, err
	}
	const detail = "declared in OrganizationWriteGrants (the switch is on) but the runtime role does not hold it (missing grant)"
	var violations []Violation
	for _, column := range g.Update {
		var held bool
		if err := conn.QueryRow(ctx, `SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, 'UPDATE')`,
			options.RuntimeRole, publicSchema, g.Table, column).Scan(&held); err != nil {
			return nil, fmt.Errorf("%w: column %s.%s.%s is missing or unreadable to the posture check",
				ErrMigrationFailed, publicSchema, g.Table, column)
		}
		if !held {
			violations = append(violations, Violation{Kind: "column", Schema: publicSchema,
				Object: g.Table + "." + column, Privilege: "UPDATE", Detail: detail})
		}
	}
	return violations, nil
}
