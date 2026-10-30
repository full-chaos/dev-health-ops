package authschema

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/aclguard"
	"github.com/jackc/pgx/v5"
)

// Options.DeferNarrowing is the 4->3 flip-back release's grants pass. The old
// owned auth-service pods still serve while its pre-upgrade hooks run, and each
// of their public writes (the sign-in stamp, a register, a password change)
// needs the users-owned grants: revoking them there answers 500 (SQLSTATE
// 42501). With the acknowledgement, a deferred run keeps every lost grant that
// the users-owned switch gives (usersOwnedPublicGrant) and narrows only the
// rest; a later release without the deferral narrows them with its own
// acknowledgement, after no owned pod is left.

// usersOwnedPublicGrant reports whether g, a direct grant of the runtime role
// on a public table (g.Object is the bare table name), is one the users-owned
// switch gives: UsersWriteGrant, RegisterOrganizationGrants or
// LegacyLinkPurgeGrants. A grant held WITH GRANT OPTION never is.
func usersOwnedPublicGrant(g GrantRecord) bool {
	if g.Grantable {
		return false
	}
	switch g.Kind {
	case "relation":
		return g.Privilege == "DELETE" && (isLegacyLinkPurgeTable(g.Object) || isMembershipWriteDelete(g.Object))
	case "column":
		if isMembershipWriteColumn(g.Object, g.Privilege, g.Column) || isInviteWriteColumn(g.Object, g.Privilege, g.Column) {
			return true
		}
		if g.Object == "users" {
			insert, update := UsersWriteGrant()
			switch g.Privilege {
			case "INSERT":
				return slices.Contains(insert, g.Column)
			case "UPDATE":
				return slices.Contains(update, g.Column)
			}
			return false
		}
		if g.Privilege == "INSERT" {
			for _, grant := range RegisterOrganizationGrants() {
				if grant.Table == g.Object {
					return slices.Contains(grant.Columns, g.Column)
				}
			}
		}
		if g.Privilege == "SELECT" {
			for _, grant := range LegacyLinkPurgeGrants() {
				if grant.Table == g.Object {
					return slices.Contains(grant.Select, g.Column)
				}
			}
		}
	}
	return false
}

// retainUsersOwnedGrants re-grants, in tx, every lost record that is a
// users-owned public grant, and returns those records with bare table names.
// lost names public objects "public.<table>" (applyRuntimeGrants). Each
// statement names its privilege as a keyword of its own format.
func retainUsersOwnedGrants(ctx context.Context, tx pgx.Tx, lost []GrantRecord, roleID ValidatedIdentifier) ([]GrantRecord, error) {
	// applyRuntimeGrants already holds the lock in tx; a second take in the same transaction returns at once.
	if err := aclguard.Lock(ctx, tx); err != nil {
		return nil, fmt.Errorf("%w: taking the privilege writer lock", ErrMigrationFailed)
	}
	var retained []GrantRecord
	for _, g := range lost {
		table, ok := strings.CutPrefix(g.Object, publicSchema+".")
		if !ok {
			continue
		}
		record := g
		record.Object = table
		if !usersOwnedPublicGrant(record) {
			continue
		}
		tableID, err := NewValidatedIdentifier(table)
		if err != nil {
			return nil, err
		}
		schemaID := mustValidatedIdentifier(publicSchema)
		var statement string
		switch {
		case record.Kind == "relation" && record.Privilege == "DELETE":
			statement = fmt.Sprintf(`GRANT DELETE ON TABLE %s.%s TO %s`,
				quoteIdentifier(schemaID), quoteIdentifier(tableID), quoteIdentifier(roleID))
		case record.Kind == "column":
			columnID, err := NewValidatedIdentifier(record.Column)
			if err != nil {
				return nil, err
			}
			switch record.Privilege {
			case "INSERT":
				statement = fmt.Sprintf(`GRANT INSERT (%s) ON TABLE %s.%s TO %s`,
					quoteIdentifier(columnID), quoteIdentifier(schemaID), quoteIdentifier(tableID), quoteIdentifier(roleID))
			case "UPDATE":
				statement = fmt.Sprintf(`GRANT UPDATE (%s) ON TABLE %s.%s TO %s`,
					quoteIdentifier(columnID), quoteIdentifier(schemaID), quoteIdentifier(tableID), quoteIdentifier(roleID))
			case "SELECT":
				statement = fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s.%s TO %s`,
					quoteIdentifier(columnID), quoteIdentifier(schemaID), quoteIdentifier(tableID), quoteIdentifier(roleID))
			}
		}
		if statement == "" {
			return nil, fmt.Errorf("%w: no re-grant form for a users-owned %s %s", ErrMigrationFailed, record.Kind, record.Privilege)
		}
		if _, err := tx.Exec(ctx, statement); err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrMigrationFailed, statement, redactPGError(err))
		}
		retained = append(retained, record)
	}
	return retained, nil
}

// isPlatformReaderColumn reports whether table.column is a column of
// PlatformRoleReaderColumns.
func isPlatformReaderColumn(table, column string) bool {
	for _, grant := range PlatformRoleReaderColumns() {
		if grant.Table == table {
			return slices.Contains(grant.Columns, column)
		}
	}
	return false
}

// readerNarrowing is what a session reader lost of the platform role read
// (column SELECTs of PlatformRoleReaderColumns) between before and after,
// named by the reader. Any other grant a reader loses is one the reader grant
// never declared (a re-apply removes it without an acknowledgement).
func readerNarrowing(reader string, before, after []GrantRecord) []GrantRecord {
	var lost []GrantRecord
	for _, g := range narrowedGrants(before, after) {
		if g.Kind != "column" || g.Privilege != "SELECT" || !isPlatformReaderColumn(g.Object, g.Column) {
			continue
		}
		g.Role = reader
		lost = append(lost, g)
	}
	return lost
}

// retainReaderPlatformGrants re-grants, in tx, every lost session reader
// record that is a column SELECT of PlatformRoleReaderColumns (the platform
// roles switch turned off while old owned pods still read), and returns them.
func retainReaderPlatformGrants(ctx context.Context, tx pgx.Tx, lost []GrantRecord, schemaID ValidatedIdentifier) ([]GrantRecord, error) {
	var retained []GrantRecord
	for _, g := range lost {
		if g.Role == "" || g.Grantable || g.Kind != "column" || g.Privilege != "SELECT" || !isPlatformReaderColumn(g.Object, g.Column) {
			continue
		}
		roleID, err := NewValidatedIdentifier(g.Role)
		if err != nil {
			return nil, err
		}
		tableID, err := NewValidatedIdentifier(g.Object)
		if err != nil {
			return nil, err
		}
		columnID, err := NewValidatedIdentifier(g.Column)
		if err != nil {
			return nil, err
		}
		statement := fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s.%s TO %s`,
			quoteIdentifier(columnID), quoteIdentifier(schemaID), quoteIdentifier(tableID), quoteIdentifier(roleID))
		if _, err := tx.Exec(ctx, statement); err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrMigrationFailed, statement, redactPGError(err))
		}
		retained = append(retained, g)
	}
	return retained, nil
}

// excuseRetained drops the posture violations that are exactly a retained
// record: a public column privilege ("table.column"), a public table
// privilege, or a session reader's retained column ("table.column", named by
// the reader). Every other violation stays.
func excuseRetained(violations []Violation, retained []GrantRecord) []Violation {
	if len(retained) == 0 {
		return violations
	}
	kept := violations[:0:0]
	for _, v := range violations {
		excused := false
		for _, g := range retained {
			if g.Role != "" {
				if v.Kind == "session_reader" && v.Privilege == g.Privilege && v.Object == g.Object+"."+g.Column &&
					strings.HasPrefix(v.Detail, "session reader "+g.Role+" ") {
					excused = true
					break
				}
				continue
			}
			if v.Schema != publicSchema || v.Privilege != g.Privilege || v.Kind != g.Kind {
				continue
			}
			if (g.Kind == "column" && v.Object == g.Object+"."+g.Column) || (g.Kind == "relation" && v.Object == g.Object) {
				excused = true
				break
			}
		}
		if !excused {
			kept = append(kept, v)
		}
	}
	return kept
}
