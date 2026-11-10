package authschema

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ErrMigratorGrantOptionMissing reports a grants pass that stopped before its first GRANT because the migration login
// holds no grant option on a public privilege the switches ask it to give the runtime role.
var ErrMigratorGrantOptionMissing = fmt.Errorf(
	"%w: the migration login lacks WITH GRANT OPTION on privileges the grants pass must give", ErrMigrationFailed,
)

// grantOptionGap is one privilege the migrator must hold WITH GRANT OPTION and does not. Column is empty for a
// table-level privilege.
type grantOptionGap struct {
	Table, Privilege, Column string
}

func (g grantOptionGap) String() string {
	if g.Column == "" {
		return fmt.Sprintf("%s ON %s.%s", g.Privilege, publicSchema, g.Table)
	}
	return fmt.Sprintf("%s (%s) ON %s.%s", g.Privilege, g.Column, publicSchema, g.Table)
}

// describeGrantOptionGaps is the refusal text: every gap, sorted, then the remedy naming the verb, the table owner
// and the migrator.
func describeGrantOptionGaps(migrator string, owners []string, gaps []grantOptionGap) error {
	gaps = append([]grantOptionGap(nil), gaps...)
	sort.Slice(gaps, func(i, j int) bool {
		a, b := gaps[i], gaps[j]
		if a.Table != b.Table {
			return a.Table < b.Table
		}
		if a.Privilege != b.Privilege {
			return a.Privilege < b.Privilege
		}
		return a.Column < b.Column
	})
	lines := make([]string, len(gaps))
	for i, g := range gaps {
		lines[i] = "  - " + g.String()
	}
	owners = append([]string(nil), owners...)
	sort.Strings(owners)
	return fmt.Errorf(
		"%w (migration login %q; nothing was granted):\n%s\n"+
			"remedy: run `auth-migrate owner-move -grants-only` (dry run first, then -apply) with the DSN of the table owner (%s) "+
			"and -to %s, then run this migrate again",
		ErrMigratorGrantOptionMissing, migrator, strings.Join(lines, "\n"), strings.Join(owners, ", "), migrator,
	)
}

// migratorGrantOptionPrecheck refuses, before the first public GRANT, when the migration login lacks WITH GRANT OPTION
// on a privilege the switches turn on. PostgreSQL grants only what the grantor holds the option on and answers a
// WARNING, so without this the run ends in a posture failure that names the runtime role only. An absent table or
// column is deferred, as the grant statements defer it.
func migratorGrantOptionPrecheck(ctx context.Context, conn querier, options Options) error {
	var migrator string
	if err := conn.QueryRow(ctx, `SELECT current_user::text`).Scan(&migrator); err != nil {
		return fmt.Errorf("%w: reading the migration login", ErrMigrationFailed)
	}
	var gaps []grantOptionGap
	for _, entry := range publicPrivilegeSetFor(options.UsersOwned) {
		if _, err := NewValidatedIdentifier(entry.Table); err != nil {
			return err
		}
		exists, err := publicTableState(ctx, conn, entry.Table)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		if entry.Columns == nil {
			held, err := hasGrantOption(ctx, conn,
				`SELECT has_table_privilege(current_user, format('%I.%I', $1::text, $2::text), $3::text)`,
				publicSchema, entry.Table, entry.Privilege+" WITH GRANT OPTION")
			if err != nil {
				return err
			}
			if !held {
				gaps = append(gaps, grantOptionGap{Table: entry.Table, Privilege: entry.Privilege})
			}
			continue
		}
		for _, column := range entry.Columns {
			if _, err := NewValidatedIdentifier(column); err != nil {
				return err
			}
			present, err := publicColumnExists(ctx, conn, entry.Table, column)
			if err != nil {
				return err
			}
			if !present {
				continue
			}
			held, err := hasGrantOption(ctx, conn,
				`SELECT has_column_privilege(current_user, format('%I.%I', $1::text, $2::text), $4::text, $3::text)`,
				publicSchema, entry.Table, entry.Privilege+" WITH GRANT OPTION", column)
			if err != nil {
				return err
			}
			if !held {
				gaps = append(gaps, grantOptionGap{Table: entry.Table, Privilege: entry.Privilege, Column: column})
			}
		}
	}
	if len(gaps) == 0 {
		return nil
	}
	owners, err := gapTableOwners(ctx, conn, gaps)
	if err != nil {
		return err
	}
	return describeGrantOptionGaps(migrator, owners, gaps)
}

func hasGrantOption(ctx context.Context, conn rowQuerier1, query string, args ...any) (bool, error) {
	var held bool
	if err := conn.QueryRow(ctx, query, args...).Scan(&held); err != nil {
		return false, fmt.Errorf("%w: reading the migration login's grant options", ErrMigrationFailed)
	}
	return held, nil
}

func gapTableOwners(ctx context.Context, conn rowQuerier1, gaps []grantOptionGap) ([]string, error) {
	seenTable := map[string]bool{}
	seenOwner := map[string]bool{}
	var owners []string
	for _, g := range gaps {
		if seenTable[g.Table] {
			continue
		}
		seenTable[g.Table] = true
		var owner string
		err := conn.QueryRow(ctx,
			`SELECT pg_get_userbyid(c.relowner)::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			  WHERE n.nspname = $1 AND c.relname = $2`, publicSchema, g.Table).Scan(&owner)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("%w: reading the owner of %s.%s", ErrMigrationFailed, publicSchema, g.Table)
		}
		if !seenOwner[owner] {
			seenOwner[owner] = true
			owners = append(owners, owner)
		}
	}
	return owners, nil
}
