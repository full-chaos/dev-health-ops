// Package authschema owns the Auth Control Plane's PostgreSQL schema and its
// migration lineage.
//
// # A second lineage, deliberately
//
// ACP-ADR-04 left "extend the ops alembic lineage or start a second one"
// open; it was ruled on 2026-09-02 in favour of a SECOND, independent
// lineage. The reason is concrete rather than aesthetic: the ops alembic
// lineage has head revisions pinned as literals across many test files, so
// sharing it would make every auth migration a cross-cutting change against
// tooling that has nothing to do with auth. This package therefore keeps its
// own ordered migration set and its own version table inside the auth schema,
// and knows nothing about alembic.
//
// # Ownership
//
// ACP-ADR-04: "auth-migrate is a separate binary and the runtime never
// auto-migrates. The runtime role owns no DDL." Every statement in this
// package runs as the MIGRATION role. The runtime role is only ever the
// SUBJECT of a GRANT here, never the executor, and Apply refuses to run when
// the two are the same role.
//
// # Additive only
//
// Wave 1 migrations create new objects and nothing else: no existing row
// moves, no writer changes, no drops, no destructive rewrites. That is the
// property that makes this deployable ahead of any code that reads it, and
// it is enforced by a test over the embedded SQL rather than by convention.
package authschema

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationNamePattern is the required filename shape: a zero-padded ordinal,
// an underscore, a lowercase description, and the .sql suffix. The ordinal is
// the lineage position and must be unique and gapless -- both checked by
// loadMigrations, so a mis-numbered file fails at load rather than applying in
// a surprising order.
var migrationNamePattern = regexp.MustCompile(`^([0-9]{4})_([a-z0-9_]+)\.sql$`)

// Migration is one ordered step in the lineage.
type Migration struct {
	// Version is the ordinal parsed from the filename. Lineage positions
	// start at 1 and increase by exactly one.
	Version int
	// Name is the descriptive half of the filename, without the ordinal or
	// the suffix. It is recorded in the version table so an operator reading
	// the database can tell which change a row corresponds to.
	Name string
	// SQL is the file's contents, applied verbatim in a single transaction.
	SQL string
}

// ErrInvalidLineage reports a migration set this package refuses to apply:
// a malformed filename, a duplicate ordinal, or a gap in the sequence. It is
// deliberately a load-time failure -- a lineage that cannot be ordered
// unambiguously must never be applied partially to find that out.
var ErrInvalidLineage = errors.New("auth migration lineage is not well formed")

// Migrations returns the embedded lineage in ascending version order.
//
// It is exported so a test can assert properties of the SQL itself -- that it
// is additive, that it introduces no plaintext credential column -- without a
// database. Those properties are checkable statically and so are checked
// statically; the ones that are not (privileges actually held by a role) are
// proven against a live PostgreSQL instead.
func Migrations() ([]Migration, error) { return loadMigrations(migrationFS) }

func loadMigrations(files fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(files, "migrations")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidLineage, err)
	}

	migrations := make([]Migration, 0, len(entries))
	seen := make(map[int]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			return nil, fmt.Errorf("%w: %q is a directory", ErrInvalidLineage, entry.Name())
		}
		match := migrationNamePattern.FindStringSubmatch(entry.Name())
		if match == nil {
			return nil, fmt.Errorf(
				"%w: %q does not match NNNN_name.sql", ErrInvalidLineage, entry.Name(),
			)
		}
		version, convErr := strconv.Atoi(match[1])
		if convErr != nil || version < 1 {
			return nil, fmt.Errorf("%w: %q has no usable version", ErrInvalidLineage, entry.Name())
		}
		if previous, duplicate := seen[version]; duplicate {
			return nil, fmt.Errorf(
				"%w: version %d is claimed by both %q and %q",
				ErrInvalidLineage, version, previous, entry.Name(),
			)
		}
		seen[version] = entry.Name()

		body, readErr := fs.ReadFile(files, "migrations/"+entry.Name())
		if readErr != nil {
			return nil, fmt.Errorf("%w: reading %q: %v", ErrInvalidLineage, entry.Name(), readErr)
		}
		if strings.TrimSpace(string(body)) == "" {
			return nil, fmt.Errorf("%w: %q is empty", ErrInvalidLineage, entry.Name())
		}
		migrations = append(migrations, Migration{
			Version: version, Name: match[2], SQL: string(body),
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})
	// Gapless from 1. A gap means a migration was deleted, which for an
	// append-only lineage is always a mistake: a database that already
	// applied the missing version would disagree with this binary about what
	// "current" means, and neither side could tell.
	for index, migration := range migrations {
		if migration.Version != index+1 {
			return nil, fmt.Errorf(
				"%w: expected version %d, found %d (%s)",
				ErrInvalidLineage, index+1, migration.Version, migration.Name,
			)
		}
	}
	if len(migrations) == 0 {
		return nil, fmt.Errorf("%w: no migrations are embedded", ErrInvalidLineage)
	}
	return migrations, nil
}

// HeadVersion is the highest version in the embedded lineage.
func HeadVersion() (int, error) {
	migrations, err := Migrations()
	if err != nil {
		return 0, err
	}
	return migrations[len(migrations)-1].Version, nil
}
