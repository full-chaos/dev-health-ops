package config

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The migration database: the elevated DSN every one-shot migrate step
// (`dho migrate postgres|river|upgrade`, `dho admin features seed`) connects
// with. It lives here, below every migrate package, so each resolves it the
// same way without importing another.

// NoMigrationDatabaseMessage is the refusal the chart hook's shell wrapper
// printed, verbatim, when --apply-and-check finds neither DSN.
const NoMigrationDatabaseMessage = "river-migrate: neither MIGRATION_DATABASE_URI nor POSTGRES_URI is set in the migration Secret; " +
	"this Job needs an ELEVATED DSN pointed DIRECTLY at PostgreSQL (5432), never at a transaction pooler"

// ResolveMigrationDatabase is CHAOS-5560's component alternative to a
// pre-built MIGRATION_DATABASE_URI: compose.yml's own entrypoint already
// assembles a fallback DSN by raw shell interpolation
// (postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/$POSTGRES_DB)
// when MIGRATION_DATABASE_URI is unset -- exactly the unescaped-password
// class this ticket fixes, just one shell layer further out.
//
// The component var names are deliberately NOT POSTGRES_HOST/_PORT/_USER/
// _PASSWORD/_DB: deploy/docker-compose/compose.go-workers.yml (deleted, CHAOS-6950) (not touched
// by this PR) already sets every one of those, unconditionally, for its own
// pre-existing shell fallback -- reusing those names would make this
// function activate every time that compose
// service ran, silently discarding a perfectly valid, already-working
// MIGRATION_DATABASE_URI override. DEV_HEALTH_MIGRATION_PG_* is a prefix
// swept against compose.yml, both overlays, deploy/helm, and docs before
// being chosen (zero hits) so setting it can never collide with anything
// today, deployed or documented. See config.ResolveDSN for the shared
// mutual-exclusion contract this now defers to instead of picking a
// precedence winner.
// MigrationDatabaseSpec is the ONE ComponentSpec for MIGRATION_DATABASE_URI,
// shared by ResolveMigrationDatabase and rivermigrate's execute's own Info-resolution
// record (config.ComponentDatabaseIdentity) so the two never risk drifting
// into two different definitions of "the migration database's component
// form".
var MigrationDatabaseSpec = ComponentSpec{
	HostKey: "DEV_HEALTH_MIGRATION_PG_HOST", PortKey: "DEV_HEALTH_MIGRATION_PG_PORT", DefaultPort: "5432",
	UserKey: "DEV_HEALTH_MIGRATION_PG_USER", PasswordKey: "DEV_HEALTH_MIGRATION_PG_PASSWORD",
	DBKey: "DEV_HEALTH_MIGRATION_PG_DB", DefaultDB: "postgres", Scheme: "postgresql",
}

// ResolveMigrationDatabase resolves the elevated migration DSN and names
// its source. MIGRATION_DATABASE_URI (or _FILE, or the
// DEV_HEALTH_MIGRATION_PG_* components) always wins. Only when it is not
// configured and fallbackToPostgres is set (--apply-and-check) does
// POSTGRES_URI (or _FILE) stand in, exactly as the hook's shell wrapper did;
// with neither, the wrapper's own message is printed.
func ResolveMigrationDatabase(
	lookup secrets.LookupEnv,
	stderr io.Writer,
	fallbackToPostgres bool,
) (secrets.Value, string, bool) {
	value, configured, err := ResolveDSN(lookup, "MIGRATION_DATABASE_URI", MigrationDatabaseSpec)
	if err != nil {
		WriteConfigError(stderr, err)
		return secrets.Value{}, "", false
	}
	if configured {
		return normalizeMigrationDSN(value), "MIGRATION_DATABASE_URI", true
	}
	if !fallbackToPostgres {
		WriteConfigError(stderr, errors.New("MIGRATION_DATABASE_URI is required"))
		return secrets.Value{}, "", false
	}
	postgres, configured, err := secrets.Resolve("POSTGRES_URI", lookup)
	if err != nil {
		WriteConfigError(stderr, err)
		return secrets.Value{}, "", false
	}
	if !configured || strings.TrimSpace(postgres.Reveal()) == "" {
		fmt.Fprintln(stderr, NoMigrationDatabaseMessage)
		return secrets.Value{}, "", false
	}
	return normalizeMigrationDSN(postgres), "POSTGRES_URI", true
}

// migrationDriverSchemes are the SQLAlchemy driver-qualified schemes the
// Python stack writes (the chart's bundled POSTGRES_URI is
// postgresql+asyncpg://). pgx parses only postgres:// and postgresql://, so
// each is rewritten to postgresql:// -- the same aliases
// internal/storage/postgres accepts at its own Go boundary.
var migrationDriverSchemes = []string{
	"postgresql+asyncpg://",
	"postgresql+psycopg://",
	"postgresql+psycopg2://",
	"postgres+asyncpg://",
}

// normalizeMigrationDSN rewrites a driver-qualified scheme to postgresql://
// and changes nothing else: credentials, host, path and query stay as given.
func normalizeMigrationDSN(value secrets.Value) secrets.Value {
	raw := value.Reveal()
	for _, scheme := range migrationDriverSchemes {
		if strings.HasPrefix(strings.ToLower(raw), scheme) {
			return secrets.NewValue("postgresql://" + raw[len(scheme):])
		}
	}
	return value
}
