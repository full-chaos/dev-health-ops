// Command auth-migrate applies the Auth Control Plane's schema migrations.
//
// It is a separate, one-shot binary and the ONLY thing that issues DDL against
// the auth schema. ACP-ADR-04 (Accepted 2026-09-02): "auth-migrate is a
// separate binary and the runtime never auto-migrates. The runtime role owns
// no DDL." auth-service therefore has no migration code path at all — it
// observes the schema's presence as a readiness question and repairs nothing.
//
// This file is a THIN ENTRYPOINT, the same constraint CHAOS-4881 put on
// cmd/auth-service: it parses arguments and environment, opens a pool, and
// hands off. The lineage, the SQL, the grant manifest and the version
// bookkeeping all live in internal/storage/postgres/authschema, so this binary
// can be re-hosted as a subcommand later without moving any of them.
//
// `auth-migrate keys` runs the signing-key rotation verbs (keys.go,
// internal/auth/keyrotation) as the same migration role: the runtime role
// reads key states and never writes them.
//
// # Roles
//
// Two distinct PostgreSQL roles are involved and the distinction is the whole
// security posture. The MIGRATION role connects here, owns every object it
// creates, and holds DDL. The RUNTIME role is only ever the subject of a GRANT
// and never executes anything in this process. Running this binary with a
// connection that authenticates AS the runtime role is refused, because it
// would make the runtime role the owner of the schema and therefore the holder
// of permanent DDL over it — the exact posture the ADR forbids, established by
// a run that otherwise looks successful.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	envsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const serviceName = "auth-migrate"

// Environment variable names, declared once.
const (
	envMigrationURI = "AUTH_MIGRATION_DATABASE_URI"
	envSchema       = "AUTH_DATABASE_SCHEMA"
	envRuntimeRole  = "AUTH_RUNTIME_DATABASE_ROLE"
	// envSessionReaders names the roles (comma-separated) of the services that
	// end an access token when its session is revoked: go-api and query-api.
	envSessionReaders = "AUTH_SESSION_READER_ROLES"
)

const (
	defaultSchema = "auth"
	// runBudget bounds the whole run. A migration that cannot finish inside
	// it is a migration that is waiting on a lock it will not get, and a
	// one-shot job that hangs forever in a deployment pipeline is worse than
	// one that fails.
	runBudget = 5 * time.Minute
)

func main() {
	// Linked packages (internal/audit) log through slog.Default(); this makes
	// that the redacting handler, on stderr.
	logging.InstallDefault(logging.NewJSON(os.Stderr, slog.LevelInfo))
	os.Exit(execute(context.Background(), os.Args[1:], envsecrets.ProcessLookup, os.Stdout, os.Stderr))
}

func execute(
	parent context.Context,
	args []string,
	lookup secrets.LookupEnv,
	stdout io.Writer,
	stderr io.Writer,
) int {
	if len(args) > 0 && args[0] == "keys" {
		return executeKeys(parent, args[1:], lookup, stdout, stderr)
	}
	grantsOnly := len(args) > 0 && args[0] == "grants"
	if grantsOnly {
		args = args[1:]
	}
	flags := flag.NewFlagSet(serviceName, flag.ContinueOnError)
	flags.SetOutput(stderr)
	check := flags.Bool("check", false,
		"report whether the auth schema is at the embedded lineage head, without issuing DDL")
	ackNarrowing := flags.String("ack-grant-narrowing", "",
		"digest printed by a refused run, acknowledging that exact deliberate narrowing of the runtime role's grants")
	showVersion := flags.Bool("version", false, "print build metadata as JSON and exit")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "argument error: positional arguments are not accepted")
		return 2
	}
	if *check && grantsOnly {
		fmt.Fprintln(stderr, "argument error: -check does not apply to grants")
		return 2
	}
	if *showVersion {
		if err := version.Current(serviceName).WriteJSON(stdout); err != nil {
			fmt.Fprintln(stderr, "could not write version metadata")
			return 1
		}
		return 0
	}

	migrationURI, configured, err := secrets.ResolveDSNSetting(envMigrationURI, lookup)
	if err != nil {
		fmt.Fprintf(stderr, "configuration error: could not resolve %s\n", envMigrationURI)
		return 1
	}
	if !configured {
		fmt.Fprintf(stderr, "configuration error: %s or %s_FILE is required\n",
			envMigrationURI, envMigrationURI)
		return 1
	}
	schema := valueOr(lookup, envSchema, defaultSchema)
	if err := authschema.ValidateIdentifier(schema); err != nil {
		fmt.Fprintf(stderr, "configuration error: %s is not a usable schema name\n", envSchema)
		return 1
	}

	ctx, cancel := context.WithTimeout(parent, runBudget)
	defer cancel()

	poolConfig := postgresstore.DefaultConfig(migrationURI.Reveal())
	// Two connections: one holds the advisory lock for the run, the second is
	// spare so a pool exhaustion cannot deadlock the run against itself.
	poolConfig.MaxConns = 2
	pool, err := newDatabaseWait(poolConfig, stderr).run(ctx)
	if err != nil {
		// The wait writes only attempt numbers and error categories, never a
		// driver error, so a DSN cannot reach a log line here.
		fmt.Fprintln(stderr, "migration error: PostgreSQL migration endpoint unavailable")
		return 1
	}
	defer pool.Close()

	if *check {
		current, head, checkErr := authschema.Check(ctx, pool, schema)
		if checkErr != nil {
			fmt.Fprintf(stderr,
				"migration check failed: auth schema is at version %d, embedded head is %d\n",
				current, head)
			return 1
		}
		fmt.Fprintf(stdout, "auth schema current at version %d\n", current)
		return 0
	}

	runtimeRole, present := lookup(envRuntimeRole)
	if !present || strings.TrimSpace(runtimeRole) == "" {
		fmt.Fprintf(stderr, "configuration error: %s is required\n", envRuntimeRole)
		return 1
	}
	if err := authschema.ValidateIdentifier(runtimeRole); err != nil {
		fmt.Fprintf(stderr, "configuration error: %s is not a usable role name\n", envRuntimeRole)
		return 1
	}

	readers, err := sessionReaderRoles(lookup)
	if err != nil {
		fmt.Fprintf(stderr, "configuration error: %v\n", err)
		return 1
	}

	logger := logging.NewJSON(stderr, slog.LevelInfo)
	if grantsOnly {
		result, err := authschema.Grants(ctx, pool, authschema.Options{
			Schema:               schema,
			RuntimeRole:          runtimeRole,
			SessionReaderRoles:   readers,
			Logger:               logger,
			AcknowledgeNarrowing: strings.TrimSpace(*ackNarrowing),
		})
		if err != nil {
			fmt.Fprintf(stderr, "grants failed: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "auth grants applied at version %d, runtime grants applied to %q, session read grants applied to %d roles\n",
			result.CurrentVersion, runtimeRole, len(readers))
		return 0
	}
	result, err := authschema.Apply(ctx, pool, authschema.Options{
		Schema:             schema,
		RuntimeRole:        runtimeRole,
		SessionReaderRoles: readers,
		Logger:             logger,

		AcknowledgeNarrowing: strings.TrimSpace(*ackNarrowing),
	})
	if err != nil {
		// authschema's errors are built from its own SQL and validated
		// identifiers, and its driver errors are reduced to SQLSTATE plus
		// message, so this is safe to print in full — and it is the only
		// thing an operator has to work from when a migration fails.
		fmt.Fprintf(stderr, "migration failed: %v\n", err)
		return 1
	}

	fmt.Fprintf(stdout,
		"auth schema current at version %d (%d applied), runtime grants applied to %q, session read grants applied to %d roles\n",
		result.CurrentVersion, len(result.AppliedVersions), runtimeRole, len(readers))
	return 0
}

// sessionReaderRoles reads AUTH_SESSION_READER_ROLES: role names separated by
// commas, blanks ignored. Unset or empty means none.
func sessionReaderRoles(lookup secrets.LookupEnv) ([]string, error) {
	raw, _ := lookup(envSessionReaders)
	var roles []string
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if err := authschema.ValidateIdentifier(entry); err != nil {
			return nil, fmt.Errorf("%s holds an unusable role name", envSessionReaders)
		}
		roles = append(roles, entry)
	}
	return roles, nil
}

func valueOr(lookup secrets.LookupEnv, key, fallback string) string {
	if value, present := lookup(key); present && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}
