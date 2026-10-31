package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/keyrotation"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const platformRoleBootstrapUsage = "usage: auth-migrate platform-role-bootstrap -principal <uuid> -confirm"

// parsePlatformRoleBootstrapArgs refuses a bad command line before any
// environment or database is read. -confirm is required: the verb writes.
func parsePlatformRoleBootstrapArgs(args []string, stderr io.Writer) (uuid.UUID, error) {
	flags := flag.NewFlagSet(serviceName+" platform-role-bootstrap", flag.ContinueOnError)
	flags.SetOutput(stderr)
	principal := flags.String("principal", "", "the user principal that receives platform_admin")
	confirm := flags.Bool("confirm", false, "required: grant platform_admin while no platform administrator is active")
	if err := flags.Parse(args); err != nil {
		return uuid.Nil, err
	}
	if flags.NArg() != 0 {
		return uuid.Nil, errors.New("positional arguments are not accepted")
	}
	id, err := uuid.Parse(*principal)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, errors.New("-principal must be a principal uuid")
	}
	if !*confirm {
		return uuid.Nil, errors.New("-confirm is required")
	}
	return id, nil
}

// executePlatformRoleBootstrap runs `auth-migrate platform-role-bootstrap`:
// the break-glass platform_admin grant when no platform administrator is
// active. It connects as the migration role, refuses to run as the runtime
// role, and prints ids only.
func executePlatformRoleBootstrap(parent context.Context, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer) int {
	principal, err := parsePlatformRoleBootstrapArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "argument error: %v\n%s\n", err, platformRoleBootstrapUsage)
		return 2
	}
	ctx, store, cleanup, code := openPlatformRoleStoreAsMigrationRole(parent, lookup, stderr, "platform-role-bootstrap", "the bootstrap")
	if code != 0 {
		return code
	}
	defer cleanup()
	id, err := store.Bootstrap(ctx, principal)
	if err != nil {
		if errors.Is(err, platformrole.ErrStore) {
			fmt.Fprintln(stderr, "platform-role-bootstrap error: the store failed; nothing was written")
			return 1
		}
		fmt.Fprintf(stderr, "platform-role-bootstrap refused: %v\n", err)
		return 3
	}
	fmt.Fprintf(stdout, "platform_role_bootstrap assignment=%s principal=%s\n", id, principal)
	return 0
}

// openPlatformRoleStoreAsMigrationRole resolves the migration endpoint, refuses
// to run as the runtime role and opens the platform role store on it, under one
// runBudget context. verb names the verb in error lines and what names it in the
// runtime-role refusal. A nonzero code is the exit code, after its message;
// otherwise the caller defers cleanup.
func openPlatformRoleStoreAsMigrationRole(parent context.Context, lookup secrets.LookupEnv, stderr io.Writer, verb, what string) (context.Context, *platformrole.Store, func(), int) {
	migrationURI, configured, err := secrets.ResolveDSNSetting(envMigrationURI, lookup)
	if err != nil || !configured {
		fmt.Fprintf(stderr, "configuration error: %s or %s_FILE is required\n", envMigrationURI, envMigrationURI)
		return nil, nil, nil, 1
	}
	schema, err := authschema.NewValidatedIdentifier(valueOr(lookup, envSchema, defaultSchema))
	if err != nil {
		fmt.Fprintf(stderr, "configuration error: %s is not a usable schema name\n", envSchema)
		return nil, nil, nil, 1
	}
	runtimeRole, present := lookup(envRuntimeRole)
	runtimeRole = strings.TrimSpace(runtimeRole)
	if !present || authschema.ValidateIdentifier(runtimeRole) != nil {
		fmt.Fprintf(stderr, "configuration error: %s is required\n", envRuntimeRole)
		return nil, nil, nil, 1
	}

	ctx, cancel := context.WithTimeout(parent, runBudget)
	poolConfig := postgresstore.DefaultConfig(migrationURI.Reveal())
	poolConfig.MaxConns = 2
	pool, err := newDatabaseWait(poolConfig, stderr).run(ctx)
	if err != nil {
		cancel()
		fmt.Fprintf(stderr, "%s error: PostgreSQL migration endpoint unavailable\n", verb)
		return nil, nil, nil, 1
	}
	cleanup := func() {
		pool.Close()
		cancel()
	}
	if err := keyrotation.CheckNotRuntimeRole(ctx, pool, runtimeRole); err != nil {
		if errors.Is(err, keyrotation.ErrRuntimeRole) {
			fmt.Fprintf(stderr, "configuration error: %s connects as the runtime role %q; %s runs as the migration role only\n",
				envMigrationURI, runtimeRole, what)
		} else {
			fmt.Fprintf(stderr, "%s error: could not read the connected role\n", verb)
		}
		cleanup()
		return nil, nil, nil, 1
	}
	store, err := platformrole.New(platformrole.Config{Pool: pool, Schema: schema})
	if err != nil {
		fmt.Fprintf(stderr, "%s error: could not open the platform role store\n", verb)
		cleanup()
		return nil, nil, nil, 1
	}
	return ctx, store, cleanup, 0
}
