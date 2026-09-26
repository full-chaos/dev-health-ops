package rivermigrate

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/roleprovision"
)

// rolesCommand is `dho migrate roles` (CHAOS-6901): the Go replacement for
// scripts/worker/provision_river_roles.sql, which the chart's provision-roles Job
// and Compose's go-river-provision ran through psql. It reads the same variables the
// script took (as environment, like every other migrate step) and creates and
// bootstraps the same logins; see internal/storage/roleprovision for exactly what
// that is, and what it never does (no runtime-role table privilege, no DROP OWNED BY; the one table
// grant is the KEDA login's SELECT on public.sync_run_units, CHAOS-6946).
func rolesCommand() cli.Command {
	return cli.Command{
		Name:    "roles",
		Summary: "create the unprivileged Go runtime logins (domain, queue, coordinator, optional api/query-api/KEDA), or check them (--check)",
		Kind:    cli.Verb,
		Run: func(ctx context.Context, env cli.Env) int {
			return ExecuteRoles(ctx, "dho", env.Args, env.Lookup, env.Stdout, env.Stderr)
		},
	}
}

// rolesEnvironment documents the variables, in --help and in the README.
const rolesEnvironment = "\nEnvironment (every password also accepts <NAME>_FILE):\n" +
	"  MIGRATION_DATABASE_URI (or _FILE)   elevated DSN, direct (never a transaction pooler); falls back to POSTGRES_URI\n" +
	"  RIVER_DOMAIN_DATABASE_ROLE / RIVER_DOMAIN_DATABASE_PASSWORD         required\n" +
	"  RIVER_QUEUE_DATABASE_ROLE / RIVER_QUEUE_DATABASE_PASSWORD           required\n" +
	"  RIVER_COORDINATOR_DATABASE_ROLE (default devhealth_coordinator) / RIVER_COORDINATOR_DATABASE_PASSWORD\n" +
	"  API_DATABASE_ROLE / API_DATABASE_PASSWORD                           optional: provisioned only when the role is set\n" +
	"  QUERY_API_DATABASE_ROLE / QUERY_API_DATABASE_PASSWORD               optional\n" +
	"  RIVER_KEDA_READONLY_DATABASE_ROLE / RIVER_KEDA_READONLY_PASSWORD    optional (KEDA read-only scaler login)\n" +
	"  RIVER_DATABASE_SCHEMA (default river)                               the schema the KEDA login may read\n"

// ExecuteRoles runs `dho migrate roles`. Default: apply, then verify the bootstrap
// postconditions on a fresh connection. --check: verify only, change nothing.
func ExecuteRoles(
	parent context.Context,
	service string,
	args []string,
	lookup platformsecrets.LookupEnv,
	stdout, stderr io.Writer,
) int {
	flags := flag.NewFlagSet(service+" migrate roles", flag.ContinueOnError)
	flags.SetOutput(stderr)
	check := flags.Bool("check", false, "verify the provisioned logins and change nothing")
	defaultUsage := flags.Usage
	flags.Usage = func() {
		defaultUsage()
		fmt.Fprint(stderr, rolesEnvironment)
	}
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

	options, ok := rolesOptionsFromEnvironment(lookup, stderr)
	if !ok {
		return 1
	}
	if err := options.Validate(); err != nil {
		config.WriteConfigError(stderr, err)
		return 1
	}
	migrationURI, source, ok := resolveMigrationDatabaseURI(lookup, stderr, true)
	if !ok {
		return 1
	}
	infoLogger := logging.NewJSON(stderr, slog.LevelInfo)
	infoLogger.InfoContext(parent, "migration database resolved", "source", source)
	migrationRole, err := postgresstore.ConnectionUser(migrationURI.Reveal())
	if err != nil {
		config.WriteConfigError(stderr, errors.New("invalid MIGRATION_DATABASE_URI"))
		return 1
	}
	// The migration login must not be one of the roles it bootstraps: REVOKE CREATE
	// and friends would be applied to the account doing the work (the same refusal
	// `dho migrate river` makes).
	for label, name := range roleNames(options) {
		if name == migrationRole {
			config.WriteConfigError(stderr, fmt.Errorf("the migration login must be distinct from the %s role", label))
			return 1
		}
	}

	poolConfig := postgresstore.DefaultConfig(migrationURI.Reveal())
	poolConfig.MaxConns = 1
	poolConfig.MaxConnIdleTime = time.Minute
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	pool, err := postgresstore.Open(ctx, poolConfig)
	if err != nil {
		fmt.Fprintln(stderr, "migration error: PostgreSQL migration endpoint unavailable")
		return 1
	}
	defer pool.Close()

	// Verify logs in as each role with the supplied password, on the same direct
	// endpoint the migration used: a stale password on a login that already existed
	// (never rotated here) is then reported instead of passing as "provisioned".
	options.Authenticate = func(authCtx context.Context, role roleprovision.Role) error {
		config, err := pgx.ParseConfig(migrationURI.Reveal())
		if err != nil {
			return err
		}
		config.User, config.Password = role.Name, role.Password
		attemptCtx, attemptCancel := context.WithTimeout(authCtx, 15*time.Second)
		defer attemptCancel()
		connection, err := pgx.ConnectConfig(attemptCtx, config)
		if err != nil {
			return err
		}
		return connection.Close(attemptCtx)
	}
	if !*check {
		if err := roleprovision.Apply(ctx, pool, options); err != nil {
			// roleprovision's errors carry no password and no server text.
			infoLogger.ErrorContext(ctx, "role provisioning failed", "error", err.Error())
			fmt.Fprintln(stderr, "migration failed: the runtime roles were not provisioned")
			return 1
		}
		infoLogger.InfoContext(ctx, "runtime roles provisioned", "roles", roleLabels(options))
		fmt.Fprintf(stdout, "runtime roles provisioned: %s\n", strings.Join(roleLabels(options), ", "))
	}

	problems, warnings, err := roleprovision.Verify(ctx, pool, options)
	if err != nil {
		infoLogger.ErrorContext(ctx, "role check failed", "error", err.Error())
		fmt.Fprintln(stderr, "migration check failed: the runtime roles could not be verified")
		return 1
	}
	for _, warning := range warnings {
		infoLogger.WarnContext(ctx, "role bootstrap warning", "role", warning.Role, "detail", warning.Detail)
	}
	if len(problems) > 0 {
		for _, problem := range problems {
			infoLogger.ErrorContext(ctx, "role bootstrap postcondition not met", "role", problem.Role, "detail", problem.Detail)
		}
		fmt.Fprintln(stderr, "migration check failed: the runtime roles do not meet the bootstrap postconditions; see the preceding structured log lines")
		return 1
	}
	fmt.Fprintf(stdout, "runtime roles meet the bootstrap postconditions: %s\n", strings.Join(roleLabels(options), ", "))
	return 0
}

// rolesOptionsFromEnvironment reads the script's variables. Passwords are resolved
// through platformsecrets (env or _FILE) and never echoed. An optional role is
// provisioned only when its role name is set, exactly like the script's
// `\if :{?api_role}` blocks.
func rolesOptionsFromEnvironment(lookup platformsecrets.LookupEnv, stderr io.Writer) (roleprovision.Options, bool) {
	var options roleprovision.Options
	fail := func(err error) (roleprovision.Options, bool) {
		config.WriteConfigError(stderr, err)
		return roleprovision.Options{}, false
	}
	password := func(key string) (string, error) {
		value, _, err := platformsecrets.Resolve(key, lookup)
		if err != nil {
			return "", err
		}
		return value.Reveal(), nil
	}
	// A role name is used EXACTLY as configured (the script took --set values
	// verbatim, and `dho migrate river` uses the raw value too): trimming it would
	// provision a different role. A blank value is "not set".
	name := func(key string) string {
		value, _ := lookup(key)
		if strings.TrimSpace(value) == "" {
			return ""
		}
		return value
	}
	role := func(roleKey, passwordKey string, required bool, fallback string) (roleprovision.Role, error) {
		roleName := name(roleKey)
		if roleName == "" {
			roleName = fallback
		}
		if roleName == "" {
			if required {
				return roleprovision.Role{}, fmt.Errorf("%s is required", roleKey)
			}
			return roleprovision.Role{}, nil
		}
		secret, err := password(passwordKey)
		if err != nil {
			return roleprovision.Role{}, err
		}
		if secret == "" {
			return roleprovision.Role{}, fmt.Errorf("%s is required when %s is set", passwordKey, roleKey)
		}
		return roleprovision.Role{Name: roleName, Password: secret}, nil
	}
	var err error
	if options.Domain, err = role("RIVER_DOMAIN_DATABASE_ROLE", "RIVER_DOMAIN_DATABASE_PASSWORD", true, ""); err != nil {
		return fail(err)
	}
	if options.Queue, err = role("RIVER_QUEUE_DATABASE_ROLE", "RIVER_QUEUE_DATABASE_PASSWORD", true, ""); err != nil {
		return fail(err)
	}
	if options.Coordinator, err = role("RIVER_COORDINATOR_DATABASE_ROLE", "RIVER_COORDINATOR_DATABASE_PASSWORD", true, defaultCoordinatorRole); err != nil {
		return fail(err)
	}
	if options.API, err = role("API_DATABASE_ROLE", "API_DATABASE_PASSWORD", false, ""); err != nil {
		return fail(err)
	}
	if options.QueryAPI, err = role("QUERY_API_DATABASE_ROLE", "QUERY_API_DATABASE_PASSWORD", false, ""); err != nil {
		return fail(err)
	}
	if options.Keda, err = role("RIVER_KEDA_READONLY_DATABASE_ROLE", "RIVER_KEDA_READONLY_PASSWORD", false, ""); err != nil {
		return fail(err)
	}
	options.RiverSchema = name("RIVER_DATABASE_SCHEMA")
	return options, true
}

func roleNames(options roleprovision.Options) map[string]string {
	names := map[string]string{"domain": options.Domain.Name, "queue": options.Queue.Name, "coordinator": options.Coordinator.Name}
	for label, role := range map[string]roleprovision.Role{"api": options.API, "query_api": options.QueryAPI, "keda": options.Keda} {
		if role.Name != "" {
			names[label] = role.Name
		}
	}
	return names
}

func roleLabels(options roleprovision.Options) []string {
	labels := []string{"domain", "queue", "coordinator"}
	if options.API.Name != "" {
		labels = append(labels, "api")
	}
	if options.QueryAPI.Name != "" {
		labels = append(labels, "query_api")
	}
	if options.Keda.Name != "" {
		labels = append(labels, "keda")
	}
	return labels
}
