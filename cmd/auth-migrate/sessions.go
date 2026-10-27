package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/keyrotation"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const sessionsUsage = "usage: auth-migrate sessions count-unpinned | revoke-unpinned [-apply]"

type sessionsArgs struct {
	verb  string
	apply bool
}

func parseSessionsArgs(args []string, stderr io.Writer) (sessionsArgs, error) {
	if len(args) == 0 {
		return sessionsArgs{}, errors.New("a verb is required")
	}
	parsed := sessionsArgs{verb: args[0]}
	flags := flag.NewFlagSet(serviceName+" sessions", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.BoolVar(&parsed.apply, "apply", false, "write the change (without it revoke-unpinned is a dry run)")
	if err := flags.Parse(args[1:]); err != nil {
		return sessionsArgs{}, err
	}
	if flags.NArg() != 0 {
		return sessionsArgs{}, errors.New("positional arguments are not accepted")
	}
	switch parsed.verb {
	case "count-unpinned":
		if parsed.apply {
			return sessionsArgs{}, errors.New("count-unpinned takes no flags")
		}
	case "revoke-unpinned":
	default:
		return sessionsArgs{}, errors.New("unknown verb")
	}
	return parsed, nil
}

// executeSessions runs `auth-migrate sessions`: the count of sessions without a
// token_version pin and their revocation. It connects as the migration role and
// refuses to run as the runtime role. It prints counts only.
func executeSessions(parent context.Context, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer) int {
	parsed, err := parseSessionsArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "argument error: %v\n%s\n", err, sessionsUsage)
		return 2
	}
	migrationURI, configured, err := secrets.ResolveDSNSetting(envMigrationURI, lookup)
	if err != nil || !configured {
		fmt.Fprintf(stderr, "configuration error: %s or %s_FILE is required\n", envMigrationURI, envMigrationURI)
		return 1
	}
	schema, err := authschema.NewValidatedIdentifier(valueOr(lookup, envSchema, defaultSchema))
	if err != nil {
		fmt.Fprintf(stderr, "configuration error: %s is not a usable schema name\n", envSchema)
		return 1
	}
	runtimeRole, present := lookup(envRuntimeRole)
	runtimeRole = strings.TrimSpace(runtimeRole)
	if !present || authschema.ValidateIdentifier(runtimeRole) != nil {
		fmt.Fprintf(stderr, "configuration error: %s is required\n", envRuntimeRole)
		return 1
	}

	ctx, cancel := context.WithTimeout(parent, runBudget)
	defer cancel()
	poolConfig := postgresstore.DefaultConfig(migrationURI.Reveal())
	poolConfig.MaxConns = 2
	pool, err := newDatabaseWait(poolConfig, stderr).run(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "sessions error: PostgreSQL migration endpoint unavailable")
		return 1
	}
	defer pool.Close()
	if err := keyrotation.CheckNotRuntimeRole(ctx, pool, runtimeRole); err != nil {
		if errors.Is(err, keyrotation.ErrRuntimeRole) {
			fmt.Fprintf(stderr, "configuration error: %s connects as the runtime role %q; sessions are revoked here by the migration role only\n",
				envMigrationURI, runtimeRole)
		} else {
			fmt.Fprintln(stderr, "sessions error: could not read the connected role")
		}
		return 1
	}

	if parsed.verb == "count-unpinned" || !parsed.apply {
		n, err := session.CountUnpinned(ctx, pool, schema)
		if err != nil {
			fmt.Fprintln(stderr, "sessions error: could not count the sessions")
			return 1
		}
		fmt.Fprintf(stdout, "sessions %s unpinned=%d apply=false\n", parsed.verb, n)
		return 0
	}
	n, err := session.RevokeUnpinned(ctx, pool, schema)
	if err != nil {
		fmt.Fprintf(stderr, "sessions error: revoke-unpinned failed after %d sessions\n", n)
		return 1
	}
	fmt.Fprintf(stdout, "sessions revoke-unpinned apply=true revoked=%d\n", n)
	return 0
}
