package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/keyrotation"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

const keysUsage = "usage: auth-migrate keys list | adopt | register|activate|retire|rollback -kid K | compromise -kid K [-successor S]  [-apply]"

// keysArgs is a parsed `keys` command line.
type keysArgs struct {
	verb      string
	kid       string
	successor string
	apply     bool
}

// parseKeysArgs refuses a bad command line before any environment or
// database is read.
func parseKeysArgs(args []string, stderr io.Writer) (keysArgs, error) {
	if len(args) == 0 {
		return keysArgs{}, errors.New("a verb is required")
	}
	parsed := keysArgs{verb: args[0]}
	flags := flag.NewFlagSet(serviceName+" keys", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&parsed.kid, "kid", "", "the key the verb acts on")
	flags.StringVar(&parsed.successor, "successor", "", "compromise: a pending key to activate in the same transaction")
	flags.BoolVar(&parsed.apply, "apply", false, "write the change (without it the verb is a dry run)")
	if err := flags.Parse(args[1:]); err != nil {
		return keysArgs{}, err
	}
	if flags.NArg() != 0 {
		return keysArgs{}, errors.New("positional arguments are not accepted")
	}
	switch keyrotation.Verb(parsed.verb) {
	case "list", keyrotation.VerbAdopt:
		if parsed.kid != "" {
			return keysArgs{}, fmt.Errorf("%s takes no -kid", parsed.verb)
		}
	case keyrotation.VerbRegister, keyrotation.VerbActivate, keyrotation.VerbRetire,
		keyrotation.VerbRollback, keyrotation.VerbCompromise:
		if !token.ValidKeyID(parsed.kid) {
			return keysArgs{}, fmt.Errorf("%s needs a valid -kid", parsed.verb)
		}
	default:
		return keysArgs{}, errors.New("unknown verb")
	}
	if parsed.successor != "" && (parsed.verb != string(keyrotation.VerbCompromise) || !token.ValidKeyID(parsed.successor)) {
		return keysArgs{}, errors.New("-successor takes a valid kid and applies to compromise only")
	}
	if parsed.apply && parsed.verb == "list" {
		return keysArgs{}, errors.New("-apply does not apply to list")
	}
	return parsed, nil
}

// executeKeys runs `auth-migrate keys`: the signing-key rotation verbs. It
// connects as the migration role (the runtime role cannot write signing_keys)
// and refuses to run as the runtime role.
func executeKeys(parent context.Context, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer) int {
	parsed, err := parseKeysArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "argument error: %v\n%s\n", err, keysUsage)
		return 2
	}

	migrationURI, configured, err := secrets.ResolveDSNSetting(envMigrationURI, lookup)
	if err != nil || !configured {
		fmt.Fprintf(stderr, "configuration error: %s or %s_FILE is required\n", envMigrationURI, envMigrationURI)
		return 1
	}
	schemaName := valueOr(lookup, envSchema, defaultSchema)
	schema, err := authschema.NewValidatedIdentifier(schemaName)
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
	request := keyrotation.Request{
		Verb: keyrotation.Verb(parsed.verb), KeyID: parsed.kid, Successor: parsed.successor,
	}

	ctx, cancel := context.WithTimeout(parent, runBudget)
	defer cancel()
	if request.Verb == keyrotation.VerbAdopt || request.Verb == keyrotation.VerbRegister {
		custody, err := custodyConfig(lookup)
		if err != nil {
			fmt.Fprintf(stderr, "configuration error: %v\n", err)
			return 1
		}
		if request.Custody, err = token.CustodyKeys(ctx, custody); err != nil {
			fmt.Fprintf(stderr, "custody error: %s\n", token.ReasonOf(err))
			return 1
		}
	}

	poolConfig := postgresstore.DefaultConfig(migrationURI.Reveal())
	poolConfig.MaxConns = 2
	pool, err := newDatabaseWait(poolConfig, stderr).run(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "keys error: PostgreSQL migration endpoint unavailable")
		return 1
	}
	defer pool.Close()
	if err := keyrotation.CheckNotRuntimeRole(ctx, pool, runtimeRole); err != nil {
		if errors.Is(err, keyrotation.ErrRuntimeRole) {
			fmt.Fprintf(stderr, "configuration error: %s connects as the runtime role %q; key states are written by the migration role only\n",
				envMigrationURI, runtimeRole)
		} else {
			fmt.Fprintln(stderr, "keys error: could not read the connected role")
		}
		return 1
	}

	operator := keyrotation.Operator{Pool: pool, Schema: schema}
	if parsed.verb == "list" {
		rows, err := operator.List(ctx)
		if err != nil {
			fmt.Fprintln(stderr, "keys error: could not read signing_keys")
			return 1
		}
		for _, row := range rows {
			fmt.Fprintf(stdout, "%s\t%s\tsince %s\n", row.KeyID, row.State, row.StateSince.UTC().Format("2006-01-02T15:04:05Z"))
		}
		return 0
	}

	var result keyrotation.Result
	if parsed.apply {
		result, err = operator.Apply(ctx, request)
	} else {
		result, err = operator.Plan(ctx, request)
	}
	var refusal *keyrotation.Refusal
	switch {
	case errors.As(err, &refusal):
		fmt.Fprintf(stderr, "refused: %s", refusal.Reason)
		if result.FloorRemaining > 0 {
			fmt.Fprintf(stderr, " (floor reached in %s)", result.FloorRemaining.Round(time.Second))
		}
		fmt.Fprintln(stderr)
		return 1
	case errors.Is(err, keyrotation.ErrStateChanged):
		fmt.Fprintln(stderr, "keys error: the key states changed while the verb ran; nothing was written, run it again")
		return 1
	case err != nil:
		fmt.Fprintf(stderr, "keys error: %v\n", err)
		return 1
	}
	mode := "dry run (add -apply to write)"
	if parsed.apply {
		mode = "applied"
	}
	fmt.Fprintf(stdout, "%s %s:\n", parsed.verb, mode)
	for _, move := range result.Moves {
		fmt.Fprintf(stdout, "  %s\n", move)
	}
	if request.Verb == keyrotation.VerbCompromise {
		fmt.Fprintf(stdout, "  sessions revoked: %d\n", result.Sessions)
	}
	return 0
}

// custodyConfig reads the same custody settings auth-service reads: a key-set
// manifest, or one key file and its kid.
func custodyConfig(lookup secrets.LookupEnv) (token.FileConfig, error) {
	get := func(name string) string {
		value, _ := lookup(name)
		return strings.TrimSpace(value)
	}
	config := token.FileConfig{
		KeySetPath: get(authconfig.EnvSigningKeySetFile),
		KeyPath:    get(authconfig.EnvSigningKeyFile),
		KeyID:      get(authconfig.EnvSigningKeyID),
	}
	switch {
	case config.KeySetPath != "" && (config.KeyPath != "" || config.KeyID != ""):
		return token.FileConfig{}, fmt.Errorf("%s excludes %s and %s",
			authconfig.EnvSigningKeySetFile, authconfig.EnvSigningKeyFile, authconfig.EnvSigningKeyID)
	case config.KeySetPath == "" && (config.KeyPath == "" || config.KeyID == ""):
		return token.FileConfig{}, fmt.Errorf("%s, or %s with %s, is required",
			authconfig.EnvSigningKeySetFile, authconfig.EnvSigningKeyFile, authconfig.EnvSigningKeyID)
	}
	return config, nil
}
