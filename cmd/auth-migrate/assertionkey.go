package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/keyrotation"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	assertionKeySetUsage = "usage: auth-migrate workload-assertion-key-set -workload N -issuer URL -kid K -status active|retiring|retired" +
		" -rotated-at RFC3339 [-retire-after RFC3339] [-retire-previous-after DUR] -confirm" +
		"\nThe key is public metadata: no key, no file and no digest is taken or printed." +
		"\nA new active key while another is active needs -retire-previous-after (up to 24h): the old one becomes retiring in the same step." +
		"\nA retiring key needs -retire-after, at most 24h after the active key's -rotated-at. A retired key never changes again."
	assertionKeyListUsage = "usage: auth-migrate workload-assertion-key-list [-workload N]"
)

type assertionKeySetArgs struct {
	name string
	spec workload.AssertionKeySpec
}

func parseWireTime(flagName, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("-%s must be an RFC 3339 time with a zone, like 2026-10-31T12:00:00Z", flagName)
	}
	return parsed.UTC(), nil
}

// parseAssertionKeySetArgs refuses a bad command line before any environment
// or database is read. -confirm is required: the verb writes. There is no flag
// that takes a key, a key file or a digest.
func parseAssertionKeySetArgs(args []string, stderr io.Writer) (assertionKeySetArgs, error) {
	flags := flag.NewFlagSet(serviceName+" workload-assertion-key-set", flag.ContinueOnError)
	flags.SetOutput(stderr)
	name := flags.String("workload", "", "the registered workload name")
	issuer := flags.String("issuer", "", "the issuer URL the verifier pins")
	kid := flags.String("kid", "", "the key id the signer puts in the token header")
	status := flags.String("status", "", "active, retiring or retired")
	rotatedAt := flags.String("rotated-at", "", "when the key was put in service (RFC 3339)")
	retireAfter := flags.String("retire-after", "", "retiring only: when the key stops verifying (RFC 3339)")
	retirePrevious := flags.Duration("retire-previous-after", 0, "a new active key only: the active key becomes retiring for this long (up to 24h)")
	confirm := flags.Bool("confirm", false, "required: write the change")
	if err := flags.Parse(args); err != nil {
		return assertionKeySetArgs{}, err
	}
	if flags.NArg() != 0 {
		return assertionKeySetArgs{}, errors.New("positional arguments are not accepted")
	}
	if !*confirm {
		return assertionKeySetArgs{}, errors.New("-confirm is required")
	}
	if workload.ValidateName(*name) != nil {
		return assertionKeySetArgs{}, errors.New("-workload must be a registered workload name")
	}
	set := map[string]bool{}
	flags.Visit(func(f *flag.Flag) { set[f.Name] = true })
	spec := workload.AssertionKeySpec{Issuer: *issuer, KID: *kid, Status: workload.AssertionKeyStatus(*status)}
	var err error
	if spec.RotatedAt, err = parseWireTime("rotated-at", *rotatedAt); err != nil {
		return assertionKeySetArgs{}, err
	}
	if set["retire-after"] {
		if spec.RetireAfter, err = parseWireTime("retire-after", *retireAfter); err != nil {
			return assertionKeySetArgs{}, err
		}
	}
	if set["retire-previous-after"] {
		if *retirePrevious <= 0 {
			return assertionKeySetArgs{}, errors.New("-retire-previous-after must be above 0 and at most 24h")
		}
		spec.RetirePreviousAfter = *retirePrevious
	}
	if err := workload.ValidateAssertionKey(spec); err != nil {
		return assertionKeySetArgs{}, err
	}
	return assertionKeySetArgs{name: *name, spec: spec}, nil
}

func parseAssertionKeyListArgs(args []string, stderr io.Writer) (string, error) {
	flags := flag.NewFlagSet(serviceName+" workload-assertion-key-list", flag.ContinueOnError)
	flags.SetOutput(stderr)
	name := flags.String("workload", "", "limit the list to one workload")
	if err := flags.Parse(args); err != nil {
		return "", err
	}
	if flags.NArg() != 0 {
		return "", errors.New("positional arguments are not accepted")
	}
	if *name != "" && workload.ValidateName(*name) != nil {
		return "", errors.New("-workload must be a workload name")
	}
	return *name, nil
}

func assertionKeyFailureClass(err error) string {
	switch {
	case errors.Is(err, workload.ErrNotFound):
		return "workload not registered"
	case errors.Is(err, workload.ErrAssertionKeyWorkloadDisabled):
		return "the workload is disabled; enable it first"
	case errors.Is(err, workload.ErrAssertionKeyInvalid):
		return "a field was refused"
	case errors.Is(err, workload.ErrAssertionKeyActiveExists):
		return "the workload has an active key; add -retire-previous-after to rotate"
	case errors.Is(err, workload.ErrAssertionKeyNoActive):
		return "the workload has no active key to overlap"
	case errors.Is(err, workload.ErrAssertionKeyOverlap):
		return "retire-after is more than 24h after the active key's rotated-at"
	case errors.Is(err, workload.ErrAssertionKeyTerminal):
		return "a retired key does not change"
	case errors.Is(err, workload.ErrAssertionKeyTransition):
		return "the change is not allowed from the key's current state"
	case errors.Is(err, workload.ErrStore):
		return "store unavailable"
	}
	return "refused"
}

// openAssertionKeyStore connects as the migration role and opens the registry
// store. It refuses to run as the runtime role. A non-zero code is the exit
// code; the caller returns it.
func openAssertionKeyStore(parent context.Context, verb string, lookup secrets.LookupEnv, stderr io.Writer) (*workload.Store, context.Context, func(), int) {
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
	closeAll := func() {
		pool.Close()
		cancel()
	}
	if err := keyrotation.CheckNotRuntimeRole(ctx, pool, runtimeRole); err != nil {
		closeAll()
		if errors.Is(err, keyrotation.ErrRuntimeRole) {
			fmt.Fprintf(stderr, "configuration error: %s connects as the runtime role %q; the registry is read and written by the migration role only\n",
				envMigrationURI, runtimeRole)
		} else {
			fmt.Fprintf(stderr, "%s error: could not read the connected role\n", verb)
		}
		return nil, nil, nil, 1
	}
	store, err := workload.New(workload.Config{Pool: pool, Schema: schema})
	if err != nil {
		closeAll()
		fmt.Fprintf(stderr, "%s error: could not open the registry\n", verb)
		return nil, nil, nil, 1
	}
	return store, ctx, closeAll, 0
}

// executeAssertionKeySet runs `auth-migrate workload-assertion-key-set`. It
// prints ids and the status only.
func executeAssertionKeySet(parent context.Context, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer) int {
	parsed, err := parseAssertionKeySetArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "argument error: %v\n%s\n", err, assertionKeySetUsage)
		return 2
	}
	store, ctx, closeAll, code := openAssertionKeyStore(parent, "workload-assertion-key-set", lookup, stderr)
	if code != 0 {
		return code
	}
	defer closeAll()
	id, err := store.SetAssertionKey(ctx, parsed.name, parsed.spec, workload.Actor{})
	if err != nil {
		if errors.Is(err, workload.ErrStore) {
			fmt.Fprintln(stderr, "workload-assertion-key-set error: the store failed; nothing was written")
			return 1
		}
		fmt.Fprintf(stderr, "workload-assertion-key-set refused: %s\n", assertionKeyFailureClass(err))
		return 3
	}
	fmt.Fprintf(stdout, "workload_assertion_key_set workload=%s key=%s status=%s\n", parsed.name, id, parsed.spec.Status)
	return 0
}

// executeAssertionKeyList runs `auth-migrate workload-assertion-key-list`: the
// public key metadata, one line a key.
func executeAssertionKeyList(parent context.Context, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer) int {
	name, err := parseAssertionKeyListArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "argument error: %v\n%s\n", err, assertionKeyListUsage)
		return 2
	}
	store, ctx, closeAll, code := openAssertionKeyStore(parent, "workload-assertion-key-list", lookup, stderr)
	if code != 0 {
		return code
	}
	defer closeAll()
	keys, err := store.ListAssertionKeys(ctx, name)
	if err != nil {
		fmt.Fprintln(stderr, "workload-assertion-key-list error: the store failed")
		return 1
	}
	for _, key := range keys {
		fmt.Fprintln(stdout, assertionKeyLine(key))
	}
	return 0
}

// assertionKeyLine is one line of the list: public metadata only.
func assertionKeyLine(key workload.AssertionKey) string {
	retire := "-"
	if key.RetireAfter != nil {
		retire = key.RetireAfter.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("workload=%s key=%s issuer=%s kid=%s status=%s rotated_at=%s retire_after=%s",
		key.Workload, key.ID, key.Issuer, key.KID, key.Status, key.RotatedAt.UTC().Format(time.RFC3339Nano), retire)
}
