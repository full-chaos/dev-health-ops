// Command mint-edge-token is the tools image's `-edge-bearer-exec` helper
// for go-api-prove: it mints a fresh edge ACCESS TOKEN for the dedicated
// proof service principal, locally, with the same signing key the api
// pod's environment holds (JWT_SECRET_KEY), reached the same way (a Secret
// key mounted into this process's environment via secretKeyRef). It is the
// sibling of cmd/mint-envelope; internal/edgetokenmint has the design.
//
// It reads the principal row and its org membership from POSTGRES_URI
// before signing, so the token carries the row's current token_version and
// a principal the edge would refuse is refused here, by name.
//
// Its stdout is exactly what go-api-prove's -edge-bearer-exec contract
// expects: the token and nothing else. No error or diagnostic this program
// writes ever carries the key, the database URI, or the token.
package edgetoken

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// postgresURIEnvVar is read by name only: a DSN carries a password, so it
// is never a flag value.
const postgresURIEnvVar = "POSTGRES_URI"

// lookupTimeout bounds the database read. It stays below go-api-prove's
// own per-invocation helper timeout, so a slow database fails here with
// this program's message rather than as a killed helper.
const lookupTimeout = 15 * time.Second

// Command is the `mint edge-token` verb of the dho binary. It prints the
// raw credential and nothing else on stdout on success -- the exemption
// documented at spec S1 §3.4.
func Command() cli.Command {
	return cli.Command{
		Name:    "edge-token",
		Kind:    cli.Verb,
		Summary: "mint a fresh edge access token for the proof service principal and print it on stdout",
		Run: func(ctx context.Context, env cli.Env) int {
			if err := run(ctx, env.Args, env.Stdout, openPostgres); err != nil {
				fmt.Fprintln(env.Stderr, "mint-edge-token:", err)
				return cli.ExitFailure
			}
			return cli.ExitOK
		},
	}
}

// openFunc opens the database the principal is read from. A test swaps in
// a fake; production uses openPostgres.
type openFunc func(ctx context.Context) (edgetokenmint.RowQuerier, func(), error)

func openPostgres(ctx context.Context) (edgetokenmint.RowQuerier, func(), error) {
	uri := os.Getenv(postgresURIEnvVar)
	if strings.TrimSpace(uri) == "" {
		return nil, nil, fmt.Errorf("%s is not set", postgresURIEnvVar)
	}
	// storage/postgres replaces every driver error with a fixed category,
	// so a malformed or unreachable URI never reaches this message.
	pool, err := postgres.Open(ctx, postgres.DefaultConfig(uri))
	if err != nil {
		return nil, nil, fmt.Errorf("connect to the database named by %s: %w", postgresURIEnvVar, err)
	}
	return pool, pool.Close, nil
}

// Mint runs the same flag parsing and minting `run` does, but returns the
// credential directly instead of writing it to an io.Writer. It is the
// in-process minting entry point internal/goapiproof.MintViaAllowlistedHelper
// calls (spec S1): a prover now mints its own edge token in process instead
// of exec'ing this package's fixed-path standalone binary. It opens its own
// Postgres connection exactly as the standalone binary did -- the prover's
// own connection is not shared, since the two lookups' lifetimes differ.
func Mint(ctx context.Context, args []string) (string, error) {
	var out bytes.Buffer
	if err := run(ctx, args, &out, openPostgres); err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

func run(ctx context.Context, args []string, stdout io.Writer, open openFunc) error {
	fs := flag.NewFlagSet("mint-edge-token", flag.ContinueOnError)
	org := fs.String("org", "", "org id (UUID) to mint the token for; the proof service principal must hold a read-level membership in it")
	ttl := fs.Duration("ttl", edgetokenmint.DefaultTTL, fmt.Sprintf("token lifetime, at most %s; go-api-prove re-runs this helper as the token ages", edgetokenmint.MaxTTL))
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*org) == "" {
		return errors.New("-org is required")
	}
	if _, err := uuid.Parse(*org); err != nil {
		return errors.New("-org must be a UUID")
	}
	if *ttl <= 0 || *ttl > edgetokenmint.MaxTTL {
		return fmt.Errorf("-ttl must be greater than 0 and at most %s", edgetokenmint.MaxTTL)
	}

	// The key is checked before the database is opened, so a pod missing
	// its secretKeyRef fails without a connection attempt.
	signingKey, err := edgetokenmint.LoadSigningKeyFromEnv()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, lookupTimeout)
	defer cancel()
	db, closeDB, err := open(ctx)
	if err != nil {
		return err
	}
	defer closeDB()

	token, err := edgetokenmint.MintForProve(ctx, db, signingKey, *org, edgetokenmint.Options{TTL: *ttl})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, token)
	return err
}
