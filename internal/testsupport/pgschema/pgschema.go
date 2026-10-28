// Package pgschema gives Go integration tests the REAL Postgres schema: the
// pgmigrate baseline and chain, the same schema the Alembic heads produce.
// Tests use it instead of hand-writing CREATE TABLE for production tables
// (Trap #412; the guard is TestHandWrittenTestDDLMatchesTheMigratedSchema in
// internal/pgmigrate). A hand-written table drifts: #3134 made a query read
// integrations.credential_id and the tests' own `integrations` table lacked
// it (CHAOS-6717).
package pgschema

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Apply builds the migrated schema in the database behind pool. It fails the
// test on any error; it never leaves a half-built schema to be read as a
// smaller one.
func Apply(ctx context.Context, t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	ApplyURI(ctx, t, pool.Config().ConnString())
}

// ApplyURI is Apply for a test that holds a connection string, not a pool.
func ApplyURI(ctx context.Context, t testing.TB, uri string) {
	t.Helper()
	defer containers.TimePhase("schema", "postgres", "")()
	if err := applyURI(ctx, uri); err != nil {
		t.Fatalf("pgschema: %v", err)
	}
}

func applyURI(ctx context.Context, uri string) error {
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(context.Background())
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		return fmt.Errorf("load the baseline: %w", err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		return fmt.Errorf("load the chain: %w", err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		return fmt.Errorf("apply the migrated schema: %w", err)
	}
	return nil
}

// Start returns a private database with the migrated schema, as a
// containers.StartPostgres followed by ApplyURI does. In shared mode the
// schema is built once per template key and each call is a copy of it
// (containers.StartPostgresFromTemplate); elsewhere it is built in the
// caller's own database. A test that needs an empty database, or that changes
// the schema's inputs after the call, keeps StartPostgres plus Apply. The key
// reads the environment at the time of this call. The caller closes the
// instance, as after StartPostgres.
func Start(ctx context.Context, t testing.TB) *containers.Instance {
	t.Helper()
	user, searchPath, err := containers.PostgresSessionState(ctx)
	if err != nil {
		t.Fatalf("pgschema: read the session state of the shared server: %v", err)
	}
	key, err := key(SessionState{CurrentUser: user, SearchPath: searchPath})
	if err != nil {
		t.Fatalf("pgschema: template key: %v", err)
	}
	instance, err := containers.StartPostgresFromTemplate(ctx, key, applyURI)
	if err != nil {
		t.Fatalf("pgschema: %v", err)
	}
	return instance
}

// keyVersion changes when the way the schema is built changes in a way the
// inputs below do not show.
const keyVersion = "pgschema-template-2"

// KeyEnv lists the environment values pgmigrate.Upgrade reads; a different
// value can build a different schema, so each is part of the key.
var KeyEnv = []string{pgmigrate.AuthSchemaEnv, pgmigrate.RiverSchemaEnv, pgmigrate.CutoverEnv}

// Key is KeyFor over the embedded baseline and chain, the harness image and
// the process environment, read now.
func Key() (string, error) { return key(SessionState{}) }

func key(state SessionState) (string, error) {
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		return "", err
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		return "", err
	}
	return KeyForSession(baseline, chain, containers.PostgresImage, os.LookupEnv, state), nil
}

// KeyFor hashes everything the migrated schema is built from: the baseline,
// every chain file (revision, name, body) in the order given, the image, and
// each KeyEnv value (a set-but-empty value differs from an unset one).
func KeyFor(baseline pgmigrate.Baseline, chain []pgmigrate.ChainFile, image string, lookup func(string) (string, bool)) string {
	return KeyForSession(baseline, chain, image, lookup, SessionState{})
}

// SessionState is what the build role sees on the server before any database
// default applies (containers.PostgresSessionState): a role default for
// search_path or another role builds the same chain into other places or under
// another owner.
type SessionState struct{ CurrentUser, SearchPath string }

// KeyForSession is KeyFor with the session state of the build role in the hash.
func KeyForSession(baseline pgmigrate.Baseline, chain []pgmigrate.ChainFile, image string, lookup func(string) (string, bool), state SessionState) string {
	hash := sha256.New()
	field := func(parts ...string) {
		for _, part := range parts {
			fmt.Fprintf(hash, "%d:%s|", len(part), part)
		}
		hash.Write([]byte{'\n'})
	}
	field("version", keyVersion)
	field("image", image)
	field("session", state.CurrentUser, state.SearchPath)
	for _, name := range KeyEnv {
		value, set := lookup(name)
		field("env", name, fmt.Sprint(set), value)
	}
	field("baseline", fmt.Sprint(baseline.Cutover), baseline.RiverSchema, strings.Join(baseline.Heads, ","), baseline.Schema, baseline.Data)
	for _, file := range chain {
		field("chain", file.Revision, file.Name, file.SQL)
	}
	return hex.EncodeToString(hash.Sum(nil))
}
