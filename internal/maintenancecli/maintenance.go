// Package maintenancecli is the `maintenance` group of dho: the operator
// housekeeping verbs `dev-hops maintenance` ran. Each verb is compared with the
// real Python verb on a real PostgreSQL by the venue oracle.
package maintenancecli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// Command is the `maintenance` group.
func Command() cli.Command {
	return cli.Command{
		Name:    "maintenance",
		Summary: "operator housekeeping over the application database",
		Kind:    cli.Group,
		Children: []cli.Command{
			{Name: "cleanup-tokens", Summary: "delete expired refresh tokens", Kind: cli.Verb, Run: runCleanupTokens},
			{Name: "cleanup-all", Summary: "run every maintenance cleanup task", Kind: cli.Verb, Run: runCleanupAll},
			{Name: "backfill-ask-dev-ephemeral-expiry", Summary: "stamp expires_at on stranded 0-day Ask Dev conversations", Kind: cli.Verb, Run: runBackfillExpiry},
			{Name: "scrub-error-text", Summary: "scrub credential material from the legacy error-text columns (dry run unless --apply)", Kind: cli.Verb, Run: runScrub},
		},
	}
}

const environmentUsage = "\nEnvironment:\n" +
	"  MIGRATION_DATABASE_URI (or _FILE, or the DEV_HEALTH_MIGRATION_PG_* component form)   the database\n" +
	"  POSTGRES_URI (or _FILE)   used when MIGRATION_DATABASE_URI is not configured\n"

// session is one verb's connection and its way of reporting.
type session struct {
	conn     *pgx.Conn
	boundary secrets.Boundary
	logger   *slog.Logger
	env      cli.Env
}

// open parses the flags (through parse, which may add its own), connects, and
// returns a session; a non-nil exit code means the verb is over.
func open(ctx context.Context, name string, env cli.Env, parse func(*flag.FlagSet)) (*session, *flag.FlagSet, *int) {
	flags := flag.NewFlagSet("dho maintenance "+name, flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	defaultUsage := flags.Usage
	flags.Usage = func() {
		defaultUsage()
		fmt.Fprint(env.Stderr, environmentUsage)
	}
	if parse != nil {
		parse(flags)
	}
	exit := func(code int) (*session, *flag.FlagSet, *int) { return nil, nil, &code }
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exit(cli.ExitOK)
		}
		return exit(cli.ExitUsage)
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted")
		return exit(cli.ExitUsage)
	}
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}
	dsn, _, ok := config.ResolveMigrationDatabase(env.Lookup, env.Stderr, true)
	if !ok {
		return exit(cli.ExitFailure)
	}
	boundary := pgstorage.Boundary(dsn.Reveal())
	conn, err := pgx.Connect(ctx, dsn.Reveal())
	if err != nil {
		writeError(env.Stderr, "postgres_unavailable", boundary.Redact(err).Error())
		return exit(cli.ExitFailure)
	}
	return &session{conn: conn, boundary: boundary, logger: logging.NewJSON(env.Stderr, slog.LevelInfo), env: env}, flags, nil
}

func (s *session) close() { _ = s.conn.Close(context.Background()) }

func (s *session) fail(code string, err error) int {
	writeError(s.env.Stderr, code, s.boundary.Redact(err).Error())
	return cli.ExitFailure
}

func writeError(stderr io.Writer, code, detail string) {
	_ = json.NewEncoder(stderr).Encode(map[string]any{"error": map[string]string{"code": code, "detail": detail}})
}

func (s *session) writeJSON(value any) int {
	if err := json.NewEncoder(s.env.Stdout).Encode(value); err != nil {
		fmt.Fprintln(s.env.Stderr, "could not write the result")
		return cli.ExitFailure
	}
	return cli.ExitOK
}

// clock is the wall clock, truncated to the microsecond a timestamptz keeps.
var clock = func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// refreshTokenGrace is how long past its expiry a refresh token is kept
// (refresh_tokens.cleanup_expired: cutoff = now - 24 hours).
const refreshTokenGrace = 24 * time.Hour

// CleanupExpiredTokens deletes the refresh tokens that expired more than a day
// ago and returns how many.
func CleanupExpiredTokens(ctx context.Context, conn *pgx.Conn, now time.Time) (int64, error) {
	tag, err := conn.Exec(ctx, "DELETE FROM refresh_tokens WHERE expires_at < $1", now.Add(-refreshTokenGrace))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func runCleanupTokens(ctx context.Context, env cli.Env) int {
	s, _, exit := open(ctx, "cleanup-tokens", env, nil)
	if exit != nil {
		return *exit
	}
	defer s.close()
	deleted, err := CleanupExpiredTokens(ctx, s.conn, clock())
	if err != nil {
		return s.fail("cleanup_failed", err)
	}
	s.logger.Info("deleted expired refresh tokens", "deleted", deleted)
	return s.writeJSON(map[string]int64{"deleted": deleted})
}

func runCleanupAll(ctx context.Context, env cli.Env) int {
	s, _, exit := open(ctx, "cleanup-all", env, nil)
	if exit != nil {
		return *exit
	}
	defer s.close()
	deleted, err := CleanupExpiredTokens(ctx, s.conn, clock())
	if err != nil {
		return s.fail("cleanup_failed", err)
	}
	s.logger.Info("maintenance cleanup complete", "deleted", deleted, "total", deleted)
	return s.writeJSON(map[string]int64{"deleted": deleted, "total": deleted})
}

const (
	// ephemeralAbandonedGrace is EPHEMERAL_ABANDONED_GRACE: a 0-day conversation
	// idle for this long cannot have a live run.
	ephemeralAbandonedGrace = time.Hour
	// expiryBackfillLimit is the batch size the Python verb uses (the service
	// accepts 1..500).
	expiryBackfillLimit = 500
)

// BackfillEphemeralExpiry stamps expires_at on the 0-day conversations
// stranded before the retention-sweep fix existed (CHAOS-3404, widened by
// CHAOS-3544 to select by age). It commits per batch and loops until a batch
// comes back short, so it drains the backlog in one run and is safe to re-run.
// Like the ORM update it replaces, it also moves updated_at (the model's
// onupdate) to the time of the write.
func BackfillEphemeralExpiry(ctx context.Context, conn *pgx.Conn, limit int, now func() time.Time) (int, error) {
	total := 0
	for {
		stamped, err := backfillBatch(ctx, conn, limit, now)
		if err != nil {
			return total, err
		}
		total += stamped
		if stamped < limit {
			return total, nil
		}
	}
}

func backfillBatch(ctx context.Context, conn *pgx.Conn, limit int, now func() time.Time) (int, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	stamp := now()
	rows, err := tx.Query(ctx, `
SELECT id FROM dev_conversations
WHERE retention_days = 0 AND expires_at IS NULL AND updated_at < $1
ORDER BY created_at, id
LIMIT $2
FOR UPDATE SKIP LOCKED`, stamp.Add(-ephemeralAbandonedGrace), limit)
	if err != nil {
		return 0, fmt.Errorf("select stranded conversations: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[[16]byte])
	if err != nil {
		return 0, fmt.Errorf("read stranded conversations: %w", err)
	}
	for _, id := range ids {
		if _, err := tx.Exec(ctx, "UPDATE dev_conversations SET expires_at = $2, updated_at = $3 WHERE id = $1", id, stamp, now()); err != nil {
			return 0, fmt.Errorf("stamp a conversation: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(ids), nil
}

func runBackfillExpiry(ctx context.Context, env cli.Env) int {
	s, _, exit := open(ctx, "backfill-ask-dev-ephemeral-expiry", env, nil)
	if exit != nil {
		return *exit
	}
	defer s.close()
	total, err := BackfillEphemeralExpiry(ctx, s.conn, expiryBackfillLimit, clock)
	if err != nil {
		return s.fail("backfill_failed", err)
	}
	s.logger.Info("Ask Dev ephemeral-expiry backfill complete", "stamped", total)
	return s.writeJSON(map[string]int{"stamped": total})
}
