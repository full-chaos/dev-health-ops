package fixturescli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/fixturesgen"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

const productTelemetryUsage = `Usage: dho fixtures product-telemetry [--org <uuid>]... [--orgs N] [--days N] [--sessions-per-day N] [--seed N]

Seeds product_telemetry_events across one or more organizations so the
platform-admin dashboard and the per-org views have data locally. It replaces
"dev-hops fixtures product-telemetry": the same seed gives the same rows.

Flags:
  --org                explicit organization id to seed; repeatable; overrides --orgs
  --orgs               organizations to seed when no --org is given: the first N rows of
                       Postgres "organizations", or synthetic ids when Postgres is
                       unavailable (default 5)
  --days               days of data per organization (default 30; capped at the table's
                       TTL horizon minus its safety margin)
  --sessions-per-day   average synthetic sessions per day per organization (default 50)
  --seed               deterministic seed, mixed with the organization id (default: none)

Environment:
  CLICKHOUSE_URI (or _FILE, or the DEV_HEALTH_CH_* component form)   ClickHouse DSN, native protocol
  POSTGRES_URI (or _FILE, or the DEV_HEALTH_PG_DOMAIN_* component form)   organizations lookup
`

// productTelemetryInsert is the statement (and column list) persist_product_telemetry_events writes.
const productTelemetryInsert = "INSERT INTO product_telemetry_events (org_id_hash,event_id,name,schema_version,session_id,anonymous_user_id,route_pattern,payload_json,occurred_at,ingested_at,source)"

type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

// telemetryConn is the part of the ClickHouse connection the seeding writes through.
type telemetryConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// SeedProductTelemetry generates and writes one batch per organization, as the
// Python verb does, and returns the rows written per organization in order.
// Volume varies slightly per organization (sessions-per-day minus index % 3, at
// least 1) so the top-orgs roll-up has a visible ranking spread.
func SeedProductTelemetry(ctx context.Context, conn telemetryConn, orgIDs []string, days, sessionsPerDay int, seed *int64, endTime time.Time, logger *slog.Logger) ([]int, error) {
	counts := make([]int, 0, len(orgIDs))
	for index, orgID := range orgIDs {
		sessions := max(1, sessionsPerDay-(index%3))
		rows, err := fixturesgen.GenerateProductTelemetry(fixturesgen.ProductTelemetrySpec{
			OrgID: orgID, Days: days, SessionsPerDay: sessions, Seed: seed, EndTime: endTime,
		})
		if err != nil {
			return counts, fmt.Errorf("generate product telemetry for org %s: %w", orgID, err)
		}
		if len(rows) > 0 {
			if err := insertProductTelemetry(ctx, conn, rows); err != nil {
				return counts, fmt.Errorf("persist product telemetry for org %s: %w", orgID, err)
			}
		}
		counts = append(counts, len(rows))
		logger.Info("seeded product telemetry events", "org_id", orgID, "org_id_hash", fixturesgen.ProductTelemetryOrgHash(orgID),
			"rows", len(rows), "days", days, "sessions_per_day", sessions)
	}
	return counts, nil
}

func insertProductTelemetry(ctx context.Context, conn telemetryConn, rows []fixturesgen.ProductTelemetryRow) error {
	batch, err := conn.PrepareBatch(ctx, productTelemetryInsert)
	if err != nil {
		return err
	}
	ingestedAt := time.Now().UTC()
	for _, row := range rows {
		if err := batch.Append(row.OrgIDHash, row.EventID, row.Name, row.SchemaVersion, row.SessionID, row.AnonymousUserID,
			row.RoutePattern, row.PayloadJSON, row.OccurredAt, ingestedAt, row.Source); err != nil {
			return err
		}
	}
	return batch.Send()
}

func runProductTelemetry(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho fixtures product-telemetry", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, productTelemetryUsage) }
	var explicit stringList
	flags.Var(&explicit, "org", "explicit organization id; repeatable")
	orgs := flags.Int("orgs", 5, "organizations to seed when no --org is given")
	days := flags.Int("days", 30, "days of data per organization")
	sessions := flags.Int("sessions-per-day", 50, "sessions per day per organization")
	seedText := flags.String("seed", "", "deterministic seed")
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted")
		return cli.ExitUsage
	}
	var seed *int64
	if strings.TrimSpace(*seedText) != "" {
		value, err := strconv.ParseInt(strings.TrimSpace(*seedText), 10, 64)
		if err != nil {
			fmt.Fprintln(env.Stderr, "argument error: --seed must be an integer")
			return cli.ExitUsage
		}
		seed = &value
	}
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}
	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)

	dsn, configured, err := config.ResolveDSN(env.Lookup, "CLICKHOUSE_URI", config.ClickHouseSpec)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "configuration_error", err.Error())
	}
	if !configured {
		return writeError(env.Stderr, cli.ExitFailure, "configuration_error", "CLICKHOUSE_URI is required")
	}

	orgIDs := make([]string, 0, len(explicit))
	for _, id := range explicit {
		orgIDs = append(orgIDs, id)
	}
	if len(orgIDs) == 0 {
		orgIDs = lookupOrganizations(ctx, env, *orgs, logger)
	}
	if len(orgIDs) == 0 {
		return writeError(env.Stderr, cli.ExitFailure, "no_orgs",
			"no orgs to seed: pass --org <uuid> (repeatable) or seed Postgres with `dho fixtures generate` first")
	}

	boundary := secrets.NewBoundary(dsn.Reveal())
	chConfig := chstorage.DefaultConfig(dsn.Reveal())
	chConfig.MaxOpenConns, chConfig.MaxIdleConns = 1, 1
	chConfig.ReadTimeout = 5 * time.Minute
	conn, err := chstorage.Open(ctx, chConfig)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "clickhouse_unavailable", boundary.Redact(err).Error())
	}
	defer conn.Close()

	counts, err := SeedProductTelemetry(ctx, conn, orgIDs, *days, *sessions, seed, time.Now().UTC(), logger)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "seed_failed", boundary.Redact(err).Error())
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	logger.Info("seeded product telemetry", "rows", total, "orgs", len(orgIDs))
	if err := json.NewEncoder(env.Stdout).Encode(map[string]any{"orgs": orgIDs, "rows": counts, "total": total}); err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return cli.ExitFailure
	}
	return cli.ExitOK
}

// lookupOrganizations reads the first n organization ids from Postgres, or, when
// Postgres cannot be read, warns and returns the synthetic ids the Python verb
// falls back to (they rank in the platform-admin roll-up but resolve to no name).
func lookupOrganizations(ctx context.Context, env cli.Env, n int, logger *slog.Logger) []string {
	if n <= 0 {
		return nil
	}
	fallback := func(reason string) []string {
		logger.Warn("could not load orgs from Postgres; falling back to synthetic ids. Set POSTGRES_URI to seed against real orgs", "reason", reason)
		return fixturesgen.SyntheticOrgIDs(n)
	}
	dsn, configured, err := config.ResolveDSN(env.Lookup, "POSTGRES_URI", config.DomainDatabaseSpec)
	if err != nil || !configured {
		return fallback("POSTGRES_URI is not configured")
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	lookupCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(lookupCtx, dsn.Reveal())
	if err != nil {
		return fallback(boundary.Redact(err).Error())
	}
	defer pool.Close()
	rows, err := pool.Query(lookupCtx, "SELECT id::text FROM organizations LIMIT $1", n)
	if err != nil {
		return fallback(boundary.Redact(err).Error())
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return fallback(boundary.Redact(err).Error())
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return fallback(boundary.Redact(err).Error())
	}
	return ids
}
