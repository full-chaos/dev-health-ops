// ask-dev-jobs-probe is a one-shot healthcheck probe for the Ask Dev
// acceptance stack (tests/acceptance/compose.ask-dev.yml), CHAOS-4065.
//
// Before this, the acceptance gate's "GENERAL scheduled jobs" healthchecks
// (scripts/acceptance/healthcheck_worker_probe.py,
// healthcheck_beat_sentinel.py) proved a REAL Celery worker+beat fleet was
// alive by round-tripping the Celery-only monitor_queue_depths task through
// the broker and result backend. That premise went stale on 2026-08-19: prod
// runs zero Celery services (CHAOS-4026), and the three cadences the gate
// exercised now run natively in Go:
//   - monitor_queue_depths  -> cmd/dev-health-worker/queue_health.go
//     (queueHealthMonitor, samples River's queue state every 60s)
//   - prune_rate_limit_observations / prune_external_ingest_batches ->
//     internal/jobs/system's RateLimitObservationStore/ExternalIngestBatchStore,
//     dispatched by internal/scheduler/fixed's RetentionProducer under
//     jobcontract.KindRetentionCleanup (schedule ids of the same names).
//
// This binary re-executes those exact production code paths directly against
// the acceptance stack's own Postgres, on every healthcheck tick, and reports
// success only if the real query/delete round-trips without error --
// "an equivalent Go-native receipt" per CHAOS-4065's own wording. It replaces
// the Celery worker/beat fleet in the acceptance overlay entirely: no queue
// name and no schedule cadence is Ask-Dev-specific, so there is nothing here
// that needs a long-running scheduler process, only proof the mechanisms
// work against the stack's real schema.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobs/system"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

const (
	probeTimeout = 10 * time.Second

	defaultRiverSchema = "river"

	rateLimitRetentionDaysEnv          = "SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS"
	defaultRateLimitRetentionDays      = 14
	externalIngestRetentionDaysEnv     = "EXTERNAL_INGEST_STATUS_RETENTION_DAYS"
	defaultExternalIngestRetentionDays = 90

	retentionBatchSize = 500
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

func run(parent context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: ask-dev-jobs-probe <queue-depth|retention>")
		return 2
	}
	switch args[0] {
	case "queue-depth", "retention":
	default:
		fmt.Fprintf(stderr, "unknown check %q: want queue-depth or retention\n", args[0])
		return 2
	}

	uri, ok := lookup("POSTGRES_URI")
	if uri == "" || !ok {
		fmt.Fprintln(stderr, "POSTGRES_URI is not set")
		return 1
	}

	ctx, cancel := context.WithTimeout(parent, probeTimeout)
	defer cancel()

	pool, err := postgresstore.Open(ctx, postgresstore.DefaultConfig(uri))
	if err != nil {
		fmt.Fprintln(stderr, "could not open a PostgreSQL pool:", err)
		return 1
	}
	defer pool.Close()

	switch args[0] {
	case "queue-depth":
		return runQueueDepth(ctx, pool, lookup, stdout, stderr)
	case "retention":
		return runRetention(ctx, pool, lookup, stdout, stderr)
	default:
		// Unreachable: the switch above already refused anything else.
		return 2
	}
}

// queueDepth is the Go-native equivalent of the Celery worker probe: proves
// River's queue state (the same table cmd/dev-health-worker's
// queueHealthMonitor samples every 60s) is live and queryable, round-tripped
// fresh on every healthcheck tick.
type queueDepthRow struct {
	Queue     string `json:"queue"`
	Available int64  `json:"available"`
}

func runQueueDepth(ctx context.Context, pool *pgxpool.Pool, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	schema := defaultRiverSchema
	if value, present := lookup("RIVER_DATABASE_SCHEMA"); present && value != "" {
		schema = value
	}
	table := pgx.Identifier{schema, "river_job"}.Sanitize()

	// Matches cmd/dev-health-worker/queue_health.go's own definition of
	// "available now": internal/storage/river/telemetry.go's queueTelemetrySQL
	// filters state='available' AND scheduled_at <= statement_timestamp() --
	// a state='available' row scheduled in the future (a delayed retry) is
	// not yet fetchable and must not inflate the depth this check reports.
	rows, err := pool.Query(ctx, fmt.Sprintf(
		"SELECT queue, count(*)::bigint AS available FROM %s "+
			"WHERE state = 'available' AND scheduled_at <= statement_timestamp() "+
			"GROUP BY queue ORDER BY queue",
		table,
	))
	if err != nil {
		fmt.Fprintln(stderr, "queue-depth query failed:", err)
		return 1
	}
	defer rows.Close()

	queues := make([]queueDepthRow, 0)
	for rows.Next() {
		var row queueDepthRow
		if err := rows.Scan(&row.Queue, &row.Available); err != nil {
			fmt.Fprintln(stderr, "queue-depth scan failed:", err)
			return 1
		}
		queues = append(queues, row)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintln(stderr, "queue-depth rows failed:", err)
		return 1
	}

	return writeReceipt(stdout, map[string]any{
		"check":      "queue-depth",
		"checked_at": time.Now().UTC().Format(time.RFC3339),
		"queues":     queues,
	})
}

// retention is the Go-native equivalent of the Celery beat probe: proves the
// two remaining beat cadences run for real against the acceptance stack's own
// tables, using the SAME production stores and retention defaults
// internal/scheduler/fixed's RetentionProducer dispatches under
// jobcontract.KindRetentionCleanup ("prune_rate_limit_observations",
// "prune_external_ingest_batches").
func runRetention(ctx context.Context, pool *pgxpool.Pool, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	rateLimitDays, err := envInt(lookup, rateLimitRetentionDaysEnv, defaultRateLimitRetentionDays)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	externalIngestDays, err := envInt(lookup, externalIngestRetentionDaysEnv, defaultExternalIngestRetentionDays)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	now := time.Now().UTC()

	rateLimitStore, err := system.NewRateLimitObservationStore(pool)
	if err != nil {
		fmt.Fprintln(stderr, "rate-limit-observation store unavailable:", err)
		return 1
	}
	rateLimitDeleted, err := rateLimitStore.DeleteBefore(ctx, now.Add(-time.Duration(rateLimitDays)*24*time.Hour), retentionBatchSize)
	if err != nil {
		fmt.Fprintln(stderr, "prune_rate_limit_observations failed:", err)
		return 1
	}

	externalIngestStore, err := system.NewExternalIngestBatchStore(pool)
	if err != nil {
		fmt.Fprintln(stderr, "external-ingest-batch store unavailable:", err)
		return 1
	}
	externalIngestDeleted, err := externalIngestStore.DeleteBefore(ctx, now.Add(-time.Duration(externalIngestDays)*24*time.Hour), retentionBatchSize)
	if err != nil {
		fmt.Fprintln(stderr, "prune_external_ingest_batches failed:", err)
		return 1
	}

	return writeReceipt(stdout, map[string]any{
		"check":      "retention",
		"checked_at": now.Format(time.RFC3339),
		"prune_rate_limit_observations": map[string]any{
			"deleted":        rateLimitDeleted,
			"retention_days": rateLimitDays,
		},
		"prune_external_ingest_batches": map[string]any{
			"deleted":        externalIngestDeleted,
			"retention_days": externalIngestDays,
		},
	})
}

// envInt mirrors internal/scheduler/fixed/producers.go's retentionDays: an
// absent/empty/unparseable value falls back to the default (no intent
// expressed); zero is a legitimate configured horizon, not a fallback
// trigger; a negative value is a hard configuration error -- silently
// falling back on it would mask a horizon that (per that function's own
// comment) "would delete every row older than the occurrence itself".
func envInt(lookup func(string) (string, bool), name string, fallback int) (int, error) {
	value, present := lookup(name)
	if !present {
		return fallback, nil
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(trimmed)
	if err != nil {
		return fallback, nil
	}
	if parsed < 0 {
		return 0, fmt.Errorf("%s=%q is negative; refusing a horizon that would delete every row older than now", name, trimmed)
	}
	return parsed, nil
}

func writeReceipt(stdout io.Writer, receipt map[string]any) int {
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return 1
	}
	fmt.Fprintln(stdout, string(encoded))
	return 0
}
