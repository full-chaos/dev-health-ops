// Package fixturescli is the `fixtures` group of dho: verbs that seed a
// throwaway database for CI and acceptance runs. Its first verb,
// `fixtures finalize-synthetic-sync`, completes a synthetic sync run the way
// `dev-hops sync finalize-synthetic-sync` did: it writes the durable
// integration/source/dataset/run/unit rows for one synthetic target, records
// the ATTEMPTED half of the executed-proof ledger, and finalizes the run
// through the same Go path a real provider sync uses, which triggers the
// post_sync fanout.
//
// It writes to the GLOBAL executed-proof ledger under a real provider
// identity, so it fails closed unless DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1
// says the database is a throwaway one.
package fixturescli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// AllowEnvVar must be "1" for the verb to write: the executed-proof ledger it
// touches is keyed globally by (provider, dataset_key), not by org.
const AllowEnvVar = "DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN"

// SyntheticProvider is the provider identity the synthetic runs are recorded
// under. It is a real provider's name because the Go capability lookup that
// plans and gates runs is keyed by real providers.
const SyntheticProvider = "gitlab"

// TriggeredBy is the sync run's triggered_by, as the Python verb wrote it.
const TriggeredBy = "metrics-executed-proof-gate"

// Targets are the synthetic targets that complete a real sync run.
var Targets = []string{"cicd", "deployments", "incidents", "tests"}

// Command is the `fixtures` group.
func Command() cli.Command {
	return cli.Command{
		Name:    "fixtures",
		Summary: "seed a throwaway database for CI and acceptance runs",
		Kind:    cli.Group,
		Children: []cli.Command{{
			Name:    "finalize-synthetic-sync",
			Summary: "complete a synthetic sync run for one target and trigger its post-sync fanout",
			Kind:    cli.Verb,
			Run:     runFinalizeSynthetic,
		}},
	}
}

// Params are the inputs of one synthetic finalize.
type Params struct {
	OrgID    string
	RepoName string
	Target   string
	Since    time.Time
	Before   time.Time
}

// syncFlags is _sync_flags_for_target: the processor flags of the unit, as the
// JSON text Python's json.dumps writes them, in the dict's key order.
func syncFlags(target string) string {
	flag := func(name string) string { return fmt.Sprint(target == name) }
	return `{"sync_git": ` + flag("git") +
		`, "sync_prs": ` + flag("prs") +
		`, "sync_cicd": ` + flag("cicd") +
		`, "sync_deployments": ` + flag("deployments") +
		`, "sync_incidents": ` + flag("incidents") +
		`, "sync_security": ` + flag("security") +
		`, "sync_tests": ` + flag("tests") +
		`, "blame_only": ` + flag("blame") + `}`
}

func validTarget(target string) bool {
	for _, known := range Targets {
		if target == known {
			return true
		}
	}
	return false
}

// Seed writes the durable rows of one synthetic run in one transaction and
// returns the run id: the find-or-create integration, source and dataset (the
// integration is matched by its marker name too, so a real integration of the
// org is never found and attached to), the running run, its successful unit,
// and the ATTEMPTED executed-proof record. It never records PROVEN: the ledger
// is global, and fake data must not satisfy the gate for a real route.
func Seed(ctx context.Context, pool *pgxpool.Pool, params Params) (string, error) {
	if !validTarget(params.Target) {
		return "", fmt.Errorf("target %q: only valid for %s", params.Target, strings.Join(Targets, ", "))
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	now := time.Now().UTC()
	integrationName := SyntheticProvider + "-synthetic-seed"

	integrationID, found, err := lookupID(ctx, tx,
		`SELECT id::text FROM integrations WHERE org_id = $1 AND provider = $2 AND name = $3`,
		params.OrgID, SyntheticProvider, integrationName)
	if err != nil {
		return "", fmt.Errorf("read integration: %w", err)
	}
	if !found {
		integrationID = uuid.NewString()
		if _, err := tx.Exec(ctx, `INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, '{}'::json, true, $5, $5)`,
			integrationID, params.OrgID, SyntheticProvider, integrationName, now); err != nil {
			return "", fmt.Errorf("insert integration: %w", err)
		}
	}

	sourceID, found, err := lookupID(ctx, tx,
		`SELECT id::text FROM integration_sources WHERE org_id = $1 AND integration_id = $2::uuid AND provider = $3 AND external_id = $4`,
		params.OrgID, integrationID, SyntheticProvider, params.RepoName)
	if err != nil {
		return "", fmt.Errorf("read source: %w", err)
	}
	if !found {
		sourceID = uuid.NewString()
		name := params.RepoName[strings.LastIndex(params.RepoName, "/")+1:]
		if _, err := tx.Exec(ctx, `INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3::uuid, $4, 'repo', $5, $6, $5, '{}'::json, true, $7, $7)`,
			sourceID, params.OrgID, integrationID, SyntheticProvider, params.RepoName, name, now); err != nil {
			return "", fmt.Errorf("insert source: %w", err)
		}
	}

	_, found, err = lookupID(ctx, tx,
		`SELECT id::text FROM integration_datasets WHERE org_id = $1 AND integration_id = $2::uuid AND dataset_key = $3`,
		params.OrgID, integrationID, params.Target)
	if err != nil {
		return "", fmt.Errorf("read dataset: %w", err)
	}
	if !found {
		if _, err := tx.Exec(ctx, `INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES ($1, $2, $3::uuid, $4, true, '{}'::json)`,
			uuid.NewString(), params.OrgID, integrationID, params.Target); err != nil {
			return "", fmt.Errorf("insert dataset: %w", err)
		}
	}

	runID := uuid.NewString()
	if _, err := tx.Exec(ctx, `INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, started_at, created_at)
VALUES ($1, $2, $3::uuid, $4, 'incremental', 'running', 1, 0, 0, $5, $6)`,
		runID, params.OrgID, integrationID, TriggeredBy, params.Since.UTC(), now); err != nil {
		return "", fmt.Errorf("insert sync run: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, since_at, before_at, status, attempts, processor_flags, created_at, updated_at)
VALUES ($1, $2, $3::uuid, $4::uuid, $5::uuid, $6, $7, 'medium', 'incremental', $8, $9, 'success', 1, $10::json, $11, $11)`,
		uuid.NewString(), params.OrgID, runID, integrationID, sourceID, SyntheticProvider, params.Target,
		params.Since.UTC(), params.Before.UTC(), syncFlags(params.Target), now); err != nil {
		return "", fmt.Errorf("insert sync run unit: %w", err)
	}
	if err := providersync.RecordExecutedProofAttempted(ctx, tx, []string{SyntheticProvider}, []string{params.Target}, params.Before); err != nil {
		return "", fmt.Errorf("record the executed-proof attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit: %w", err)
	}
	return runID, nil
}

// lookupID runs a query that returns at most one id.
func lookupID(ctx context.Context, tx pgx.Tx, query string, args ...any) (string, bool, error) {
	var id string
	err := tx.QueryRow(ctx, query, args...).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

// Finalize seeds the run and finalizes it through the native Go finalize the
// River worker uses, which writes the terminal run state and the once-only
// post_sync outbox wakeup the reconciler relays.
func Finalize(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger, params Params) (string, error) {
	runID, err := Seed(ctx, pool, params)
	if err != nil {
		return "", err
	}
	service, err := syncdispatchruntime.NewNativeFinalizeSyncRunService(pool, logger)
	if err != nil {
		return runID, fmt.Errorf("finalize service: %w", err)
	}
	if err := service.FinalizeRun(ctx, params.OrgID, runID); err != nil {
		return runID, fmt.Errorf("finalize run %s: %w", runID, err)
	}
	return runID, nil
}

const finalizeUsage = `Usage: dho fixtures finalize-synthetic-sync --target <cicd|deployments|incidents|tests> --repo-name <owner/repo> [--org <uuid>] [--backfill <days>]

Completes a synthetic sync run for one target seeded earlier, and triggers the
real post_sync fanout. It writes no analytics rows. Not idempotent: every call
mints a new sync run. Call it once per seeded target, after every dora-relevant
target has been seeded.

Flags:
  --target      the synthetic target (required)
  --repo-name   the synthetic repo name the target was seeded with (required)
  --org         the organization id (default: the ORG_ID environment variable)
  --backfill    window in days ending now (default 1)

Environment:
  DEV_HEALTH_ALLOW_SYNTHETIC_SYNC_RUN=1   required: the verb writes the global
                                          executed-proof ledger, so it refuses a
                                          database that is not a throwaway one
  MIGRATION_DATABASE_URI (or _FILE, or the DEV_HEALTH_MIGRATION_PG_* component
  form), else POSTGRES_URI (or _FILE)     the database
`

func runFinalizeSynthetic(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho fixtures finalize-synthetic-sync", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, finalizeUsage) }
	target := flags.String("target", "", "the synthetic target")
	repoName := flags.String("repo-name", "", "the synthetic repo name")
	org := flags.String("org", "", "the organization id")
	backfill := flags.Int("backfill", 1, "window in days ending now")
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
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}
	if !validTarget(*target) {
		fmt.Fprintf(env.Stderr, "argument error: --target must be one of %s\n", strings.Join(Targets, ", "))
		return cli.ExitUsage
	}
	if strings.TrimSpace(*repoName) == "" {
		fmt.Fprintln(env.Stderr, "argument error: --repo-name is required")
		return cli.ExitUsage
	}
	if *backfill < 1 {
		fmt.Fprintln(env.Stderr, "argument error: --backfill must be at least 1")
		return cli.ExitUsage
	}
	if value, _ := env.Lookup(AllowEnvVar); value != "1" {
		return writeError(env.Stderr, cli.ExitRefused, "not_a_throwaway_database",
			"finalize-synthetic-sync writes to the GLOBAL executed-proof ledger under a real provider identity and must never run against a shared or production-adjacent database. Set "+AllowEnvVar+"=1 explicitly if this really is a throwaway CI/test database")
	}
	orgID := strings.TrimSpace(*org)
	if orgID == "" {
		orgID, _ = env.Lookup("ORG_ID")
		orgID = strings.TrimSpace(orgID)
	}
	if orgID == "" {
		fmt.Fprintln(env.Stderr, "argument error: an organization is required: --org or the ORG_ID environment variable")
		return cli.ExitUsage
	}

	dsn, source, ok := config.ResolveMigrationDatabase(env.Lookup, env.Stderr, true)
	if !ok {
		return cli.ExitFailure
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	pool, err := pgxpool.New(ctx, dsn.Reveal())
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "postgres_unavailable", boundary.Redact(err).Error())
	}
	defer pool.Close()
	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	logger.Info("synthetic finalize database", "source", source, "host", pool.Config().ConnConfig.Host, "database", pool.Config().ConnConfig.Database)

	before := time.Now().UTC()
	params := Params{
		OrgID: orgID, RepoName: *repoName, Target: *target,
		Since: before.AddDate(0, 0, -*backfill), Before: before,
	}
	started := time.Now()
	runID, err := Finalize(ctx, pool, logger, params)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "finalize_failed", boundary.Redact(err).Error())
	}
	logger.Info("synthetic finalize done", "target", *target, "sync_run_id", runID, "duration_ms", time.Since(started).Milliseconds())
	if _, err := io.WriteString(env.Stdout, runID+"\n"); err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return cli.ExitFailure
	}
	return cli.ExitOK
}

func writeError(stderr io.Writer, exit int, code, detail string) int {
	fmt.Fprintf(stderr, "{\"error\":{\"code\":%q,\"detail\":%q}}\n", code, detail)
	return exit
}
