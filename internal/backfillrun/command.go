package backfillrun

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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Verb is the `run` verb of the `backfill` group.
func Verb() cli.Command {
	return cli.Command{
		Name:    "run",
		Summary: "start a historical backfill of one sync configuration",
		Kind:    cli.Verb,
		Run:     runVerb,
	}
}

const usage = `Usage: dho backfill run --config-id <uuid> [--since <YYYY-MM-DD> | --backfill <days>] [--before <YYYY-MM-DD>]
                        [--org <uuid>] [--no-wait] [--wait-seconds <n>]

Starts a backfill of the sync configuration over a window of whole days. The window
ends the day before --before (default: today) and starts --since, or --backfill days
earlier (default 1). The configuration is validated as before, then one scheduled
occurrence and its backfill trigger are written; the scheduler plans and dispatches
the run. The verb waits for the plan (30 s by default), or prints the occurrence id
at once with --no-wait.

Only a planner-managed configuration, or one pinned to a single source, can be
started this way: the scheduler materializes no other.

Environment:
  MIGRATION_DATABASE_URI (or the DEV_HEALTH_PG_* component form)   PostgreSQL DSN
`

// clock is the wall clock the occurrence is stamped with; tests fix it.
var clock = time.Now

// DefaultWait is how long the verb waits for the scheduler to plan the run.
const DefaultWait = 30 * time.Second

func parseDay(text string) (time.Time, error) {
	return time.Parse("2006-01-02", text)
}

func runVerb(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho backfill run", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, usage) }
	configID := flags.String("config-id", "", "the sync configuration id")
	since := flags.String("since", "", "start day, inclusive (YYYY-MM-DD); not with --backfill")
	before := flags.String("before", "", "end day, exclusive (YYYY-MM-DD); default tomorrow")
	backfill := flags.Int("backfill", 1, "process N days ending before --before; not with --since")
	org := flags.String("org", "", "assert the configuration's organization")
	noWait := flags.Bool("no-wait", false, "print the occurrence id without waiting for the plan")
	waitSeconds := flags.Int("wait-seconds", int(DefaultWait/time.Second), "how long to wait for the plan")
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
	given := map[string]bool{}
	flags.Visit(func(set *flag.Flag) { given[set.Name] = true })
	if *configID == "" {
		fmt.Fprintln(env.Stderr, "argument error: --config-id is required")
		return cli.ExitUsage
	}
	if given["since"] && given["backfill"] {
		fmt.Fprintln(env.Stderr, "argument error: --since and --backfill are mutually exclusive")
		return cli.ExitUsage
	}
	if *waitSeconds < 0 {
		fmt.Fprintln(env.Stderr, "argument error: --wait-seconds must not be negative")
		return cli.ExitUsage
	}
	var sinceDay, beforeDay *time.Time
	for _, item := range []struct {
		name  string
		text  string
		given bool
		into  **time.Time
	}{{"--since", *since, given["since"], &sinceDay}, {"--before", *before, given["before"], &beforeDay}} {
		if !item.given {
			continue
		}
		day, err := parseDay(item.text)
		if err != nil {
			fmt.Fprintf(env.Stderr, "argument error: %s must be a YYYY-MM-DD date, got %q\n", item.name, item.text)
			return cli.ExitUsage
		}
		*item.into = &day
	}
	window, err := ResolveWindow(sinceDay, beforeDay, *backfill, clock())
	if err != nil {
		fmt.Fprintln(env.Stderr, "argument error: "+err.Error())
		return cli.ExitUsage
	}
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}
	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	fail := func(boundary secrets.Boundary, err error) int {
		logger.Error("backfill failed", "error", boundary.Redact(err).Error())
		var refused RefusedError
		if errors.As(err, &refused) {
			return cli.ExitRefused
		}
		return cli.ExitFailure
	}

	dsn, _, ok := config.ResolveMigrationDatabase(env.Lookup, env.Stderr, true)
	if !ok {
		return cli.ExitFailure
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	pool, err := pgxpool.New(ctx, dsn.Reveal())
	if err != nil {
		return fail(boundary, err)
	}
	defer pool.Close()

	normalized, err := normalizeUUID(*configID)
	if err != nil {
		return fail(boundary, err)
	}
	params := Params{ConfigID: *configID, RequestedOrg: *org, Window: window}
	trigger, err := Start(ctx, pool, normalized, params, clock)
	if err != nil {
		return fail(boundary, err)
	}
	logger.Info("backfill queued",
		"since", window.Since.Format(time.RFC3339Nano), "before", window.Before.Format(time.RFC3339Nano),
		"provider", trigger.Provider, "sync_targets", trigger.SyncTargets, "dataset_keys", trigger.DatasetKeys,
		"enabled_sources", trigger.EnabledSources, "occurrence_id", trigger.OccurrenceID)
	if *noWait {
		fmt.Fprintf(env.Stdout, "Backfill queued: occurrence_id=%s\n", trigger.OccurrenceID)
		return cli.ExitOK
	}
	outcome, err := Wait(ctx, pool, trigger.OccurrenceID, time.Duration(*waitSeconds)*time.Second, 250*time.Millisecond)
	if err != nil {
		return fail(boundary, err)
	}
	return report(env.Stdout, logger, trigger, outcome, time.Duration(*waitSeconds)*time.Second)
}

func report(stdout io.Writer, logger *slog.Logger, trigger Trigger, outcome Outcome, waited time.Duration) int {
	switch outcome.State {
	case StateMaterialized:
		fmt.Fprintf(stdout, "Backfill materialized: %d unit(s) planned for sync_run_id=%s\n", outcome.TotalUnits, outcome.SyncRunID)
		return cli.ExitOK
	case StateQuarantined:
		logger.Error("backfill failed", "error", "scheduled sync occurrence quarantined: "+outcome.ErrorCode, "occurrence_id", trigger.OccurrenceID)
		return cli.ExitFailure
	case StateTerminal:
		logger.Error("backfill failed", "error", outcome.Reason, "sync_run_id", outcome.SyncRunID)
		return cli.ExitFailure
	}
	fmt.Fprintf(stdout, "Backfill pending: occurrence_id=%s (not planned within %s; the scheduler keeps going)\n", trigger.OccurrenceID, waited)
	return cli.ExitOK
}

// Start validates the configuration and writes the occurrence and its trigger,
// in one transaction.
func Start(ctx context.Context, pool *pgxpool.Pool, configID string, params Params, now func() time.Time) (Trigger, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return Trigger{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	cfg, err := LoadConfig(ctx, tx, configID)
	if err != nil {
		return Trigger{}, err
	}
	if cfg == nil {
		return Trigger{}, fmt.Errorf("Sync configuration not found: %s", params.ConfigID)
	}
	validated, err := Validate(ctx, tx, cfg, params)
	if err != nil {
		return Trigger{}, err
	}
	trigger, err := Mint(ctx, tx, cfg, params, validated, now())
	if err != nil {
		return Trigger{}, err
	}
	trigger.EnabledSources = validated.EnabledSources
	if err := tx.Commit(ctx); err != nil {
		return Trigger{}, fmt.Errorf("commit: %w", err)
	}
	return trigger, nil
}

// State is what the scheduler did with the occurrence.
type State int

const (
	StatePending State = iota
	StateMaterialized
	StateQuarantined
	StateTerminal
)

// Outcome is the result of Wait.
type Outcome struct {
	State      State
	SyncRunID  string
	TotalUnits int
	ErrorCode  string
	Reason     string
}

// Wait polls the occurrence until the scheduler has planned it, quarantined it,
// or the wait is over. A pending outcome is not an error.
func Wait(ctx context.Context, pool *pgxpool.Pool, occurrenceID string, wait, poll time.Duration) (Outcome, error) {
	deadline := time.Now().Add(wait)
	for {
		var status string
		var syncRunID, errorCode *string
		err := pool.QueryRow(ctx, `
SELECT reconcile_status, sync_run_id::text, reconcile_error_code
FROM public.scheduled_sync_occurrences WHERE occurrence_id = $1`, occurrenceID).Scan(&status, &syncRunID, &errorCode)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return Outcome{}, fmt.Errorf("read the occurrence: %w", err)
		}
		if err == nil {
			switch status {
			case "completed":
				if syncRunID != nil {
					return readRun(ctx, pool, *syncRunID)
				}
			case "quarantined":
				code := "unknown"
				if errorCode != nil && *errorCode != "" {
					code = *errorCode
				}
				return Outcome{State: StateQuarantined, ErrorCode: code}, nil
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return Outcome{State: StatePending}, nil
		}
		select {
		case <-ctx.Done():
			return Outcome{}, ctx.Err()
		case <-time.After(min(poll, remaining)):
		}
	}
}

// readRun is _materialized_trigger_result: the planned run's unit count, or its
// terminal reason when the run ended as a disabled PagerDuty sync.
func readRun(ctx context.Context, pool *pgxpool.Pool, syncRunID string) (Outcome, error) {
	var units int
	var status string
	var runError *string
	var category *string
	err := pool.QueryRow(ctx, `
SELECT COALESCE(total_units, 0), status::text, error, result->>'error_category'
FROM public.sync_runs WHERE id = $1::uuid`, syncRunID).Scan(&units, &status, &runError, &category)
	if errors.Is(err, pgx.ErrNoRows) {
		return Outcome{State: StatePending}, nil
	}
	if err != nil {
		return Outcome{}, fmt.Errorf("read the sync run: %w", err)
	}
	if strings.EqualFold(status, "failed") && category != nil && *category == "pagerduty_sync_disabled" {
		reason := ""
		if runError != nil {
			reason = *runError
		}
		return Outcome{State: StateTerminal, SyncRunID: syncRunID, Reason: reason}, nil
	}
	return Outcome{State: StateMaterialized, SyncRunID: syncRunID, TotalUnits: units}, nil
}
