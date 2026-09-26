package pgmigrate

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// `dho migrate postgres preflight` is the read-only verdict of what the
// migrate hook (`dho migrate postgres upgrade`) will do to this database with
// this build. It is the same binary, the same embedded baseline and chain and
// the same Decide as the hook, so the roll pre-check and the hook cannot
// disagree (Trap #415: a check that derives its answer some other way proves
// nothing about the hook).
//
// It changes nothing: one READ ONLY transaction (ACCESS SHARE locks only), no
// write, and no advisory lock taken (the hook's lock is only looked at).
//
// Verdicts and exit codes:
//
//	at_head          0   the hook has nothing to apply
//	applies_cleanly  10  the hook will apply the pending revisions (1 with --strict)
//	needs_manual     1   the hook refuses; Reason names why
//	(no verdict)     3   the measurement did not happen: fails loudly, never a verdict
const (
	// VerdictAtHead: the database is at this build's heads.
	VerdictAtHead = "at_head"
	// VerdictAppliesCleanly: the hook will bring the database to this build's heads.
	VerdictAppliesCleanly = "applies_cleanly"
	// VerdictNeedsManual: the hook refuses the database.
	VerdictNeedsManual = "needs_manual"
)

// The reasons a verdict carries. applies_cleanly and at_head have one too, so the
// field is always set.
const (
	ReasonUpToDate        = "up_to_date"
	ReasonEmptyDatabase   = "empty_database"
	ReasonPendingRevs     = "pending_revisions"
	ReasonBelowBaseline   = "below_baseline"
	ReasonAheadOfBuild    = "ahead_of_build"
	ReasonCutoverMissing  = "cutover_missing"
	ReasonForeignDatabase = "foreign_database"
	ReasonSchemaMismatch  = "schema_mismatch"
	// ReasonSettingsMismatch: the environment the hook would run in differs from
	// the baseline's settings, and the hook refuses before it touches the database.
	ReasonSettingsMismatch = "settings_mismatch"
)

const (
	// ExitAppliesCleanly is the exit code of an applies_cleanly verdict.
	ExitAppliesCleanly = 10
	// ExitMeasurementFailed means the measurement did not happen (cannot connect,
	// cannot read alembic_version, cannot load the baseline or chain): never a verdict.
	ExitMeasurementFailed = 3
)

// PreflightReport is the JSON a preflight prints on stdout. It holds revision ids
// and nothing else that the database or the environment could make secret.
type PreflightReport struct {
	Verdict string `json:"verdict"`
	Reason  string `json:"reason"`
	// Recorded is what alembic_version holds now.
	Recorded []string `json:"recorded"`
	// BuildHeads are the heads this build brings a database to.
	BuildHeads []string `json:"build_heads"`
	// Pending are the chain revisions the hook will apply.
	Pending []string `json:"pending"`
	// Missing are the baseline heads the database lacks (below_baseline, cutover_missing).
	Missing []string `json:"missing"`
	// MigratorActive reports that a migrator holds, or waits for, the dho advisory
	// lock. It does not change the verdict.
	MigratorActive bool `json:"migrator_active"`
}

// KnownRevisions is every revision this build knows: the Alembic walk, the chain
// after the baseline, and the baseline's heads.
func KnownRevisions(history []HistoryEntry, baseline Baseline, chain []ChainFile) map[string]bool {
	known := map[string]bool{}
	for _, entry := range WithChain(history, baseline, chain) {
		known[entry.Revision] = true
	}
	for _, head := range baseline.Heads {
		known[head] = true
	}
	for _, file := range chain {
		known[file.Revision] = true
	}
	return known
}

// Classify is the verdict for one observation. It is Decide, the function the
// hook acts on, with the settings check the hook runs first, and nothing else:
// a verdict the hook would not reach is a bug here.
func Classify(observation Observation, settings Settings, baseline Baseline, chain []ChainFile, known map[string]bool) PreflightReport {
	report := PreflightReport{
		Recorded:   nonNil(observation.Versions),
		BuildHeads: Heads(baseline, chain),
		Pending:    []string{},
		Missing:    []string{},
	}
	if err := CheckSettings(settings, baseline); err != nil {
		report.Verdict, report.Reason = VerdictNeedsManual, ReasonSettingsMismatch
		return report
	}
	plan := Decide(observation, baseline, chain)
	report.Missing = nonNil(plan.Missing)
	switch plan.State {
	case StateEmpty:
		report.Verdict, report.Reason = VerdictAppliesCleanly, ReasonEmptyDatabase
		report.Pending = pendingRevisions(plan.Pending)
	case StateAtHead:
		report.Pending = pendingRevisions(plan.Pending)
		if len(plan.Pending) == 0 {
			report.Verdict, report.Reason = VerdictAtHead, ReasonUpToDate
		} else {
			report.Verdict, report.Reason = VerdictAppliesCleanly, ReasonPendingRevs
		}
	case StateBelowHead:
		report.Verdict = VerdictNeedsManual
		switch {
		case hasUnknown(observation.Versions, known):
			// A revision this build has never heard of: the image was rolled back
			// under a database a newer build migrated. dho never downgrades.
			report.Reason = ReasonAheadOfBuild
		case len(plan.Missing) == 1 && plan.Missing[0] == cutoverRevision:
			report.Reason = ReasonCutoverMissing
		default:
			report.Reason = ReasonBelowBaseline
		}
	case StateForeign:
		report.Verdict, report.Reason = VerdictNeedsManual, ReasonForeignDatabase
	case StateSchemaMismatch:
		report.Verdict, report.Reason = VerdictNeedsManual, ReasonSchemaMismatch
	}
	return report
}

func nonNil(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func pendingRevisions(files []ChainFile) []string {
	revisions := make([]string, 0, len(files))
	for _, file := range files {
		revisions = append(revisions, file.Revision)
	}
	return revisions
}

func hasUnknown(recorded []string, known map[string]bool) bool {
	for _, revision := range recorded {
		if !known[revision] {
			return true
		}
	}
	return false
}

// ExitCode is the exit code of a report; strict makes applies_cleanly a failure
// for callers that need the database to already be at head.
func (r PreflightReport) ExitCode(strict bool) int {
	switch r.Verdict {
	case VerdictAtHead:
		return cli.ExitOK
	case VerdictAppliesCleanly:
		if strict {
			return cli.ExitFailure
		}
		return ExitAppliesCleanly
	default:
		return cli.ExitFailure
	}
}

// migratorActiveSQL looks at the advisory lock the hook takes (lockKey): a
// bigint key is stored as classid (high 32 bits), objid (low 32 bits) and
// objsubid 1. Any row, granted or waiting, means a migrator is active. pg_locks
// is readable without a privilege and taking no lock is the point.
const migratorActiveSQL = `SELECT EXISTS (
	SELECT 1 FROM pg_locks
	WHERE locktype = 'advisory'
	  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
	  AND classid = (($1::bigint >> 32) & 4294967295)::oid
	  AND objid = ($1::bigint & 4294967295)::oid
	  AND objsubid = 1)`

// Preflight measures the database behind conn and classifies it. One READ ONLY
// transaction; nothing is written and no advisory lock is taken.
func Preflight(ctx context.Context, conn *pgx.Conn, settings Settings, baseline Baseline, chain []ChainFile, history []HistoryEntry) (PreflightReport, error) {
	var report PreflightReport
	err := readOnlyTransaction(ctx, conn, func(tx pgx.Tx) error {
		observation, err := observe(ctx, tx)
		if err != nil {
			return err
		}
		report = Classify(observation, settings, baseline, chain, KnownRevisions(history, baseline, chain))
		if err := tx.QueryRow(ctx, migratorActiveSQL, lockKey).Scan(&report.MigratorActive); err != nil {
			return fmt.Errorf("look at the migration lock: %w", err)
		}
		return nil
	})
	return report, err
}

func preflight(ctx context.Context, resolve ResolveDSN, env cli.Env) int {
	flags := flag.NewFlagSet("dho migrate postgres preflight", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	strict := flags.Bool("strict", false, "exit 1 unless the database is already at head (applies_cleanly becomes a failure), for callers that need at_head")
	defaultUsage := flags.Usage
	flags.Usage = func() {
		defaultUsage()
		fmt.Fprint(env.Stderr, "\nRead-only: prints what `dho migrate postgres upgrade` will do to this database with this build.\n"+
			"Exit codes: 0 at_head, 10 applies_cleanly (1 with --strict), 1 needs_manual, 3 the measurement did not happen.\n"+
			"\nEnvironment (the same the upgrade verb reads):\n"+
			"  MIGRATION_DATABASE_URI (or _FILE, or the DEV_HEALTH_MIGRATION_PG_* component form)   elevated DSN, direct to PostgreSQL\n"+
			"  POSTGRES_URI (or _FILE)                   used when MIGRATION_DATABASE_URI is not configured\n"+
			"  DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER     must be 1, as the upgrade verb requires\n"+
			"  RIVER_DATABASE_SCHEMA                     must be the head's River schema (river)\n")
	}
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

	baseline, err := LoadBaseline()
	if err != nil {
		return writeMeasurementError(env.Stderr, "baseline_unavailable", err.Error())
	}
	chain, err := LoadChain()
	if err != nil {
		return writeMeasurementError(env.Stderr, "chain_unavailable", err.Error())
	}
	history, err := LoadHistory()
	if err != nil {
		return writeMeasurementError(env.Stderr, "history_unavailable", err.Error())
	}
	var dsn secrets.Value
	var ok bool
	if dsn, _, ok = resolve(env.Lookup, env.Stderr); !ok {
		// The resolver wrote why to stderr. No DSN means no measurement, which is
		// exit 3, not a verdict.
		return ExitMeasurementFailed
	}
	boundary := pgstorage.Boundary(dsn.Reveal())
	conn, err := pgx.Connect(ctx, dsn.Reveal())
	if err != nil {
		return writeMeasurementError(env.Stderr, "postgres_unavailable", boundary.Redact(err).Error())
	}
	defer conn.Close(context.Background())

	report, err := Preflight(ctx, conn, ReadSettings(env.Lookup), baseline, chain, history)
	if err != nil {
		return writeMeasurementError(env.Stderr, "preflight_failed", boundary.Redact(err).Error())
	}
	if err := writeReport(env.Stdout, report); err != nil {
		return writeMeasurementError(env.Stderr, "write_failed", "could not write the report")
	}
	return report.ExitCode(*strict)
}

func writeReport(out io.Writer, report PreflightReport) error {
	return json.NewEncoder(out).Encode(report)
}

// writeMeasurementError prints the error the way the sibling verbs do and exits 3:
// the measurement did not happen.
func writeMeasurementError(stderr io.Writer, code, detail string) int {
	writeError(stderr, code, detail)
	return ExitMeasurementFailed
}
