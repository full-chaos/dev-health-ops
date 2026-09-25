package rivermigrate

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/admincli"
	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// upgradeStep is one step of `dho migrate upgrade`: the verb it runs, named
// by its path under dho.
type upgradeStep struct {
	name string
	args []string
	run  func(context.Context, cli.Env) int
	// when, if set, decides at run time whether the step runs; skip names
	// why it did not.
	when func(lookup platformsecrets.LookupEnv) (run bool, skip string)
}

// upgradeSteps are the migrate Job's steps, in the order the Python Job ran
// them: the PostgreSQL schema, then the standard feature flags it seeds, then
// the ClickHouse schema. Each is the stand-alone verb itself, run through its
// own Run, so the composite cannot drift from the verbs.
//
// With river, `migrate river --apply-and-check` runs right after the
// PostgreSQL step, and only when MIGRATION_DATABASE_URI (or its _FILE or
// component form) is configured -- what `dev-hops migrate postgres` did when
// no River hook owned that step. A skipped step is logged at Warn: an explicit
// --river that does not run must be visible at the default log level.
func upgradeSteps(river bool) []upgradeStep {
	steps := []upgradeStep{{name: "migrate postgres upgrade", run: verbRun(migrationPostgresCommand(), "upgrade")}}
	if river {
		steps = append(steps, upgradeStep{
			name: "migrate river --apply-and-check",
			args: []string{"--apply-and-check"},
			run:  verbRun(Command(), "river"),
			when: migrationDatabaseConfigured,
		})
	}
	return append(steps,
		upgradeStep{name: "admin features seed", run: verbRun(admincli.Command(), "features", "seed")},
		upgradeStep{name: "migrate clickhouse upgrade", run: verbRun(chmigrate.Command(), "upgrade")},
	)
}

// migrationDatabaseConfigured reports whether MIGRATION_DATABASE_URI is
// configured in any of its forms. A malformed setting counts as configured,
// so the River step runs and reports it.
func migrationDatabaseConfigured(lookup platformsecrets.LookupEnv) (bool, string) {
	_, configured, err := config.ResolveDSN(lookup, "MIGRATION_DATABASE_URI", config.MigrationDatabaseSpec)
	if err != nil || configured {
		return true, ""
	}
	return false, "MIGRATION_DATABASE_URI is not configured"
}

// migrationPostgresCommand is `dho migrate postgres`, resolving the database
// the way every migrate step does.
func migrationPostgresCommand() cli.Command {
	return pgmigrate.Command(migrationDatabaseResolver)
}

// migrationDatabaseResolver finds the elevated migration DSN the way every
// migrate step does (MIGRATION_DATABASE_URI, then POSTGRES_URI).
func migrationDatabaseResolver(lookup platformsecrets.LookupEnv, stderr io.Writer) (platformsecrets.Value, string, bool) {
	return config.ResolveMigrationDatabase(lookup, stderr, true)
}

// verbRun finds the verb at path under group. A path that names no verb is a
// programming error, caught by the tests that build the steps.
func verbRun(group cli.Command, path ...string) func(context.Context, cli.Env) int {
	node := group
	for _, name := range path {
		found := false
		for _, child := range node.Children {
			if child.Name == name {
				node, found = child, true
				break
			}
		}
		if !found {
			panic(fmt.Sprintf("dho migrate upgrade: %s has no %q", group.Name, name))
		}
	}
	if node.Kind != cli.Verb || node.Run == nil {
		panic(fmt.Sprintf("dho migrate upgrade: %s %v is not a verb", group.Name, path))
	}
	return node.Run
}

const upgradeUsage = `Usage: dho migrate upgrade

Runs the migrate Job's steps in order, each exactly as its own verb runs:

  1. dho migrate postgres upgrade
     (with --river: dho migrate river --apply-and-check, when
     MIGRATION_DATABASE_URI is configured)
  2. dho admin features seed
  3. dho migrate clickhouse upgrade

Flags:
  --river   also apply the River schema after the PostgreSQL step, as the
            migrate Job does when no River hook owns that step.
            --river=false: a River hook or the operator applies it. When
            MIGRATION_DATABASE_URI is configured one of the two is required,
            so River is never skipped without a word.

Each step prints its JSON result on stdout and logs its duration at Info on
stderr. The first step that fails stops the run: later steps do not run, and
the exit code is that step's, with an error naming the step. Every setting is
the steps' own environment (see each verb's -h).
`

// runUpgrade is `dho migrate upgrade`.
func runUpgrade(ctx context.Context, env cli.Env) int {
	return runSteps(ctx, env, upgradeSteps)
}

func runSteps(ctx context.Context, env cli.Env, build func(river bool) []upgradeStep) int {
	flags := flag.NewFlagSet("dho migrate upgrade", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, upgradeUsage) }
	river := flags.Bool("river", false, "also run `migrate river --apply-and-check` after the PostgreSQL step, when MIGRATION_DATABASE_URI is configured")
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
	riverChosen := false
	flags.Visit(func(set *flag.Flag) { riverChosen = riverChosen || set.Name == "river" })
	if configured, _ := migrationDatabaseConfigured(env.Lookup); configured && !riverChosen {
		// The Python Job ran River whenever MIGRATION_DATABASE_URI was set.
		// A caller that does not say whether this run owns River would skip
		// it without a word, so it must choose: --river, or --river=false
		// when a River hook or the operator applies it.
		_ = json.NewEncoder(env.Stderr).Encode(map[string]any{"error": map[string]string{
			"code":   "river_step_unspecified",
			"detail": "MIGRATION_DATABASE_URI is configured, so this run must say whether it applies River: pass --river, or --river=false when a River hook or the operator applies it",
		}})
		return cli.ExitUsage
	}
	steps := build(*river)
	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	started := time.Now()
	for index, step := range steps {
		if step.when != nil {
			if run, skip := step.when(env.Lookup); !run {
				logger.Warn("migrate step skipped", "step", step.name, "reason", skip)
				continue
			}
		}
		logger.Info("migrate step started", "step", step.name, "position", index+1, "of", len(steps))
		stepStarted := time.Now()
		code := step.run(ctx, cli.Env{Args: step.args, Lookup: env.Lookup, Stdout: env.Stdout, Stderr: env.Stderr})
		duration := time.Since(stepStarted).Milliseconds()
		if code != cli.ExitOK {
			logger.Error("migrate step failed", "step", step.name, "exit_code", code, "duration_ms", duration)
			var skipped []string
			for _, later := range steps[index+1:] {
				skipped = append(skipped, later.name)
			}
			_ = json.NewEncoder(env.Stderr).Encode(map[string]any{"error": map[string]any{
				"code":    "step_failed",
				"detail":  fmt.Sprintf("%s exited %d; the steps after it did not run", step.name, code),
				"step":    step.name,
				"skipped": skipped,
			}})
			return code
		}
		logger.Info("migrate step done", "step", step.name, "duration_ms", duration)
	}
	logger.Info("migrate upgrade done", "steps", len(steps), "duration_ms", time.Since(started).Milliseconds())
	return cli.ExitOK
}
