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
	run  func(context.Context, cli.Env) int
}

// upgradeSteps are the migrate Job's steps, in the order the Python Job ran
// them: the PostgreSQL schema, then the standard feature flags it seeds, then
// the ClickHouse schema. Each is the stand-alone verb itself, run through its
// own Run, so the composite cannot drift from the verbs.
func upgradeSteps() []upgradeStep {
	return []upgradeStep{
		{"migrate postgres upgrade", verbRun(migrationPostgresCommand(), "upgrade")},
		{"admin features seed", verbRun(admincli.Command(), "features", "seed")},
		{"migrate clickhouse upgrade", verbRun(chmigrate.Command(), "upgrade")},
	}
}

// migrationPostgresCommand is `dho migrate postgres`, resolving the database
// the way every migrate step does.
func migrationPostgresCommand() cli.Command {
	return pgmigrate.Command(func(lookup platformsecrets.LookupEnv, stderr io.Writer) (platformsecrets.Value, string, bool) {
		return config.ResolveMigrationDatabase(lookup, stderr, true)
	})
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
  2. dho admin features seed
  3. dho migrate clickhouse upgrade

Each step prints its JSON result on stdout and logs its duration at Info on
stderr. The first step that fails stops the run: later steps do not run, and
the exit code is that step's, with an error naming the step. Every setting is
the steps' own environment (see each verb's -h).
`

// runUpgrade is `dho migrate upgrade`.
func runUpgrade(ctx context.Context, env cli.Env) int {
	return runSteps(ctx, env, upgradeSteps())
}

func runSteps(ctx context.Context, env cli.Env, steps []upgradeStep) int {
	flags := flag.NewFlagSet("dho migrate upgrade", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, upgradeUsage) }
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
	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	started := time.Now()
	for index, step := range steps {
		logger.Info("migrate step started", "step", step.name, "position", index+1, "of", len(steps))
		stepStarted := time.Now()
		code := step.run(ctx, cli.Env{Lookup: env.Lookup, Stdout: env.Stdout, Stderr: env.Stderr})
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
