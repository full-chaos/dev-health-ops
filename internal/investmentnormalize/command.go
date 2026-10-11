package investmentnormalize

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// Verb is `dho backfill normalize-investment`.
func Verb() cli.Command {
	return cli.Command{
		Name:    "normalize-investment",
		Summary: "normalize legacy investment-area history to the canonical themes (an estimate, kept with its provenance)",
		Kind:    cli.Verb,
		Run:     run,
	}
}

const usage = `Usage: dho backfill normalize-investment --org <id> --from <YYYY-MM-DD> --to <YYYY-MM-DD> [--apply]
       dho backfill normalize-investment --org <id> --rollback [--run <run id>] [--apply]

Reads the organization's completed work items that carry only the deprecated rule classifier's keys
(investment_classifications_daily) and writes ONE work_unit_investments row per item with the canonical
theme and sub-theme (status normalized_legacy; low evidence quality; the matched rule named in the
evidence), plus its work_unit_repo_effort rows. It is an ESTIMATE and never an LLM result.
The repository of an item comes through the entity tree: a pull request / merge request item keeps its own
repository; any other item reaches repositories only through its work_graph_issue_pr links (the tier is named
in the evidence), the effort split by the churn of the linked pull requests per repository (the canonical
units' rule; the sum of the repositories is the item's effort). Left out and counted: items a canonical unit
already names in its evidence, items with no reachable repository, and product/general items no estimate rule
matches. The default is a DRY RUN that prints counts only; --apply writes. A window with a legacy key the
mapping table does not hold is refused. A second apply writes nothing for a row that is already stored
identically (same id, input and mapping version); --rollback --run <the first run id> then restores the
before-state. --rollback counts (and with --apply deletes) the normalized rows of the organization, or of one --run.

Environment:
  CLICKHOUSE_URI (or _FILE, or the DEV_HEALTH_CH_* component form)   ClickHouse DSN, native protocol
`

func run(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho backfill normalize-investment", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, usage) }
	org := flags.String("org", "", "the organization id")
	from := flags.String("from", "", "first day (inclusive), YYYY-MM-DD")
	to := flags.String("to", "", "last day (exclusive), YYYY-MM-DD")
	apply := flags.Bool("apply", false, "write (default: dry run, counts only)")
	rollback := flags.Bool("rollback", false, "remove normalized rows instead of writing them")
	runID := flags.String("run", "", "with --rollback: only the rows of this run id")
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 || *org == "" {
		fmt.Fprintln(env.Stderr, "argument error: --org is required and no positional argument is accepted")
		return cli.ExitUsage
	}
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}
	var start, end time.Time
	if !*rollback {
		var err error
		if start, err = time.Parse("2006-01-02", *from); err != nil {
			fmt.Fprintln(env.Stderr, "argument error: --from must be YYYY-MM-DD")
			return cli.ExitUsage
		}
		if end, err = time.Parse("2006-01-02", *to); err != nil {
			fmt.Fprintln(env.Stderr, "argument error: --to must be YYYY-MM-DD")
			return cli.ExitUsage
		}
	}
	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	dsn, configured, err := config.ResolveDSN(env.Lookup, "CLICKHOUSE_URI", config.ClickHouseSpec)
	if err != nil || !configured {
		logger.Error("normalize-investment failed", "error", "ClickHouse URI is required (set CLICKHOUSE_URI).")
		return cli.ExitFailure
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	fail := func(err error) int {
		logger.Error("normalize-investment failed", "error", boundary.Redact(err).Error())
		return cli.ExitFailure
	}
	chConfig := chstorage.DefaultConfig(dsn.Reveal())
	chConfig.MaxOpenConns, chConfig.MaxIdleConns = 1, 1
	chConfig.ReadTimeout = 10 * time.Minute
	conn, err := chstorage.Open(ctx, chConfig)
	if err != nil {
		return fail(err)
	}
	defer conn.Close()
	out := func(line string) { fmt.Fprintln(env.Stdout, line) }

	if *rollback {
		n, err := RollbackCount(ctx, conn, *org, *runID)
		if err != nil {
			return fail(err)
		}
		out(fmt.Sprintf("normalized rows: %d", n))
		if *apply {
			if err := Rollback(ctx, conn, *org, *runID); err != nil {
				return fail(err)
			}
			out("rolled back")
		} else {
			out("dry run: nothing deleted (add --apply)")
		}
		return cli.ExitOK
	}
	options := Options{Org: *org, From: start, To: end, RunID: "normalized_legacy:" + uuid.NewString()}
	records, efforts, report, err := Plan(ctx, conn, options)
	if err != nil {
		return fail(err)
	}
	for _, line := range report.Lines() {
		out(line)
	}
	if !*apply {
		out("dry run: nothing written (add --apply)")
		return cli.ExitOK
	}
	// The run id is printed BEFORE the first write: a failed apply leaves rows under it, and a rollback names it.
	out(fmt.Sprintf("apply started, run %s", options.RunID))
	if err := Apply(ctx, conn, options, records, efforts, report); err != nil {
		logger.Error("normalize-investment apply failed; rows of this run may exist: roll back with --rollback --run", "run", options.RunID)
		return fail(err)
	}
	out(fmt.Sprintf("written: %d rows, run %s", len(records), options.RunID))
	return cli.ExitOK
}
