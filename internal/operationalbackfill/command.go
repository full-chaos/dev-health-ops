// Package operationalbackfill is the `backfill` group of dho: the one-off
// migrations that rewrite stored rows, which `dev-hops backfill` ran. Its verb
// `backfill operational` migrates the legacy Atlassian Ops tables
// (atlassian_ops_incidents, _alerts, _schedules) into the canonical operational
// tables, then proves every migrated incident is a current row.
//
// The canonical row (its id, its conflict key and its two revision numbers) is
// the Python producer's, byte for byte: the venue oracle compares every column
// with the real Python code.
package operationalbackfill

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// Command is the `backfill` group.
func Command() cli.Command {
	return cli.Command{
		Name:    "backfill",
		Summary: "historical backfill operations",
		Kind:    cli.Group,
		Children: []cli.Command{{
			Name:    "operational",
			Summary: "migrate legacy Atlassian Ops rows into the canonical operational tables",
			Kind:    cli.Verb,
			Run:     runOperational,
		}},
	}
}

const operationalUsage = `Usage: dho backfill operational --org <uuid> [--atlassian-provider-instance-id <id>] [--sink clickhouse]

Reads the organization's legacy Atlassian Ops incidents, alerts and schedules,
writes their canonical operational rows, and verifies that every migrated
incident is a current row. Re-running it writes the same identities again.

Environment:
  CLICKHOUSE_URI (or _FILE, or the DEV_HEALTH_CH_* component form)   ClickHouse DSN, native protocol
  OPERATIONAL_ORDERING_CONTRACT   1 (legacy table shape, the default) or 2; when set, every
                                  operational table must already be in that shape
  SERVICE_NAME, SERVICE_VERSION   named in a writer rejection
`

func runOperational(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho backfill operational", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	flags.Usage = func() { fmt.Fprint(env.Stderr, operationalUsage) }
	org := flags.String("org", "", "the organization id")
	instance := flags.String("atlassian-provider-instance-id", "atlassian-ops", "Atlassian Ops instance")
	sink := flags.String("sink", "clickhouse", "analytics sink backend; only clickhouse is supported")
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
	if *org == "" {
		fmt.Fprintln(env.Stderr, "argument error: --org is required")
		return cli.ExitUsage
	}
	if message := validateSink(*sink); message != "" {
		fmt.Fprintln(env.Stderr, "argument error: "+message)
		return cli.ExitUsage
	}
	if env.Lookup == nil {
		env.Lookup = func(string) (string, bool) { return "", false }
	}

	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	dsn, configured, err := config.ResolveDSN(env.Lookup, "CLICKHOUSE_URI", config.ClickHouseSpec)
	if err != nil {
		logger.Error("canonical operational backfill failed", "error", err.Error())
		return cli.ExitFailure
	}
	if !configured {
		logger.Error("canonical operational backfill failed", "error", "ClickHouse URI is required (set CLICKHOUSE_URI).")
		return cli.ExitFailure
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	fail := func(err error) int {
		logger.Error("canonical operational backfill failed", "error", boundary.Redact(err).Error())
		return cli.ExitFailure
	}

	rawContract, explicit := env.Lookup(ContractEnv)
	contract, err := ParseContract(rawContract, explicit)
	if err != nil {
		return fail(err)
	}
	chConfig := chstorage.DefaultConfig(dsn.Reveal())
	chConfig.MaxOpenConns, chConfig.MaxIdleConns = 1, 1
	chConfig.ReadTimeout = 10 * time.Minute
	conn, err := chstorage.Open(ctx, chConfig)
	if err != nil {
		return fail(err)
	}
	defer conn.Close()

	if explicit {
		service, version := "dev-health-ops", "unknown"
		if value, ok := env.Lookup("SERVICE_NAME"); ok {
			service = value
		}
		if value, ok := env.Lookup("SERVICE_VERSION"); ok {
			version = value
		}
		if err := GuardTables(ctx, conn, contract, service, version); err != nil {
			return fail(err)
		}
	}
	result, err := Run(ctx, conn, *org, *instance, contract, time.Now)
	if err != nil {
		return fail(err)
	}
	if _, err := io.WriteString(env.Stdout, result.Summary()+"\n"); err != nil {
		return cli.ExitFailure
	}
	return cli.ExitOK
}

// validateSink is validate_sink: only clickhouse (or auto) is a sink.
func validateSink(sink string) string {
	sink = strings.ToLower(pyStrip(sink))
	if sink == "" {
		sink = "clickhouse"
	}
	switch sink {
	case "mongo", "sqlite", "postgres", "both":
		return fmt.Sprintf("Backend '%s' is no longer supported for analytics. ClickHouse is the only supported "+
			"analytics backend. Set CLICKHOUSE_URI and use --sink clickhouse (or omit --sink).", sink)
	case "clickhouse", "auto":
		return ""
	}
	return fmt.Sprintf("Unknown sink '%s'. Only 'clickhouse' is supported.", sink)
}
