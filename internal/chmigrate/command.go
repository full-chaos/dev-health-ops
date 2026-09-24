package chmigrate

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// ClickHouseURIKey is the DSN this verb reads, through config.ResolveDSN, so
// the DEV_HEALTH_CH_* component form works too.
const ClickHouseURIKey = "CLICKHOUSE_URI"

// migrationReadTimeout bounds one statement. A baseline table rebuild or a
// chain file's DDL can run far longer than a query.
const migrationReadTimeout = 10 * time.Minute

// Command is the `clickhouse` group of `dho migrate`.
func Command() cli.Command {
	return cli.Command{
		Name:    "clickhouse",
		Summary: "bring the ClickHouse analytics schema to its head, or report where it stands",
		Kind:    cli.Group,
		Children: []cli.Command{
			{
				Name:    "upgrade",
				Summary: "apply the head baseline to an empty database, then every migration after the head",
				Kind:    cli.Verb,
				Run: func(ctx context.Context, env cli.Env) int {
					return run(ctx, "upgrade", env)
				},
			},
			{
				Name:    "status",
				Summary: "report applied, missing and pending versions without changing the database",
				Kind:    cli.Verb,
				Run: func(ctx context.Context, env cli.Env) int {
					return run(ctx, "status", env)
				},
			},
		},
	}
}

func run(ctx context.Context, verb string, env cli.Env) int {
	flags := flag.NewFlagSet("dho migrate clickhouse "+verb, flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	defaultUsage := flags.Usage
	flags.Usage = func() {
		defaultUsage()
		fmt.Fprint(env.Stderr, "\nEnvironment:\n"+
			"  CLICKHOUSE_URI (or _FILE)       ClickHouse DSN (native protocol); the database it names is migrated\n"+
			"  DEV_HEALTH_CH_HOST, _PORT, _USER, _PASSWORD, _DB   component form, exclusive with CLICKHOUSE_URI\n"+
			"  OPERATIONAL_ORDERING_CONTRACT   must be 2: the head is production's contract-2 schema\n")
	}
	var check *bool
	if verb == "status" {
		check = flags.Bool("check", false, "exit 1 unless the database is at the head, with nothing pending (read-only; the JSON status is still printed)")
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

	raw, present := env.Lookup(OrderingContractEnv)
	contract, err := ParseContract(raw, present)
	if err != nil {
		return writeError(env.Stderr, "configuration_error", err.Error())
	}
	baseline, err := LoadBaseline()
	if err != nil {
		return writeError(env.Stderr, "baseline_unavailable", err.Error())
	}
	if err := CheckContract(contract, baseline); err != nil {
		return writeError(env.Stderr, "settings_mismatch", err.Error())
	}
	chain, err := LoadChain()
	if err != nil {
		return writeError(env.Stderr, "chain_unavailable", err.Error())
	}
	dsn, configured, err := platformconfig.ResolveDSN(env.Lookup, ClickHouseURIKey, platformconfig.ClickHouseSpec)
	if err != nil {
		return writeError(env.Stderr, "configuration_error", err.Error())
	}
	if !configured {
		return writeError(env.Stderr, "configuration_error", ClickHouseURIKey+" is required")
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	config := chstorage.DefaultConfig(dsn.Reveal())
	config.MaxOpenConns, config.MaxIdleConns = 1, 1
	config.ReadTimeout = migrationReadTimeout
	conn, err := chstorage.Open(ctx, config)
	if err != nil {
		return writeError(env.Stderr, "clickhouse_unavailable", boundary.Redact(err).Error())
	}
	defer conn.Close()
	db, _, err := NewConnDB(ctx, conn)
	if err != nil {
		return writeError(env.Stderr, "clickhouse_unavailable", boundary.Redact(err).Error())
	}

	if verb == "status" {
		status, err := ReadStatus(ctx, db, baseline, chain)
		if err != nil {
			return writeError(env.Stderr, "status_failed", boundary.Redact(err).Error())
		}
		code := writeResult(env.Stdout, env.Stderr, status)
		if code == cli.ExitOK && *check && (status.State != "at_head" || len(status.Pending) > 0) {
			// The wait-for-migrations probe: anything but a database at the
			// head with nothing pending is "not yet".
			return cli.ExitFailure
		}
		return code
	}
	result, err := Upgrade(ctx, db, baseline, chain)
	if err != nil {
		var below BelowHeadError
		var foreign ForeignDatabaseError
		var mismatch SchemaMismatchError
		switch {
		case errors.As(err, &mismatch):
			return writeError(env.Stderr, "schema_mismatch", err.Error())
		case errors.As(err, &below):
			return writeError(env.Stderr, "below_head", err.Error())
		case errors.As(err, &foreign):
			return writeError(env.Stderr, "foreign_database", err.Error())
		}
		writeResultQuietly(env.Stderr, result)
		return writeError(env.Stderr, "migration_failed", boundary.Redact(err).Error())
	}
	return writeResult(env.Stdout, env.Stderr, result)
}

func writeResult(stdout, stderr io.Writer, value any) int {
	if err := json.NewEncoder(stdout).Encode(value); err != nil {
		fmt.Fprintln(stderr, "could not write the result")
		return cli.ExitFailure
	}
	return cli.ExitOK
}

// writeResultQuietly records how far a failed upgrade got, on stderr beside
// the error, so an operator sees which objects already exist.
func writeResultQuietly(stderr io.Writer, result Result) {
	_ = json.NewEncoder(stderr).Encode(map[string]any{"partial": result})
}

func writeError(stderr io.Writer, code, detail string) int {
	_ = json.NewEncoder(stderr).Encode(map[string]any{"error": map[string]string{"code": code, "detail": detail}})
	return cli.ExitFailure
}
