// Command capacity-native-producer runs the NATIVE Go capacity executor
// against one ClickHouse destination, as the right-hand side of the CUT-20 R2
// parity comparison.
//
// A separate binary rather than an in-process call, for the same reason the
// DORA one is: computeparity identifies a producer by its resolved program and
// entry point, so a Go side invoked from inside the test would either share the
// test binary's identity or have to be told what it was -- and a port proof
// that trusts its own caller's label is not a proof.
//
// Deliberately the thinnest possible wrapper. Anything computed here rather
// than in the executor is computation production does not do, and the
// comparison would be measuring this file instead of the port.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "capacity-native-producer: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 || os.Args[1] != "produce" {
		return fmt.Errorf("usage: capacity-native-producer produce --dsn ... --org-id ... --seed ...")
	}
	flags := flag.NewFlagSet("produce", flag.ContinueOnError)
	dsn := flags.String("dsn", "", "ClickHouse DSN of the destination store")
	orgID := flags.String("org-id", "", "organization the partition belongs to")
	seed := flags.Int64("seed", 0, "generation seed; mandatory for this family")
	teamID := flags.String("team-id", "", "team scope")
	workScopeID := flags.String("work-scope-id", "", "work scope")
	historyDays := flags.Int("history-days", 90, "throughput history window")
	simulations := flags.Int("simulations", 10000, "Monte Carlo simulation count")
	// Derived from this process's own today, exactly as the Python producer
	// derives it from its own -- production supplies a target date in the
	// scope, so pinning one here would test a configuration production never
	// runs. The harness refuses a run that crosses UTC midnight, which is what
	// makes the two sides agree on what today is.
	targetDateOffset := flags.Int("target-date-offset-days", 0,
		"target date as an offset from today; 0 leaves fixed-date mode unused")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if *dsn == "" || *orgID == "" {
		return fmt.Errorf("--dsn and --org-id are required")
	}
	// Refused rather than defaulted: seed 0 is a VALID seed, so silently using
	// it would produce a real, reproducible, and wrong comparison rather than
	// an error.
	if !seedWasProvided(flags) {
		return fmt.Errorf("--seed is required; capacity is not reproducible without it")
	}

	ctx := context.Background()
	options, err := httpOptions(*dsn)
	if err != nil {
		return err
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		return fmt.Errorf("open clickhouse: %w", err)
	}
	defer func() { _ = conn.Close() }()

	executor, err := remaining.NewCapacityExecutor(ctx, conn, nil)
	if err != nil {
		return err
	}

	scopeFields := map[string]any{
		"version":       1,
		"team_id":       *teamID,
		"work_scope_id": *workScopeID,
		"history_days":  *historyDays,
		"simulations":   *simulations,
		"all_teams":     false,
	}
	if *targetDateOffset != 0 {
		target := time.Now().UTC().AddDate(0, 0, *targetDateOffset)
		scopeFields["target_date"] = target.Format("2006-01-02")
	}
	scope, err := json.Marshal(scopeFields)
	if err != nil {
		return err
	}
	_, err = executor.ComputePartition(
		ctx,
		remaining.Run{
			ID: "parity-run", OrganizationID: *orgID,
			Family: "capacity", Seed: seed,
		},
		remaining.Partition{ID: "parity-partition", RunID: "parity-run", Scope: scope},
	)
	return err
}

func seedWasProvided(flags *flag.FlagSet) bool {
	provided := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "seed" {
			provided = true
		}
	})
	return provided
}

// httpOptions dials the container's HTTP port. Pointing the native protocol at
// an HTTP port fails as "[handshake] unexpected packet [72] from server".
func httpOptions(dsn string) (*clickhouse.Options, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	password, _ := parsed.User.Password()
	return &clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{parsed.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Username: parsed.User.Username(),
			Password: password,
		},
	}, nil
}
