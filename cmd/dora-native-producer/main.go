// Command dora-native-producer runs the NATIVE Go DORA executor against one
// ClickHouse destination, as the right-hand side of the CHAOS-3092 parity
// comparison.
//
// It exists so the two sides of that comparison are genuinely two
// implementations. computeparity identifies a producer by the resolved binary
// plus its entry point, so a Go producer invoked in-process from the test
// would either share the test binary's identity or need the harness to be told
// what it was -- and a port proof that trusts a caller's label is not a proof.
// A separate binary makes the difference observable rather than asserted.
//
// It is deliberately the THINNEST possible wrapper: it builds the same
// DORAExecutor cmd/dev-health-worker builds and hands it the same scope shape
// the dispatcher would. Any computation performed here rather than in the
// executor would be computation the production worker does not do, and the
// comparison would then be measuring this file.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "dora-native-producer: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// The subcommand mirrors the Python fixture CLI's verb so the two
	// producers are invoked the same way; computeparity records it as the
	// entry point.
	if len(os.Args) < 2 || os.Args[1] != "produce" {
		return fmt.Errorf("usage: dora-native-producer produce --dsn ... --as-of ... --days ...")
	}
	flags := flag.NewFlagSet("produce", flag.ContinueOnError)
	dsn := flags.String("dsn", "", "ClickHouse DSN of the destination store")
	asOf := flags.String("as-of", "", "end day, YYYY-MM-DD or an RFC3339 timestamp")
	days := flags.Int("days", 14, "backfill window, ending at --as-of")
	orgID := flags.String("org-id", "", "organization the partition belongs to")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if *dsn == "" || *asOf == "" || *orgID == "" {
		return fmt.Errorf("--dsn, --as-of and --org-id are all required")
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

	executor, err := remaining.NewDORAExecutor(ctx, conn, nil)
	if err != nil {
		return err
	}

	// The scope is built here as JSON rather than as a struct because
	// doraScope is unexported: the executor's contract with the dispatcher IS
	// the serialized scope, and going through it means this producer exercises
	// the same decoding path a real partition does.
	scope, err := json.Marshal(map[string]any{
		"version":       1,
		"day":           dayOf(*asOf),
		"backfill_days": *days,
		"sink":          "clickhouse",
		"interval":      "daily",
	})
	if err != nil {
		return err
	}
	return executor.ComputePartition(
		ctx,
		remaining.Run{ID: "parity-run", OrganizationID: *orgID, Family: "dora"},
		remaining.Partition{ID: "parity-partition", RunID: "parity-run", Scope: scope},
	)
}

// dayOf accepts the same --as-of spellings the Python fixture CLI does, where
// a full timestamp is truncated to its date. Rejecting the timestamp form
// would make the two producers take different arguments for the same run.
func dayOf(asOf string) string {
	if index := strings.IndexAny(asOf, "T "); index > 0 {
		return asOf[:index]
	}
	return asOf
}

// httpOptions dials the container's HTTP port. Pointing the native protocol at
// an HTTP port fails as "[handshake] unexpected packet [72] from server" -- 72
// is 'H', the first byte of the HTTP response.
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
