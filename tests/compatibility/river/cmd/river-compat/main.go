package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	rivercompat "github.com/full-chaos/dev-health-ops/tests/compatibility/river/go"
)

const (
	operationConsume = "consume"
	operationCrash   = "crash-candidate"
	operationInsert  = "insert"
	operationMatrix  = "matrix"
	operationMigrate = "migrate"
)

type failure struct {
	Status string `json:"status"`
	Phase  string `json:"phase"`
}

type startedSignal struct {
	Event   string           `json:"event"`
	Mode    rivercompat.Mode `json:"mode"`
	Marker  string           `json:"marker"`
	JobID   int64            `json:"job_id"`
	Attempt int              `json:"attempt"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("river-compat", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databaseURL := flags.String(
		"database-url",
		getenv("RIVER_COMPAT_DATABASE_URL"),
		"PostgreSQL DSN (or set RIVER_COMPAT_DATABASE_URL)",
	)
	mode := flags.String("mode", string(rivercompat.ModeDirect), "direct or poll-only")
	operation := flags.String(
		"operation",
		operationMatrix,
		"matrix, migrate, insert, consume, or crash-candidate",
	)
	queue := flags.String("queue", "chaos3034", "River queue to probe")
	marker := flags.String("marker", "", "external or crash-candidate marker")
	priority := flags.Int("priority", 2, "insert priority (1 highest, 4 lowest)")
	maxAttempts := flags.Int("max-attempts", 3, "maximum job attempts")
	expectedAttempt := flags.Int("expected-attempt", 0, "attempt required for consume operation")
	fetchPollInterval := flags.Duration(
		"fetch-poll-interval",
		250*time.Millisecond,
		"River fetch fallback/poll-only interval",
	)
	jobTimeout := flags.Duration("job-timeout", 0, "River job timeout (0 uses River default)")
	rescueStuckAfter := flags.Duration(
		"rescue-stuck-after",
		0,
		"age at which River rescues a running job (0 uses River default)",
	)
	samples := flags.Int("samples", 20, "number of execute jobs used for latency percentiles")
	timeout := flags.Duration("timeout", 60*time.Second, "overall probe timeout")
	schema := flags.String("schema", "", "optional PostgreSQL schema for River tables")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	if *databaseURL == "" {
		fmt.Fprintln(stderr, "database URL is required via --database-url or RIVER_COMPAT_DATABASE_URL")
		return 2
	}
	if *timeout <= 0 {
		fmt.Fprintln(stderr, "timeout must be positive")
		return 2
	}
	if *operation != operationMatrix &&
		*operation != operationMigrate &&
		*operation != operationInsert &&
		*operation != operationConsume &&
		*operation != operationCrash {
		fmt.Fprintln(stderr, "operation must be matrix, migrate, insert, consume, or crash-candidate")
		return 2
	}
	if (*operation == operationInsert || *operation == operationConsume) && *marker == "" {
		fmt.Fprintln(stderr, "insert and consume operations require --marker")
		return 2
	}

	encoder := json.NewEncoder(stdout)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	opts := rivercompat.Options{
		CrashFirstAttempt: *operation == operationCrash,
		CrashMarker:       *marker,
		DatabaseURL:       *databaseURL,
		ExpectedAttempt:   *expectedAttempt,
		FetchPollInterval: *fetchPollInterval,
		JobTimeout:        *jobTimeout,
		MaxAttempts:       *maxAttempts,
		MigrateOnly:       *operation == operationMigrate,
		Mode:              rivercompat.Mode(*mode),
		Priority:          *priority,
		Queue:             *queue,
		RescueStuckAfter:  *rescueStuckAfter,
		Samples:           *samples,
		Schema:            *schema,
	}
	if *operation == operationConsume {
		opts.ConsumeMarker = *marker
	}
	if *operation == operationInsert {
		opts.InsertMarker = *marker
	}
	if *operation == operationCrash {
		opts.Started = func(start rivercompat.Start) error {
			return encoder.Encode(startedSignal{
				Event:   "started",
				Mode:    rivercompat.Mode(*mode),
				Marker:  start.Args.Marker,
				JobID:   start.JobID,
				Attempt: start.Attempt,
			})
		}
	}

	result, err := rivercompat.Run(ctx, opts)
	if err != nil {
		// Never serialize the underlying pgx error: connection errors may echo
		// credentials from the supplied DSN. The phase is enough for the runner
		// to identify the failed gate while retaining sanitized output.
		_ = encoder.Encode(failure{Status: "error", Phase: rivercompat.ErrorPhase(err)})
		return 1
	}
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintln(stderr, "encode result:", err)
		return 1
	}
	return 0
}
