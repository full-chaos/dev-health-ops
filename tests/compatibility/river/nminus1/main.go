package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type failureResult struct {
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
	Operation     string `json:"operation,omitempty"`
	Phase         string `json:"phase"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("river-nminus1", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databaseURL := flags.String(
		"database-url",
		getenv("RIVER_COMPAT_DATABASE_URL"),
		"PostgreSQL DSN (or set RIVER_COMPAT_DATABASE_URL)",
	)
	operation := flags.String("operation", "", "migrate, insert, or work")
	mode := flags.String("mode", "", "alias for --operation")
	marker := flags.String("marker", "", "compatibility marker (required for insert/work)")
	queue := flags.String("queue", "", "River queue (required for insert/work)")
	pollOnly := flags.Bool("poll-only", false, "disable LISTEN and poll for work")
	consumeExisting := flags.Bool(
		"consume-existing",
		false,
		"work an already-inserted marker instead of inserting it",
	)
	timeout := flags.Duration("timeout", 30*time.Second, "overall operation timeout")
	schema := flags.String("schema", "", "optional PostgreSQL schema for River tables")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	resolvedOperation, err := resolveOperation(*operation, *mode)
	if err != nil {
		fmt.Fprintln(stderr, "operation must be migrate, insert, or work")
		return 2
	}
	if strings.TrimSpace(*databaseURL) == "" {
		fmt.Fprintln(stderr, "database URL is required")
		return 2
	}
	if *timeout <= 0 {
		fmt.Fprintln(stderr, "timeout must be positive")
		return 2
	}
	if resolvedOperation != "migrate" {
		if strings.TrimSpace(*marker) == "" || strings.TrimSpace(*queue) == "" {
			fmt.Fprintln(stderr, "marker and queue are required for insert/work")
			return 2
		}
	}
	if *consumeExisting && resolvedOperation != "work" {
		fmt.Fprintln(stderr, "consume-existing is valid only for work")
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	probeResult, err := runProbe(ctx, options{
		consumeExisting: *consumeExisting,
		databaseURL:     *databaseURL,
		marker:          *marker,
		operation:       resolvedOperation,
		pollOnly:        *pollOnly,
		queue:           *queue,
		schema:          *schema,
	})
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	if err != nil {
		// The underlying error may include the supplied DSN. Emit only a bounded
		// phase and never serialize raw pgx/River error text.
		_ = encoder.Encode(failureResult{
			SchemaVersion: resultSchemaVersion,
			Status:        "error",
			Operation:     resolvedOperation,
			Phase:         errorPhase(err),
		})
		return 1
	}
	if err := encoder.Encode(probeResult); err != nil {
		fmt.Fprintln(stderr, "encode result failed")
		return 1
	}
	return 0
}

func resolveOperation(operation, mode string) (string, error) {
	if operation != "" && mode != "" && operation != mode {
		return "", fmt.Errorf("conflicting operation and mode")
	}
	resolved := operation
	if resolved == "" {
		resolved = mode
	}
	switch resolved {
	case "migrate", "insert", "work":
		return resolved, nil
	default:
		return "", fmt.Errorf("unsupported operation")
	}
}
