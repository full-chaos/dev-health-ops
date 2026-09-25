// Package pushcli is the `push` group of dho: the customer-push external
// ingestion CLI (Python's `dev-hops push`). Its verbs open no database
// connection, make no network call and need no server (the record validators
// they share link the api's JSON-body helpers, which link the auth package's
// HTTP envelope types; nothing here serves or sends a request): validate checks a batch envelope locally with the same record
// validators the API runs (internal/api/recordvalidation), sample prints the
// packaged example batches, and export is the extension point that says it is not
// implemented.
package pushcli

import (
	"errors"
	"flag"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// Exit codes of the push verbs (output.py): 0 success, 1 data-level failure
// (invalid payload), 2 usage error, 3 transport error, 4 poll timeout.
const (
	exitOK          = 0
	exitDataFailure = 1
	exitUsage       = 2
)

// schemaVersion is schemas.py's SCHEMA_VERSION, the only version there is.
const schemaVersion = "external-ingest.v1"

// Default limits (schemas.py's MAX_RECORDS_DEFAULT and MAX_BODY_BYTES_DEFAULT).
const (
	maxRecordsDefault   = 1000
	maxBodyBytesDefault = 10_000_000
)

// Command is the `push` group.
func Command() cli.Command {
	return cli.Command{
		Name: "push", Summary: "customer-push external ingestion", Kind: cli.Group,
		Children: []cli.Command{
			{Name: "validate", Summary: "validate a batch payload locally (no network call)", Kind: cli.Verb, Run: runValidate},
			{Name: "sample", Summary: "print a canonical sample batch envelope", Kind: cli.Verb, Run: runSample},
			{Name: "export", Summary: "provider export helpers (not implemented; extension point)", Kind: cli.Verb, Run: runExport},
		},
	}
}

// parseArgs parses a verb's arguments the way argparse does: flags and
// positionals may be interleaved. ok false means the exit code in code.
func parseArgs(flags *flag.FlagSet, env cli.Env, args []string) (positional []string, code int, ok bool) {
	for {
		if err := flags.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil, exitOK, false
			}
			return nil, exitUsage, false
		}
		if flags.NArg() == 0 {
			return positional, 0, true
		}
		positional = append(positional, flags.Arg(0))
		args = flags.Args()[1:]
	}
}

func newFlags(env cli.Env, name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	return flags
}

func usageError(env cli.Env, format string, args ...any) int {
	fmt.Fprintf(env.Stderr, "argument error: "+format+"\n", args...)
	return exitUsage
}
