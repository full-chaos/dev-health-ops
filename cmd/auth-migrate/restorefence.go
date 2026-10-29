package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/keyrotation"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const restoreFenceUsage = "usage: auth-migrate restore-fence export -out FILE | apply [-manifest FILE] [-blind-margin N] [-apply] | status"

type restoreFenceArgs struct {
	verb        string
	out         string
	manifest    string
	blindMargin int64
	apply       bool
}

func parseRestoreFenceArgs(args []string, stderr io.Writer) (restoreFenceArgs, error) {
	if len(args) == 0 {
		return restoreFenceArgs{}, errors.New("a verb is required")
	}
	parsed := restoreFenceArgs{verb: args[0]}
	flags := flag.NewFlagSet(serviceName+" restore-fence", flag.ContinueOnError)
	flags.SetOutput(stderr)
	switch parsed.verb {
	case "export":
		flags.StringVar(&parsed.out, "out", "", "the manifest file to create (it must not exist)")
	case "apply":
		flags.StringVar(&parsed.manifest, "manifest", "", "the manifest exported from the shadow restore")
		flags.Int64Var(&parsed.blindMargin, "blind-margin", 0, "raise every revision by this much (the fallback when the WAL after the target is gone)")
		flags.BoolVar(&parsed.apply, "apply", false, "write the change (without it the run is a dry run that rolls back)")
	case "status":
	default:
		return restoreFenceArgs{}, errors.New("unknown verb")
	}
	if err := flags.Parse(args[1:]); err != nil {
		return restoreFenceArgs{}, err
	}
	if flags.NArg() != 0 {
		return restoreFenceArgs{}, errors.New("positional arguments are not accepted")
	}
	if parsed.verb == "export" && parsed.out == "" {
		return restoreFenceArgs{}, errors.New("export needs -out")
	}
	if parsed.verb == "apply" && parsed.manifest == "" && parsed.blindMargin <= 0 {
		return restoreFenceArgs{}, errors.New("apply needs -manifest or -blind-margin")
	}
	if parsed.verb == "apply" && parsed.manifest != "" && parsed.blindMargin != 0 {
		return restoreFenceArgs{}, errors.New("-blind-margin applies only without -manifest")
	}
	return parsed, nil
}

// executeRestoreFence runs `auth-migrate restore-fence`. It connects as the
// migration role and refuses to run as the runtime role. It prints counts and
// table names only: never an id, a hash or a DSN.
func executeRestoreFence(parent context.Context, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer) int {
	parsed, err := parseRestoreFenceArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "argument error: %v\n%s\n", err, restoreFenceUsage)
		return 2
	}
	migrationURI, configured, err := secrets.ResolveDSNSetting(envMigrationURI, lookup)
	if err != nil || !configured {
		fmt.Fprintf(stderr, "configuration error: %s or %s_FILE is required\n", envMigrationURI, envMigrationURI)
		return 1
	}
	schema, err := authschema.NewValidatedIdentifier(valueOr(lookup, envSchema, defaultSchema))
	if err != nil {
		fmt.Fprintf(stderr, "configuration error: %s is not a usable schema name\n", envSchema)
		return 1
	}
	runtimeRole, present := lookup(envRuntimeRole)
	runtimeRole = strings.TrimSpace(runtimeRole)
	if !present || authschema.ValidateIdentifier(runtimeRole) != nil {
		fmt.Fprintf(stderr, "configuration error: %s is required\n", envRuntimeRole)
		return 1
	}

	ctx, cancel := context.WithTimeout(parent, runBudget)
	defer cancel()
	poolConfig := postgresstore.DefaultConfig(migrationURI.Reveal())
	poolConfig.MaxConns = 2
	pool, err := newDatabaseWait(poolConfig, stderr).run(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "restore-fence error: PostgreSQL migration endpoint unavailable")
		return 1
	}
	defer pool.Close()
	if err := keyrotation.CheckNotRuntimeRole(ctx, pool, runtimeRole); err != nil {
		if errors.Is(err, keyrotation.ErrRuntimeRole) {
			fmt.Fprintf(stderr, "configuration error: %s connects as the runtime role %q; the fence runs as the migration role only\n", envMigrationURI, runtimeRole)
		} else {
			fmt.Fprintln(stderr, "restore-fence error: could not read the connected role")
		}
		return 1
	}

	switch parsed.verb {
	case "status":
		timeline, err := restorefence.Timeline(ctx, pool, schema)
		if err != nil || timeline == nil {
			fmt.Fprintln(stdout, "restore-fence status timeline=none fenced=false")
			return 1
		}
		if err := restorefence.Check(ctx, pool, schema); err != nil {
			fmt.Fprintf(stdout, "restore-fence status timeline=%d fenced=false\n", *timeline)
			printInputStatus(ctx, stdout)
			return 1
		}
		fmt.Fprintf(stdout, "restore-fence status timeline=%d fenced=true\n", *timeline)
		printInputStatus(ctx, stdout)
		printActions(ctx, pool, schema, stdout)
		return 0
	case "export":
		manifest, err := restorefence.ExportManifest(ctx, pool, schema)
		if err != nil {
			fmt.Fprintf(stderr, "restore-fence error: %v\n", err)
			return 1
		}
		raw, err := manifest.Marshal()
		if err != nil {
			fmt.Fprintln(stderr, "restore-fence error: could not encode the manifest")
			return 1
		}
		file, err := os.OpenFile(parsed.out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			fmt.Fprintln(stderr, "restore-fence error: could not create the manifest file (it must not exist)")
			return 1
		}
		if _, err := file.Write(raw); err != nil {
			_ = file.Close()
			fmt.Fprintln(stderr, "restore-fence error: could not write the manifest file")
			return 1
		}
		if err := file.Close(); err != nil {
			fmt.Fprintln(stderr, "restore-fence error: could not close the manifest file")
			return 1
		}
		fmt.Fprintf(stdout, "restore-fence export timeline=%d floors=%d sections=%d sha256=%s\n",
			manifest.SourceTimeline, len(manifest.Floors), len(manifest.Sections), manifest.SHA256)
		return 0
	}

	var manifest *restorefence.Manifest
	if parsed.manifest != "" {
		raw, err := os.ReadFile(parsed.manifest)
		if err != nil {
			fmt.Fprintln(stderr, "restore-fence error: could not read the manifest file")
			return 1
		}
		parsedManifest, err := restorefence.ParseManifest(raw)
		if err != nil {
			fmt.Fprintf(stderr, "restore-fence error: %v\n", err)
			return 1
		}
		manifest = &parsedManifest
	}
	report, err := restorefence.Apply(ctx, pool, schema, manifest, restorefence.Options{Apply: parsed.apply, BlindMargin: parsed.blindMargin})
	if err != nil {
		fmt.Fprintf(stderr, "restore-fence error: %v\n", err)
		return 1
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		fmt.Fprintln(stderr, "restore-fence error: could not encode the report")
		return 1
	}
	fmt.Fprintf(stdout, "restore-fence apply %s\n", encoded)
	for _, line := range restorefence.ActionLines(report.Actions) {
		fmt.Fprintln(stdout, line)
	}
	return 0
}

// printInputStatus prints one line per floor input: the two the operator
// supplies to apply, and every registered consumer input with its status.
func printInputStatus(ctx context.Context, stdout io.Writer) {
	fmt.Fprintln(stdout, "input wal_manifest kind=wal_manifest supplied=apply -manifest")
	fmt.Fprintln(stdout, "input blind_margin kind=blind_margin supplied=apply -blind-margin (only when no other input exists)")
	for _, in := range restorefence.RegisteredFloorInputs() {
		usable := in.Status(ctx) == nil
		fmt.Fprintf(stdout, "input %s kind=consumer usable=%t\n", in.Name(), usable)
	}
}

// printActions prints the operator actions of the live timeline's fence.
func printActions(ctx context.Context, pool restorefence.Querier, schema authschema.ValidatedIdentifier, stdout io.Writer) {
	actions, err := restorefence.PendingActions(ctx, pool, schema)
	if err != nil {
		fmt.Fprintln(stdout, "operator-actions unreadable")
		return
	}
	for _, line := range restorefence.ActionLines(actions) {
		fmt.Fprintln(stdout, line)
	}
}
