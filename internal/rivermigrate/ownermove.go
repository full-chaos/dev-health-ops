package rivermigrate

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/ownermove"
)

const ownerMoveUsage = "usage: dho migrate owner-move -schemas <a,b> -from <role> -to <role> [-database-owner] [-plan | -apply] [-restore]"

// ownerMoveCommand is `dho migrate owner-move`: the ops schemas (and the database) to a new owner, run once by an
// operator with the superuser login. See internal/storage/postgres/ownermove.
func ownerMoveCommand() cli.Command {
	return cli.Command{
		Name:    "owner-move",
		Summary: "move schemas public and river (and the database) to the NOSUPERUSER migration login, or back (-restore); -plan reads only",
		Kind:    cli.Verb,
		Run: func(ctx context.Context, env cli.Env) int {
			return ExecuteOwnerMove(ctx, env.Args, env.Lookup, env.Stdout, env.Stderr)
		},
	}
}

func parseOwnerMoveArgs(args []string, stderr io.Writer) (ownermove.Options, error) {
	flags := flag.NewFlagSet("dho migrate owner-move", flag.ContinueOnError)
	flags.SetOutput(stderr)
	schemas := flags.String("schemas", "", "comma-separated schemas whose objects move (public, river)")
	databaseOwner := flags.Bool("database-owner", false, "also move the current database (schema public follows it through pg_database_owner)")
	from := flags.String("from", "", "the role that owns the objects now")
	to := flags.String("to", "", "the role that owns them afterwards (the migration login; any role with -restore)")
	planOnly := flags.Bool("plan", false, "read the catalog only and print the counts line: no ALTER and no lock")
	apply := flags.Bool("apply", false, "make the changes; without -apply or -plan the run alters inside a transaction and rolls it back")
	restore := flags.Bool("restore", false, "the rollback: -to may be any role")
	if err := flags.Parse(args); err != nil {
		return ownermove.Options{}, err
	}
	if flags.NArg() != 0 {
		return ownermove.Options{}, errors.New("positional arguments are not accepted")
	}
	if *planOnly && *apply {
		return ownermove.Options{}, errors.New("-plan and -apply exclude each other")
	}
	options := ownermove.Options{
		DatabaseOwner: *databaseOwner, From: strings.TrimSpace(*from), To: strings.TrimSpace(*to), Restore: *restore, Mode: ownermove.DryRun,
	}
	for _, schema := range strings.Split(*schemas, ",") {
		if schema = strings.TrimSpace(schema); schema != "" {
			options.Schemas = append(options.Schemas, schema)
		}
	}
	switch {
	case *planOnly:
		options.Mode = ownermove.Plan
	case *apply:
		options.Mode = ownermove.Apply
	}
	if err := options.Validate(); err != nil {
		return ownermove.Options{}, err
	}
	return options, nil
}

// ExecuteOwnerMove runs `dho migrate owner-move` on the migration endpoint (MIGRATION_DATABASE_URI, else POSTGRES_URI),
// which must be a superuser login. It prints one line of counts. Exit 0 done, 1 a refusal, a lock timeout or a
// database failure (nothing changed), 2 a bad command line.
func ExecuteOwnerMove(parent context.Context, args []string, lookup platformsecrets.LookupEnv, stdout, stderr io.Writer) int {
	options, err := parseOwnerMoveArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		fmt.Fprintf(stderr, "argument error: %v\n%s\n", err, ownerMoveUsage)
		return cli.ExitUsage
	}
	uri, _, ok := resolveMigrationDatabaseURI(lookup, stderr, true)
	if !ok {
		return cli.ExitFailure
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Minute)
	defer cancel()
	boundary := pgstorage.Boundary(uri.Reveal())
	conn, err := pgx.Connect(ctx, uri.Reveal())
	if err != nil {
		fmt.Fprintf(stderr, "owner-move error: PostgreSQL migration endpoint unavailable: %v\n", boundary.Redact(err))
		return cli.ExitFailure
	}
	defer conn.Close(context.Background())
	report, err := ownermove.Move(ctx, conn, options)
	if err != nil {
		fmt.Fprintf(stderr, "owner-move refused: %v\n", err)
		return cli.ExitFailure
	}
	fmt.Fprintln(stdout, report.Line())
	return cli.ExitOK
}
