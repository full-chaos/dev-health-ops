package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	ownerMoveVerb = "owner-move"
	// envSchemaOwner pins the owner role of the auth schema and of every object in it (authschema.Options.SchemaOwnerRole).
	envSchemaOwner = "AUTH_SCHEMA_OWNER_ROLE"
)

func ownerMoveUsage() string {
	return "usage: auth-migrate owner-move -from <role> -to <role> [-runtime-role <role>] [-schema <name>] [-apply] [-grants-only] [-restore]"
}

// parseOwnerMoveArgs refuses a bad command line before any database is read. Without -apply the verb reports what it
// would change and changes nothing.
func parseOwnerMoveArgs(args []string, lookup secrets.LookupEnv, stderr io.Writer) (authschema.OwnerMoveOptions, error) {
	flags := flag.NewFlagSet(serviceName+" "+ownerMoveVerb, flag.ContinueOnError)
	flags.SetOutput(stderr)
	from := flags.String("from", "", "the role that owns the auth schema and its objects now")
	to := flags.String("to", "", "the role that owns them afterwards (the migration login)")
	runtimeRole := flags.String("runtime-role", "", "the auth runtime login (default: "+envRuntimeRole+")")
	schema := flags.String("schema", "", "the auth schema (default: "+envSchema+", else auth)")
	apply := flags.Bool("apply", false, "make the changes; without it the verb reports and changes nothing")
	grantsOnly := flags.Bool("grants-only", false, "only give the new owner the public-table privileges of the grants pass; no owner and no runtime row changes")
	restore := flags.Bool("restore", false, "the rollback: -to may be any role and the public grant rows return to the table owner")
	if err := flags.Parse(args); err != nil {
		return authschema.OwnerMoveOptions{}, err
	}
	if flags.NArg() != 0 {
		return authschema.OwnerMoveOptions{}, errors.New("positional arguments are not accepted")
	}
	if *restore && *grantsOnly {
		return authschema.OwnerMoveOptions{}, errors.New("-restore and -grants-only exclude each other")
	}
	options := authschema.OwnerMoveOptions{
		Schema: strings.TrimSpace(*schema), From: strings.TrimSpace(*from), To: strings.TrimSpace(*to),
		RuntimeRole: strings.TrimSpace(*runtimeRole), Apply: *apply, GrantsOnly: *grantsOnly, Restore: *restore,
	}
	if options.Schema == "" {
		options.Schema = valueOr(lookup, envSchema, defaultSchema)
	}
	if options.RuntimeRole == "" {
		options.RuntimeRole = valueOr(lookup, envRuntimeRole, "")
	}
	if err := options.Validate(); err != nil {
		return authschema.OwnerMoveOptions{}, err
	}
	return options, nil
}

// executeOwnerMove runs `auth-migrate owner-move` on the migration endpoint, which must be a superuser login: the
// verb changes owners and grants as the table owner. It prints counts only. Exit 0 done, 1 a configuration or database
// failure or a refusal, 2 a bad command line.
func executeOwnerMove(parent context.Context, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer) int {
	options, err := parseOwnerMoveArgs(args, lookup, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "argument error: %v\n%s\n", err, ownerMoveUsage())
		return 2
	}
	uri, configured, err := secrets.ResolveDSNSetting(envMigrationURI, lookup)
	if err != nil {
		fmt.Fprintf(stderr, "configuration error: could not resolve %s\n", envMigrationURI)
		return 1
	}
	if !configured {
		fmt.Fprintf(stderr, "configuration error: %s or %s_FILE is required\n", envMigrationURI, envMigrationURI)
		return 1
	}
	ctx, cancel := context.WithTimeout(parent, runBudget)
	defer cancel()
	poolConfig := postgresstore.DefaultConfig(uri.Reveal())
	poolConfig.MaxConns = 2
	pool, err := newDatabaseWait(poolConfig, stderr).run(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "owner-move error: PostgreSQL migration endpoint unavailable")
		return 1
	}
	defer pool.Close()
	report, err := authschema.OwnerMove(ctx, pool, options)
	if err != nil {
		fmt.Fprintf(stderr, "owner-move refused: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "owner-move applied=%t moved=%s followed=%s public_tables=%d grant_rows=%d\n",
		report.Applied, countList(report.Moved), countList(report.Followed), report.PublicTables, report.GrantRowsMoved)
	return 0
}

func countList(counts map[string]int) string {
	keys := make([]string, 0, len(counts))
	for kind := range counts {
		keys = append(keys, kind)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, kind := range keys {
		parts = append(parts, kind+":"+strconv.Itoa(counts[kind]))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// schemaOwnerRole reads the owner pin: empty (unset) turns it off.
func schemaOwnerRole(lookup secrets.LookupEnv) (string, error) {
	role := valueOr(lookup, envSchemaOwner, "")
	if role == "" {
		return "", nil
	}
	if err := authschema.ValidateIdentifier(role); err != nil {
		return "", fmt.Errorf("%s is not a usable role name", envSchemaOwner)
	}
	return role, nil
}
