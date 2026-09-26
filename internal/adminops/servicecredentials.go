package adminops

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// `service-credentials create|list|rotate|revoke` port the Python verbs of the same
// name (src/dev_health_ops/service_credentials.py). create and rotate print the new
// token on stdout, once; list prints the credentials' metadata as one JSON line,
// never a secret; revoke prints nothing.
//
// Named differences from `dev-hops service-credentials`:
//   - a refusal Python raised as a ValueError (a traceback, exit 1, nothing on stdout)
//     is one JSON error on stderr, exit 1, nothing on stdout; the message is the same text;
//   - the database is MIGRATION_DATABASE_URI, else POSTGRES_URI, as every dho admin verb
//     resolves it (Python reads POSTGRES_URI or --db, and a missing one is a usage error);
//   - rotate locks the credential row (FOR UPDATE) while it checks and changes it;
//   - options must be spelled in full (argparse also accepts unique prefixes).
// list keeps Python's shape: it shows no computed validity (CHAOS-4032 stays open: an
// expired credential and a live one print alike).

// ServiceCredentialsCommand is the `service-credentials` group.
func ServiceCredentialsCommand() cli.Command {
	return cli.Command{
		Name: "service-credentials", Summary: "internal service credentials (create, list, rotate, revoke)", Kind: cli.Group,
		Children: []cli.Command{
			{Name: "create", Summary: "create a service credential and print its token", Kind: cli.Verb, Run: runCredentialCreate},
			{Name: "list", Summary: "list credential metadata without secrets", Kind: cli.Verb, Run: runCredentialList},
			{Name: "rotate", Summary: "issue a replacement credential and let the old one lapse after --overlap-seconds", Kind: cli.Verb, Run: runCredentialRotate},
			{Name: "revoke", Summary: "revoke a service credential", Kind: cli.Verb, Run: runCredentialRevoke},
		},
	}
}

// stringList is a repeatable string flag (argparse action="append").
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

// credentialFlags are the options create and rotate share.
type credentialFlags struct {
	service   string
	scopes    stringList
	expiresAt optString
	createdBy optString
}

func addCredentialFlags(flags *flag.FlagSet, into *credentialFlags) {
	flags.StringVar(&into.service, "service", admin.ServiceACR, "the service: "+strings.Join(admin.ServiceNames(), ", "))
	flags.Var(&into.scopes, "scope", "a scope to grant (required, repeatable)")
	flags.Var(&into.expiresAt, "expires-at", "ISO 8601 instant with a timezone, in the future")
	flags.Var(&into.createdBy, "created-by-user-id", "the creating user's id")
}

func checkService(env cli.Env, service string) bool {
	if slices.Contains(admin.ServiceNames(), service) {
		return true
	}
	fmt.Fprintf(env.Stderr, "argument error: --service: invalid choice: %q (choose from %s)\n", service, strings.Join(admin.ServiceNames(), ", "))
	return false
}

// parseInterleaved parses flags and takes want positional arguments, wherever they
// stand among the flags (argparse lets a positional come first or last). A standalone
// "--" ends the options, as in argparse: every token after it is a positional, even one
// that looks like a flag.
func parseInterleaved(flags *flag.FlagSet, env cli.Env, want int) (positionals []string, code int, ok bool) {
	args := env.Args
	for {
		if err := flags.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil, cli.ExitOK, false
			}
			return nil, cli.ExitUsage, false
		}
		rest := flags.Args()
		consumed := args[:len(args)-len(rest)]
		terminator, isValue := endedByTerminator(flags, consumed)
		if isValue {
			// argparse reads "--" after an option that needs a value as the end of the options:
			// the option has no value, a usage error.
			fmt.Fprintf(env.Stderr, "argument error: %s: expected one argument\n", consumed[len(consumed)-2])
			return nil, cli.ExitUsage, false
		}
		if terminator && want == 0 {
			// A parser with no positional to take it leaves "--" unrecognized.
			fmt.Fprintln(env.Stderr, "argument error: unrecognized arguments: --")
			return nil, cli.ExitUsage, false
		}
		if terminator {
			positionals = append(positionals, rest...)
			break
		}
		if len(rest) == 0 {
			break
		}
		positionals = append(positionals, rest[0])
		args = rest[1:]
	}
	if len(positionals) != want {
		fmt.Fprintf(env.Stderr, "argument error: expected %d positional argument(s), got %d\n", want, len(positionals))
		return nil, cli.ExitUsage, false
	}
	return positionals, 0, true
}

// endedByTerminator says how a Parse stopped when the last token it consumed is "--": the
// terminator (terminator) or the value of the option before it (isValue: Go's flag package
// takes "--" as the value of a string option, argparse does not).
func endedByTerminator(flags *flag.FlagSet, consumed []string) (terminator, isValue bool) {
	if len(consumed) == 0 || consumed[len(consumed)-1] != "--" {
		return false, false
	}
	if len(consumed) >= 2 {
		previous := consumed[len(consumed)-2]
		if strings.HasPrefix(previous, "-") && !strings.Contains(previous, "=") && flags.Lookup(strings.TrimLeft(previous, "-")) != nil {
			return false, true
		}
	}
	return true, false
}

// credentialSpec validates the shared options in Python's order: scopes, creator, expiry.
func credentialSpec(flags credentialFlags, now time.Time) (admin.ServiceCredentialSpec, error) {
	scopes, err := admin.ServiceCredentialScopes(flags.service, flags.scopes)
	if err != nil {
		return admin.ServiceCredentialSpec{}, err
	}
	creator, err := admin.ParseServiceCredentialCreator(flags.createdBy.value)
	if err != nil {
		return admin.ServiceCredentialSpec{}, err
	}
	expiry, err := admin.ParseServiceCredentialExpiry(flags.expiresAt.ptr(), now)
	if err != nil {
		return admin.ServiceCredentialSpec{}, err
	}
	return admin.ServiceCredentialSpec{Service: flags.service, Scopes: scopes, CreatedBy: creator, ExpiresAt: expiry}, nil
}

// finishCredential reports a refusal or a failure on stderr as one JSON error, exit 1.
func finishCredential(env cli.Env, err error) int {
	var refusal *admin.ServiceCredentialError
	if errors.As(err, &refusal) {
		return writeError(env.Stderr, "service_credential_refused", refusal.Message)
	}
	return writeError(env.Stderr, "service_credential_failed", redactor(env)(err).Error())
}

func runCredentialCreate(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho service-credentials create")
	var options credentialFlags
	addCredentialFlags(flags, &options)
	if _, code, ok := parseInterleaved(flags, env, 0); !ok {
		return code
	}
	if !checkService(env, options.service) {
		return cli.ExitUsage
	}
	if len(options.scopes) == 0 {
		fmt.Fprintln(env.Stderr, "argument error: --scope is required")
		return cli.ExitUsage
	}
	spec, err := credentialSpec(options, time.Now())
	if err != nil {
		return finishCredential(env, err)
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	token, err := op.IssueServiceCredential(ctx, spec)
	if err != nil {
		return finishCredential(env, err)
	}
	fmt.Fprintln(env.Stdout, token)
	return cli.ExitOK
}

func runCredentialList(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho service-credentials list")
	service := flags.String("service", admin.ServiceACR, "the service: "+strings.Join(admin.ServiceNames(), ", "))
	if _, code, ok := parseInterleaved(flags, env, 0); !ok {
		return code
	}
	if !checkService(env, *service) {
		return cli.ExitUsage
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	list, err := op.ListServiceCredentials(ctx, *service)
	if err != nil {
		return finishCredential(env, err)
	}
	documents := make([]any, len(list))
	for i, item := range list {
		documents[i] = item.Document()
	}
	line, err := pythonparity.MarshalPythonJSONSorted(documents)
	if err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return cli.ExitFailure
	}
	fmt.Fprintln(env.Stdout, string(line))
	return cli.ExitOK
}

func runCredentialRotate(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho service-credentials rotate <credential_id>")
	var options credentialFlags
	addCredentialFlags(flags, &options)
	var overlap string
	flags.StringVar(&overlap, "overlap-seconds", "0", "seconds the old credential stays valid (0 to 3600)")
	positionals, code, ok := parseInterleaved(flags, env, 1)
	if !ok {
		return code
	}
	if !checkService(env, options.service) {
		return cli.ExitUsage
	}
	if len(options.scopes) == 0 {
		fmt.Fprintln(env.Stderr, "argument error: --scope is required")
		return cli.ExitUsage
	}
	overlapSeconds, err := pythonparity.ParseInt(overlap)
	if err != nil {
		fmt.Fprintf(env.Stderr, "argument error: --overlap-seconds: %v\n", err)
		return cli.ExitUsage
	}
	// From here Python raises ValueError, in this order.
	if overlapSeconds.Sign() < 0 || overlapSeconds.Cmp(big.NewInt(admin.MaxOverlapSeconds)) > 0 {
		return finishCredential(env, &admin.ServiceCredentialError{Message: "--overlap-seconds must be between 0 and 3600"})
	}
	id, err := admin.ParseServiceCredentialID(positionals[0])
	if err != nil {
		return finishCredential(env, err)
	}
	spec, err := credentialSpec(options, time.Now())
	if err != nil {
		return finishCredential(env, err)
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	token, err := op.RotateServiceCredential(ctx, id, int(overlapSeconds.Int64()), spec)
	if err != nil {
		return finishCredential(env, err)
	}
	fmt.Fprintln(env.Stdout, token)
	return cli.ExitOK
}

func runCredentialRevoke(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho service-credentials revoke <credential_id>")
	positionals, code, ok := parseInterleaved(flags, env, 1)
	if !ok {
		return code
	}
	id, err := admin.ParseServiceCredentialID(positionals[0])
	if err != nil {
		return finishCredential(env, err)
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	if err := op.RevokeServiceCredential(ctx, id); err != nil {
		return finishCredential(env, err)
	}
	return cli.ExitOK
}
