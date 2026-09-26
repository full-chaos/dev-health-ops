package adminops

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pyargparse"
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
//   - --help prints dho's usage (on stdout, exit 0) rather than argparse's text.
// The command line is parsed by internal/pyargparse, the port of argparse's own algorithm the other dho
// commands share, so option abbreviations, single-dash forms, "--", repeated options and the type
// checks of each occurrence mean what they mean to argparse; parseCredentialArgs is compared with the
// real parser over a generated corpus (servicecredentials_args_oracle_test.go).
// list keeps Python's shape: it shows no computed validity (CHAOS-4032 stays open: an
// expired credential and a live one print alike).

// ServiceCredentialsCommand is the `service-credentials` group.
func ServiceCredentialsCommand() cli.Command {
	rootFlags := []cli.RootFlag{cli.RootLogLevel, cli.RootLLMProvider, cli.RootModel}
	return cli.Command{
		Name: "service-credentials", Summary: "internal service credentials (create, list, rotate, revoke)", Kind: cli.Group,
		Children: []cli.Command{
			{Name: "create", Summary: "create a service credential and print its token", Kind: cli.Verb, Run: runCredentialCreate, RootFlags: rootFlags},
			{Name: "list", Summary: "list credential metadata without secrets", Kind: cli.Verb, Run: runCredentialList, RootFlags: rootFlags},
			{Name: "rotate", Summary: "issue a replacement credential and let the old one lapse after --overlap-seconds", Kind: cli.Verb, Run: runCredentialRotate, RootFlags: rootFlags},
			{Name: "revoke", Summary: "revoke a service credential", Kind: cli.Verb, Run: runCredentialRevoke, RootFlags: rootFlags},
		},
	}
}

// credentialArgs is what a service-credentials command line means (argparse's namespace).
type credentialArgs struct {
	help         bool
	service      string
	scopes       []string
	expiresAt    *string
	createdBy    *string
	overlap      string // the text of --overlap-seconds; "0" when absent
	overlapValue *big.Int
	credentialID string
	hasID        bool
	db           *string
}

// The leaf options `dev-hops` re-adds to every command (build_parser's _GLOBAL_FLAG_SPECS): they are
// accepted; --db names the database, the rest configure other commands and are ignored here.
var credentialGlobalSpecs = []pyargparse.Spec{
	{Strs: []string{"--log-level"}, Dest: "log_level", Kind: pyargparse.OptValue},
	{Strs: []string{"--db"}, Dest: "db", Kind: pyargparse.OptValue},
	{Strs: []string{"--analytics-db"}, Dest: "analytics_db", Kind: pyargparse.OptValue},
	{Strs: []string{"--org"}, Dest: "org", Kind: pyargparse.OptValue},
	{Strs: []string{"-l", "--llm-provider"}, Dest: "llm_provider", Kind: pyargparse.OptValue},
	{Strs: []string{"-m", "--model"}, Dest: "model", Kind: pyargparse.OptValue},
}

func credentialSpecs(verb string) []pyargparse.Spec {
	specs := []pyargparse.Spec{{Strs: []string{"-h", "--help"}, Dest: "help", Kind: pyargparse.OptHelp}}
	switch verb {
	case "create", "rotate":
		specs = append(specs,
			pyargparse.Spec{Strs: []string{"--service"}, Dest: "service", Kind: pyargparse.OptValue},
			pyargparse.Spec{Strs: []string{"--scope"}, Dest: "scope", Kind: pyargparse.OptValue},
			pyargparse.Spec{Strs: []string{"--expires-at"}, Dest: "expires_at", Kind: pyargparse.OptValue},
			pyargparse.Spec{Strs: []string{"--created-by-user-id"}, Dest: "created_by_user_id", Kind: pyargparse.OptValue})
		if verb == "rotate" {
			specs = append(specs, pyargparse.Spec{Strs: []string{"--overlap-seconds"}, Dest: "overlap_seconds", Kind: pyargparse.OptValue})
		}
	case "list":
		specs = append(specs, pyargparse.Spec{Strs: []string{"--service"}, Dest: "service", Kind: pyargparse.OptValue})
	}
	return append(specs, credentialGlobalSpecs...)
}

// parseCredentialArgs parses the arguments after `service-credentials <verb>` as argparse would.
// A returned error is a usage error (exit 2).
func parseCredentialArgs(verb string, args []string) (credentialArgs, *pyargparse.Error) {
	out := credentialArgs{service: admin.ServiceACR, overlap: "0", overlapValue: big.NewInt(0)}
	// The type and choice checks argparse makes when it takes each occurrence of an option.
	act := func(spec *pyargparse.Spec, value string) *pyargparse.Error {
		switch spec.Dest {
		case "service":
			if !slices.Contains(admin.ServiceNames(), value) {
				return &pyargparse.Error{Msg: fmt.Sprintf("argument --service: invalid choice: %s (choose from %s)", pythonparity.StrRepr(value), quotedList(admin.ServiceNames()))}
			}
			out.service = value
		case "scope":
			out.scopes = append(out.scopes, value)
		case "expires_at":
			v := value
			out.expiresAt = &v
		case "created_by_user_id":
			v := value
			out.createdBy = &v
		case "overlap_seconds":
			number, err := pythonparity.ParseInt(value)
			if err != nil {
				return &pyargparse.Error{Msg: fmt.Sprintf("argument --overlap-seconds: invalid int value: %s", pythonparity.StrRepr(value))}
			}
			out.overlap, out.overlapValue = value, number
		case "db":
			v := value
			out.db = &v
		}
		return nil
	}
	parser := pyargparse.New(credentialSpecs(verb))
	var parsed *pyargparse.Parsed
	var positional *string
	var err *pyargparse.Error
	if verb == "rotate" || verb == "revoke" {
		parsed, positional, err = parser.ParseWithPositional(args, act)
	} else {
		parsed, err = parser.Parse(args, act)
	}
	if err != nil {
		return credentialArgs{}, err
	}
	if parsed.Help {
		out.help = true
		return out, nil
	}
	// argparse's own order: the required arguments, then what is left over.
	var missing []string
	if (verb == "rotate" || verb == "revoke") && positional == nil {
		missing = append(missing, "credential_id")
	}
	if (verb == "create" || verb == "rotate") && len(out.scopes) == 0 {
		missing = append(missing, "--scope")
	}
	if len(missing) > 0 {
		return credentialArgs{}, &pyargparse.Error{Msg: "the following arguments are required: " + strings.Join(missing, ", ")}
	}
	if len(parsed.Unrecognized) > 0 {
		return credentialArgs{}, &pyargparse.Error{Msg: "unrecognized arguments: " + strings.Join(parsed.Unrecognized, " ")}
	}
	if positional != nil {
		out.credentialID, out.hasID = *positional, true
	}
	return out, nil
}

func quotedList(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = "'" + item + "'"
	}
	return strings.Join(quoted, ", ")
}

// credentialUsage is the text --help prints.
const credentialUsage = `usage: dho service-credentials <create|list|rotate|revoke> [options]

  create  [--service {acr,worker-operator}] --scope SCOPE [--scope SCOPE ...] [--expires-at ISO8601]
          [--created-by-user-id UUID]
  list    [--service {acr,worker-operator}]
  rotate  <credential_id> [--service {acr,worker-operator}] --scope SCOPE ... [--expires-at ISO8601]
          [--created-by-user-id UUID] [--overlap-seconds 0..3600]
  revoke  <credential_id>

The token of create and rotate is printed once, on stdout. Every option takes the same forms as in dev-hops
(unique prefixes, "--" ends the options); --db names the database. The root options --log-level LEVEL,
--llm-provider NAME (-l) and --model NAME (-m) are accepted and unused.
` + dbEnvHelp

// prepareCredentialArgs parses a verb's command line and handles help and usage errors: ok false means
// the exit code in code.
func prepareCredentialArgs(env cli.Env, verb string) (credentialArgs, cli.Env, int, bool) {
	args, err := parseCredentialArgs(verb, env.Args)
	if err != nil {
		fmt.Fprintf(env.Stderr, "argument error: %s\n", err.Msg)
		return credentialArgs{}, env, cli.ExitUsage, false
	}
	if args.help {
		fmt.Fprint(env.Stdout, credentialUsage)
		return credentialArgs{}, env, cli.ExitOK, false
	}
	if args.db != nil {
		// --db names the database as POSTGRES_URI does for dev-hops (it also takes the async driver spelling).
		uri := strings.Replace(*args.db, "postgresql+asyncpg://", "postgresql://", 1)
		lookup := env.Lookup
		env.Lookup = func(key string) (string, bool) {
			if key == "MIGRATION_DATABASE_URI" {
				return uri, true
			}
			return lookup(key)
		}
	}
	return args, env, 0, true
}

// credentialSpec validates the shared options in Python's order: scopes, creator, expiry.
func credentialSpec(args credentialArgs, now time.Time) (admin.ServiceCredentialSpec, error) {
	scopes, err := admin.ServiceCredentialScopes(args.service, args.scopes)
	if err != nil {
		return admin.ServiceCredentialSpec{}, err
	}
	createdBy := ""
	if args.createdBy != nil {
		createdBy = *args.createdBy
	}
	creator, err := admin.ParseServiceCredentialCreator(createdBy)
	if err != nil {
		return admin.ServiceCredentialSpec{}, err
	}
	expiry, err := admin.ParseServiceCredentialExpiry(args.expiresAt, now)
	if err != nil {
		return admin.ServiceCredentialSpec{}, err
	}
	return admin.ServiceCredentialSpec{Service: args.service, Scopes: scopes, CreatedBy: creator, ExpiresAt: expiry}, nil
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
	args, env, code, ok := prepareCredentialArgs(env, "create")
	if !ok {
		return code
	}
	spec, err := credentialSpec(args, time.Now())
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
	args, env, code, ok := prepareCredentialArgs(env, "list")
	if !ok {
		return code
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	list, err := op.ListServiceCredentials(ctx, args.service)
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
	args, env, code, ok := prepareCredentialArgs(env, "rotate")
	if !ok {
		return code
	}
	// From here Python raises ValueError, in this order.
	if args.overlapValue.Sign() < 0 || args.overlapValue.Cmp(big.NewInt(admin.MaxOverlapSeconds)) > 0 {
		return finishCredential(env, &admin.ServiceCredentialError{Message: "--overlap-seconds must be between 0 and 3600"})
	}
	id, err := admin.ParseServiceCredentialID(args.credentialID)
	if err != nil {
		return finishCredential(env, err)
	}
	spec, err := credentialSpec(args, time.Now())
	if err != nil {
		return finishCredential(env, err)
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	token, err := op.RotateServiceCredential(ctx, id, int(args.overlapValue.Int64()), spec)
	if err != nil {
		return finishCredential(env, err)
	}
	fmt.Fprintln(env.Stdout, token)
	return cli.ExitOK
}

func runCredentialRevoke(ctx context.Context, env cli.Env) int {
	args, env, code, ok := prepareCredentialArgs(env, "revoke")
	if !ok {
		return code
	}
	id, err := admin.ParseServiceCredentialID(args.credentialID)
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
