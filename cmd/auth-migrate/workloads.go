package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/keyrotation"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const workloadsUsage = "usage: auth-migrate workloads seed [-apply] | bind -workload N -environment E -trust-domain T -issuer I -namespace NS -service-account SA [-uid U] [-apply]" +
	" | bootstrap-create -workload N -out PATH [-audience A]... [-expires-in DUR] [-apply]" +
	" | bootstrap-rotate -workload N -out PATH -overlap DUR [-expires-in DUR] [-apply]" +
	" | bootstrap-revoke -credential ID -reason compromised|owner_request [-apply]" +
	" | bootstrap-disable -workload N [-apply]" +
	"\nA bootstrap value is written once, to a new 0600 -out file, and never to argv, stdout or a log." +
	"\nPut -out in a directory writable only by the operator: a failed verb's file is removed by path." +
	"\nbootstrap-disable revokes every live bootstrap value of N at once; workload tokens issued before it live at most 300 s."

type workloadsArgs struct {
	verb    string
	apply   bool
	name    string
	binding workload.BindingSpec

	out          string
	audiences    audienceList
	expiresIn    time.Duration
	overlap      time.Duration
	credentialID uuid.UUID
	reason       string

	operator        uuid.UUID
	enablePrincipal bool
}

// audienceList is a repeatable -audience flag.
type audienceList []string

func (l *audienceList) String() string { return strings.Join(*l, ",") }

func (l *audienceList) Set(v string) error {
	*l = append(*l, v)
	return nil
}

// parseWorkloadsArgs refuses a bad command line before any environment or
// database is read.
func parseWorkloadsArgs(args []string, stderr io.Writer) (workloadsArgs, error) {
	if len(args) == 0 {
		return workloadsArgs{}, errors.New("a verb is required")
	}
	parsed := workloadsArgs{verb: args[0]}
	flags := flag.NewFlagSet(serviceName+" workloads", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.BoolVar(&parsed.apply, "apply", false, "write the change (without it the verb is a dry run)")
	flags.StringVar(&parsed.name, "workload", "", "bind: the registered workload name")
	flags.StringVar(&parsed.binding.Environment, "environment", "", "bind: the deployment environment")
	flags.StringVar(&parsed.binding.TrustDomain, "trust-domain", "", "bind: the cluster trust domain")
	flags.StringVar(&parsed.binding.Issuer, "issuer", "", "bind: the service account token issuer")
	flags.StringVar(&parsed.binding.Namespace, "namespace", "", "bind: the Kubernetes namespace")
	flags.StringVar(&parsed.binding.ServiceAccountName, "service-account", "", "bind: the Kubernetes service account name")
	flags.StringVar(&parsed.binding.ServiceAccountUID, "uid", "", "bind: the service account uid, when it is to be pinned")
	flags.StringVar(&parsed.out, "out", "", "bootstrap-create, bootstrap-rotate: the NEW file (mode 0600) the value is written to once, in a directory writable only by the operator")
	flags.Var(&parsed.audiences, "audience", "bootstrap-create: an audience of the value (repeatable; default: the workload's audiences)")
	flags.DurationVar(&parsed.expiresIn, "expires-in", workload.BootstrapExpiryDefault, "bootstrap-create, bootstrap-rotate: the value's lifetime (at most 2160h)")
	flags.DurationVar(&parsed.overlap, "overlap", 0, "bootstrap-rotate: how long the old value stays live (0 to 1h)")
	credential := flags.String("credential", "", "bootstrap-revoke: the credential id")
	flags.StringVar(&parsed.reason, "reason", "", "bootstrap-revoke: compromised or owner_request")
	if err := flags.Parse(args[1:]); err != nil {
		return workloadsArgs{}, err
	}
	if flags.NArg() != 0 {
		return workloadsArgs{}, errors.New("positional arguments are not accepted")
	}
	set := map[string]bool{}
	flags.Visit(func(f *flag.Flag) { set[f.Name] = true })
	only := func(verb string, allowed ...string) error {
		for name := range set {
			if name != "apply" && !slices.Contains(allowed, name) {
				return fmt.Errorf("%s does not take -%s", verb, name)
			}
		}
		return nil
	}
	switch parsed.verb {
	case "seed":
		if parsed.name != "" || parsed.binding != (workload.BindingSpec{}) {
			return workloadsArgs{}, errors.New("seed takes no binding flags")
		}
		if err := only("seed"); err != nil {
			return workloadsArgs{}, err
		}
	case "bind":
		if workload.ValidateName(parsed.name) != nil {
			return workloadsArgs{}, errors.New("bind needs a valid -workload")
		}
		if workload.ValidateBinding(parsed.binding) != nil {
			return workloadsArgs{}, errors.New("bind needs a valid -environment, -trust-domain, -issuer, -namespace and -service-account (and -uid, if given)")
		}
		if err := only("bind", "workload", "environment", "trust-domain", "issuer", "namespace", "service-account", "uid"); err != nil {
			return workloadsArgs{}, err
		}
	case "bootstrap-create", "bootstrap-rotate":
		allowed := []string{"workload", "out", "expires-in", "audience"}
		if parsed.verb == "bootstrap-rotate" {
			allowed = []string{"workload", "out", "expires-in", "overlap"}
		}
		if err := only(parsed.verb, allowed...); err != nil {
			return workloadsArgs{}, err
		}
		if workload.ValidateName(parsed.name) != nil {
			return workloadsArgs{}, fmt.Errorf("%s needs a valid -workload", parsed.verb)
		}
		if parsed.out == "" || parsed.out == "-" {
			return workloadsArgs{}, fmt.Errorf("%s needs -out, a file path (never stdout)", parsed.verb)
		}
		if parsed.expiresIn <= 0 || parsed.expiresIn > workload.BootstrapExpiryCeiling {
			return workloadsArgs{}, errors.New("-expires-in must be above 0 and at most 2160h (90 d)")
		}
		if workload.ValidateAudiences(parsed.audiences) != nil {
			return workloadsArgs{}, errors.New("an -audience is malformed or repeated")
		}
		if parsed.verb == "bootstrap-rotate" {
			if !set["overlap"] {
				return workloadsArgs{}, errors.New("bootstrap-rotate needs -overlap (0 cuts over at once)")
			}
			if parsed.overlap < 0 || parsed.overlap > workload.BootstrapOverlapCeiling {
				return workloadsArgs{}, errors.New("-overlap must be from 0 to 1h")
			}
		}
	case "bootstrap-revoke":
		if err := only(parsed.verb, "credential", "reason"); err != nil {
			return workloadsArgs{}, err
		}
		id, err := uuid.Parse(*credential)
		if err != nil || id == uuid.Nil || id.String() != *credential {
			return workloadsArgs{}, errors.New("bootstrap-revoke needs -credential, a credential id")
		}
		parsed.credentialID = id
		if parsed.reason != workload.BootstrapReasonCompromised && parsed.reason != workload.BootstrapReasonOwnerRequest {
			return workloadsArgs{}, errors.New("bootstrap-revoke needs -reason compromised or owner_request")
		}
	case "bootstrap-disable":
		if err := only(parsed.verb, "workload"); err != nil {
			return workloadsArgs{}, err
		}
		if workload.ValidateName(parsed.name) != nil {
			return workloadsArgs{}, errors.New("bootstrap-disable needs a valid -workload")
		}
	default:
		return workloadsArgs{}, errors.New("unknown verb")
	}
	return parsed, nil
}

// executeWorkloads runs `auth-migrate workloads`: the registry seed and the
// deployment bindings. It connects as the migration role and refuses to run as
// the runtime role. Nothing it prints is a credential.
func executeWorkloads(parent context.Context, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer) int {
	parsed, err := parseWorkloadsArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "argument error: %v\n%s\n", err, workloadsUsage)
		return 2
	}
	specs, err := workload.DefaultRegistry()
	if err != nil {
		fmt.Fprintln(stderr, "registry contract error: the embedded workload registry is invalid")
		return 1
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
		fmt.Fprintln(stderr, "workloads error: PostgreSQL migration endpoint unavailable")
		return 1
	}
	defer pool.Close()
	if err := keyrotation.CheckNotRuntimeRole(ctx, pool, runtimeRole); err != nil {
		if errors.Is(err, keyrotation.ErrRuntimeRole) {
			fmt.Fprintf(stderr, "configuration error: %s connects as the runtime role %q; the registry is written by the migration role only\n",
				envMigrationURI, runtimeRole)
		} else {
			fmt.Fprintln(stderr, "workloads error: could not read the connected role")
		}
		return 1
	}
	store, err := workload.New(workload.Config{Pool: pool, Schema: schema})
	if err != nil {
		fmt.Fprintln(stderr, "workloads error: could not open the registry")
		return 1
	}

	switch parsed.verb {
	case "seed":
		var report workload.Report
		if parsed.apply {
			report, err = store.Seed(ctx, specs, workload.Actor{})
		} else {
			report, err = store.PlanSeed(ctx, specs)
		}
		if err != nil {
			fmt.Fprintf(stderr, "workloads error: seed failed (%s)\n", failureClass(err))
			return 1
		}
		fmt.Fprintf(stdout, "workloads seed apply=%t created=%d updated=%d unchanged=%d unlisted=%d\n",
			parsed.apply, report.Created, report.Updated, report.Unchanged, len(report.Unlisted))
		for _, name := range report.Unlisted {
			fmt.Fprintf(stdout, "unlisted workload %s (left as is)\n", name)
		}
	case "bind":
		if !parsed.apply {
			fmt.Fprintf(stdout, "workloads bind dry run: workload=%s environment=%s (no change written)\n", parsed.name, parsed.binding.Environment)
			return 0
		}
		id, err := store.AddBinding(ctx, parsed.name, parsed.binding, workload.Actor{})
		if err != nil {
			fmt.Fprintf(stderr, "workloads error: bind failed (%s)\n", failureClass(err))
			return 1
		}
		fmt.Fprintf(stdout, "workloads bind applied: workload=%s environment=%s binding=%s\n", parsed.name, parsed.binding.Environment, id)
	default:
		return runBootstrap(ctx, store, parsed, stdout, stderr)
	}
	return 0
}

func failureClass(err error) string {
	switch {
	case errors.Is(err, workload.ErrNotFound):
		return "workload not registered"
	case errors.Is(err, workload.ErrBindingTaken):
		return "federation tuple already bound"
	case errors.Is(err, workload.ErrBindingInvalid):
		return "binding field refused"
	case errors.Is(err, workload.ErrBootstrapLive):
		return "the workload has a live bootstrap value; rotate it"
	case errors.Is(err, workload.ErrBootstrapRotating):
		return "a rotation is still in its overlap"
	case errors.Is(err, workload.ErrBootstrapNone):
		return "the workload has no live bootstrap value"
	case errors.Is(err, workload.ErrBootstrapNotFound):
		return "bootstrap credential not found"
	case errors.Is(err, workload.ErrBootstrapWorkloadDisabled):
		return "the workload row is disabled; no auth-migrate verb enables it today: re-enabling needs a reviewed change"
	case errors.Is(err, workload.ErrBootstrapPrincipalDisabled):
		return "the workload's principal is disabled, its row is not; no auth-migrate verb enables it today: re-enabling needs a reviewed change"
	case errors.Is(err, workload.ErrAudienceInvalid):
		return "audience refused"
	case errors.Is(err, workload.ErrStore):
		return "store unavailable"
	}
	return "refused"
}
