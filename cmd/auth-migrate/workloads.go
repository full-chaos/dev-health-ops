package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/keyrotation"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const workloadsUsage = "usage: auth-migrate workloads seed [-apply] | bind -workload N -environment E -trust-domain T -issuer I -namespace NS -service-account SA [-uid U] [-apply]"

type workloadsArgs struct {
	verb    string
	apply   bool
	name    string
	binding workload.BindingSpec
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
	if err := flags.Parse(args[1:]); err != nil {
		return workloadsArgs{}, err
	}
	if flags.NArg() != 0 {
		return workloadsArgs{}, errors.New("positional arguments are not accepted")
	}
	switch parsed.verb {
	case "seed":
		if parsed.name != "" || parsed.binding != (workload.BindingSpec{}) {
			return workloadsArgs{}, errors.New("seed takes no binding flags")
		}
	case "bind":
		if workload.ValidateName(parsed.name) != nil {
			return workloadsArgs{}, errors.New("bind needs a valid -workload")
		}
		if workload.ValidateBinding(parsed.binding) != nil {
			return workloadsArgs{}, errors.New("bind needs a valid -environment, -trust-domain, -issuer, -namespace and -service-account (and -uid, if given)")
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
	case errors.Is(err, workload.ErrStore):
		return "store unavailable"
	}
	return "refused"
}
