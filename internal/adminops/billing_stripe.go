package adminops

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// `admin billing pull-stripe|sync-stripe` port the two Stripe directions of the
// Python plan sync: plans are pulled from Stripe products and prices into the
// database, or the plans without a Stripe product are pushed to Stripe.

// stripeOptions is how the verbs build their Stripe client: only a test replaces
// it (to point the client at a fake Stripe server); the command line and the
// environment have no knob for it.
var stripeOptions = func(key string) stripeclient.Options { return stripeclient.Options{Key: key} }

// pyList is str(list of str): ['a', 'b'].
func pyList(items []string) string {
	reprs := make([]string, len(items))
	for index, item := range items {
		reprs[index] = pythonparity.StrRepr(item)
	}
	return "[" + strings.Join(reprs, ", ") + "]"
}

func printPlanSyncReport(env cli.Env, report billing.PlanSyncReport) {
	fmt.Fprintf(env.Stdout, "Created:  %s\n", pyList(report.Created))
	fmt.Fprintf(env.Stdout, "Updated:  %s\n", pyList(report.Updated))
	fmt.Fprintf(env.Stdout, "Skipped:  %s\n", pyList(report.Skipped))
	if len(report.Errors) > 0 {
		fmt.Fprintf(env.Stdout, "Errors:   %s\n", pyList(report.Errors))
	}
}

// planSync builds the sync of the verb: the database, and Stripe from
// STRIPE_SECRET_KEY (unset is left for the sync to refuse, with Python's text).
func planSync(env cli.Env, pool *pgxpool.Pool) (billing.PlanSync, secrets.Value, bool) {
	key, _, err := secrets.Resolve("STRIPE_SECRET_KEY", env.Lookup)
	if err != nil {
		fmt.Fprintf(env.Stderr, "configuration error: %v\n", err)
		return billing.PlanSync{}, key, false
	}
	return billing.PlanSync{Pool: pool, Stripe: stripeclient.New(stripeOptions(key.Reveal())),
		Logger: slog.New(slog.DiscardHandler)}, key, true
}

// finishSync prints a failure of the sync as Python does ("Error: <text>" on
// stdout, exit 1), with the Stripe key and the database credentials removed.
func finishSync(env cli.Env, key secrets.Value, err error) int {
	text := billing.ErrorText(err)
	if key.Configured() {
		text = secrets.RedactValues(text, key.Reveal())
	}
	fmt.Fprintf(env.Stdout, "Error: %s\n", redactor(env)(fmt.Errorf("%s", text)).Error())
	return cli.ExitFailure
}

func runBillingPullStripe(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin billing pull-stripe")
	dryRun := flags.Bool("dry-run", false, "preview changes without writing to the database")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	sync, key, ok := planSync(env, op.Pool)
	if !ok {
		return cli.ExitFailure
	}
	report, err := sync.Pull(ctx, *dryRun)
	if err != nil {
		return finishSync(env, key, err)
	}
	if *dryRun {
		fmt.Fprintln(env.Stdout, "[dry-run] No changes written.")
	}
	printPlanSyncReport(env, report)
	return cli.ExitOK
}

func runBillingSyncStripe(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin billing sync-stripe")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	sync, key, ok := planSync(env, op.Pool)
	if !ok {
		return cli.ExitFailure
	}
	report, err := sync.SyncAll(ctx)
	if err != nil {
		return finishSync(env, key, err)
	}
	printPlanSyncReport(env, report)
	return cli.ExitOK
}
