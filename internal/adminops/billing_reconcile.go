package adminops

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// RunBillingReconcile is api/billing/cli.py run_reconcile (the verb of package
// billingcli, which owns the `billing` group; the database and flag plumbing it
// needs lives here): reconcile_all for the
// organisation (every organisation without --org-id), then, with --since,
// reconcile_invoices from that date, and the report as one line of json.dumps.
//
// Named divergences (CHAOS-6893): a bad --org-id or --since is a Python traceback
// (ValueError, exit 1) and here an "argument error" line on stderr, exit 1 too; and
// the Stripe side is listed as intended (CHAOS-6479): Python's list(limit=100) is
// refused by stripe-python, so its Stripe side is always empty and every local row
// reads as missing in Stripe.
func RunBillingReconcile(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho billing reconcile")
	orgFlag := flags.String("org-id", "", "reconcile a single organization (UUID); omit to reconcile all organizations")
	sinceFlag := flags.String("since", "", "only reconcile invoices on or after this date (ISO YYYY-MM-DD)")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	var org *uuid.UUID
	if *orgFlag != "" {
		parsed, err := pythonparity.ParseUUID(*orgFlag)
		if err != nil {
			fmt.Fprintf(env.Stderr, "argument error: --org-id %q is not a valid UUID\n", *orgFlag)
			return cli.ExitFailure
		}
		org = &parsed
	}
	var since *time.Time
	if *sinceFlag != "" {
		parsed, ok := pytime.FromISOFormat(*sinceFlag)
		if !ok {
			fmt.Fprintf(env.Stderr, "argument error: --since %q is not an ISO date\n", *sinceFlag)
			return cli.ExitFailure
		}
		// A date or datetime without an offset is read as UTC (Python hands the naive
		// value to the database driver, which reads it in the process's zone).
		since = &parsed.Time
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	key, _, err := secrets.Resolve("STRIPE_SECRET_KEY", env.Lookup)
	if err != nil {
		fmt.Fprintf(env.Stderr, "configuration error: %v\n", err)
		return cli.ExitFailure
	}
	run := billing.Reconcile{Pool: op.Pool, Stripe: stripeclient.New(stripeOptions(key.Reveal())), Logger: slog.New(slog.DiscardHandler)}
	report, err := run.Run(ctx, org, since)
	if err != nil {
		text := billing.ErrorText(err)
		if key.Configured() {
			text = secrets.RedactValues(text, key.Reveal())
		}
		fmt.Fprintf(env.Stderr, "reconcile failed: %s\n", redactor(env)(fmt.Errorf("%s", text)).Error())
		return cli.ExitFailure
	}
	line, err := pyjson.Dumps(report)
	if err != nil {
		fmt.Fprintf(env.Stderr, "reconcile failed: %v\n", err)
		return cli.ExitFailure
	}
	fmt.Fprintln(env.Stdout, line)
	return cli.ExitOK
}
