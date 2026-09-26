// Package billingcli is the `billing` group of dho: Python's `dev-hops billing`
// (api/billing/cli.py), whose one verb reconciles the billing rows with Stripe
// (CHAOS-6893). The verb's body lives in package adminops beside the database and
// flag plumbing the operator verbs share; it is not under `admin` because the
// Python group was not.
package billingcli

import (
	"github.com/full-chaos/dev-health-ops/internal/adminops"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// Command is the `billing` group.
func Command() cli.Command {
	return cli.Command{
		Name: "billing", Summary: "billing operations", Kind: cli.Group,
		Children: []cli.Command{
			{Name: "reconcile", Summary: "reconcile the subscriptions, invoices and refunds with Stripe", Kind: cli.Verb, Run: adminops.RunBillingReconcile},
		},
	}
}
