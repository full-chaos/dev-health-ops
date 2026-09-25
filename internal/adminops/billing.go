package adminops

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// `admin billing seed|list` port the Python verbs over the billing plans: the
// three standard plans with their prices, and a listing. (The two Stripe verbs,
// pull-stripe and sync-stripe, are a separate change.)

func billingGroup() cli.Command {
	return cli.Command{
		Name: "billing", Summary: "billing plan management", Kind: cli.Group,
		Children: []cli.Command{
			{Name: "seed", Summary: "seed the standard billing plans (Community, Team, Enterprise) with prices", Kind: cli.Verb, Run: runBillingSeed},
			{Name: "list", Summary: "list billing plans with prices and Stripe sync status", Kind: cli.Verb, Run: runBillingList},
		},
	}
}

type standardPrice struct {
	interval string
	amount   int
}

type standardPlan struct {
	key, name, description, tier string
	displayOrder                 int
	prices                       []standardPrice
}

// standardPlans is STANDARD_PLANS of the Python verb, in its order; every price
// is in usd.
var standardPlans = []standardPlan{
	{"community", "Community", "For individuals and small teams getting started with engineering analytics.", "community", 0,
		[]standardPrice{{"monthly", 0}, {"yearly", 0}}},
	{"team", "Team", "For growing teams that need full visibility into delivery health and investment patterns.", "team", 1,
		[]standardPrice{{"monthly", 1200}, {"yearly", 11500}}},
	{"enterprise", "Enterprise", "For organizations that need enterprise-grade security, compliance, and dedicated support.", "enterprise", 2,
		[]standardPrice{{"monthly", 12900}, {"yearly", 124000}}},
}

func runBillingSeed(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin billing seed")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	tx, err := op.Pool.Begin(ctx)
	if err != nil {
		return databaseFailure(env, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	rows, err := tx.Query(ctx, `SELECT key FROM billing_plans`)
	if err != nil {
		return databaseFailure(env, err)
	}
	keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return databaseFailure(env, err)
	}
	existing := make(map[string]bool, len(keys))
	for _, key := range keys {
		existing[key] = true
	}
	created := 0
	for _, plan := range standardPlans {
		if existing[plan.key] {
			continue
		}
		planID := uuid.New()
		if _, err := tx.Exec(ctx, `INSERT INTO billing_plans (id, key, name, description, tier, display_order) VALUES ($1, $2, $3, $4, $5, $6)`,
			planID, plan.key, plan.name, plan.description, plan.tier, plan.displayOrder); err != nil {
			return databaseFailure(env, err)
		}
		for _, price := range plan.prices {
			if _, err := tx.Exec(ctx, `INSERT INTO billing_prices (id, plan_id, interval, amount, currency) VALUES ($1, $2, $3, $4, $5)`,
				uuid.New(), planID, price.interval, price.amount, "usd"); err != nil {
				return databaseFailure(env, err)
			}
		}
		created++
	}
	if created == 0 {
		fmt.Fprintln(env.Stdout, "All standard billing plans already exist.")
		return cli.ExitOK
	}
	if err := tx.Commit(ctx); err != nil {
		return databaseFailure(env, err)
	}
	fmt.Fprintf(env.Stdout, "Seeded %d billing plan(s) with prices.\n", created)
	return cli.ExitOK
}

func runBillingList(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin billing list")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	rows, err := op.Pool.Query(ctx, `SELECT id, key, name, tier, is_active, stripe_product_id FROM billing_plans ORDER BY display_order`)
	if err != nil {
		return databaseFailure(env, err)
	}
	type plan struct {
		id              uuid.UUID
		key, name, tier string
		active          bool
		stripeProductID *string
	}
	var plans []plan
	for rows.Next() {
		var p plan
		if err := rows.Scan(&p.id, &p.key, &p.name, &p.tier, &p.active, &p.stripeProductID); err != nil {
			rows.Close()
			return databaseFailure(env, err)
		}
		plans = append(plans, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return databaseFailure(env, err)
	}
	if len(plans) == 0 {
		fmt.Fprintln(env.Stdout, "No billing plans found.")
		return cli.ExitOK
	}
	fmt.Fprintf(env.Stdout, "%s %s %s %s %s %s\n", pad("Key", 15), pad("Name", 15), pad("Tier", 12), pad("Active", 8), pad("Stripe Product ID", 30), "Prices")
	fmt.Fprintln(env.Stdout, strings.Repeat("-", 110))
	for _, p := range plans {
		priceRows, err := op.Pool.Query(ctx, `SELECT interval, amount FROM billing_prices WHERE plan_id = $1`, p.id)
		if err != nil {
			return databaseFailure(env, err)
		}
		var summaries []string
		for priceRows.Next() {
			var interval string
			var amount int64
			if err := priceRows.Scan(&interval, &amount); err != nil {
				priceRows.Close()
				return databaseFailure(env, err)
			}
			summaries = append(summaries, fmt.Sprintf("%s $%.2f", interval, float64(amount)/100))
		}
		priceRows.Close()
		if err := priceRows.Err(); err != nil {
			return databaseFailure(env, err)
		}
		prices := "none"
		if len(summaries) > 0 {
			prices = strings.Join(summaries, ", ")
		}
		stripeID := "-"
		if p.stripeProductID != nil && *p.stripeProductID != "" {
			stripeID = *p.stripeProductID
		}
		fmt.Fprintf(env.Stdout, "%s %s %s %s %s %s\n", pad(p.key, 15), pad(p.name, 15), pad(p.tier, 12), pad(yesNo(p.active), 8), pad(stripeID, 30), prices)
	}
	return cli.ExitOK
}
