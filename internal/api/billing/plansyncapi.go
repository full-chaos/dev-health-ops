package billing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stripe/stripe-go/v86"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
)

// PlanSyncReport is plan_sync_service.SyncReport: what a pull or a sync did, one
// text per plan, each list in the order the run met its plans.
type PlanSyncReport struct {
	Created, Updated, Skipped, Errors []string
}

// PlanSync runs plan_sync_service's two directions outside HTTP, for the operator
// verbs (`admin billing pull-stripe|sync-stripe`); the API's pull route runs the
// same pull code.
type PlanSync struct {
	Pool   *pgxpool.Pool
	Stripe *stripeclient.Provider
	Logger *slog.Logger
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

func (s PlanSync) handlers() handlers {
	h := handlers{pool: s.Pool, stripe: s.Stripe, logger: s.Logger, now: s.Now}
	if h.logger == nil {
		h.logger = slog.New(slog.DiscardHandler)
	}
	if h.now == nil {
		h.now = time.Now
	}
	return h
}

// ErrorText is the text Python prints for a failure of a sync: the Stripe error's
// own text for a failed product list, the error's text otherwise.
func ErrorText(err error) string {
	var listing interface{ PythonText() string }
	if errors.As(err, &listing) {
		return listing.PythonText()
	}
	return err.Error()
}

// Pull is pull_from_stripe: every active Stripe product with an active price is
// matched to a local plan (by Stripe product id, then the plan_key metadata, then
// the slug of its name) and updated, or created; its prices are upserted. A dry
// run reports what it would do and writes nothing. The writes are committed when
// the run ends without a dry run.
func (s PlanSync) Pull(ctx context.Context, dryRun bool) (PlanSyncReport, error) {
	client, err := s.Stripe.Client()
	if err != nil {
		return PlanSyncReport{}, err
	}
	run := &pullRun{h: s.handlers(), ctx: ctx, dryRun: dryRun}
	if err := run.begin(); err != nil {
		return PlanSyncReport{}, err
	}
	defer run.rollback()
	report, err := run.pull(client)
	if err != nil {
		return PlanSyncReport{}, err
	}
	if !dryRun {
		if err := run.flush(); err != nil {
			return PlanSyncReport{}, err
		}
		if err := run.tx.Commit(ctx); err != nil {
			return PlanSyncReport{}, err
		}
		run.tx = nil
	}
	return PlanSyncReport{Created: report.created, Updated: report.updated, Skipped: report.skipped, Errors: report.errs}, nil
}

// SyncAll is sync_all_to_stripe: every active plan without a Stripe product gets
// one, and a recurring Stripe price for each of its prices without one; the ids
// are stored. A Stripe failure for one plan is reported and the next plan goes on;
// what the failed plan had stored before the failure (its product id, the prices
// already created) stays stored.
func (s PlanSync) SyncAll(ctx context.Context) (PlanSyncReport, error) {
	var report PlanSyncReport
	client, err := s.Stripe.Client()
	if err != nil {
		return report, err
	}
	h := s.handlers()
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return report, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	rows, err := tx.Query(ctx, `SELECT id, key, name, description, tier FROM billing_plans WHERE is_active IS true AND stripe_product_id IS NULL`)
	if err != nil {
		return report, err
	}
	type plan struct {
		id              uuid.UUID
		key, name, tier string
		description     *string
	}
	var plans []plan
	for rows.Next() {
		var p plan
		if err := rows.Scan(&p.id, &p.key, &p.name, &p.description, &p.tier); err != nil {
			rows.Close()
			return report, err
		}
		plans = append(plans, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return report, err
	}
	for _, p := range plans {
		productID, err := h.syncOnePlan(ctx, tx, client, p.id, p.key, p.name, p.description, p.tier)
		if err != nil {
			var stepFailure stripeStepError
			if !errors.As(err, &stepFailure) {
				return report, err
			}
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %s", p.key, pyStripeError(stepFailure.err)))
			h.logger.ErrorContext(ctx, "billing sync: plan failed", "plan", p.key, "error", err.Error())
			continue
		}
		report.Created = append(report.Created, fmt.Sprintf("%s → %s", p.key, productID))
	}
	if err := tx.Commit(ctx); err != nil {
		return report, err
	}
	return report, nil
}

// syncOnePlan creates the plan's Stripe product and prices; a Stripe failure is a
// stripeStepError, a database failure any other error.
func (h handlers) syncOnePlan(ctx context.Context, tx pgx.Tx, client *stripe.Client, id uuid.UUID, key, name string, description *string, tier string) (string, error) {
	text := ""
	if description != nil {
		text = *description
	}
	product, err := client.V1Products.Create(ctx, &stripe.ProductCreateParams{
		Name:        stripe.String(name),
		Description: stripe.String(text),
		Metadata:    map[string]string{"plan_key": key, "tier": normalizeTier(tier)},
	})
	if err != nil {
		return "", stripeStepError{err}
	}
	if _, err := tx.Exec(ctx, `UPDATE billing_plans SET stripe_product_id = $2 WHERE id = $1`, id, product.ID); err != nil {
		return "", err
	}
	priceRows, err := tx.Query(ctx, `SELECT id, interval, amount, currency, stripe_price_id FROM billing_prices WHERE plan_id = $1`, id)
	if err != nil {
		return "", err
	}
	type price struct {
		id                 uuid.UUID
		interval, currency string
		amount             int64
		stripePriceID      *string
	}
	var prices []price
	for priceRows.Next() {
		var p price
		if err := priceRows.Scan(&p.id, &p.interval, &p.amount, &p.currency, &p.stripePriceID); err != nil {
			priceRows.Close()
			return "", err
		}
		prices = append(prices, p)
	}
	priceRows.Close()
	if err := priceRows.Err(); err != nil {
		return "", err
	}
	now := h.nowUTC()
	for _, p := range prices {
		if p.stripePriceID != nil && *p.stripePriceID != "" {
			continue
		}
		interval := "year"
		if p.interval == "monthly" {
			interval = "month"
		}
		created, err := client.V1Prices.Create(ctx, &stripe.PriceCreateParams{
			Product:    stripe.String(product.ID),
			UnitAmount: stripe.Int64(p.amount),
			Currency:   stripe.String(p.currency),
			Recurring:  &stripe.PriceCreateRecurringParams{Interval: stripe.String(interval)},
			Metadata:   map[string]string{"plan_key": key, "interval": p.interval},
		})
		if err != nil {
			return "", stripeStepError{err}
		}
		if _, err := tx.Exec(ctx, `UPDATE billing_prices SET stripe_price_id = $2, updated_at = $3 WHERE id = $1`, p.id, created.ID, now); err != nil {
			return "", err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE billing_plans SET updated_at = $2 WHERE id = $1`, id, now); err != nil {
		return "", err
	}
	return product.ID, nil
}
