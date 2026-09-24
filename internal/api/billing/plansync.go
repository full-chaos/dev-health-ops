package billing

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stripe/stripe-go/v85"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// syncPlan is sync_plan_to_stripe: create the Stripe product (unless the
// plan has one) and a recurring price for every local price without a
// Stripe id, then store the ids. A Stripe failure is the unhandled 500, and
// the Stripe objects created before it stay in Stripe, as in Python.
func (h handlers) syncPlan(w http.ResponseWriter, r *http.Request) {
	if !isSuperuser(policy.UserFrom(r.Context())) {
		h.write(w, superadminRequired)
		return
	}
	h.serve(w, r, "sync plan to stripe", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		id, bad := parseID(r.PathValue("plan_id"), "plan_id")
		if bad != nil {
			return *bad, nil
		}
		plan, err := loadPlan(ctx, tx, id)
		if err != nil {
			return reply{}, err
		}
		if plan == nil {
			return detail(http.StatusNotFound, "Plan not found"), nil
		}
		client, err := h.stripe.Client()
		if err != nil {
			return reply{}, err
		}
		if plan.StripeProductID == nil || *plan.StripeProductID == "" {
			description := ""
			if plan.Description != nil {
				description = *plan.Description
			}
			product, err := client.V1Products.Create(ctx, &stripe.ProductCreateParams{
				Name:        stripe.String(plan.Name),
				Description: stripe.String(description),
				Metadata:    map[string]string{"plan_key": plan.Key, "tier": normalizeTier(plan.Tier)},
			})
			if err != nil {
				return reply{}, fmt.Errorf("stripe product create: %s", pyStripeError(err))
			}
			if _, err := tx.Exec(ctx, `UPDATE billing_plans SET stripe_product_id = $2 WHERE id = $1`, id, product.ID); err != nil {
				return reply{}, err
			}
			plan.StripeProductID = &product.ID
		}
		prices, err := loadPrices(ctx, tx, id, true)
		if err != nil {
			return reply{}, err
		}
		for _, price := range prices {
			if price.StripePriceID != nil && *price.StripePriceID != "" {
				continue
			}
			interval := "year"
			if price.Interval == "monthly" {
				interval = "month"
			}
			created, err := client.V1Prices.Create(ctx, &stripe.PriceCreateParams{
				Product:    plan.StripeProductID,
				UnitAmount: stripe.Int64(price.Amount),
				Currency:   stripe.String(price.Currency),
				Recurring:  &stripe.PriceCreateRecurringParams{Interval: stripe.String(interval)},
				Metadata:   map[string]string{"plan_key": plan.Key, "interval": price.Interval},
			})
			if err != nil {
				return reply{}, fmt.Errorf("stripe price create: %s", pyStripeError(err))
			}
			if _, err := tx.Exec(ctx, `UPDATE billing_prices SET stripe_price_id = $2, updated_at = $3 WHERE id = $1`,
				price.ID, created.ID, h.nowUTC()); err != nil {
				return reply{}, err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE billing_plans SET updated_at = $2 WHERE id = $1`, id, h.nowUTC()); err != nil {
			return reply{}, err
		}
		return h.planAnswer(ctx, tx, id)
	})
}

// stripeProduct is StripeProductRecord.
type stripeProduct struct {
	ID, Name    string
	Description *string
	Metadata    []stringPair
}

type stringPair struct{ key, value string }

// stripePrice is StripePriceRecord.
type stripePrice struct {
	ID, Currency, Interval string
	UnitAmount             int64
	Active                 bool
}

func rawObject(response *stripe.APIResponse) (*pyjson.Object, error) {
	if response == nil {
		return nil, errors.New("stripe list item without its JSON")
	}
	value, err := pyjson.Decode(response.RawJSON)
	if err != nil {
		return nil, err
	}
	object, _ := value.(*pyjson.Object)
	if object == nil {
		return nil, errors.New("stripe list item is not an object")
	}
	return object, nil
}

func stringField(object *pyjson.Object, name string) (string, bool) {
	value, _ := object.Get(name)
	text, ok := value.(string)
	return text, ok
}

// productRecord is _product_to_record over the product's own JSON: a
// product without a string id and name is dropped; metadata keeps only its
// string values.
func productRecord(object *pyjson.Object) (stripeProduct, bool) {
	id, idOK := stringField(object, "id")
	name, nameOK := stringField(object, "name")
	if !idOK || !nameOK {
		return stripeProduct{}, false
	}
	out := stripeProduct{ID: id, Name: name}
	if description, ok := stringField(object, "description"); ok {
		out.Description = &description
	}
	if metadata, _ := object.Get("metadata"); metadata != nil {
		if dict, ok := metadata.(*pyjson.Object); ok {
			for _, key := range dict.Keys() {
				if value, isString := stringField(dict, key); isString {
					out.Metadata = append(out.Metadata, stringPair{key, value})
				}
			}
		}
	}
	return out, true
}

// priceRecord is _price_to_record: every field must have its type (an
// integer unit_amount, not a bool; a recurring interval), else dropped.
func priceRecord(object *pyjson.Object) (stripePrice, bool) {
	id, idOK := stringField(object, "id")
	currency, currencyOK := stringField(object, "currency")
	amountValue, _ := object.Get("unit_amount")
	amount, amountOK := amountValue.(pyjson.Int)
	activeValue, _ := object.Get("active")
	active, activeOK := activeValue.(bool)
	var interval string
	intervalOK := false
	if recurringValue, _ := object.Get("recurring"); recurringValue != nil {
		if recurring, ok := recurringValue.(*pyjson.Object); ok {
			interval, intervalOK = stringField(recurring, "interval")
		}
	}
	if !idOK || !currencyOK || !amountOK || !activeOK || !intervalOK || !amount.IsInt64() {
		return stripePrice{}, false
	}
	return stripePrice{ID: id, Currency: currency, Interval: interval, UnitAmount: amount.Int64(), Active: active}, true
}

func metadataValue(pairs []stringPair, key string) string {
	for index := len(pairs) - 1; index >= 0; index-- {
		if pairs[index].key == key {
			return pairs[index].value
		}
	}
	return ""
}

// slugify is _slugify: lower(), then every run of characters outside
// [a-z0-9] becomes "_", then "_" is stripped from both ends.
func slugify(name string) string {
	var out strings.Builder
	inRun := false
	for _, r := range pythonparity.Lower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			out.WriteRune(r)
			inRun = false
			continue
		}
		if !inRun {
			out.WriteByte('_')
			inRun = true
		}
	}
	return strings.Trim(out.String(), "_")
}

func localInterval(stripeInterval string) string {
	switch stripeInterval {
	case "month":
		return "monthly"
	case "year":
		return "yearly"
	}
	return ""
}

// pullReport is SyncReport.
type pullReport struct{ created, updated, skipped, errs []string }

func (p pullReport) json() *pyjson.Object {
	list := func(items []string) []pyjson.Value {
		out := make([]pyjson.Value, len(items))
		for index, item := range items {
			out[index] = item
		}
		return out
	}
	out := pyjson.NewObject()
	out.Set("created", list(p.created))
	out.Set("updated", list(p.updated))
	out.Set("skipped", list(p.skipped))
	out.Set("errors", list(p.errs))
	return out
}

// localPlan is the part of a billing_plans row the pull matches on.
type localPlan struct {
	id              uuid.UUID
	key, name       string
	stripeProductID *string
}

type planIndex struct{ byStripeID, byKey, bySlug map[string]*localPlan }

// loadPlanIndex is _load_existing_plans: later rows overwrite earlier ones
// in each index, in the order the table returns them.
func loadPlanIndex(ctx context.Context, q querier) (planIndex, error) {
	index := planIndex{map[string]*localPlan{}, map[string]*localPlan{}, map[string]*localPlan{}}
	rows, err := q.Query(ctx, `SELECT id, key, name, stripe_product_id FROM billing_plans`)
	if err != nil {
		return index, err
	}
	defer rows.Close()
	for rows.Next() {
		plan := &localPlan{}
		if err := rows.Scan(&plan.id, &plan.key, &plan.name, &plan.stripeProductID); err != nil {
			return index, err
		}
		index.add(plan)
	}
	return index, rows.Err()
}

func (i planIndex) add(plan *localPlan) {
	if plan.stripeProductID != nil && *plan.stripeProductID != "" {
		i.byStripeID[*plan.stripeProductID] = plan
	}
	i.byKey[plan.key] = plan
	i.bySlug[slugify(plan.name)] = plan
}

// match is _match_plan.
func (i planIndex) match(product stripeProduct) *localPlan {
	if plan := i.byStripeID[product.ID]; plan != nil {
		return plan
	}
	if key := metadataValue(product.Metadata, "plan_key"); key != "" {
		if plan := i.byKey[key]; plan != nil {
			return plan
		}
	}
	return i.bySlug[slugify(product.Name)]
}

// pullStripe is pull_plans_from_stripe (plan_sync_service.pull_from_stripe
// with dry_run=False).
//
// The Python session flushes pending writes lazily, at its next statement;
// this port keeps the same timing with a queue (pending) run before the
// next statement, so a constraint failure is attributed to the product
// whose step ran that statement, and one at the final flush is the
// unhandled 500. A constraint failure rolls the WHOLE transaction back
// (earlier products' writes included) and continues on a fresh one, the
// report keeping what it already listed; its error text is the Postgres
// message where Python embeds SQLAlchemy's (a named limit).
func (h handlers) pullStripe(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	client, err := h.stripe.Client()
	if err != nil {
		h.internal(w, r, "pull plans: stripe client", err)
		return
	}
	if h.pool == nil {
		h.internal(w, r, "pull plans", errors.New("billing: no database pool"))
		return
	}
	run := &pullRun{h: h, ctx: ctx}
	if err := run.begin(); err != nil {
		h.internal(w, r, "pull plans: begin", err)
		return
	}
	defer run.rollback()
	report, err := run.pull(client)
	if err != nil {
		h.internal(w, r, "pull plans", err)
		return
	}
	if err := run.flush(); err != nil {
		h.internal(w, r, "pull plans: final flush", err)
		return
	}
	if err := run.tx.Commit(ctx); err != nil {
		h.internal(w, r, "pull plans: commit", err)
		return
	}
	run.tx = nil
	h.write(w, ok(report.json()))
}

type pullRun struct {
	h       handlers
	ctx     context.Context
	tx      pgx.Tx
	pending []func(pgx.Tx) error
	index   planIndex
}

func (p *pullRun) begin() error {
	tx, err := p.h.pool.Begin(p.ctx)
	if err != nil {
		return err
	}
	p.tx = tx
	p.index, err = loadPlanIndex(p.ctx, tx)
	return err
}

func (p *pullRun) rollback() {
	if p.tx != nil {
		_ = p.tx.Rollback(p.ctx)
		p.tx = nil
	}
}

// flush runs the queued writes, as the Python session's autoflush does
// before its next statement.
func (p *pullRun) flush() error {
	queued := p.pending
	p.pending = nil
	for _, write := range queued {
		if err := write(p.tx); err != nil {
			return err
		}
	}
	return nil
}

func isIntegrityError(err error) bool {
	var pgError *pgconn.PgError
	return errors.As(err, &pgError) && strings.HasPrefix(pgError.Code, "23")
}

func (p *pullRun) pull(client *stripe.Client) (pullReport, error) {
	var report pullReport
	products, err := fetchProducts(p.ctx, client)
	if err != nil {
		return report, err
	}
	for _, product := range products {
		stepErr := p.pullProduct(client, product, &report)
		if stepErr == nil {
			continue
		}
		label := fmt.Sprintf("%s (%s): ", product.Name, product.ID)
		if isIntegrityError(stepErr) {
			report.errs = append(report.errs, label+integrityText(stepErr))
			p.h.logger.ErrorContext(p.ctx, "billing pull: product failed", "product", product.ID, "error", stepErr.Error())
			p.rollback()
			p.pending = nil
			if err := p.begin(); err != nil {
				return report, err
			}
			continue
		}
		var stripeFailure stripeStepError
		if errors.As(stepErr, &stripeFailure) {
			report.errs = append(report.errs, label+pyStripeError(stripeFailure.err))
			p.h.logger.ErrorContext(p.ctx, "billing pull: product failed", "product", product.ID, "error", stepErr.Error())
			continue
		}
		return report, stepErr
	}
	return report, nil
}

// stripeStepError marks a Stripe failure inside one product's step, which
// Python reports and moves past.
type stripeStepError struct{ err error }

func (e stripeStepError) Error() string { return e.err.Error() }

func integrityText(err error) string {
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		return pgError.Message
	}
	return err.Error()
}

func (p *pullRun) pullProduct(client *stripe.Client, product stripeProduct, report *pullReport) error {
	prices, err := fetchPrices(p.ctx, client, product.ID)
	if err != nil {
		return stripeStepError{err}
	}
	if len(prices) == 0 {
		report.skipped = append(report.skipped, fmt.Sprintf("%s (%s): no recurring prices", product.Name, product.ID))
		return nil
	}
	now := p.h.nowUTC()
	if existing := p.index.match(product); existing != nil {
		name := product.Name
		if name == "" {
			name = existing.name
		}
		tier := ""
		if value := metadataValue(product.Metadata, "tier"); value != "" {
			tier = normalizeTier(value)
		}
		planID, productID := existing.id, product.ID
		existing.stripeProductID, existing.name = &productID, name
		p.pending = append(p.pending, func(tx pgx.Tx) error {
			if tier != "" {
				_, err := tx.Exec(p.ctx, `UPDATE billing_plans SET stripe_product_id = $2, name = $3, tier = $4, updated_at = $5 WHERE id = $1`,
					planID, productID, name, tier, now)
				return err
			}
			_, err := tx.Exec(p.ctx, `UPDATE billing_plans SET stripe_product_id = $2, name = $3, updated_at = $4 WHERE id = $1`,
				planID, productID, name, now)
			return err
		})
		if err := p.upsertPrices(planID, prices); err != nil {
			return err
		}
		report.updated = append(report.updated, existing.key+" ← "+product.ID)
		return nil
	}
	planKey := metadataValue(product.Metadata, "plan_key")
	if planKey == "" {
		planKey = slugify(product.Name)
	}
	metadata := pyjson.NewObject()
	for _, pair := range product.Metadata {
		metadata.Set(pair.key, pair.value)
	}
	metadataText, err := pyjson.Dumps(metadata)
	if err != nil {
		return err
	}
	// db.add + an explicit flush: the insert runs now, inside this step.
	if err := p.flush(); err != nil {
		return err
	}
	planID := uuid.New()
	if _, err := p.tx.Exec(p.ctx, `INSERT INTO billing_plans
		(id, key, name, description, tier, stripe_product_id, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7::json, $8, $8)`,
		planID, planKey, product.Name, product.Description, normalizeTier(metadataValue(product.Metadata, "tier")),
		product.ID, metadataText, now); err != nil {
		return err
	}
	if err := p.upsertPrices(planID, prices); err != nil {
		return err
	}
	productID := product.ID
	p.index.add(&localPlan{id: planID, key: planKey, name: product.Name, stripeProductID: &productID})
	report.created = append(report.created, planKey+" ← "+product.ID)
	return nil
}

// upsertPrices is _upsert_prices: its SELECT flushes what is queued, then
// the price writes it decides are queued in turn (updates, then inserts,
// as the flush emits them).
func (p *pullRun) upsertPrices(planID uuid.UUID, prices []stripePrice) error {
	if err := p.flush(); err != nil {
		return err
	}
	rows, err := p.tx.Query(p.ctx, `SELECT id, interval, stripe_price_id FROM billing_prices WHERE plan_id = $1`, planID)
	if err != nil {
		return err
	}
	byStripeID := map[string]uuid.UUID{}
	byInterval := map[string]uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		var interval string
		var stripeID *string
		if err := rows.Scan(&id, &interval, &stripeID); err != nil {
			rows.Close()
			return err
		}
		if stripeID != nil && *stripeID != "" {
			byStripeID[*stripeID] = id
		}
		byInterval[interval] = id
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	now := p.h.nowUTC()
	var updates, inserts []func(pgx.Tx) error
	for _, price := range prices {
		interval := localInterval(price.Interval)
		if interval == "" {
			continue
		}
		price := price
		if id, found := byStripeID[price.ID]; found {
			updates = append(updates, func(tx pgx.Tx) error {
				_, err := tx.Exec(p.ctx, `UPDATE billing_prices SET amount = $2, currency = $3, is_active = $4, updated_at = $5 WHERE id = $1`,
					id, price.UnitAmount, price.Currency, price.Active, now)
				return err
			})
			continue
		}
		if id, found := byInterval[interval]; found {
			updates = append(updates, func(tx pgx.Tx) error {
				_, err := tx.Exec(p.ctx, `UPDATE billing_prices SET stripe_price_id = $2, amount = $3, currency = $4, is_active = $5, updated_at = $6 WHERE id = $1`,
					id, price.ID, price.UnitAmount, price.Currency, price.Active, now)
				return err
			})
			continue
		}
		inserts = append(inserts, func(tx pgx.Tx) error {
			_, err := tx.Exec(p.ctx, `INSERT INTO billing_prices
				(id, plan_id, interval, amount, currency, is_active, stripe_price_id, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
				uuid.New(), planID, interval, price.UnitAmount, price.Currency, price.Active, price.ID, now)
			return err
		})
	}
	p.pending = append(p.pending, append(updates, inserts...)...)
	return nil
}

// fetchProducts is _fetch_all_products: every active product, 100 a page.
func fetchProducts(ctx context.Context, client *stripe.Client) ([]stripeProduct, error) {
	params := &stripe.ProductListParams{Active: stripe.Bool(true)}
	params.Limit = stripe.Int64(100)
	var out []stripeProduct
	for product, err := range client.V1Products.List(ctx, params).All(ctx) {
		if err != nil {
			return nil, fmt.Errorf("stripe product list: %s", pyStripeError(err))
		}
		object, err := rawObject(product.LastResponse)
		if err != nil {
			return nil, err
		}
		if record, ok := productRecord(object); ok {
			out = append(out, record)
		}
	}
	return out, nil
}

// fetchPrices is _fetch_prices_for_product: the product's active prices.
func fetchPrices(ctx context.Context, client *stripe.Client, productID string) ([]stripePrice, error) {
	params := &stripe.PriceListParams{Product: stripe.String(productID), Active: stripe.Bool(true)}
	params.Limit = stripe.Int64(100)
	var out []stripePrice
	for price, err := range client.V1Prices.List(ctx, params).All(ctx) {
		if err != nil {
			return nil, err
		}
		object, err := rawObject(price.LastResponse)
		if err != nil {
			return nil, err
		}
		if record, ok := priceRecord(object); ok {
			out = append(out, record)
		}
	}
	return out, nil
}
