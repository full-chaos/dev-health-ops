// Package billing is plan area E's plans, subscriptions, checkout and portal
// routes (api/billing/plans.py, subscriptions.py, and router.py's checkout
// and portal), ported 1:1 from the Python api, with one named deviation: a
// subscription route given another org's org_id requires the caller to be
// a member of that org or a superuser (the Python helper does not check).
package billing

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

const prefix = "/api/v1/billing"

// Deps are the area's dependencies.
type Deps struct {
	Pool   *pgxpool.Pool
	Guard  *policy.Guard
	Stripe *stripeclient.Provider
	Config config.BillingConfig
	Logger *slog.Logger
	// Now is the clock (nil = time.Now).
	Now func() time.Time
}

type handlers struct {
	pool   *pgxpool.Pool
	stripe *stripeclient.Provider
	config config.BillingConfig
	logger *slog.Logger
	now    func() time.Time
}

// Routes returns the area's routes, in the Python router's registration
// order. Allow names the first route Starlette would find for a path, which
// is the method its 405 reports.
func Routes(deps Deps) []httpapi.Route {
	h := handlers{pool: deps.Pool, stripe: deps.Stripe, config: deps.Config, logger: deps.Logger, now: deps.Now}
	if h.logger == nil {
		h.logger = slog.Default()
	}
	if h.now == nil {
		h.now = time.Now
	}
	g := deps.Guard
	body := func(level policy.Authz, handler http.HandlerFunc) http.Handler { return g.BodyFirst(level, handler) }
	wrap := func(level policy.Authz, handler http.HandlerFunc) http.Handler { return g.Wrap(level, handler) }
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: prefix + "/plans", Allow: http.MethodGet, Handler: wrap(policy.Optional, h.listPlans)},
		{Method: http.MethodPost, Pattern: prefix + "/plans", Handler: body(policy.Authenticated, h.createPlan)},
		// The literal path is its own pattern, so a wrong method on it
		// answers its own 405 (Allow: POST, as Starlette's first partial
		// match), while GET/PUT/DELETE on it reach the {plan_id} routes.
		{Method: http.MethodPost, Pattern: prefix + "/plans/pull-stripe", Allow: http.MethodPost, Handler: g.Wrap(policy.Superuser, http.HandlerFunc(h.pullStripe))},
		// HEAD would otherwise reach GET /plans/{plan_id}, whose 405 names
		// GET; Starlette's first partial match for this path is the POST.
		{Method: http.MethodHead, Pattern: prefix + "/plans/pull-stripe", Handler: http.HandlerFunc(pullStripeNotAllowed)},
		{Method: http.MethodGet, Pattern: prefix + "/plans/{plan_id}", Allow: http.MethodGet, Handler: wrap(policy.Optional, h.getPlan)},
		{Method: http.MethodPost, Pattern: prefix + "/plans/{plan_id}", Handler: http.HandlerFunc(planPostNotAllowed)},
		{Method: http.MethodPut, Pattern: prefix + "/plans/{plan_id}", Handler: body(policy.Authenticated, h.updatePlan)},
		{Method: http.MethodDelete, Pattern: prefix + "/plans/{plan_id}", Handler: wrap(policy.Authenticated, h.deletePlan)},
		{Method: http.MethodPost, Pattern: prefix + "/plans/{plan_id}/sync-stripe", Allow: http.MethodPost, Handler: wrap(policy.Authenticated, h.syncPlan)},
		{Method: http.MethodGet, Pattern: prefix + "/invoices", Allow: http.MethodGet, Handler: wrap(policy.Authenticated, h.listInvoices)},
		{Method: http.MethodGet, Pattern: prefix + "/invoices/{invoice_id}", Allow: http.MethodGet, Handler: wrap(policy.Authenticated, h.getInvoice)},
		{Method: http.MethodPost, Pattern: prefix + "/invoices/{invoice_id}/void", Allow: http.MethodPost, Handler: wrap(policy.Admin, h.voidInvoice)},
		{Method: http.MethodPost, Pattern: prefix + "/refunds", Allow: http.MethodPost, Handler: body(policy.Authenticated, h.createRefund)},
		{Method: http.MethodGet, Pattern: prefix + "/refunds", Handler: wrap(policy.Authenticated, h.listRefunds)},
		{Method: http.MethodGet, Pattern: prefix + "/refunds/{refund_id}", Allow: http.MethodGet, Handler: wrap(policy.Authenticated, h.getRefund)},
		{Method: http.MethodPost, Pattern: prefix + "/checkout", Allow: http.MethodPost, Handler: body(policy.Authenticated, h.checkout)},
		{Method: http.MethodPost, Pattern: prefix + "/portal", Allow: http.MethodPost, Handler: wrap(policy.Authenticated, h.portal)},
		{Method: http.MethodGet, Pattern: prefix + "/entitlements/{org_id}", Allow: http.MethodGet, Handler: wrap(policy.Authenticated, h.entitlements)},
		{Method: http.MethodGet, Pattern: prefix + "/subscriptions/list", Allow: http.MethodGet, Handler: wrap(policy.Authenticated, h.listSubscriptions)},
		{Method: http.MethodGet, Pattern: prefix + "/subscriptions", Allow: http.MethodGet, Handler: wrap(policy.Authenticated, h.getSubscription)},
		{Method: http.MethodGet, Pattern: prefix + "/subscriptions/history", Allow: http.MethodGet, Handler: wrap(policy.Authenticated, h.subscriptionHistory)},
		{Method: http.MethodPost, Pattern: prefix + "/subscriptions/change-plan", Allow: http.MethodPost, Handler: body(policy.Admin, h.changePlan)},
		{Method: http.MethodPost, Pattern: prefix + "/subscriptions/cancel", Allow: http.MethodPost, Handler: body(policy.Admin, h.cancelSubscription)},
		{Method: http.MethodPost, Pattern: prefix + "/subscriptions/reactivate", Allow: http.MethodPost, Handler: wrap(policy.Admin, h.reactivateSubscription)},
		{Method: http.MethodGet, Pattern: prefix + "/audit", Allow: http.MethodGet, Handler: wrap(policy.Authenticated, h.listAudit)},
		{Method: http.MethodGet, Pattern: prefix + "/audit/{audit_id}", Allow: http.MethodGet, Handler: wrap(policy.Authenticated, h.getAudit)},
		{Method: http.MethodPost, Pattern: prefix + "/audit/{audit_id}/resolve", Allow: http.MethodPost, Handler: body(policy.Authenticated, h.resolveAudit)},
		{Method: http.MethodPost, Pattern: prefix + "/reconcile", Allow: http.MethodPost, Handler: wrap(policy.Authenticated, h.reconcile)},
	}
}

// pullStripeNotAllowed is any method but POST on /plans/pull-stripe that
// the mux would otherwise hand to a /plans/{plan_id} route.
func pullStripeNotAllowed(w http.ResponseWriter, _ *http.Request) {
	header := http.Header{}
	header.Set("Allow", http.MethodPost)
	policy.WriteDetail(w, http.StatusMethodNotAllowed, "Method Not Allowed", header)
}

// planPostNotAllowed is POST /plans/{plan_id} for any id but
// "pull-stripe": Python has no such route, so Starlette answers 405 with the
// first partially matching route's methods (GET /plans/{plan_id}).
func planPostNotAllowed(w http.ResponseWriter, _ *http.Request) {
	header := http.Header{}
	header.Set("Allow", http.MethodGet)
	policy.WriteDetail(w, http.StatusMethodNotAllowed, "Method Not Allowed", header)
}

// reply is one route answer.
type reply struct {
	status int
	body   pyjson.Value
}

func ok(body pyjson.Value) reply { return reply{http.StatusOK, body} }

func detail(status int, message string) reply {
	out := pyjson.NewObject()
	out.Set("detail", message)
	return reply{status, out}
}

func validation(errs pybody.Errors) reply {
	return reply{http.StatusUnprocessableEntity, pybody.Detail(errs)}
}

func (h handlers) write(w http.ResponseWriter, answer reply) {
	policy.WriteJSON(w, answer.status, answer.body, nil)
}

// internal logs err with the route's fields and writes the generic 500 an
// unhandled Python exception produces.
func (h handlers) internal(w http.ResponseWriter, r *http.Request, step string, err error) {
	h.logger.ErrorContext(r.Context(), "api route failed", slog.String("path", r.URL.Path),
		slog.String("step", step), slog.String("error", err.Error()))
	policy.WriteInternal(w)
}

// inTx runs fn in one transaction, committed only for a success answer: the
// Python session dependency commits after the route returns and rolls back
// when it raises, HTTPException included.
func (h handlers) inTx(ctx context.Context, fn func(pgx.Tx) (reply, error)) (reply, error) {
	if h.pool == nil {
		return reply{}, errors.New("billing: no database pool")
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return reply{}, err
	}
	answer, err := fn(tx)
	if err != nil || answer.status >= http.StatusBadRequest {
		_ = tx.Rollback(ctx)
		return answer, err
	}
	if err := tx.Commit(ctx); err != nil {
		return reply{}, err
	}
	return answer, nil
}

// serve runs a transactional route body and writes its answer.
func (h handlers) serve(w http.ResponseWriter, r *http.Request, step string, fn func(pgx.Tx) (reply, error)) {
	answer, err := h.inTx(r.Context(), fn)
	if err != nil {
		h.internal(w, r, step, err)
		return
	}
	h.write(w, answer)
}
