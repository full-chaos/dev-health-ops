package billing

import (
	"context"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// The billing-edge routes: what the Python billing edge
// (src/dev_health_ops/api/billing_edge.py, since deleted, CHAOS-6939) served
// on its own port, served here from a second listener of dho api so the
// billing host reaches only this. Its three behaviours are the Stripe webhook (the handler of the main
// listener), GET|HEAD /health, and a JSON 404 for every other path and every
// method (Python's catch-all route answers 404 where the main app would
// answer 405).

// Health values of one edge service.
const (
	healthOK            = "ok"
	healthNotConfigured = "not_configured"
	healthDown          = "down"
)

// edgeHealthInput is what the edge /health reads: one value per service.
type edgeHealthInput struct {
	Postgres, StripeSecretKey, StripeWebhookSecret, LicensePrivateKey, StripeClient string
}

// edgeHealthStatus is the /health status: "ok" only when the four required
// services (postgres and the three secrets) are ok; stripe_client is
// reported but never decides it, as in Python.
func edgeHealthStatus(in edgeHealthInput) string {
	if in.Postgres == healthOK && in.StripeSecretKey == healthOK && in.StripeWebhookSecret == healthOK && in.LicensePrivateKey == healthOK {
		return "ok"
	}
	return "down"
}

// edgeHealthBody is the body: {"status", "services": {...}} with the services
// in Python's insertion order (the three secrets, stripe_client, postgres).
func edgeHealthBody(in edgeHealthInput) *pyjson.Object {
	services := pyjson.NewObject()
	services.Set("stripe_secret_key", in.StripeSecretKey)
	services.Set("stripe_webhook_secret", in.StripeWebhookSecret)
	services.Set("license_private_key", in.LicensePrivateKey)
	services.Set("stripe_client", in.StripeClient)
	services.Set("postgres", in.Postgres)
	body := pyjson.NewObject()
	body.Set("status", edgeHealthStatus(in))
	body.Set("services", services)
	return body
}

// configured is "ok" for a secret that is set.
func configured(set bool) string {
	if set {
		return healthOK
	}
	return healthNotConfigured
}

// edgeHealth reads the edge's services now.
func (h handlers) edgeHealth(ctx context.Context) edgeHealthInput {
	in := edgeHealthInput{
		StripeSecretKey:     configured(h.stripeKey.Reveal() != ""),
		StripeWebhookSecret: configured(h.webhookSecret.Reveal() != ""),
		LicensePrivateKey:   configured(h.licenseKey.Reveal() != ""),
		StripeClient:        healthOK,
	}
	if _, err := h.stripe.Client(); err != nil {
		in.StripeClient = healthDown
	}
	switch {
	case h.pool == nil:
		in.Postgres = healthNotConfigured
	default:
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := h.pool.Ping(pingCtx); err != nil {
			in.Postgres = healthDown
		} else {
			in.Postgres = healthOK
		}
	}
	return in
}

// health is GET|HEAD /health: 200 with the body when the four required
// services are ok, else 503 with the same body.
func (h handlers) health(w http.ResponseWriter, r *http.Request) {
	in := h.edgeHealth(r.Context())
	status := http.StatusServiceUnavailable
	if edgeHealthStatus(in) == "ok" {
		status = http.StatusOK
	}
	h.write(w, reply{status, edgeHealthBody(in)})
}

// EdgeRoutes returns the billing-edge listener's routes: the Stripe webhook
// and GET|HEAD /health. The server (apiservice.NewEdgeServer) answers every
// other path, and every other method on these, with the 404 the Python edge's
// catch-all route gives.
func EdgeRoutes(deps Deps) []httpapi.Route {
	h := newHandlers(deps)
	routes := []httpapi.Route{
		// `-> dict` makes FastAPI dump the answer as a response_model.
		{Method: http.MethodPost, Pattern: prefix + "/webhooks/stripe", Handler: http.HandlerFunc(h.stripeWebhook), ResponseModel: true},
		{Method: http.MethodGet, Pattern: "/health", Handler: http.HandlerFunc(h.health), ResponseModel: true},
		{Method: http.MethodHead, Pattern: "/health", Handler: http.HandlerFunc(h.health), ResponseModel: true},
	}
	return routes
}
