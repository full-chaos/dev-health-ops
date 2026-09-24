//go:build integration

// Package billingvenue holds the billing venue oracles (plans, checkout,
// portal, ledger, the Stripe webhook, and the test-mode leg), in a package of
// their own so they neither sit in nor extend internal/apiservice's venue
// time budget.
package billingvenue

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venueKey is the venue's JWT signing key (a fixed test value).
const venueKey = "venue-oracle-signing-key-0123456789abcdef"

// venueUUID matches a UUID in a response body or a row.
var venueUUID = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

func venueRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// startVenueAPI serves the Go plane for venue as dho api wires it (the
// routes, the org scope and impersonation middlewares, security headers and
// CORS), connected as the api role, after proving that role holds exactly
// its declared grants.
func startVenueAPI(t *testing.T, ctx context.Context, cfg config.Config, venue *venueoracle.Venue) string {
	t.Helper()
	return startBillingVenueAPI(t, ctx, cfg, venue, "")
}

// startBillingVenueAPI is startVenueAPI with the Go plane's Stripe client
// pointed at stripeBase ("" = Stripe's own base).
func startBillingVenueAPI(t *testing.T, ctx context.Context, cfg config.Config, venue *venueoracle.Venue, stripeBase string) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, cfg.APIDatabaseURI.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := postgres.CheckAPIAuthorization(ctx, pool, cfg.APIDatabaseRole, cfg.RiverDatabaseSchema); err != nil {
		t.Fatalf("dho api not ready as the api role: %v %s", err, venue.DiagnoseAPIRole(t, ctx))
	}
	logger := quietLogger()
	verifier, err := edgetoken.New(cfg.APIJWTSecret.Reveal(), cfg.APIJWTIssuer, cfg.APIJWTAudience)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := webhookintake.LoadJobRegistry(filepath.Join(venueRoot(), "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	deps := apiservice.Deps{
		Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger), Producer: producer,
		Stripe:        stripeclient.New(stripeclient.Options{Key: cfg.StripeSecretKey.Reveal(), BaseURL: stripeBase}),
		BillingConfig: cfg.APIBilling, StripeWebhookSecret: cfg.StripeWebhookSecret, LicensePrivateKey: cfg.LicensePrivateKey,
	}
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, apiservice.Routes(deps, logger), scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}
