//go:build integration

package billingvenue

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// An admin of one org who is only a viewer in another must not change, cancel or reactivate the other org's
// subscription by naming it in org_id: the Admin level is checked on the token's org, so the org_id org needs its own
// admin role. Control: the same calls for an org the caller holds no membership in are refused today.
func TestSubscriptionWritesRefuseAViewerOfTheOrgIDOrg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fake := newFakeStripe()
	goStripe := httptest.NewServer(fake.plane("go"))
	t.Cleanup(goStripe.Close)
	env := webhookEnv()
	var pythonEnv []string
	for name, value := range env {
		pythonEnv = append(pythonEnv, name+"="+value)
	}
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true,
		Root:   venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			// adminC (admin of org C) is also a viewer of org A.
			if _, err := admin.Exec(ctx, `INSERT INTO memberships (id, user_id, org_id, role) VALUES (gen_random_uuid(), $1, $2, 'viewer')`,
				seed.adminC, seed.orgA); err != nil {
				t.Fatal(err)
			}
			return seed.tokenSpecs()
		},
	})
	loaded, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTIssuer:   "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins: []string{"http://localhost:3000"},
		StripeSecretKey:    loaded.StripeSecretKey, APIBilling: loaded.APIBilling,
		StripeWebhookSecret: loaded.StripeWebhookSecret, LicensePrivateKey: loaded.LicensePrivateKey,
	}
	base := startBillingVenueAPI(t, ctx, cfg, venue, goStripe.URL)

	stripeCallsFor := func(prefix string) []string {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		var out []string
		for _, call := range fake.calls["go"] {
			if strings.Contains(call, " /v1/subscriptions/"+prefix) {
				out = append(out, call)
			}
		}
		return out
	}
	const s = "/api/v1/billing/subscriptions"
	writes := []struct{ name, path, body string }{
		{"change-plan", s + "/change-plan", `{"price_id":"price_seed_y"}`},
		{"reactivate", s + "/reactivate", ""},
		{"cancel", s + "/cancel", `{}`},
	}
	for _, target := range []struct {
		label  string
		org    string
		prefix string
	}{
		{"control: org B, no membership", seed.orgB.String(), "sub_err"},
		{"org A, viewer membership", seed.orgA.String(), "sub_A"},
	} {
		for _, write := range writes {
			before := len(stripeCallsFor(target.prefix))
			request := venueoracle.Request{Name: write.name + " " + target.label, Method: "POST", Path: write.path + "?org_id=" + target.org,
				Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["adminC"], "Content-Type": "application/json"}}
			if write.body != "" {
				request.Body = venueoracle.B64(write.body)
			}
			response := venueoracle.Do(t, base, request)
			calls := stripeCallsFor(target.prefix)[before:]
			t.Logf("OBSERVED %s: status=%d body=%s stripe_calls=%d %v", request.Name, response.Status, response.Body, len(calls), calls)
			if response.Status != 403 || len(calls) != 0 {
				t.Errorf("%s: got %d with %d Stripe call(s) on %s's subscription, want 403 and none", request.Name, response.Status, len(calls),
					fmt.Sprint(target.org))
			}
		}
	}
	venueoracle.WriteGoOnlyProof(t, "an org admin who is only a viewer of the org_id org is refused subscription writes there")
}
