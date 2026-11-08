package apiservice

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

// On the production route set, server chain and guard, the encoded-slash variant of the Stripe webhook is
// not exempt: the scope judges its bearer on the stripe row, and the stripe handler refuses the unsigned body.
func TestTheEncodedSlashStripeWebhookIsJudgedAndRefusedOnTheProductionChain(t *testing.T) {
	refusals := &refusalLog{}
	store := memberScopeStore{scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}, mountedUser, mountedOrg}
	auth := mountedTokens.AuthenticatorOver(t, store, slog.New(refusals))
	verifier, err := edgetoken.New(mountedKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(mountedKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	clickHouse, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickHouse.Close() })
	deps := Deps{Pool: pool, ClickHouse: clickHouse, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()), Verifier: verifier, Signer: signer}
	server, err := newServer(apiTestConfig(), quietLogger(), Routes(deps, quietLogger()), nil, deps.AuthProxy.ScopeExemptRoutes(),
		policy.NewScope(auth, quietLogger()).Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	send := func(target string) (int, string, int) {
		before := refusals.profileCount("POST " + exemptPath)
		request := httptest.NewRequest(http.MethodPost, target, strings.NewReader("{}"))
		request.Header.Set("Authorization", "Bearer "+mountedTokens.MustMint(t, edtokens.Claims{UserID: mountedUser, OrgID: mountedOrg.String()}))
		request.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		return recorder.Code, recorder.Body.String(), refusals.profileCount("POST "+exemptPath) - before
	}

	code, body, judged := send(exemptPath)
	if code != http.StatusBadRequest || !strings.Contains(body, "Invalid Stripe signature") || judged != 0 {
		t.Errorf("exact route: status %d body %q, judged %d on the stripe row; want 400 Invalid Stripe signature, judged 0 (exempt)", code, body, judged)
	}
	code, body, judged = send("/api/v1/billing/webhooks%2Fstripe")
	if code != http.StatusBadRequest || !strings.Contains(body, "Invalid Stripe signature") || judged != 1 {
		t.Errorf("encoded slash: status %d body %q, judged %d on the stripe row; want 400 Invalid Stripe signature, judged 1 (not exempt)", code, body, judged)
	}
}
