//go:build integration

package apiservice

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

const ruleOneKey = "rule-one-legacy-signing-key-0123456789abcdef"

// ruleOneServer is the api listener with the wired handlers over pool and ch. issuer nil: the HS256 path alone
// (no AUTH_ACCESS_TOKEN_ISSUER); set: the profile-routed validator with the HS256 path and issuer's key, as
// production builds it with the issuer set.
func ruleOneServer(t *testing.T, pool *pgxpool.Pool, ch driver.Conn, issuer *edtokens.Issuer) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	verifier, err := edgetoken.New(ruleOneKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(ruleOneKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	var store policy.Store = policy.PGStore{Pool: pool}
	if issuer != nil {
		store = issuer.Store(policy.PGStore{Pool: pool})
	}
	auth, err := policy.NewAuthenticator(verifier, store, logger)
	if err != nil {
		t.Fatal(err)
	}
	delegated, err := goAPIDelegatedRows()
	if err != nil {
		t.Fatal(err)
	}
	auth.DelegatedRows(delegated)
	if issuer != nil {
		validator, err := accessverify.New(accessverify.Config{
			Rows: realRows(t), EdDSA: issuer.Verifier(t), Legacy: legacyVerifier(auth.AuthenticateLegacy),
			Meter: sdkmetric.NewMeterProvider().Meter("test"), Logger: logger,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := auth.RouteAccess(routedAccessVerify{validator}, goAPIAccessService); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := goAPIScopeRows()
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, logger)
	scope.DeclaredRows(rows)
	deps := Deps{Pool: pool, ClickHouse: ch, Auth: auth, Guard: policy.NewGuard(auth, logger), Verifier: verifier, Signer: signer}
	server, err := NewServer(apiTestConfig(), logger, Routes(deps, logger), scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler()
}

// Product rule 1 (D6773): through the wired handlers on Postgres and ClickHouse, a platform admin whose token
// names an org it holds no membership row in gets, with a user access token, the HS256 path's answer byte for
// byte. A user without the platform action and without the membership gets the HS256 path's answer too
// (CHAOS-13227: the X-Org-Id refusal). CHAOS-13126
// retired the HS256 class on every routed row: the HS256 path's answer is still read on the HS256-only listener
// (issuer unset, the legacy path), and on the routed listener the HS256 token is the generic 401.
func TestProductRule1PlatformAdminWithoutMembershipWiredHandlers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	chc, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chc.Close(context.Background()) })
	chschema.Apply(ctx, t, chc)
	ch, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(chc.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close() })

	org, super, plain := uuid.New(), uuid.New(), uuid.New()
	pgseed.Org(ctx, t, pool, org.String(), "enterprise")
	for _, user := range []struct {
		id        uuid.UUID
		email     string
		superuser bool
	}{{super, "rule-one-super@example.com", true}, {plain, "rule-one-plain@example.com", false}} {
		if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'not-a-hash', true, true, $3, 0, now(), now())`, user.id, user.email, user.superuser); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, created_at, updated_at)
VALUES ($1, $2, 'rule-one', 'github', '[]'::json, '{}'::json, true, now(), now())`, uuid.New(), org); err != nil {
		t.Fatal(err)
	}

	issuer := edtokens.New(t)
	legacy, routed := ruleOneServer(t, pool, ch, nil), ruleOneServer(t, pool, ch, issuer)
	signer, err := edgetoken.NewSigner(ruleOneKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	hs256 := func(id uuid.UUID, email string) string {
		token, err := signer.Access(edgetoken.AccessClaims{UserID: id.String(), Email: email, OrgID: org.String(), Role: "member"}, time.Now(), uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	eddsa := func(id uuid.UUID) string {
		token, err := issuer.Mint(edtokens.Claims{UserID: id, OrgID: org.String()})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	serve := func(handler http.Handler, path, token string) (int, string) {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Code, recorder.Body.String()
	}
	for _, path := range []string{"/api/v1/admin/settings/general", "/api/v1/admin/teams", "/api/v1/admin/sync-configs"} {
		wantStatus, wantBody := serve(legacy, path, hs256(super, "rule-one-super@example.com"))
		gotStatus, gotBody := serve(routed, path, eddsa(super))
		t.Logf("%s platform admin: HS256 %d %.200s | user access token %d %.200s", path, wantStatus, wantBody, gotStatus, gotBody)
		if wantStatus != http.StatusOK || gotStatus != wantStatus || gotBody != wantBody {
			t.Errorf("%s platform admin without membership: user access token %d %s, HS256 %d %s", path, gotStatus, gotBody, wantStatus, wantBody)
		}
		if status, body := serve(routed, path, hs256(super, "rule-one-super@example.com")); status != http.StatusUnauthorized || strings.TrimSpace(body) != genericRefusal {
			t.Errorf("%s HS256 on the routed listener %d %s, want the retired-class 401 %s", path, status, body, genericRefusal)
		}
		wantStatus, wantBody = serve(legacy, path, hs256(plain, "rule-one-plain@example.com"))
		if status, body := serve(routed, path, eddsa(plain)); wantStatus != http.StatusForbidden || status != wantStatus || body != wantBody {
			t.Errorf("%s user without the platform action or the membership: user access token %d %s, HS256 %d %s", path, status, body, wantStatus, wantBody)
		}
	}
}
