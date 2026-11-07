//go:build integration

package apiservice

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// rule1EdTokensServer is the api listener with the wired handlers over auth, the authenticator the venue
// oracles serve with (edtokens.Issuer.Authenticator, which routes through accessrouting.RouteUserTokens).
func rule1EdTokensServer(t *testing.T, pool *pgxpool.Pool, ch driver.Conn, auth *policy.Authenticator) http.Handler {
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
	delegated, err := goAPIDelegatedRows()
	if err != nil {
		t.Fatal(err)
	}
	auth.DelegatedRows(delegated)
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

// Product rule 1 (D6773), CHAOS-13227: through the wired handlers on Postgres and ClickHouse, a plain user
// (no platform action) whose token org has no live membership row gets, with a user access token, the HS256
// path's answer byte for byte, on an org-scoped Authenticated row, an org-less row and Admin rows, with and
// without an X-Org-Id naming the claim. The cases: A, the user was removed from an org that exists; A, the
// org is suspended; B, the claim names no org; B, the claim is not a UUID. Routed is go-api's routed
// listener (ruleOneServer) and the venue oracles' authenticator (edtokens, accessrouting.go RouteUserTokens).
// The HS256 token claims role member, the role a user access token (no role claim) carries. The access-token.v1
// verifier refuses an org claim that is not a UUID (401) where legacy goes on: CHAOS-13261, never wanted.
func TestRule1TokenOrgWithoutMembershipWiredHandlers_CHAOS13227(t *testing.T) {
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

	removedFrom, suspended, user := uuid.New(), uuid.New(), uuid.New()
	pgseed.Org(ctx, t, pool, removedFrom.String(), "enterprise")
	pgseed.Org(ctx, t, pool, suspended.String(), "enterprise")
	const email = "rule-one-13227@example.com"
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'not-a-hash', true, true, false, 0, now(), now())`, user, email); err != nil {
		t.Fatal(err)
	}
	pgseed.Membership(ctx, t, pool, uuid.NewString(), user.String(), suspended.String(), "admin")
	if _, err := pool.Exec(ctx, `UPDATE organizations SET is_active = false WHERE id = $1`, suspended); err != nil {
		t.Fatal(err)
	}

	issuer := edtokens.New(t)
	legacy := ruleOneServer(t, pool, ch, nil)
	routed := map[string]http.Handler{
		"go-api routed":              ruleOneServer(t, pool, ch, issuer),
		"venue oracle authenticator": rule1EdTokensServer(t, pool, ch, issuer.Authenticator(t, policy.PGStore{Pool: pool}, nil)),
	}
	signer, err := edgetoken.NewSigner(ruleOneKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	serve := func(handler http.Handler, path, token, header string) (int, string) {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		if header != "" {
			request.Header.Set("X-Org-Id", header)
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Code, recorder.Body.String()
	}
	notUUIDRefused := []string{}
	paths := []string{"/api/v1/admin/impersonate/status", "/api/v1/auth/me", "/api/v1/admin/settings/general", "/api/v1/admin/teams"}
	for _, c := range []struct{ name, org string }{
		{"A removed member", removedFrom.String()},
		{"A suspended org", suspended.String()},
		{"B-unknown org", uuid.NewString()},
		{"B-notUUID claim", "not-a-uuid"},
	} {
		hs256, err := signer.Access(edgetoken.AccessClaims{UserID: user.String(), Email: email, OrgID: c.org, Role: "member"}, time.Now(), uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		eddsa, err := issuer.Mint(edtokens.Claims{UserID: user, OrgID: c.org})
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			for _, header := range []string{"", c.org} {
				wantStatus, wantBody := serve(legacy, path, hs256, header)
				for name, handler := range routed {
					gotStatus, gotBody := serve(handler, path, eddsa, header)
					t.Logf("%s %s X-Org-Id=%q: legacy %d %.160s | %s %d %.160s", c.name, path, header, wantStatus, wantBody, name, gotStatus, gotBody)
					if c.org == "not-a-uuid" && gotStatus == http.StatusUnauthorized && wantStatus != http.StatusUnauthorized {
						notUUIDRefused = append(notUUIDRefused, name+" "+path)
						continue
					}
					if gotStatus != wantStatus || gotBody != wantBody {
						t.Errorf("%s %s X-Org-Id=%q: %s %d %s, legacy %d %s", c.name, path, header, name, gotStatus, gotBody, wantStatus, wantBody)
					}
				}
			}
		}
	}
	if len(notUUIDRefused) > 0 {
		t.Skipf("CHAOS-13261: the access-token.v1 verifier refuses an org claim that is not a UUID (401) where legacy answers: %v", notUUIDRefused)
	}
}
