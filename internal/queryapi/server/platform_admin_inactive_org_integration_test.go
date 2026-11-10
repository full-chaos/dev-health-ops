//go:build integration

package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pinrig"
)

// CHAOS-13209 D3, the legacy yardstick for "in any ACTIVE organization": the answers the legacy query-api path gave
// over the migrated database (HS256 edge token, the live users row, CheckTokenOrg) for a superuser with no membership
// row whose org claim names an active or a suspended organization, and, as the control, a viewer member of each. The
// HS256 class is retired (CHAOS-13126): the same callers come on user access tokens of real sessions through the
// routed edge verifier, and the wanted statuses stay the legacy ones. The answers are recorded as CELL lines.
func TestLegacyQueryAPISuperuserInAnInactiveOrganization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = pg.Close(closeCtx)
	})
	pgschema.ApplyURI(ctx, t, pg.URI)
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	addUser := func(superuser bool) uuid.UUID {
		id := uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO users
	(id, email, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'local', true, true, $3, 0, now(), now())`, id, id.String()+"@example.test", superuser); err != nil {
			t.Fatalf("seed user: %v", err)
		}
		return id
	}
	addOrg := func(slug string, active bool) uuid.UUID {
		id := uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', $3, now(), now())`, id, slug, active); err != nil {
			t.Fatalf("seed org: %v", err)
		}
		return id
	}
	superuser, viewer := addUser(true), addUser(false)
	active, inactive := addOrg("parity-active", true), addOrg("parity-inactive", false)
	for _, org := range []uuid.UUID{active, inactive} {
		if _, err := pool.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'viewer', now(), now(), now())`, uuid.New(), org, viewer); err != nil {
			t.Fatalf("seed membership: %v", err)
		}
	}

	rig := pinrig.NewPinRig(t, ctx, pool, pg)
	sessionOf := map[uuid.UUID]uuid.UUID{superuser: rig.SignIn(t, ctx, superuser), viewer: rig.SignIn(t, ctx, viewer)}
	verifier, _ := iaVerifier(t)
	edge, err := principal.NewEdgeVerifier(newQueryAPIUserStore(policy.PGStore{Pool: pool}))
	if err != nil {
		t.Fatal(err)
	}
	m := newAccessMinter(t)
	if err := routeEdgeVerifier(edge, testAccess(m.keys, nil, accessTestNow).opts); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, row := range paRows {
		mux.HandleFunc(row, func(w http.ResponseWriter, r *http.Request) {
			claims, ok := authenticateRESTRequest(w, r, verifier, edge, "inactive-org")
			if !ok {
				return
			}
			r = r.WithContext(authctx.WithClaims(r.Context(), claims))
			org, has := claims.Scope.OrgID()
			_, _ = fmt.Fprintf(w, `{"org":%q,"scope_kind":%d,"scope_org":%q,"scope_has_org":%v}`, claims.OrgID, claims.Scope.Kind(), org, has)
		})
	}
	legacy := planeHandler(mux, paGate(t, pdp.ModeOff, "", nil))

	for _, c := range []struct {
		who        string
		user       uuid.UUID
		super      bool
		org        uuid.UUID
		orgLabel   string
		wantStatus int
	}{
		{"superuser, no membership row", superuser, true, active, "active", http.StatusOK},
		{"superuser, no membership row", superuser, true, inactive, "inactive", http.StatusOK},
		{"viewer member", viewer, false, active, "active", http.StatusOK},
		{"viewer member", viewer, false, inactive, "inactive", http.StatusUnauthorized},
	} {
		token := m.mint(c.user, map[string]any{"sid": sessionid.Wire(sessionOf[c.user]), "org_id": c.org.String()}, nil)
		for _, row := range paRows {
			req := httptest.NewRequest(http.MethodGet, row, nil)
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()
			legacy.ServeHTTP(rec, req)
			body := strings.TrimSpace(rec.Body.String())
			t.Logf("CELL|D3|%s|org %s|GET %s|legacy|%d|%s", c.who, c.orgLabel, row, rec.Code, body)
			if rec.Code != c.wantStatus {
				t.Errorf("%s, org %s, GET %s: legacy %d %s, want %d", c.who, c.orgLabel, row, rec.Code, body, c.wantStatus)
			}
		}
	}
}
