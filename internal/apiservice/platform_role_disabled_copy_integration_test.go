//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisionstate"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// decisionStatePDP answers /v1/authorize with the production evaluator over the production decision snapshot
// (decisionstate.Reader over the same database, AUTH_PLATFORM_ROLES_OWNED on): the bearer names the principal.
func decisionStatePDP(t *testing.T, reader *decisionstate.Reader, principals map[string]uuid.UUID) *httptest.Server {
	t.Helper()
	evaluator := authorization.NewEvaluator()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			PrincipalReference string `json:"principal_reference"`
			Action             string `json:"action"`
			Resource           struct {
				Type string  `json:"type"`
				Org  *string `json:"organization_id"`
			} `json:"resource"`
		}
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		req := authorization.Request{Action: body.Action, ResourceKind: body.Resource.Type}
		var org any
		if body.Resource.Org != nil {
			org, req.OrganizationID = *body.Resource.Org, *body.Resource.Org
		}
		d := authorization.Decision{Reason: authorization.ReasonInvalidRequest}
		if principal, ok := principals[body.PrincipalReference]; ok {
			if err := reader.Read(r.Context(), func(tx authapi.DecisionTx) error {
				snap, err := tx.Snapshot(r.Context(), principal, req.OrganizationID)
				if err == nil {
					d = evaluator.Evaluate(req, snap)
				}
				return err
			}); err != nil {
				http.Error(w, "state", http.StatusServiceUnavailable)
				return
			}
		}
		reason := string(d.Reason)
		if d.Reason == authorization.ReasonAllowedByPlatformAdmin {
			reason = string(authorization.ReasonAllowedByRole)
		}
		answer := map[string]any{
			"schema_version": "authorization_decision.v1", "decision_id": "dec_" + strings.Repeat("A", 26), "allowed": d.Allowed,
			"reason_code": reason, "principal_id": "prn_TESTPRINCIPAL", "organization_id": org, "policy_revision": 1,
			"expires_at": time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		}
		if d.Allowed && d.Obligations != nil && d.Obligations.TenantScope != "" {
			answer["obligations"] = map[string]any{"tenant_scope": d.Obligations.TenantScope}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Product rule 1 (D6773), CHAOS-13209 pm row: AUTH_PLATFORM_ROLES_OWNED on, an active platform_admin assignment, the
// auth principal copy disabled (auth.principals.disabled_at set) and public.users active. go-api's platform flag
// (rule.HoldsSQL) is false, so the legacy guard refuses the platform route; the enforced route must give the same
// answer. On base the decision snapshot reads rule.PublicHoldsSQL (public wins) and allows: WIDER.
func TestDisabledAuthCopyPlatformRouteAnswerEqualsLegacy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	user := env.User(ctx, t, false, true)
	env.Disable(ctx, t, user)
	var active, disabled bool
	if err := env.Admin.QueryRow(ctx, `SELECT u.is_active, p.disabled_at IS NOT NULL FROM public.users u JOIN auth.principals p ON p.id = u.id WHERE u.id = $1`,
		user).Scan(&active, &disabled); err != nil || !active || !disabled {
		t.Fatalf("seed: users.is_active %v, principals.disabled_at set %v, err %v; want both true", active, disabled, err)
	}

	sid := uuid.NewSHA1(user, []byte("sid"))
	store := parityStore{PGStore: policy.PGStore{Pool: env.Admin, AuthSchema: "auth", PlatformRolesOwned: true},
		sessions: map[uuid.UUID]policy.SessionRow{sid: {PrincipalID: user, LegacyTokenVersion: i64ptr(0)}}}
	minter := newEdDSAMinter(t)
	edToken := minter.mint(user, map[string]any{"sid": sessionid.Wire(sid), "org_id": nil})

	reader, err := decisionstate.New(decisionstate.Config{Pool: env.Admin, Schema: env.Schema,
		Users: authapi.PGUsers{Pool: env.Admin, AuthSchema: env.Schema, PlatformRolesOwned: true}, RefreshTTL: session.DefaultRefreshTTL})
	if err != nil {
		t.Fatal(err)
	}
	pdpSrv := decisionStatePDP(t, reader, map[string]uuid.UUID{edToken: user})
	client, err := pep.New(pep.Config{BaseURL: pdpSrv.URL, AllowPlainHTTP: true, HTTPClient: pdpSrv.Client(), ClientService: goAPIAccessService, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	enf, err := policy.NewEnforcer(policy.EnforceConfig{Decider: client, Rows: rows, Switch: authzenforce.Parse(authzenforce.GoAPI, "admin"),
		Timeout: 2 * time.Second, Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	build := func(enforcer *policy.Enforcer) http.Handler {
		verifier, err := edgetoken.New(probeKey, "dev-health-ops", "dev-health-api")
		if err != nil {
			t.Fatal(err)
		}
		signer, err := edgetoken.NewSigner(probeKey, "dev-health-ops", "dev-health-api")
		if err != nil {
			t.Fatal(err)
		}
		auth, err := policy.NewAuthenticator(verifier, store, quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		delegated, err := goAPIDelegatedRows()
		if err != nil {
			t.Fatal(err)
		}
		auth.DelegatedRows(delegated)
		eddsa, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: minter.keys})
		if err != nil {
			t.Fatal(err)
		}
		validator, err := accessverify.New(accessverify.Config{
			Rows: realRows(t), EdDSA: eddsa, Legacy: legacyVerifier(auth.AuthenticateLegacy),
			Meter: sdkmetric.NewMeterProvider().Meter("test"), Logger: quietLogger(), Now: func() time.Time { return probeNow },
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := auth.RouteAccess(routedAccessVerify{validator}, goAPIAccessService); err != nil {
			t.Fatal(err)
		}
		clickHouse, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:1"}, DialTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = clickHouse.Close() })
		guard := policy.NewGuard(auth, quietLogger())
		if enforcer != nil {
			guard.SetEnforcer(enforcer)
		}
		deps := Deps{Pool: env.Admin, ClickHouse: clickHouse, Auth: auth, Guard: guard, Verifier: verifier, Signer: signer}
		scope := policy.NewScope(auth, quietLogger())
		server, err := NewServer(apiTestConfig(), quietLogger(), Routes(deps, quietLogger()), scope.Layers()...)
		if err != nil {
			t.Fatal(err)
		}
		return server.Handler()
	}
	legacy, enforced := build(nil), build(enf)
	serve := func(h http.Handler) (int, string) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/orgs", nil)
		req.Header.Set("Authorization", "Bearer "+edToken)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code, strings.TrimSpace(rec.Body.String())
	}
	wantStatus, wantBody := serve(legacy)
	if wantStatus != http.StatusForbidden {
		t.Errorf("legacy GET /api/v1/admin/orgs: %d %s, want 403 (go-api's HoldsSQL reads the disabled copy)", wantStatus, wantBody)
	}
	if gotStatus, gotBody := serve(enforced); gotStatus != wantStatus || gotBody != wantBody {
		t.Errorf("enforced GET /api/v1/admin/orgs: %d %s, want the legacy %d %s", gotStatus, gotBody, wantStatus, wantBody)
	}
}
