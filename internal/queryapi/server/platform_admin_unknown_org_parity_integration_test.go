//go:build integration

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// Product rule 1 parity cells: the query-api REST authentication (authenticateRESTRequest, the edge verifier routed by
// row, the live users store over a migrated database) behind the plane handler, for a superuser
// with no membership row whose token names an organization with no row (U: never existed; R: its row deleted).
// Legacy is the gate off; enforced is the gate in enforce mode with the four read cells on and the production
// evaluator behind the PDP wire. The handler past authentication echoes the org and the scope org it may serve (the
// handlers read only Scope.OrgID, not the scope kind). The enforced answer must equal the legacy answer.

var (
	msqOrgU  = uuid.MustParse("00000000-0000-4000-8000-0000000014c1")
	msqOrgR  = uuid.MustParse("00000000-0000-4000-8000-0000000014d1")
	msqSU0   = uuid.MustParse("00000000-0000-4000-8000-0000000014f0")
	msqPlain = uuid.MustParse("00000000-0000-4000-8000-0000000014f1")
)

var msqRows = []string{"/api/v1/home", "/api/v1/flame", "/api/v1/drilldown/prs", "/api/v1/drilldown/issues"}

func msqSID(u uuid.UUID) uuid.UUID { return uuid.NewSHA1(u, []byte("msq-sid")) }

type msqStore struct {
	policy.PGStore
	sessions map[uuid.UUID]policy.SessionRow
}

func (s msqStore) SessionState(_ context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	row, ok := s.sessions[id]
	return row, ok, nil
}

type msqSnap struct {
	platformRole string
	orgStatus    authorization.Status
}

func (s msqSnap) PrincipalStatus() authorization.Status          { return authorization.StatusActive }
func (s msqSnap) PlatformRole() string                           { return s.platformRole }
func (s msqSnap) OrganizationStatus(string) authorization.Status { return s.orgStatus }
func (s msqSnap) Membership(string) (authorization.Membership, bool) {
	return authorization.Membership{}, false
}
func (s msqSnap) Grants(string) ([]authorization.Grant, error) { return nil, nil }
func (s msqSnap) Workload() (authorization.WorkloadLimits, bool) {
	return authorization.WorkloadLimits{}, false
}
func (s msqSnap) ServiceAccount(string) (authorization.ServiceAccount, bool) {
	return authorization.ServiceAccount{}, false
}
func (s msqSnap) NowUnixNano() int64 { return time.Now().UnixNano() }
func (s msqSnap) Delegation() (authorization.Delegation, bool) {
	return authorization.Delegation{}, false
}

var msqStatusName = map[authorization.Status]string{
	authorization.StatusUnknown: "Unknown", authorization.StatusActive: "Active", authorization.StatusInvited: "Invited",
	authorization.StatusSuspended: "Suspended", authorization.StatusRemoved: "Removed",
}

// msqOrgStatus is decisionstate's organization rule over the same database: row active -> Active, inactive ->
// Suspended, no row -> Unknown.
func msqOrgStatus(ctx context.Context, pool *pgxpool.Pool, org string) (authorization.Status, error) {
	var active bool
	err := pool.QueryRow(ctx, `SELECT COALESCE(is_active, false) FROM organizations WHERE id::text = $1`, org).Scan(&active)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return authorization.StatusUnknown, nil
	case err != nil:
		return authorization.StatusUnknown, err
	case active:
		return authorization.StatusActive, nil
	}
	return authorization.StatusSuspended, nil
}

type msqPDP struct {
	srv   *httptest.Server
	mu    sync.Mutex
	snaps map[string]msqSnap
	label map[string]string
	seen  []string
}

func newMsqPDP(t *testing.T, pool *pgxpool.Pool) *msqPDP {
	t.Helper()
	p := &msqPDP{snaps: map[string]msqSnap{}, label: map[string]string{}}
	evaluator := authorization.NewEvaluator()
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			PrincipalReference string `json:"principal_reference"`
			Action             string `json:"action"`
			Resource           struct {
				Type string  `json:"type"`
				Key  string  `json:"key"`
				Org  *string `json:"organization_id"`
			} `json:"resource"`
		}
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		snap, known := p.snaps[body.PrincipalReference]
		label := p.label[body.PrincipalReference]
		p.mu.Unlock()
		req := authorization.Request{Action: body.Action, ResourceKind: body.Resource.Type}
		var org any
		if body.Resource.Org != nil {
			org = *body.Resource.Org
			req.OrganizationID = *body.Resource.Org
			status, err := msqOrgStatus(r.Context(), pool, *body.Resource.Org)
			if err != nil {
				http.Error(w, "store", http.StatusBadGateway)
				return
			}
			snap.orgStatus = status
		}
		d := authorization.Decision{Reason: authorization.ReasonInvalidRequest}
		if known {
			d = evaluator.Evaluate(req, snap)
		}
		reason := string(d.Reason)
		answer := map[string]any{
			"schema_version": "authorization_decision.v1", "decision_id": "dec_" + strings.Repeat("A", 26), "allowed": d.Allowed,
			"reason_code": reason, "principal_id": "prn_TESTPRINCIPAL", "organization_id": org, "policy_revision": 1,
			"expires_at": time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		}
		if d.Allowed && d.Obligations != nil && d.Obligations.TenantScope != "" {
			answer["obligations"] = map[string]any{"tenant_scope": d.Obligations.TenantScope}
		}
		p.mu.Lock()
		p.seen = append(p.seen, fmt.Sprintf("%s %s %s/%v org_status=%s -> allowed=%v reason=%s", label, body.Action, body.Resource.Type, org,
			msqStatusName[snap.orgStatus], d.Allowed, d.Reason))
		p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *msqPDP) register(token, label string, s msqSnap) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snaps[token], p.label[token] = s, label
}

func (p *msqPDP) take() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.seen
	p.seen = nil
	return out
}

func msqHandler(t *testing.T, store queryAPIUserStore, m *accessMinter, gate *pdp.Gate) http.Handler {
	t.Helper()
	verifier, _ := iaVerifier(t)
	edge, err := principal.NewEdgeVerifier(store)
	if err != nil {
		t.Fatal(err)
	}
	setup := testAccess(m.keys, nil, accessTestNow)
	if err := routeEdgeVerifier(edge, setup.opts); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, row := range msqRows {
		mux.HandleFunc(row, func(w http.ResponseWriter, r *http.Request) {
			claims, ok := authenticateRESTRequest(w, r, verifier, edge, "platform-admin-unknown-org-measure")
			if !ok {
				return
			}
			r = r.WithContext(authctx.WithClaims(r.Context(), claims))
			org, has := claims.Scope.OrgID()
			_, _ = fmt.Fprintf(w, `{"org":%q,"scope_org":%q,"scope_has_org":%v}`, claims.OrgID, org, has)
		})
	}
	return planeHandler(mux, gate)
}

func msqGate(t *testing.T, mode, cells string, p *msqPDP) *pdp.Gate {
	t.Helper()
	cfg := pdp.Config{Mode: mode, Switch: authzenforce.Parse(authzenforce.QueryAPI, cells), Meter: sdkmetric.NewMeterProvider().Meter("test"),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if p != nil {
		client, err := pep.New(pep.Config{BaseURL: p.srv.URL, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: pdp.ClientService, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		cfg.Decider = client
	}
	return pdp.New(cfg)
}

func msqVerdict(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusServiceUnavailable:
		return "REFUSE"
	}
	return "ADMIT"
}

func TestPlatformAdminInAnUnknownOrDeletedOrganizationQueryAPIAnswersEqualLegacy(t *testing.T) {
	quietStdLog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	sessions := map[uuid.UUID]policy.SessionRow{}
	for _, u := range []uuid.UUID{msqSU0, msqPlain} {
		pgseed.User(ctx, t, pool, u.String())
		sessions[msqSID(u)] = policy.SessionRow{PrincipalID: u, LegacyTokenVersion: i64(0)}
	}
	pgseed.SetUserState(ctx, t, pool, msqSU0.String(), true, true, 0)
	pgseed.Org(ctx, t, pool, msqOrgR.String(), "community")
	if tag, err := pool.Exec(ctx, `DELETE FROM organizations WHERE id = $1`, msqOrgR); err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("delete org R: %v (%d rows)", err, tag.RowsAffected())
	}
	store := queryAPIUserStore{queryAPIStoreBase: msqStore{PGStore: policy.PGStore{Pool: pool}, sessions: sessions}}

	m := newAccessMinter(t)
	p := newMsqPDP(t, pool)
	legacy := msqHandler(t, store, m, msqGate(t, pdp.ModeOff, "", nil))
	enforced := msqHandler(t, store, m, msqGate(t, pdp.ModeEnforce, "analytics,metrics,git,work_items", p))
	ed := func(u, org uuid.UUID) string {
		return m.mint(u, map[string]any{"sid": sessionid.Wire(msqSID(u)), "org_id": org.String()}, nil)
	}
	su0 := msqSnap{platformRole: rule.PlatformAdmin}
	for _, o := range []struct {
		label string
		id    uuid.UUID
	}{{"org U no row (never existed)", msqOrgU}, {"org R no row (deleted)", msqOrgR}} {
		for _, c := range []struct {
			label, token string
			snap         msqSnap
		}{
			{"SU0/EdDSA/" + o.label, ed(msqSU0, o.id), su0},
			{"control plain/EdDSA/" + o.label, ed(msqPlain, o.id), msqSnap{}},
		} {
			control := strings.HasPrefix(c.label, "control")
			p.register(c.token, c.label, c.snap)
			for _, row := range msqRows {
				serve := func(h http.Handler) (int, string) {
					req := httptest.NewRequest(http.MethodGet, row, nil)
					req.Header.Set("Authorization", "Bearer "+c.token)
					rec := httptest.NewRecorder()
					h.ServeHTTP(rec, req)
					return rec.Code, strings.TrimSpace(rec.Body.String())
				}
				legStatus, legBody := serve(legacy)
				legacyCalls := p.take()
				enfStatus, enfBody := serve(enforced)
				calls := p.take()
				if legStatus == 0 || enfStatus == 0 {
					t.Fatalf("%s GET %s: no answer (legacy %d, enforced %d)", c.label, row, legStatus, enfStatus)
				}
				t.Logf("CELL|M-q|query-api|%s|%s|GET %s|legacy|%d|%s|%s|pdp_calls=%d", o.label, c.label, row, legStatus, msqVerdict(legStatus), legBody, len(legacyCalls))
				t.Logf("CELL|M-q|query-api|%s|%s|GET %s|enforced|%d|%s|%s|pdp=%v", o.label, c.label, row, enfStatus, msqVerdict(enfStatus), enfBody, calls)
				t.Run(c.label+" GET "+row, func(t *testing.T) {
					if enfStatus != legStatus || enfBody != legBody {
						t.Errorf("enforced %d %s, legacy %d %s; product rule 1 wants the legacy answer", enfStatus, enfBody, legStatus, legBody)
					}
					switch {
					case control && msqVerdict(legStatus) != "REFUSE":
						t.Errorf("control: legacy %d %s, want refused", legStatus, legBody)
					case !control && strings.Contains(c.label, "/EdDSA/") && msqVerdict(legStatus) != "ADMIT":
						t.Errorf("legacy %d %s, want an answer past the guard", legStatus, legBody)
					}
				})
			}
		}
	}
}
