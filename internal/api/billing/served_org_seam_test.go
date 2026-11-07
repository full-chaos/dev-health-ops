package billing

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

var (
	seamUser     = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	seamOrgA     = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	seamOrgB     = uuid.MustParse("33333333-3333-4333-8333-333333333333")
	seamSession  = uuid.MustParse("44444444-4444-4444-8444-444444444444")
	seamBearer   = "aaaa.bbbb.cccc-dddd-eeee-ffff"
	seamLegacyV0 = int64(0)
)

// seamStore is the caller: an admin of A, a member of B, superuser or not.
type seamStore struct{ superuser bool }

func (s seamStore) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true, IsSuperuser: s.superuser}, true, nil
}

func (s seamStore) Membership(_ context.Context, _, org uuid.UUID) (string, bool, error) {
	switch org {
	case seamOrgA:
		return "admin", true, nil
	case seamOrgB:
		return "member", true, nil
	}
	return "", false, nil
}

func (s seamStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
}

func (seamStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

func (seamStore) ReadsPlatformRoles() bool { return false }

func (seamStore) SessionState(context.Context, uuid.UUID) (policy.SessionRow, bool, error) {
	return policy.SessionRow{PrincipalID: seamUser, LegacyTokenVersion: &seamLegacyV0}, true, nil
}

func (s seamStore) AccessUser(context.Context, uuid.UUID) (policy.AccessUserRow, bool, error) {
	return policy.AccessUserRow{State: policy.UserState{IsActive: true, IsSuperuser: s.superuser}, Email: "user@example.com"}, true, nil
}

type seamValidator struct{}

func (seamValidator) Validate(context.Context, string, string, string) (*policy.RoutedResult, error) {
	org := seamOrgA.String()
	wire := sessionid.Wire(seamUser)
	return &policy.RoutedResult{Claims: &policy.AccessTokenClaims{Subject: "prn_" + wire[len("ses_"):], SessionID: sessionid.Wire(seamSession), OrgID: &org}}, nil
}

// memberQuerier answers resolveOrg's membership read: the caller is a member of every org asked about.
type memberQuerier struct{}

func (memberQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("seam test: no query")
}

func (memberQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return trueRow{} }

func (memberQuerier) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("seam test: no exec")
}

type trueRow struct{}

func (trueRow) Scan(dest ...any) error {
	*(dest[0].(*bool)) = true
	return nil
}

// allowPDP allows every question and records the organization of each.
type allowPDP struct {
	srv  *httptest.Server
	mu   sync.Mutex
	orgs []string
}

func newAllowPDP(t *testing.T) *allowPDP {
	t.Helper()
	p := &allowPDP{}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Resource struct {
				Org *string `json:"organization_id"`
			} `json:"resource"`
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		var org any
		p.mu.Lock()
		if body.Resource.Org != nil {
			p.orgs = append(p.orgs, *body.Resource.Org)
			org = *body.Resource.Org
		} else {
			p.orgs = append(p.orgs, "")
		}
		p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schema_version": "authorization_decision.v1", "decision_id": "dec_" + strings.Repeat("A", 26), "allowed": true,
			"reason_code": "allowed_by_role", "principal_id": "prn_TESTPRINCIPAL", "organization_id": org, "policy_revision": 1,
			"expires_at": time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		})
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *allowPDP) asked() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.orgs...)
}

// seamHandler mounts pattern behind the real guard and enforcer (billing switched on; a nil decider is no enforcer,
// the legacy guard). The route body is the real
// billing org resolution of that route (invoiceOrg for the invoice rows, resolveOrg for the subscription rows) over a
// querier whose membership read says the caller is a member; it answers the organization it would serve. The full
// route bodies need a Postgres pool, which a unit test has none of.
func seamHandler(t *testing.T, pattern string, level policy.Authz, invoices, superuser bool, decider pep.Decider) http.Handler {
	t.Helper()
	auth, err := policy.NewAuthenticator(seamStore{superuser: superuser}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(seamValidator{}, "dev-health-ops-api"); err != nil {
		t.Fatal(err)
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	guard := policy.NewGuard(auth, quiet())
	if decider != nil {
		enforcer, err := policy.NewEnforcer(policy.EnforceConfig{Decider: decider, Rows: rows, Switch: authzenforce.Parse(authzenforce.GoAPI, "billing"),
			Timeout: time.Second, Logger: quiet()})
		if err != nil {
			t.Fatal(err)
		}
		guard.SetEnforcer(enforcer)
	}
	h := handlers{logger: quiet()}
	resolve := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		param := seamOrgParam(r)
		user := policy.UserFrom(r.Context())
		var org *uuid.UUID
		var answer *reply
		var err error
		if invoices {
			org, answer, err = invoiceOrg(r.Context(), h, memberQuerier{}, user, param)
		} else {
			org, answer, err = h.resolveOrg(r.Context(), memberQuerier{}, user, param)
		}
		switch {
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		case answer != nil:
			h.write(w, *answer)
		case org == nil:
			_, _ = io.WriteString(w, `{"served":"every org"}`)
		default:
			_, _ = io.WriteString(w, `{"served":"`+org.String()+`"}`)
		}
	})
	mux := http.NewServeMux()
	mux.Handle(pattern, guard.Wrap(level, resolve))
	return mux
}

// seamOrgParam reads org_id as the billing routes do: the last value, parsed as a pydantic UUID.
func seamOrgParam(r *http.Request) *uuid.UUID {
	raw := pybody.LastQueryValue(r.URL.Query(), "org_id")
	if raw == nil {
		return nil
	}
	id, failure := pybody.ParsePydanticUUID(*raw)
	if failure != "" {
		return nil
	}
	return &id
}

// With billing enforced, a caller whose token org is A asks a billing org_id row for B: a member of B, and a superuser.
// The request keeps the legacy answer (the guard, then the route's own org resolution) with no PDP call (product rule
// 1, D6773): the PDP judges one organization and the route serves another. org_id=A is judged on A and served A.
func TestBillingOrgIDQueryRowsKeepTheLegacyAnswerForAnotherOrg(t *testing.T) {
	cases := []struct {
		pattern  string
		level    policy.Authz
		invoices bool
	}{
		{"GET /api/v1/billing/invoices", policy.Authenticated, true},
		{"GET /api/v1/billing/invoices/{invoice_id}", policy.Authenticated, true},
		{"POST /api/v1/billing/invoices/{invoice_id}/void", policy.Admin, true},
		{"GET /api/v1/billing/subscriptions", policy.Authenticated, false},
		{"GET /api/v1/billing/subscriptions/list", policy.Authenticated, false},
		{"GET /api/v1/billing/subscriptions/history", policy.Authenticated, false},
		{"POST /api/v1/billing/subscriptions/change-plan", policy.Admin, false},
		{"POST /api/v1/billing/subscriptions/cancel", policy.Admin, false},
		{"POST /api/v1/billing/subscriptions/reactivate", policy.Admin, false},
	}
	for _, tc := range cases {
		t.Run(tc.pattern, func(t *testing.T) {
			method, route, _ := strings.Cut(tc.pattern, " ")
			path := strings.ReplaceAll(route, "{invoice_id}", uuid.NewString())
			serve := func(superuser bool, org uuid.UUID, decider pep.Decider) (int, string) {
				req := httptest.NewRequest(method, path+"?org_id="+org.String(), nil)
				req.Header.Set("Authorization", "Bearer "+seamBearer)
				rec := httptest.NewRecorder()
				seamHandler(t, tc.pattern, tc.level, tc.invoices, superuser, decider).ServeHTTP(rec, req)
				return rec.Code, strings.TrimSpace(rec.Body.String())
			}
			do := func(superuser bool, org uuid.UUID) (int, string, []string) {
				pdp := newAllowPDP(t)
				client, err := pep.New(pep.Config{BaseURL: pdp.srv.URL, AllowPlainHTTP: true, HTTPClient: pdp.srv.Client(),
					ClientService: "dev-health-ops-api", Timeout: 2 * time.Second})
				if err != nil {
					t.Fatal(err)
				}
				status, body := serve(superuser, org, client)
				return status, body, pdp.asked()
			}
			for _, superuser := range []bool{false, true} {
				wantStatus, wantBody := serve(superuser, seamOrgB, nil)
				if status, body, asked := do(superuser, seamOrgB); status != wantStatus || body != wantBody || len(asked) != 0 {
					t.Errorf("superuser %v, token A, org_id=B: %d %s, PDP asked %v, want the legacy %d %s and no PDP call", superuser, status, body,
						asked, wantStatus, wantBody)
				}
			}
			if status, body, asked := do(false, seamOrgA); status != http.StatusOK || body != `{"served":"`+seamOrgA.String()+`"}` ||
				len(asked) != 1 || asked[0] != seamOrgA.String() {
				t.Errorf("token A, org_id=A: %d %s, PDP asked %v, want 200 serving A and one question on A", status, body, asked)
			}
		})
	}
}
