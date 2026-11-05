package billing

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// writeDB is the memberships and subscriptions rows the three subscription writes read.
type writeDB struct {
	roles map[[2]uuid.UUID]string
	subs  map[uuid.UUID]string
	execs int
}

type scanFunc func(dest ...any) error

func (f scanFunc) Scan(dest ...any) error { return f(dest...) }

func (d *writeDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("writeDB: Query unused")
}

func (d *writeDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	d.execs++
	return pgconn.CommandTag{}, errors.New("writeDB: a subscription write wrote a row")
}

func (d *writeDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	switch {
	case strings.Contains(sql, "FROM memberships"):
		user, org := args[0].(uuid.UUID), args[1].(uuid.UUID)
		role, member := d.roles[[2]uuid.UUID{user, org}]
		if strings.Contains(sql, "SELECT EXISTS") {
			return scanFunc(func(dest ...any) error { *(dest[0].(*bool)) = member; return nil })
		}
		return scanFunc(func(dest ...any) error {
			if !member {
				return pgx.ErrNoRows
			}
			*(dest[0].(*string)) = role
			return nil
		})
	case strings.Contains(sql, "FROM subscriptions s"):
		if len(args) != 1 {
			return scanFunc(func(...any) error { return errors.New("writeDB: subscription read without an org") })
		}
		org := args[0].(uuid.UUID)
		stripeID, found := d.subs[org]
		return scanFunc(func(dest ...any) error {
			if !found {
				return pgx.ErrNoRows
			}
			*(dest[0].(*uuid.UUID)) = uuid.New()
			*(dest[1].(*uuid.UUID)) = org
			*(dest[4].(*string)) = stripeID
			*(dest[5].(*string)) = "cus_" + stripeID
			*(dest[6].(*string)) = "active"
			return nil
		})
	}
	return scanFunc(func(...any) error { return errors.New("writeDB: unexpected statement: " + sql) })
}

// fakeStripeAPI records every Stripe request and answers a subscription with one item.
type fakeStripeAPI struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeStripeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.calls = append(f.calls, r.Method+" "+r.URL.Path+" "+string(body))
	f.mu.Unlock()
	id := strings.TrimPrefix(r.URL.Path, "/v1/subscriptions/")
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"id":"`+id+`","object":"subscription","items":{"object":"list","data":[{"id":"si_`+id+`","object":"subscription_item"}]}}`)
}

func (f *fakeStripeAPI) since(n int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls[n:]...)
}

func (f *fakeStripeAPI) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type subscriptionWrite struct {
	name  string
	serve func(handlers) http.HandlerFunc
	body  string
}

var subscriptionWrites = []subscriptionWrite{
	{"change-plan", func(h handlers) http.HandlerFunc { return h.changePlan }, `{"price_id":"price_new"}`},
	{"reactivate", func(h handlers) http.HandlerFunc { return h.reactivateSubscription }, ""},
	{"cancel", func(h handlers) http.HandlerFunc { return h.cancelSubscription }, `{}`},
}

type writeRig struct {
	h      handlers
	db     *writeDB
	stripe *fakeStripeAPI
}

func newWriteRig(t *testing.T) *writeRig {
	t.Helper()
	fake := &fakeStripeAPI{}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	db := &writeDB{roles: map[[2]uuid.UUID]string{}, subs: map[uuid.UUID]string{}}
	h := handlers{db: db, logger: slog.New(slog.DiscardHandler),
		stripe: stripeclient.New(stripeclient.Options{Key: "sk_test_unit", BaseURL: server.URL, HTTPClient: server.Client()})}
	return &writeRig{h: h, db: db, stripe: fake}
}

// do serves one write as user, with org_id when org is set, in ctx's extras (an impersonation session).
func (rig *writeRig) do(t *testing.T, write subscriptionWrite, user *policy.User, org *uuid.UUID, extra func(context.Context) context.Context) (int, string, []string) {
	t.Helper()
	target := "/api/v1/billing/subscriptions/" + write.name
	if org != nil {
		target += "?" + url.Values{"org_id": {org.String()}}.Encode()
	}
	r := httptest.NewRequest(http.MethodPost, target, strings.NewReader(write.body))
	ctx := policy.WithUser(r.Context(), user)
	if write.body != "" {
		r.Header.Set("Content-Type", "application/json")
		body, _, failure, err := pybody.Read(r)
		if err != nil || failure != nil {
			t.Fatalf("%s: body read: %v %v", write.name, err, failure)
		}
		ctx = policy.WithBody(ctx, body)
	}
	if extra != nil {
		ctx = extra(ctx)
	}
	before := rig.stripe.count()
	w := httptest.NewRecorder()
	write.serve(rig.h)(w, r.WithContext(ctx))
	return w.Code, w.Body.String(), rig.stripe.since(before)
}

// touches reports whether there were Stripe calls and every one names stripeID.
func touches(calls []string, stripeID string) bool {
	for _, call := range calls {
		if !strings.Contains(call, "/v1/subscriptions/"+stripeID) {
			return false
		}
	}
	return len(calls) > 0
}

// CHAOS-13143: an admin of org C (the token's org) who holds only a non-admin role in org A must not change,
// cancel or reactivate A's subscription by naming A in org_id: the Admin level judged the role in C only.
// Owner and admin of A pass; no membership in A keeps today's 403; a write without org_id, or with
// org_id = the token's org, keeps today's answer.
func TestSubscriptionWritesNeedAnAdminRoleInTheServedOrg(t *testing.T) {
	orgA, orgC := uuid.New(), uuid.New()
	for _, write := range subscriptionWrites {
		for _, c := range []struct {
			name     string
			roleInA  string // "" = no membership in A
			org      *uuid.UUID
			want     int
			stripeOn string // the subscription the Stripe calls name; "" = no Stripe call
		}{
			{"viewer of A, org_id=A", "viewer", &orgA, http.StatusForbidden, ""},
			{"member of A, org_id=A", "member", &orgA, http.StatusForbidden, ""},
			{"admin of A, org_id=A", "admin", &orgA, http.StatusOK, "sub_A"},
			{"owner of A, org_id=A", "owner", &orgA, http.StatusOK, "sub_A"},
			{"no membership in A, org_id=A", "", &orgA, http.StatusForbidden, ""},
			{"viewer of A, no org_id (served = token org C)", "viewer", nil, http.StatusOK, "sub_C"},
			{"viewer of A, org_id=C (the token org)", "viewer", &orgC, http.StatusOK, "sub_C"},
		} {
			t.Run(write.name+"/"+c.name, func(t *testing.T) {
				rig := newWriteRig(t)
				user := &policy.User{ID: uuid.New(), OrgID: orgC.String(), Role: "admin"}
				rig.db.roles[[2]uuid.UUID{user.ID, orgC}] = "admin"
				if c.roleInA != "" {
					rig.db.roles[[2]uuid.UUID{user.ID, orgA}] = c.roleInA
				}
				rig.db.subs[orgA], rig.db.subs[orgC] = "sub_A", "sub_C"
				status, body, calls := rig.do(t, write, user, c.org, nil)
				checkWrite(t, status, body, calls, c.want, c.stripeOn)
				if rig.db.execs != 0 {
					t.Errorf("%d database write(s), want 0", rig.db.execs)
				}
			})
		}
	}
}

func checkWrite(t *testing.T, status int, body string, calls []string, want int, stripeOn string) {
	t.Helper()
	if status != want {
		t.Errorf("status %d %s, want %d", status, body, want)
	}
	if stripeOn == "" {
		if len(calls) != 0 {
			t.Errorf("%d Stripe call(s) %v, want none", len(calls), calls)
		}
		return
	}
	if !touches(calls, stripeOn) {
		t.Errorf("Stripe calls %v, want calls on %s only", calls, stripeOn)
	}
}

// The handler reads no enforce switch: the caller the legacy guard hands it (role from the token org's
// membership row) and the caller an enforce allow hands it (role claim as signed) get the same refusal in an
// org where they are only a viewer.
func TestSubscriptionWritesServedOrgRefusalIsTheSameWithEnforceOnAndOff(t *testing.T) {
	orgA, orgC := uuid.New(), uuid.New()
	for _, write := range subscriptionWrites {
		for _, shape := range []struct {
			name string
			role string
		}{{"enforce off (legacy guard: role of the token org's row)", "admin"}, {"enforce on (allow: role claim as signed)", "owner"}} {
			t.Run(write.name+"/"+shape.name, func(t *testing.T) {
				rig := newWriteRig(t)
				user := &policy.User{ID: uuid.New(), OrgID: orgC.String(), Role: shape.role}
				rig.db.roles[[2]uuid.UUID{user.ID, orgC}] = "admin"
				rig.db.roles[[2]uuid.UUID{user.ID, orgA}] = "viewer"
				rig.db.subs[orgA] = "sub_A"
				status, body, calls := rig.do(t, write, user, &orgA, nil)
				checkWrite(t, status, body, calls, http.StatusForbidden, "")
				if !strings.Contains(body, "Admin access required") {
					t.Errorf("body %s, want the Admin refusal", body)
				}
			})
		}
	}
}

// A holder of admin.billing acts on any org (resolveOrg admits it without a membership); a caller that
// holds every other platform action but not admin.billing needs the admin role in the served org.
func TestSubscriptionWritesServedOrgRoleByPlatformAction(t *testing.T) {
	orgA, orgD := uuid.New(), uuid.New()
	only := rule.PlatformOf(rule.ActionAdminBilling)
	for _, write := range subscriptionWrites {
		for _, c := range []struct {
			name     string
			user     func(id uuid.UUID) *policy.User
			roleInA  string
			want     int
			stripeOn string
		}{
			{"only admin.billing, no membership", func(id uuid.UUID) *policy.User {
				return &policy.User{ID: id, OrgID: orgD.String(), Platform: &only}
			}, "", http.StatusOK, "sub_A"},
			{"the superuser flag only", func(id uuid.UUID) *policy.User {
				return &policy.User{ID: id, OrgID: orgD.String(), IsSuperuser: true}
			}, "", http.StatusOK, "sub_A"},
			{"every action but admin.billing, viewer of A", func(id uuid.UUID) *policy.User {
				return &policy.User{ID: id, OrgID: orgD.String(), IsSuperuser: true, Platform: allBut(rule.ActionAdminBilling)}
			}, "viewer", http.StatusForbidden, ""},
			{"every action but admin.billing, admin of A", func(id uuid.UUID) *policy.User {
				return &policy.User{ID: id, OrgID: orgD.String(), IsSuperuser: true, Platform: allBut(rule.ActionAdminBilling)}
			}, "admin", http.StatusOK, "sub_A"},
		} {
			t.Run(write.name+"/"+c.name, func(t *testing.T) {
				rig := newWriteRig(t)
				user := c.user(uuid.New())
				if c.roleInA != "" {
					rig.db.roles[[2]uuid.UUID{user.ID, orgA}] = c.roleInA
				}
				rig.db.subs[orgA] = "sub_A"
				status, body, calls := rig.do(t, write, user, &orgA, nil)
				checkWrite(t, status, body, calls, c.want, c.stripeOn)
			})
		}
	}
}

// The invoice void is the other billing write that resolves an org: its org_id counts only for a holder of
// admin.billing, so for anyone else it serves the token's org, the org the Admin level judged.
func TestVoidInvoiceServesTheTokenOrgForAViewerOfTheOrgIDOrg(t *testing.T) {
	orgA, orgC := uuid.New(), uuid.New()
	user := &policy.User{ID: uuid.New(), OrgID: orgC.String(), Role: "admin"}
	db := &writeDB{roles: map[[2]uuid.UUID]string{{user.ID, orgC}: "admin", {user.ID, orgA}: "viewer"}}
	got, refusal, err := invoiceOrg(context.Background(), handlers{logger: slog.New(slog.DiscardHandler)}, db, user, &orgA)
	if err != nil || refusal != nil || got == nil || *got != orgC {
		t.Fatalf("served org %v, refusal %v, err %v; want the token org %s", got, refusal, err, orgC)
	}
}
