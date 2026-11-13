package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

type roleRow struct {
	role   string
	member bool
}

type roleStore struct {
	rows map[uuid.UUID]roleRow
	err  error
}

func (s roleStore) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{}, false, nil
}

func (s roleStore) Membership(_ context.Context, _, org uuid.UUID) (string, bool, error) {
	if s.err != nil {
		return "", false, s.err
	}
	row := s.rows[org]
	return row.role, row.member, nil
}

func (s roleStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
}

func (s roleStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

func (s roleStore) ReadsPlatformRoles() bool { return false }

// reachedWrite drives one route of the real route table with a nil pool: a
// refused caller answers before any pool use, a served caller reaches
// setOptIn and panics on the nil pool.
func reachedWrite(t *testing.T, h handlers, method, path string, r *http.Request) (status int, reached bool) {
	t.Helper()
	var handler http.Handler
	for _, route := range h.routeTable(func(next http.Handler) http.Handler { return next }, nil) {
		if route.Method == method && route.Pattern == path {
			handler = route.Handler
		}
	}
	if handler == nil {
		t.Fatalf("no route %s %s", method, path)
	}
	w := httptest.NewRecorder()
	func() {
		defer func() {
			if recover() != nil {
				reached = true
			}
		}()
		handler.ServeHTTP(w, r)
	}()
	return w.Code, reached
}

func TestOptInOptOutNeedOrgAdminOrOwnerOfTheResolvedOrg(t *testing.T) {
	orgA, orgB := uuid.New(), uuid.New()
	user := uuid.New()
	only := rule.PlatformOf(rule.ActionAdminTelemetry)
	var others []string
	for _, a := range rule.PlatformActions {
		if a != rule.ActionAdminTelemetry {
			others = append(others, a)
		}
	}
	rest := rule.PlatformOf(others...)
	row := func(role string) map[uuid.UUID]roleRow {
		return map[uuid.UUID]roleRow{orgA: {role, true}}
	}
	cases := []struct {
		name    string
		user    policy.User
		rows    map[uuid.UUID]roleRow
		err     error
		header  string
		session *policy.Impersonation
		want    int
		reached bool
	}{
		{"viewer row", policy.User{OrgID: orgA.String()}, row("viewer"), nil, "", nil, 403, false},
		{"member row", policy.User{OrgID: orgA.String()}, row("member"), nil, "", nil, 403, false},
		{"admin row", policy.User{OrgID: orgA.String()}, row("admin"), nil, "", nil, 0, true},
		{"owner row", policy.User{OrgID: orgA.String()}, row("owner"), nil, "", nil, 0, true},
		{"uppercase role", policy.User{OrgID: orgA.String()}, row("Admin"), nil, "", nil, 403, false},
		{"empty role", policy.User{OrgID: orgA.String()}, row(""), nil, "", nil, 403, false},
		{"token org without a row", policy.User{OrgID: orgA.String()}, nil, nil, "", nil, 403, false},
		{"platform admin flag, no row", policy.User{OrgID: orgA.String(), IsSuperuser: true}, nil, nil, "", nil, 0, true},
		{"platform admin admin.telemetry only, no row", policy.User{OrgID: orgA.String(), Platform: &only}, nil, nil, "", nil, 0, true},
		{"platform admin, no row, header org", policy.User{OrgID: orgA.String(), IsSuperuser: true}, nil, nil, orgB.String(), nil, 0, true},
		{"platform role without admin.telemetry, no row", policy.User{OrgID: orgA.String(), IsSuperuser: true, Platform: &rest}, nil, nil, "", nil, 403, false},
		{"header org where the caller is admin, token org viewer", policy.User{OrgID: orgA.String(), Role: "viewer"},
			map[uuid.UUID]roleRow{orgA: {"viewer", true}, orgB: {"admin", true}}, nil, orgB.String(), nil, 0, true},
		{"header org where the caller is viewer, token org admin", policy.User{OrgID: orgA.String(), Role: "admin"},
			map[uuid.UUID]roleRow{orgA: {"admin", true}, orgB: {"viewer", true}}, nil, orgB.String(), nil, 403, false},
		{"no org", policy.User{}, nil, nil, "", nil, 403, false},
		{"header org, not a member", policy.User{OrgID: orgA.String()}, row("admin"), nil, orgB.String(), nil, 403, false},
		{"membership read fails", policy.User{OrgID: orgA.String()}, nil, errors.New("down"), "", nil, 500, false},
		{"impersonation, target org", policy.User{OrgID: orgB.String()}, nil, nil, "", &policy.Impersonation{TargetOrgID: orgA}, 0, true},
		{"impersonation, other header org", policy.User{OrgID: orgB.String()}, nil, nil, orgB.String(), &policy.Impersonation{TargetOrgID: orgA}, 403, false},
	}
	for _, c := range cases {
		for _, route := range []string{"/api/v1/telemetry/opt-in", "/api/v1/telemetry/opt-out"} {
			t.Run(c.name+" "+route, func(t *testing.T) {
				auth, err := policy.NewAuthenticator(roleStore{rows: c.rows, err: c.err}, slog.New(slog.DiscardHandler))
				if err != nil {
					t.Fatal(err)
				}
				h := handlers{auth: auth, logger: slog.New(slog.DiscardHandler)}
				u := c.user
				u.ID, u.UserID = user, user.String()
				r := httptest.NewRequest(http.MethodPost, route, nil)
				if c.header != "" {
					r.Header.Set("X-Org-Id", c.header)
				}
				ctx := policy.WithUser(r.Context(), &u)
				if c.session != nil {
					ctx = policy.WithImpersonation(ctx, c.session)
				}
				status, reached := reachedWrite(t, h, http.MethodPost, route, r.WithContext(ctx))
				if reached != c.reached || (!c.reached && status != c.want) {
					t.Errorf("status %d reached %v, want status %d reached %v", status, reached, c.want, c.reached)
				}
			})
		}
	}
}

func TestStatusStaysOpenToAnyOrgMember(t *testing.T) {
	orgA, user := uuid.New(), uuid.New()
	auth, _ := policy.NewAuthenticator(roleStore{rows: map[uuid.UUID]roleRow{orgA: {"viewer", true}}}, slog.New(slog.DiscardHandler))
	h := handlers{auth: auth, logger: slog.New(slog.DiscardHandler)}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/telemetry/status", nil)
	r = r.WithContext(policy.WithUser(r.Context(), &policy.User{ID: user, UserID: user.String(), OrgID: orgA.String()}))
	if status, reached := reachedWrite(t, h, http.MethodGet, "/api/v1/telemetry/status", r); !reached {
		t.Errorf("viewer GET /status: status %d, want it served", status)
	}
}
