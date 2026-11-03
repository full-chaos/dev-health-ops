//go:build integration

package authapi

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// TestSessionStateReadsAuthServicesOwnDecisionAgainstRealPostgres drives the
// state route over a real auth schema. Every session is made by the session
// store auth-service runs; the route answers each one's owner, organization,
// effective revoke (an explicit revoke, a password change after it) and
// effective expiry (the absolute cap, the idle expiry), as Current decides
// them, and writes nothing (no last_seen_at stamp, so a read never extends the
// idle expiry).
func TestSessionStateReadsAuthServicesOwnDecisionAgainstRealPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	p := newPreflightStore(t, ctx)
	user, other := p.principal(t, ctx), p.principal(t, ctx)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	org := uuid.New()
	if err := audit.Commit(ctx, p.admin, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES ($1, $2, $2)`, org, "state-"+org.String())
			return err
		},
		Audit:  audit.AuditEvent{EventType: "organization.created", Outcome: audit.OutcomeAllowed},
		Events: []audit.OutboxEvent{{AggregateType: "organization", AggregateID: "org_" + org.String(), EventType: "organization.updated"}},
	}); err != nil {
		t.Fatalf("seed organization: %v", err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	store := func(lifetime, refresh time.Duration) *session.Store {
		s, err := session.New(session.Config{Pool: p.admin, Schema: schema, SessionLifetime: lifetime, RefreshTTL: refresh,
			SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(key))})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	create := func(s *session.Store, principal uuid.UUID, organization *uuid.UUID) uuid.UUID {
		issued, err := s.Create(ctx, principal, organization, []string{"pwd"})
		if err != nil {
			t.Fatalf("create session: %v", err)
		}
		return issued.Session.ID
	}
	const life = 2 * time.Second
	live := create(p.store, user, &org)
	unbound := create(p.store, user, nil)
	revoked := create(p.store, user, &org)
	if ok, err := p.store.Revoke(ctx, revoked, session.Actor{}); err != nil || !ok {
		t.Fatalf("revoke: %v %v", ok, err)
	}
	stamped := create(p.store, other, &org)
	if _, err := p.admin.Exec(ctx, `INSERT INTO auth.users (principal_id, email, email_lower, password_updated_at)
		VALUES ($1, $2, $2, clock_timestamp())
		ON CONFLICT (principal_id) DO UPDATE SET password_updated_at = EXCLUDED.password_updated_at`,
		other.String(), other.String()+"@example.com"); err != nil {
		t.Fatalf("password stamp: %v", err)
	}
	absolute := create(store(life, 0), user, &org)
	idle := create(store(0, life), user, &org)

	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := newRecorder(slog.New(slog.DiscardHandler), sdkmetric.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	h := compatHandlers{CompatDeps: CompatDeps{Sessions: p.store, Issuer: issuer,
		ServiceCredentials: &fakeServiceCredentials{id: uuid.NewString()}}, rec: rec}
	server := serveRoutes(h.withSessionStateRouteRows(nil, stateRowsDocument))
	read := func(t *testing.T, id uuid.UUID) map[string]any {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, PatternSessionState, strings.NewReader(stateBody(id.String())))
		request.Header.Set("Authorization", "Bearer "+issuerToken)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		var answer map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil || response.Code != http.StatusOK {
			t.Fatalf("%d %s: %v", response.Code, response.Body, err)
		}
		return answer
	}
	lastSeen := func(id uuid.UUID) *time.Time {
		var at *time.Time
		if err := p.admin.QueryRow(ctx, `SELECT last_seen_at FROM auth.sessions WHERE id = $1`, id).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	idleSeen := lastSeen(idle)
	for name, tc := range map[string]struct {
		id        uuid.UUID
		principal uuid.UUID
		org       any
		revoked   bool
	}{
		"live, bound":                  {live, user, org.String(), false},
		"bound to no organization":     {unbound, user, nil, false},
		"revoked":                      {revoked, user, org.String(), true},
		"older than a password change": {stamped, other, org.String(), true},
		"absolute lifetime":            {absolute, user, org.String(), false},
		"refresh lifetime":             {idle, user, org.String(), false},
	} {
		got := read(t, tc.id)
		if got["principal_id"] != tc.principal.String() || got["organization_id"] != tc.org || got["revoked"] != tc.revoked || got["expired"] != false {
			t.Fatalf("%s (before the lifetimes end): %v", name, got)
		}
		if _, pinned := got["legacy_token_version"].(float64); !pinned {
			t.Fatalf("%s: no token_version pin in %v", name, got)
		}
	}
	time.Sleep(life + 500*time.Millisecond)
	for name, tc := range map[string]struct {
		id      uuid.UUID
		expired bool
	}{
		"live, bound":       {live, false},
		"absolute lifetime": {absolute, true},
		"refresh lifetime":  {idle, true},
	} {
		if got := read(t, tc.id); got["expired"] != tc.expired {
			t.Fatalf("%s after %s: expired = %v, want %v", name, life, got["expired"], tc.expired)
		}
	}
	if after := lastSeen(idle); (after == nil) != (idleSeen == nil) || (after != nil && !after.Equal(*idleSeen)) {
		t.Fatalf("a state read moved last_seen_at from %v to %v", idleSeen, after)
	}
	request := httptest.NewRequest(http.MethodPost, PatternSessionState, strings.NewReader(stateBody(uuid.NewString())))
	request.Header.Set("Authorization", "Bearer "+issuerToken)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), `"reason_code":"`+reasonSessionNotFound+`"`) {
		t.Fatalf("an unknown session: %d %s, want 404 %s", response.Code, response.Body, reasonSessionNotFound)
	}
}
