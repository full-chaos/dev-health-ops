//go:build integration

package authapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/outboxprojection"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// acrLive is the route over a real auth schema: the runtime role, the real
// workload row, the real engine.
type acrLive struct {
	*preflightStore
	ctx       context.Context
	org       uuid.UUID
	identity  *identity.Store
	workloads *workload.Store
	signer    *token.Signer
	subject   string
	principal uuid.UUID
	reader    *sdkmetric.ManualReader
	server    http.Handler
}

func newAcrLive(t *testing.T, fenced bool) *acrLive {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	p := newPreflightStoreOwned(t, ctx, true)
	if fenced {
		if _, err := restorefence.Init(ctx, p.admin, p.schema, restorefence.InitOptions{IfAbsent: true}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := identity.New(identity.Config{Pool: p.runtime, Schema: p.schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	seed, err := workload.New(workload.Config{Pool: p.admin, Schema: p.schema, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := seed.Register(ctx, workload.Spec{Name: "acr-api", Audiences: []string{"auth-service"}, AllowedActions: []string{acrEventsAction}}, workload.Actor{})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	reads, err := workload.New(workload.Config{Pool: p.runtime, Schema: p.schema, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	signer, _, _ := fileSigner(t)
	subject := session.WirePrincipalID(principal)
	caller, err := NewSessionIssueWorkload(SessionIssueWorkloadConfig{IssuerURL: goldenIssuer, Keys: signer, Subject: subject})
	if err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	engine, err := outboxprojection.New(outboxprojection.Config{Pool: p.runtime, Schema: p.schema, Meter: meter,
		ProjectOrganization: id.EnsureOrganizationFound})
	if err != nil {
		t.Fatal(err)
	}
	routes, err := acrEventRoutes(AcrEventDeps{Workload: caller, Workloads: reads, Projector: engine, Meter: meter,
		Logger: slog.New(slog.DiscardHandler)}, acrEventRows(workloadTokenClass))
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	l := &acrLive{preflightStore: p, ctx: ctx, identity: id, workloads: seed, signer: signer, subject: subject, principal: principal, reader: reader, server: mux, org: uuid.New()}
	l.publicOrg(t, l.org)
	if err := id.ProjectOrganization(ctx, l.org); err != nil {
		t.Fatal(err)
	}
	return l
}

func (l *acrLive) publicOrg(t *testing.T, id uuid.UUID) {
	t.Helper()
	if _, err := l.admin.Exec(l.ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'acr-events')`, id, "org-"+id.String()[:8]); err != nil {
		t.Fatal(err)
	}
}

func (l *acrLive) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := l.admin.QueryRow(l.ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (l *acrLive) bearer(t *testing.T) string {
	t.Helper()
	now := time.Now()
	payload, err := json.Marshal(contracts.WorkloadTokenClaims{Iss: goldenIssuer, Aud: "auth-service", Sub: l.subject, PrincipalType: "workload",
		Jti: uuid.NewString(), Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300})
	if err != nil {
		t.Fatal(err)
	}
	compact, _, err := l.signer.Sign(l.ctx, "workload+jwt", payload)
	if err != nil {
		t.Fatal(err)
	}
	return "Bearer " + compact
}

func (l *acrLive) post(t *testing.T, body string) (*httptest.ResponseRecorder, *acrCountingBody) {
	t.Helper()
	counted := &acrCountingBody{inner: strings.NewReader(body)}
	request := httptest.NewRequest(http.MethodPost, PatternAcrCredentialEvents, counted).WithContext(l.ctx)
	request.Header.Set("Authorization", l.bearer(t))
	response := httptest.NewRecorder()
	l.server.ServeHTTP(response, request)
	return response, counted
}

func (l *acrLive) want(t *testing.T, body string, status int, reason string) {
	t.Helper()
	response, _ := l.post(t, body)
	if response.Code != status {
		t.Fatalf("status %d, want %d: %s", response.Code, status, response.Body)
	}
	if reason != "" && jsonBody(t, response)["reason_code"] != reason {
		t.Fatalf("reason: %s, want %s", response.Body, reason)
	}
}

type acrEvent struct {
	id, kind string
	org      uuid.UUID
	revision int
	at       string
	change   string
}

func (e acrEvent) String() string {
	actor := `"actor_type":"operator","actor_ref":"operator_EXAMPLE00000001"`
	if e.kind == "expired" {
		actor = `"actor_type":"system"`
	}
	at := e.at
	if at == "" {
		at = "2026-10-23T10:00:00Z"
	}
	return fmt.Sprintf(`{"schema_version":"acr-credential-lifecycle-event.v1","aggregate_type":"acr_credential","aggregate_id":%q,`+
		`"event_type":"acr_credential.%s","idempotency_key":"acr_credential:%s:%d","created_at":%q,`+
		`"payload":{"revision":%d,"organization_id":"org_%s",%s,"change":%s}}`,
		e.id, e.kind, e.id, e.revision, at, e.revision, e.org, actor, e.change)
}

func acrID() string { return "cred_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:20] }

func (l *acrLive) issued(id string) string {
	return acrEvent{id: id, kind: "issued", org: l.org, revision: 1,
		change: `{"status":"active","acr_class":"org_bearer","scopes":["context:read"],"owner_subject":"subject_EXAMPLE0000000001"}`}.String()
}

func (l *acrLive) revoked(id string, revision int) string {
	return acrEvent{id: id, kind: "revoked", org: l.org, revision: revision, change: `{"status":"revoked"}`}.String()
}

func (l *acrLive) expired(id string, revision int) string {
	return acrEvent{id: id, kind: "expired", org: l.org, revision: revision, change: `{"status":"expired","expires_at":"2026-10-23T10:15:00Z"}`}.String()
}

func (l *acrLive) rotated(id, parent, scopes string) string {
	return acrEvent{id: id, kind: "rotated", org: l.org, revision: 1,
		change: fmt.Sprintf(`{"status":"active","acr_class":"org_bearer","scopes":%s,"rotation_parent_id":%q}`, scopes, parent)}.String()
}

func (l *acrLive) projected(t *testing.T, id string) (revision int, status string) {
	t.Helper()
	if err := l.admin.QueryRow(l.ctx, `SELECT revision, status FROM auth.credential_projection WHERE domain_owner = 'acr' AND external_reference = $1`, id).Scan(&revision, &status); err != nil {
		t.Fatalf("projection of %s: %v", id, err)
	}
	return revision, status
}

func TestAcrEventsLiveRowRefuses(t *testing.T) {
	l := newAcrLive(t, true)
	for name, change := range map[string]struct{ break_, mend func() error }{
		"disabled": {func() error { return l.workloads.Disable(l.ctx, "acr-api", workload.Actor{}) },
			func() error { return l.workloads.Enable(l.ctx, "acr-api", workload.Actor{}) }},
		"action not listed": {func() error { return l.workloads.SetAllowedActions(l.ctx, "acr-api", []string{}, workload.Actor{}) },
			func() error {
				return l.workloads.SetAllowedActions(l.ctx, "acr-api", []string{acrEventsAction}, workload.Actor{})
			}},
		"audience not listed": {func() error {
			return l.workloads.SetAudiences(l.ctx, "acr-api", []string{"dev-health-api"}, workload.Actor{})
		},
			func() error {
				return l.workloads.SetAudiences(l.ctx, "acr-api", []string{"auth-service"}, workload.Actor{})
			}},
	} {
		if err := change.break_(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		response, body := l.post(t, l.issued(acrID()))
		if response.Code != http.StatusForbidden || jsonBody(t, response)["reason_code"] != reasonNotEntitled || body.reads != 0 {
			t.Fatalf("%s: %d, body reads %d: %s", name, response.Code, body.reads, response.Body)
		}
		if err := change.mend(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if got := l.count(t, `SELECT count(*) FROM auth.credential_projection`); got != 0 {
		t.Fatalf("projection rows = %d", got)
	}
	l.want(t, l.issued(acrID()), http.StatusNoContent, "")
}

func TestAcrEventsOrganizationProjection(t *testing.T) {
	l := newAcrLive(t, true)
	home := l.org
	t.Run("a real organization with no auth row applies", func(t *testing.T) {
		l.org = uuid.New()
		l.publicOrg(t, l.org)
		l.want(t, l.issued(acrID()), http.StatusNoContent, "")
		if got := l.count(t, `SELECT count(*) FROM auth.organizations WHERE id = $1`, l.org); got != 1 {
			t.Fatalf("auth rows = %d", got)
		}
	})
	t.Run("a suspended organization stays suspended", func(t *testing.T) {
		l.org = uuid.New()
		l.publicOrg(t, l.org)
		if err := l.identity.ProjectOrganization(l.ctx, l.org); err != nil {
			t.Fatal(err)
		}
		suspend(t, l)
		l.want(t, l.issued(acrID()), http.StatusNoContent, "")
		if got := l.count(t, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL`, l.org); got != 1 {
			t.Fatal("the organization is no longer suspended")
		}
	})
	t.Run("an organization in neither schema is refused", func(t *testing.T) {
		l.org = uuid.New()
		id := acrID()
		l.want(t, l.issued(id), http.StatusConflict, reasonProjectionRefused)
		if got := l.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1`, id); got != 0 {
			t.Fatalf("projection rows = %d", got)
		}
	})
	l.org = home
}

func suspend(t *testing.T, l *acrLive) {
	t.Helper()
	if _, err := l.admin.Exec(l.ctx, `ALTER TABLE auth.organizations DISABLE TRIGGER USER`); err != nil {
		t.Fatal(err)
	}
	if _, err := l.admin.Exec(l.ctx, `UPDATE auth.organizations SET disabled_at = now() WHERE id = $1`, l.org); err != nil {
		t.Fatal(err)
	}
	if _, err := l.admin.Exec(l.ctx, `ALTER TABLE auth.organizations ENABLE TRIGGER USER`); err != nil {
		t.Fatal(err)
	}
}

func TestAcrEventsIssuedWritesEveryRow(t *testing.T) {
	l := newAcrLive(t, true)
	id := acrID()
	l.want(t, l.issued(id), http.StatusNoContent, "")
	fingerprint := outboxprojection.AcrFingerprint(id)
	if got := l.count(t, `SELECT count(*) FROM auth.push_sources s JOIN auth.principals p ON p.id = s.principal_id
		WHERE s.organization_id = $1 AND s.source_id = 'acr' AND p.kind = 'external_source'`, l.org); got != 1 {
		t.Fatalf("source principals = %d", got)
	}
	if got := l.count(t, `SELECT count(*) FROM auth.credential_registry WHERE domain_owner = 'acr' AND external_reference = $1
		AND credential_class = 'acr_client_credential' AND hash_algorithm = 'fingerprint' AND fingerprint = $2
		AND secret_hash = decode($2, 'hex') AND octet_length(secret_hash) = 8 AND rotation_parent_id IS NULL AND cardinality(audiences) = 0`, id, fingerprint); got != 1 {
		t.Fatalf("registry rows = %d", got)
	}
	if got := l.count(t, `SELECT count(*) FROM auth.credential_projection WHERE domain_owner = 'acr' AND external_reference = $1
		AND organization_id = $2 AND revision = 1 AND status = 'active' AND acr_class = 'org_bearer' AND owner_subject = 'subject_EXAMPLE0000000001'`, id, l.org); got != 1 {
		t.Fatalf("projection rows = %d", got)
	}
	if got := l.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1 AND revision = 1`, id); got != 1 {
		t.Fatalf("digest rows = %d", got)
	}
	if got := l.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.credential_projection.decided'
		AND attributes->>'reason_code' = 'credential_issued' AND actor_principal_id = $1`, l.principal); got != 1 {
		t.Fatalf("audit rows = %d", got)
	}
	var rm metricdata.ResourceMetrics
	if err := l.reader.Collect(l.ctx, &rm); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "devhealth_auth_credential_projection_total" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("no engine counter")
	}
}

func TestAcrEventsSameRevision(t *testing.T) {
	l := newAcrLive(t, true)
	id := acrID()
	first := l.issued(id)
	l.want(t, first, http.StatusNoContent, "")
	audits := l.count(t, `SELECT count(*) FROM auth.security_audit_events`)
	l.want(t, first, http.StatusNoContent, "")
	if got := l.count(t, `SELECT count(*) FROM auth.security_audit_events`); got != audits {
		t.Fatalf("a duplicate wrote audit rows: %d, want %d", got, audits)
	}
	other := strings.Replace(first, "subject_EXAMPLE0000000001", "subject_EXAMPLE0000000002", 1)
	l.want(t, other, http.StatusConflict, reasonProjectionConflict)
	l.want(t, l.revoked(id, 2), http.StatusNoContent, "")
	l.want(t, other, http.StatusConflict, reasonProjectionConflict)
	if revision, status := l.projected(t, id); revision != 2 || status != "revoked" {
		t.Fatalf("projection = %d %s", revision, status)
	}
}

func TestAcrEventsGapThenOrder(t *testing.T) {
	l := newAcrLive(t, true)
	id := acrID()
	l.want(t, l.revoked(id, 2), http.StatusConflict, reasonProjectionRetry)
	if got := l.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1`, id); got != 0 {
		t.Fatalf("a gap wrote %d rows", got)
	}
	l.want(t, l.issued(id), http.StatusNoContent, "")
	l.want(t, l.revoked(id, 2), http.StatusNoContent, "")
	if revision, status := l.projected(t, id); revision != 2 || status != "revoked" {
		t.Fatalf("projection = %d %s", revision, status)
	}
}

func TestAcrEventsRotation(t *testing.T) {
	l := newAcrLive(t, true)
	t.Run("parent active", func(t *testing.T) {
		parent, child := acrID(), acrID()
		l.want(t, l.issued(parent), http.StatusNoContent, "")
		l.want(t, l.rotated(child, parent, `["context:read"]`), http.StatusNoContent, "")
		l.want(t, l.revoked(parent, 2), http.StatusNoContent, "")
	})
	t.Run("parent revoked first", func(t *testing.T) {
		parent, child := acrID(), acrID()
		l.want(t, l.issued(parent), http.StatusNoContent, "")
		l.want(t, l.revoked(parent, 2), http.StatusNoContent, "")
		l.want(t, l.rotated(child, parent, `["context:read"]`), http.StatusNoContent, "")
		if got := l.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND rotation_parent_id IS NULL`, child); got != 1 {
			t.Fatal("the registry row names a parent")
		}
		if got := l.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND rotation_parent_reference = $2`, child, parent); got != 1 {
			t.Fatal("the projection lost the lineage")
		}
	})
	t.Run("nil parent retries", func(t *testing.T) {
		child := acrID()
		l.want(t, l.rotated(child, acrID(), `["context:read"]`), http.StatusConflict, reasonProjectionRetry)
	})
	t.Run("a parent of another organization is no parent", func(t *testing.T) {
		parent, child := acrID(), acrID()
		l.want(t, l.issued(parent), http.StatusNoContent, "")
		home := l.org
		l.org = uuid.New()
		l.publicOrg(t, l.org)
		defer func() { l.org = home }()
		l.want(t, l.rotated(child, parent, `["context:read"]`), http.StatusConflict, reasonProjectionRetry)
	})
	t.Run("a widening is refused", func(t *testing.T) {
		parent, child := acrID(), acrID()
		l.want(t, l.issued(parent), http.StatusNoContent, "")
		l.want(t, l.rotated(child, parent, `["context:read","evidence:read"]`), http.StatusConflict, reasonProjectionRefused)
	})
}

func TestAcrEventsRevokedStaysRevoked(t *testing.T) {
	l := newAcrLive(t, true)
	id := acrID()
	l.want(t, l.issued(id), http.StatusNoContent, "")
	l.want(t, l.revoked(id, 2), http.StatusNoContent, "")
	l.want(t, l.expired(id, 3), http.StatusConflict, reasonProjectionRefused)
	if revision, status := l.projected(t, id); revision != 2 || status != "revoked" {
		t.Fatalf("projection = %d %s", revision, status)
	}
	if got := l.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND revoked_at IS NOT NULL AND revoked_reason = 'domain_revoked'`, id); got != 1 {
		t.Fatal("the registry row is not revoked with domain_revoked")
	}
}

func TestAcrEventsExpiry(t *testing.T) {
	l := newAcrLive(t, true)
	id := acrID()
	l.want(t, l.issued(id), http.StatusNoContent, "")
	l.want(t, l.expired(id, 2), http.StatusNoContent, "")
	if revision, status := l.projected(t, id); revision != 2 || status != "expired" {
		t.Fatalf("projection = %d %s", revision, status)
	}
	past := acrID()
	l.want(t, acrEvent{id: past, kind: "issued", org: l.org, revision: 1,
		change: `{"status":"active","acr_class":"org_bearer","scopes":["context:read"],"expires_at":"2026-01-01T00:00:00Z"}`}.String(), http.StatusNoContent, "")
	var expires string
	if err := l.admin.QueryRow(l.ctx, `SELECT expires_at FROM auth.credential_projection WHERE external_reference = $1`, past).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if pushlifecycle.Live(pushlifecycle.State{Revision: 1, Status: pushlifecycle.StatusActive, ExpiresAt: expires}, time.Now()) {
		t.Fatal("a past expires_at reads live with no event")
	}
}

func TestAcrEventsParallelDeliveryWritesOnce(t *testing.T) {
	l := newAcrLive(t, true)
	id := acrID()
	body := l.issued(id)
	var wg sync.WaitGroup
	codes := make([]int, 8)
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, _ := l.post(t, body)
			codes[i] = response.Code
		}()
	}
	wg.Wait()
	for _, code := range codes {
		if code != http.StatusNoContent {
			t.Fatalf("codes = %v", codes)
		}
	}
	if got := l.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, id); got != 1 {
		t.Fatalf("registry rows = %d", got)
	}
	if got := l.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.credential_projection.decided'`); got != 1 {
		t.Fatalf("audit rows = %d", got)
	}
}

// An event of another organization cannot touch a credential by its id.
func TestAcrEventsOrganizationIsolation(t *testing.T) {
	l := newAcrLive(t, true)
	id := acrID()
	l.want(t, l.issued(id), http.StatusNoContent, "")
	home := l.org
	l.org = uuid.New()
	l.publicOrg(t, l.org)
	l.want(t, l.issued(id), http.StatusConflict, reasonProjectionConflict)
	l.want(t, l.revoked(id, 2), http.StatusConflict, reasonProjectionRetry)
	l.org = home
	if revision, status := l.projected(t, id); revision != 1 || status != "active" {
		t.Fatalf("projection = %d %s", revision, status)
	}
	if got := l.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND revoked_at IS NULL`, id); got != 1 {
		t.Fatal("the registry row changed")
	}
}

func TestAcrEventsUnfencedAnswers503(t *testing.T) {
	l := newAcrLive(t, false)
	id := acrID()
	l.want(t, l.issued(id), http.StatusServiceUnavailable, reasonStoreUnavailable)
	if got := l.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1`, id); got != 0 {
		t.Fatalf("projection rows = %d", got)
	}
}

// The route needs no cursor row, and a gap parks nothing: the publisher holds.
func TestAcrEventsParkNothing(t *testing.T) {
	l := newAcrLive(t, true)
	id := acrID()
	l.want(t, l.revoked(id, 2), http.StatusConflict, reasonProjectionRetry)
	l.want(t, l.issued(id), http.StatusNoContent, "")
	if got := l.count(t, `SELECT (SELECT count(*) FROM auth.projection_cursor) + (SELECT count(*) FROM auth.projection_retry)`); got != 0 {
		t.Fatalf("cursor and retry rows = %d", got)
	}
}
