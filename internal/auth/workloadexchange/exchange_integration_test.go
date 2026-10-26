//go:build integration

package workloadexchange

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// storeEnv: the registry is written through the operator pool; the exchange
// reads it and appends its audit row through the runtime role, as
// auth-service does.
type storeEnv struct {
	admin   *pgxpool.Pool
	runtime *pgxpool.Pool
	schema  authschema.ValidatedIdentifier
	writer  *workload.Store
	reader  *workload.Store
}

var (
	sharedStore     *storeEnv
	sharedStoreErr  error
	sharedStoreOnce sync.Once
	storeClosers    []func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(storeClosers) - 1; i >= 0; i-- {
		storeClosers[i]()
	}
	os.Exit(code)
}

func getStore(t *testing.T) *storeEnv {
	t.Helper()
	sharedStoreOnce.Do(func() { sharedStore, sharedStoreErr = newStoreEnv(t) })
	if sharedStoreErr != nil {
		t.Fatal(sharedStoreErr)
	}
	return sharedStore
}

func newStoreEnv(t *testing.T) (*storeEnv, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		return nil, err
	}
	storeClosers = append(storeClosers, func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		return nil, fmt.Errorf("admin pool: %w", err)
	}
	storeClosers = append(storeClosers, admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("exchange_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	const password = "exchange-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, fmt.Errorf("create role: %w", err)
	}
	storeClosers = append(storeClosers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		return nil, fmt.Errorf("authschema.Apply: %w", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("runtime pool: %w", err)
	}
	storeClosers = append(storeClosers, runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return nil, err
	}
	writer, err := workload.New(workload.Config{Pool: admin, Schema: schema})
	if err != nil {
		return nil, err
	}
	reader, err := workload.New(workload.Config{Pool: runtime, Schema: schema})
	if err != nil {
		return nil, err
	}
	return &storeEnv{admin: admin, runtime: runtime, schema: schema, writer: writer, reader: reader}, nil
}

type storeRegistry struct{ store *workload.Store }

func (r storeRegistry) Resolve(ctx context.Context, id Identity) (Registration, error) {
	reg, err := r.store.Resolve(ctx, workload.ResolveInput{
		TrustDomain: id.TrustDomain, Issuer: id.Issuer, Namespace: id.Namespace,
		ServiceAccountName: id.ServiceAccountName, ServiceAccountUID: id.ServiceAccountUID,
	})
	if err != nil {
		return Registration{}, err
	}
	return Registration{PrincipalID: reg.PrincipalID, BindingID: reg.BindingID, Revision: reg.Revision,
		Audiences: reg.Audiences, Narrow: reg.Narrow}, nil
}

// registeredExchange registers a workload bound to a fresh namespace and
// returns a harness whose TokenReview answers for that namespace and whose
// registry and audit are the real store and audit.Append.
func registeredExchange(t *testing.T, e *storeEnv) (*harness, string, uuid.UUID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	name := "x-" + uuid.NewString()[:8]
	id, err := e.writer.Register(ctx, workload.Spec{Name: name, Audiences: []string{"auth-service"}}, workload.Actor{})
	if err != nil {
		t.Fatal(err)
	}
	namespace := "ns-" + name
	if _, err := e.writer.AddBinding(ctx, name, workload.BindingSpec{
		Environment: "test", TrustDomain: testTrustDomain, Issuer: testClusterIssuer, Namespace: namespace,
		ServiceAccountName: testSAName, ServiceAccountUID: testSAUID,
	}, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, func(_ *harness, d *Deps) {
		d.Registry = storeRegistry{store: e.reader}
		d.Audit = func(ctx context.Context, event audit.AuditEvent) error {
			return audit.Append(ctx, e.runtime, e.schema, event)
		}
	})
	h.review.set(func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
		writeReview(w, http.StatusCreated, true, []string{ReviewAudience}, "system:serviceaccount:"+namespace+":"+testSAName, testSAUID)
	})
	return h, name, id
}

func TestTheExchangeIssuesThroughTheRuntimeRoleAndRecordsLastUse(t *testing.T) {
	e := getStore(t)
	h, _, id := registeredExchange(t, e)
	answer := decodeAnswer(t, h.post(form(h.validSubject(), "auth-service", "").Encode()))
	claims := verifyIssued(t, h, answer.AccessToken, "auth-service")
	ctx := context.Background()
	var attributes, subject string
	var lastUsed time.Time
	err := e.admin.QueryRow(ctx, `SELECT attributes::text, subject_principal_id::text, occurred_at FROM auth.security_audit_events
WHERE event_type = $1 AND attributes->>'jti' = $2`, audit.EventWorkloadTokenIssued, claims.TokenID).Scan(&attributes, &subject, &lastUsed)
	if err != nil {
		t.Fatalf("the issued-token audit row: %v", err)
	}
	if subject != id.String() {
		t.Fatalf("audit subject %s, want the workload %s", subject, id)
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(attributes), &row); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if got := strings.Join(keys, ","); got != "actions,audience,binding_id,jti,principal_id,revision" {
		t.Fatalf("audit attribute keys %s", got)
	}
	for _, marker := range []string{subjectMarker, reviewerMarker} {
		if strings.Contains(attributes, marker) {
			t.Fatal("the audit row carries a credential marker")
		}
	}
	for _, part := range strings.Split(answer.AccessToken, ".") {
		if strings.Contains(attributes, part) {
			t.Fatal("the audit row carries a segment of the issued token")
		}
	}
	// last_used of the binding is the newest issued row for it.
	var newest time.Time
	if err := e.admin.QueryRow(ctx, `SELECT max(occurred_at) FROM auth.security_audit_events
WHERE event_type = $1 AND attributes->>'binding_id' = $2`, audit.EventWorkloadTokenIssued, row["binding_id"]).Scan(&newest); err != nil {
		t.Fatal(err)
	}
	if !newest.Equal(lastUsed) {
		t.Fatalf("last used %s, want the issued row's %s", newest, lastUsed)
	}
}

func TestADisabledWorkloadIsRefusedAtOnce(t *testing.T) {
	e := getStore(t)
	h, name, _ := registeredExchange(t, e)
	decodeAnswer(t, h.post(form(h.validSubject(), "auth-service", "").Encode()))
	if err := e.writer.Disable(context.Background(), name, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	rec := h.post(form(h.validSubject(), "auth-service", "").Encode())
	if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
		t.Fatalf("after disable: %d %q, want the uniform 400", rec.Code, rec.Body.String())
	}
	if got := h.metricPoints()["refused/disabled/auth-service"]; got != 1 {
		t.Fatalf("counter refused/disabled = %d; points %v", got, h.metricPoints())
	}
}

func TestAnUnboundServiceAccountIsRefused(t *testing.T) {
	e := getStore(t)
	h, _, _ := registeredExchange(t, e)
	h.review.set(func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
		writeReview(w, http.StatusCreated, true, []string{ReviewAudience}, "system:serviceaccount:other-ns:"+testSAName, testSAUID)
	})
	rec := h.post(form(h.validSubject(), "auth-service", "").Encode())
	if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
		t.Fatalf("unbound: %d %q", rec.Code, rec.Body.String())
	}
	if got := h.metricPoints()["refused/unknown/auth-service"]; got != 1 {
		t.Fatalf("counter refused/unknown = %d; points %v", got, h.metricPoints())
	}
}
