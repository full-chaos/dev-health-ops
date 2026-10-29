//go:build integration

package workloadexchange

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

type storeBootstrap struct{ store *workload.Store }

func (r storeBootstrap) ResolveBootstrap(ctx context.Context, digest [sha256.Size]byte, now time.Time) (BootstrapRegistration, error) {
	reg, err := r.store.ResolveBootstrap(ctx, digest, now)
	if err != nil {
		return BootstrapRegistration{}, err
	}
	return BootstrapRegistration{
		Registration: Registration{PrincipalID: reg.PrincipalID, Revision: reg.Revision, Audiences: reg.Audiences, Narrow: reg.Narrow},
		CredentialID: reg.CredentialID, ExpiresAt: reg.ExpiresAt,
	}, nil
}

// bootstrapExchange registers a workload, stores one bootstrap credential for
// it (its digest only, through the operator pool) and returns a
// bootstrap-only harness reading the real store and appending the real audit
// row through the runtime role.
func bootstrapExchange(t *testing.T, e *storeEnv) (*harness, string, uuid.UUID, uuid.UUID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	name := "b-" + uuid.NewString()[:8]
	principal, err := e.writer.Register(ctx, workload.Spec{Name: name, Audiences: []string{"auth-service"}}, workload.Actor{})
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	value := workload.BootstrapPrefix + base64.RawURLEncoding.EncodeToString(raw)
	digest := workload.BootstrapDigest(value)
	var credential uuid.UUID
	if err := e.admin.QueryRow(ctx, `INSERT INTO auth.credential_registry
(principal_id, credential_class, display_prefix, secret_hash, hash_algorithm, audiences, fingerprint, created_at, updated_at)
VALUES ($1, $2, 'fcwlb_', $3, $4, ARRAY['auth-service']::text[], $5, now() - interval '1 hour', now() - interval '1 hour')
RETURNING id`, principal, workload.BootstrapClass, digest[:], workload.BootstrapHashAlgorithm, "00000000"+uuid.NewString()[:8]).Scan(&credential); err != nil {
		t.Fatalf("store the credential: %v", err)
	}
	h := newHarness(t, withoutKubernetes(), func(h *harness, d *Deps) {
		d.Bootstrap = storeBootstrap{store: e.reader}
		d.Audit = func(ctx context.Context, event audit.AuditEvent) error {
			return audit.Append(ctx, e.runtime, e.schema, event)
		}
	})
	h.now = time.Now().UTC().Truncate(time.Second)
	return h, value, principal, credential
}

func TestABootstrapExchangeIssuesThroughTheRuntimeRoleAndRecordsLastUse(t *testing.T) {
	e := getStore(t)
	h, value, principal, credential := bootstrapExchange(t, e)
	answer := decodeAnswer(t, h.post(bootstrapForm(value, "auth-service", "").Encode()))
	claims := verifyIssued(t, h, answer.AccessToken, "auth-service")
	ctx := context.Background()
	var attributes, subject string
	var issuedAt time.Time
	if err := e.admin.QueryRow(ctx, `SELECT attributes::text, subject_principal_id::text, occurred_at FROM auth.security_audit_events
WHERE event_type = $1 AND attributes->>'jti' = $2`, audit.EventWorkloadTokenIssued, claims.TokenID).Scan(&attributes, &subject, &issuedAt); err != nil {
		t.Fatalf("the issued-token audit row: %v", err)
	}
	if subject != principal.String() {
		t.Fatalf("audit subject %s, want the workload %s", subject, principal)
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
	if got := strings.Join(keys, ","); got != "actions,audience,credential_id,jti,principal_id,revision" || row["credential_id"] != credential.String() {
		t.Fatalf("audit attributes %s", attributes)
	}
	if strings.Contains(attributes, value) || strings.Contains(attributes, strings.TrimPrefix(value, workload.BootstrapPrefix)) {
		t.Fatal("the audit row carries the bootstrap value")
	}
	// last_used of the credential is the newest issued row naming it; the
	// registry row itself is not written.
	var newest time.Time
	var lastUsed *time.Time
	if err := e.admin.QueryRow(ctx, `SELECT max(occurred_at) FROM auth.security_audit_events
WHERE event_type = $1 AND attributes->>'credential_id' = $2`, audit.EventWorkloadTokenIssued, credential.String()).Scan(&newest); err != nil {
		t.Fatal(err)
	}
	if !newest.Equal(issuedAt) {
		t.Fatalf("last used %s, want the issued row's %s", newest, issuedAt)
	}
	if err := e.admin.QueryRow(ctx, `SELECT last_used_at FROM auth.credential_registry WHERE id = $1`, credential).Scan(&lastUsed); err != nil || lastUsed != nil {
		t.Fatalf("the exchange wrote the registry row's last_used_at (%v, %v)", lastUsed, err)
	}
}

func TestARevokedBootstrapCredentialIsRefusedAtOnce(t *testing.T) {
	e := getStore(t)
	h, value, _, credential := bootstrapExchange(t, e)
	decodeAnswer(t, h.post(bootstrapForm(value, "auth-service", "").Encode()))
	if _, err := e.admin.Exec(context.Background(), `UPDATE auth.credential_registry SET revoked_at = now(), revoked_reason = 'owner_request' WHERE id = $1`, credential); err != nil {
		t.Fatal(err)
	}
	rec := h.post(bootstrapForm(value, "auth-service", "").Encode())
	if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
		t.Fatalf("after revoke: %d %q, want the uniform 400", rec.Code, rec.Body.String())
	}
	if got := h.bootstrapPoints()["refused/revoked/auth-service"]; got != 1 {
		t.Fatalf("counter refused/revoked = %d; points %v", got, nonZero(h.bootstrapPoints()))
	}
}

func TestADisabledBootstrapWorkloadIsRefusedAtOnce(t *testing.T) {
	e := getStore(t)
	h, value, principal, _ := bootstrapExchange(t, e)
	decodeAnswer(t, h.post(bootstrapForm(value, "auth-service", "").Encode()))
	var name string
	if err := e.admin.QueryRow(context.Background(), `SELECT name FROM auth.workloads WHERE principal_id = $1`, principal).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if err := e.writer.Disable(context.Background(), name, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	rec := h.post(bootstrapForm(value, "auth-service", "").Encode())
	if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
		t.Fatalf("after disable: %d %q, want the uniform 400", rec.Code, rec.Body.String())
	}
	if got := h.bootstrapPoints()["refused/disabled/auth-service"]; got != 1 {
		t.Fatalf("counter refused/disabled = %d; points %v", got, nonZero(h.bootstrapPoints()))
	}
}
