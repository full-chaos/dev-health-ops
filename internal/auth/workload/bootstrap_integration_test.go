//go:build integration

package workload

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// newBootstrapValue is a fresh value of the bootstrap shape, built at run time.
func newBootstrapValue(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return BootstrapPrefix + base64.RawURLEncoding.EncodeToString(raw)
}

type credentialRow struct {
	class     string
	audiences []string
	expiresAt *time.Time
	revoked   bool
}

// commitSeed runs one write to a guarded auth table (credential_registry, or
// principals.disabled_at) through audit.Commit on the operator pool, the only
// path the guards admit (the lifecycle verbs of a later change are the real
// writer). It returns the row id.
func commitSeed(t *testing.T, e *env, sql string, args ...any) uuid.UUID {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	var id uuid.UUID
	err = audit.Commit(ctxT(t), e.admin, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			return tx.QueryRow(ctx, sql, args...).Scan(&id)
		},
		Audit:     audit.AuditEvent{EventType: "credential.test", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamWorkloadRegistry,
	})
	if err != nil {
		t.Fatalf("guarded write through audit.Commit: %v", err)
	}
	return id
}

// insertCredential writes one credential_registry row for principal through
// audit.Commit and returns its id. The row stores the value's digest only.
func insertCredential(t *testing.T, e *env, principal uuid.UUID, value string, row credentialRow) uuid.UUID {
	t.Helper()
	if row.class == "" {
		row.class = BootstrapClass
	}
	if row.audiences == nil {
		row.audiences = []string{}
	}
	digest := BootstrapDigest(value)
	var revokedAt *time.Time
	var revokedReason *string
	if row.revoked {
		at, why := time.Now().Add(-time.Hour), "owner_request"
		revokedAt, revokedReason = &at, &why
	}
	return commitSeed(t, e, `INSERT INTO auth.credential_registry
(principal_id, credential_class, display_prefix, secret_hash, hash_algorithm, expires_at, revoked_at, revoked_reason,
 audiences, fingerprint, created_at, updated_at)
VALUES ($1, $2, 'fcwlb_', $3, $4, $5, $6, $7, $8::text[], $9, now() - interval '2 hours', now() - interval '2 hours')
RETURNING id`, principal, row.class, digest[:], BootstrapHashAlgorithm, row.expiresAt, revokedAt, revokedReason,
		row.audiences, strings.Repeat("0", 8)+uuid.NewString()[:8])
}

func registerWorkload(t *testing.T, e *env, audiences []string) (string, uuid.UUID) {
	t.Helper()
	n := name()
	id, err := e.writer.Register(ctxT(t), Spec{Name: n, Audiences: audiences}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	return n, id
}

func TestResolveBootstrapAcceptsALiveCredentialThroughTheRuntimeRole(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	_, principal := registerWorkload(t, e, []string{"auth-service", "dev-health-api"})
	value := newBootstrapValue(t)
	expires := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	credential := insertCredential(t, e, principal, value, credentialRow{audiences: []string{"dev-health-api"}, expiresAt: &expires})
	var xminBefore, rowBefore string
	if err := e.admin.QueryRow(ctx, `SELECT xmin::text, c::text FROM auth.credential_registry c WHERE id = $1`, credential).Scan(&xminBefore, &rowBefore); err != nil {
		t.Fatal(err)
	}
	reg, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(value), time.Now())
	if err != nil {
		t.Fatalf("ResolveBootstrap: %v", err)
	}
	if reg.PrincipalID != principal || reg.CredentialID != credential || !reg.ExpiresAt.Equal(expires) || reg.Revision < 1 ||
		reg.BindingID != uuid.Nil || !slices.Equal(reg.Audiences, []string{"dev-health-api"}) {
		t.Fatalf("registration %+v", reg)
	}
	if _, err := reg.Narrow("auth-service", nil); !errors.Is(err, ErrRefused) {
		t.Fatalf("Narrow to a workload audience outside the credential's: %v, want refused", err)
	}
	if scope, err := reg.Narrow("dev-health-api", nil); err != nil || len(scope) != 0 {
		t.Fatalf("Narrow to the credential's audience: %v %v", scope, err)
	}
	// The hot path writes nothing: the row (last_used_at and updated_at
	// included) and its tuple version are unchanged.
	var xminAfter, rowAfter string
	if err := e.admin.QueryRow(ctx, `SELECT xmin::text, c::text FROM auth.credential_registry c WHERE id = $1`, credential).Scan(&xminAfter, &rowAfter); err != nil {
		t.Fatal(err)
	}
	if xminAfter != xminBefore || rowAfter != rowBefore {
		t.Fatalf("ResolveBootstrap changed the credential row:\n%s\n%s", rowBefore, rowAfter)
	}
	if strings.Contains(rowAfter, value) || strings.Contains(rowAfter, strings.TrimPrefix(value, BootstrapPrefix)) {
		t.Fatal("the credential row holds the value")
	}
}

func TestResolveBootstrapRefusals(t *testing.T) {
	e := getEnv(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	past, future := now.Add(-time.Second), now.Add(time.Hour)
	for _, tc := range []struct {
		name      string
		audiences []string
		row       credentialRow
		after     func(t *testing.T, workloadName string, principal uuid.UUID)
		present   func(value string) string
		want      Reason
	}{
		{name: "unknown value", row: credentialRow{audiences: []string{"auth-service"}},
			present: func(string) string { return BootstrapPrefix + strings.Repeat("u", 43) }, want: ReasonUnknown},
		{name: "another class", row: credentialRow{class: "internal_svc_authsess_token", audiences: []string{"auth-service"}}, want: ReasonUnknown},
		{name: "revoked", row: credentialRow{audiences: []string{"auth-service"}, revoked: true}, want: ReasonRevoked},
		{name: "expired", row: credentialRow{audiences: []string{"auth-service"}, expiresAt: &past}, want: ReasonExpired},
		{name: "expires now", row: credentialRow{audiences: []string{"auth-service"}, expiresAt: &now}, want: ReasonExpired},
		{name: "workload disabled", row: credentialRow{audiences: []string{"auth-service"}, expiresAt: &future},
			after: func(t *testing.T, n string, _ uuid.UUID) {
				if err := e.writer.Disable(ctxT(t), n, Actor{}); err != nil {
					t.Fatal(err)
				}
			}, want: ReasonDisabled},
		{name: "principal disabled", row: credentialRow{audiences: []string{"auth-service"}},
			after: func(t *testing.T, _ string, p uuid.UUID) {
				commitSeed(t, e, `UPDATE auth.principals SET disabled_at = now() - interval '1 minute' WHERE id = $1 RETURNING id`, p)
			}, want: ReasonDisabled},
		{name: "credential with no audience", row: credentialRow{audiences: []string{}}, want: ReasonNoAudience},
		{name: "workload with no audience", audiences: []string{}, row: credentialRow{audiences: []string{"auth-service"}}, want: ReasonNoAudience},
		{name: "no shared audience", audiences: []string{"dev-health-api"}, row: credentialRow{audiences: []string{"auth-service"}}, want: ReasonNoAudience},
	} {
		t.Run(tc.name, func(t *testing.T) {
			audiences := tc.audiences
			if audiences == nil {
				audiences = []string{"auth-service"}
			}
			workloadName, principal := registerWorkload(t, e, audiences)
			value := newBootstrapValue(t)
			insertCredential(t, e, principal, value, tc.row)
			if tc.after != nil {
				tc.after(t, workloadName, principal)
			}
			presented := value
			if tc.present != nil {
				presented = tc.present(value)
			}
			reg, err := e.reader.ResolveBootstrap(ctxT(t), BootstrapDigest(presented), now)
			if !errors.Is(err, ErrRefused) || RefusalReason(err) != string(tc.want) {
				t.Fatalf("ResolveBootstrap = %+v, %v; want refused %s", reg, err, tc.want)
			}
			if strings.Contains(err.Error(), presented) || reg.PrincipalID != uuid.Nil {
				t.Fatalf("the refusal carries the value or a registration: %v %+v", err, reg)
			}
		})
	}
}

// Nothing is cached: a revoke after a successful resolve refuses the next one.
func TestResolveBootstrapReadsTheCurrentRowOnEveryCall(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	_, principal := registerWorkload(t, e, []string{"auth-service"})
	value := newBootstrapValue(t)
	credential := insertCredential(t, e, principal, value, credentialRow{audiences: []string{"auth-service"}})
	if _, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(value), time.Now()); err != nil {
		t.Fatalf("live credential: %v", err)
	}
	commitSeed(t, e, `UPDATE auth.credential_registry SET revoked_at = now(), revoked_reason = 'compromised' WHERE id = $1 RETURNING id`, credential)
	if _, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(value), time.Now()); RefusalReason(err) != string(ReasonRevoked) {
		t.Fatalf("after revoke: %v, want revoked", err)
	}
}

func TestResolveBootstrapStoreFailureIsARefusal(t *testing.T) {
	e := getEnv(t)
	missing, err := authschema.NewValidatedIdentifier("auth_absent")
	if err != nil {
		t.Fatal(err)
	}
	broken, err := New(Config{Pool: e.runtime, Schema: missing})
	if err != nil {
		t.Fatal(err)
	}
	_, err = broken.ResolveBootstrap(ctxT(t), BootstrapDigest(newBootstrapValue(t)), time.Now())
	if !errors.Is(err, ErrRefused) || RefusalReason(err) != string(ReasonStoreError) {
		t.Fatalf("a store failure: %v, want refused store_error", err)
	}
}
