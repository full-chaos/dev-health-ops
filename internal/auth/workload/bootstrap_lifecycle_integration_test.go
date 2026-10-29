//go:build integration

package workload

import (
	"context"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func mint(t *testing.T) string {
	t.Helper()
	v, err := MintBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// expiry reads a credential's stored expires_at.
func expiry(t *testing.T, e *env, id uuid.UUID) time.Time {
	t.Helper()
	var at time.Time
	if err := e.admin.QueryRow(ctxT(t), `SELECT expires_at FROM auth.credential_registry WHERE id = $1`, id).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

// noSecretIn fails if any audit attributes or outbox payload of the workload
// holds the value, its base64 body or its digest.
func noSecretIn(t *testing.T, e *env, principal uuid.UUID, values ...string) {
	t.Helper()
	var texts string
	if err := e.admin.QueryRow(ctxT(t), `SELECT coalesce(string_agg(attributes::text, ' '), '') || ' ' ||
coalesce((SELECT string_agg(payload::text, ' ') FROM auth.auth_outbox_events WHERE aggregate_id = $2), '')
FROM auth.security_audit_events WHERE subject_principal_id = $1`, principal, wire(principal)).Scan(&texts); err != nil {
		t.Fatal(err)
	}
	for _, v := range values {
		d := BootstrapDigest(v)
		for _, leak := range []string{v, strings.TrimPrefix(v, BootstrapPrefix), hex.EncodeToString(d[:])} {
			if strings.Contains(texts, leak) {
				t.Fatal("an audit row or outbox event holds a bootstrap value or its digest")
			}
		}
	}
}

func bootstrapEvents(t *testing.T, e *env, principal uuid.UUID) int {
	t.Helper()
	return e.count(t, ctxT(t), `SELECT count(*) FROM auth.auth_outbox_events
WHERE aggregate_type = 'workload' AND aggregate_id = 'prn_' || $1::text AND event_type = 'workload.bootstrap_changed'`, principal)
}

func auditRows(t *testing.T, e *env, principal uuid.UUID, eventType string) int {
	t.Helper()
	return e.count(t, ctxT(t), `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = $2`, principal, eventType)
}

func TestBootstrapCreateStoresTheDigestOnlyAndAuditsOnce(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, principal := registerWorkload(t, e, []string{"auth-service", "dev-health-api"})
	v := mint(t)
	res, err := e.writer.CreateBootstrap(ctx, n, v, []string{"dev-health-api"}, time.Hour, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	d := BootstrapDigest(v)
	var (
		hash                     []byte
		algorithm, prefix, class string
		audiences                []string
		fingerprint              string
		expires                  time.Time
	)
	if err := e.admin.QueryRow(ctx, `SELECT secret_hash, hash_algorithm, display_prefix, credential_class, audiences::text[], fingerprint, expires_at
FROM auth.credential_registry WHERE id = $1 AND principal_id = $2 AND revoked_at IS NULL`, res.CredentialID, principal).
		Scan(&hash, &algorithm, &prefix, &class, &audiences, &fingerprint, &expires); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(hash, d[:]) || algorithm != "sha256" || prefix != BootstrapPrefix || class != BootstrapClass ||
		!slices.Equal(audiences, []string{"dev-health-api"}) || fingerprint != hex.EncodeToString(d[:8]) || !expires.Equal(res.ExpiresAt) {
		t.Fatalf("row %s %s %s %v %s %s", algorithm, prefix, class, audiences, fingerprint, expires)
	}
	if d := time.Until(expires); d <= 50*time.Minute || d > time.Hour {
		t.Fatalf("expires in %s, want about 1 h", d)
	}
	if _, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(v), time.Now()); err != nil {
		t.Fatalf("the created value does not resolve: %v", err)
	}
	if a, ev := auditRows(t, e, principal, "auth.workload.bootstrap_created"), bootstrapEvents(t, e, principal); a != 1 || ev != 1 {
		t.Fatalf("audit rows %d, events %d; want 1 and 1", a, ev)
	}
	var change string
	if err := e.admin.QueryRow(ctx, `SELECT payload->'change' ->> 'credential_id' FROM auth.auth_outbox_events
WHERE aggregate_id = 'prn_' || $1::text AND event_type = 'workload.bootstrap_changed'`, principal).Scan(&change); err != nil || change != "crd_"+res.CredentialID.String() {
		t.Fatalf("event credential_id %q %v", change, err)
	}
	noSecretIn(t, e, principal, v)

	if _, err := e.writer.CreateBootstrap(ctx, n, mint(t), nil, time.Hour, Actor{}); !errors.Is(err, ErrBootstrapLive) {
		t.Fatalf("a second create with a live value = %v, want ErrBootstrapLive", err)
	}
	if _, err := e.writer.CreateBootstrap(ctx, "t-absent", mint(t), nil, time.Hour, Actor{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an unknown workload = %v", err)
	}
}

func TestBootstrapCreateRefusesAnAudienceOutsideTheWorkloads(t *testing.T) {
	e := getEnv(t)
	n, principal := registerWorkload(t, e, []string{"auth-service"})
	if _, err := e.writer.CreateBootstrap(ctxT(t), n, mint(t), []string{"dev-health-api"}, time.Hour, Actor{}); !errors.Is(err, ErrAudienceInvalid) {
		t.Fatalf("create = %v, want ErrAudienceInvalid", err)
	}
	if c := e.count(t, ctxT(t), `SELECT count(*) FROM auth.credential_registry WHERE principal_id = $1`, principal); c != 0 {
		t.Fatal("a refused create wrote a row")
	}
}

// TestBootstrapRotationOverlapIsBoundedAtItsEdge: the old value resolves up to
// one instant before its lowered expiry and is refused at it; the new value
// resolves at once; a second rotation inside the overlap is refused.
func TestBootstrapRotationOverlapIsBoundedAtItsEdge(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, principal := registerWorkload(t, e, []string{"auth-service"})
	old := mint(t)
	first, err := e.writer.CreateBootstrap(ctx, n, old, nil, 24*time.Hour, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	next := mint(t)
	before := time.Now()
	rot, err := e.writer.RotateBootstrap(ctx, n, next, BootstrapOverlapCeiling, 48*time.Hour, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	if rot.ParentID != first.CredentialID {
		t.Fatalf("rotated %s, want %s", rot.ParentID, first.CredentialID)
	}
	edge := expiry(t, e, first.CredentialID)
	if !edge.Equal(rot.ParentExpiresAt) || edge.Before(before.Add(BootstrapOverlapCeiling).Add(-5*time.Second)) || edge.After(after.Add(BootstrapOverlapCeiling).Add(5*time.Second)) {
		t.Fatalf("parent expiry %s, want now + 1 h", edge)
	}
	if _, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(old), edge.Add(-time.Microsecond)); err != nil {
		t.Fatalf("old value one instant before the overlap end: %v", err)
	}
	if _, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(old), edge); RefusalReason(err) != string(ReasonExpired) {
		t.Fatalf("old value at the overlap end = %v, want expired", err)
	}
	if _, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(next), time.Now()); err != nil {
		t.Fatalf("the new value does not resolve at once: %v", err)
	}
	var parent uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT rotation_parent_id FROM auth.credential_registry WHERE id = $1`, rot.CredentialID).Scan(&parent); err != nil || parent != first.CredentialID {
		t.Fatalf("lineage %s %v", parent, err)
	}
	if _, err := e.writer.RotateBootstrap(ctx, n, mint(t), 0, time.Hour, Actor{}); !errors.Is(err, ErrBootstrapRotating) {
		t.Fatalf("a rotation inside the overlap = %v, want ErrBootstrapRotating", err)
	}
	if _, err := e.writer.CreateBootstrap(ctx, n, mint(t), nil, time.Hour, Actor{}); !errors.Is(err, ErrBootstrapLive) {
		t.Fatalf("create inside the overlap = %v, want ErrBootstrapLive", err)
	}
	if a := auditRows(t, e, principal, "auth.workload.bootstrap_rotated"); a != 1 {
		t.Fatalf("rotate audit rows = %d", a)
	}
	noSecretIn(t, e, principal, old, next)
}

func TestBootstrapRotateWithOverlapZeroCutsOverAtOnceAndNeedsALiveValue(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, _ := registerWorkload(t, e, []string{"auth-service"})
	if _, err := e.writer.RotateBootstrap(ctx, n, mint(t), 0, time.Hour, Actor{}); !errors.Is(err, ErrBootstrapNone) {
		t.Fatalf("rotate with no value = %v, want ErrBootstrapNone", err)
	}
	old := mint(t)
	if _, err := e.writer.CreateBootstrap(ctx, n, old, nil, time.Hour, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.writer.RotateBootstrap(ctx, n, mint(t), 0, time.Hour, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(old), time.Now()); RefusalReason(err) != string(ReasonExpired) {
		t.Fatalf("old value after a 0 overlap = %v, want expired", err)
	}
	// After the overlap a further rotation is allowed again.
	if _, err := e.writer.RotateBootstrap(ctx, n, mint(t), 0, time.Hour, Actor{}); err != nil {
		t.Fatalf("rotation after the overlap ended: %v", err)
	}
}

// TestNoBootstrapVerbRaisesAnExpiry: rotate only lowers the parent expiry
// (an already earlier expiry is kept), revoke and disable leave it alone.
func TestNoBootstrapVerbRaisesAnExpiry(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, _ := registerWorkload(t, e, []string{"auth-service"})
	first, err := e.writer.CreateBootstrap(ctx, n, mint(t), nil, 10*time.Minute, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	short := expiry(t, e, first.CredentialID)
	rot, err := e.writer.RotateBootstrap(ctx, n, mint(t), BootstrapOverlapCeiling, time.Hour, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	if got := expiry(t, e, first.CredentialID); !got.Equal(short) {
		t.Fatalf("rotate raised the parent expiry from %s to %s", short, got)
	}
	child := expiry(t, e, rot.CredentialID)
	if err := e.writer.RevokeBootstrap(ctx, rot.CredentialID, BootstrapReasonOwnerRequest, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.writer.DisableBootstrap(ctx, n, Actor{}); err != nil {
		t.Fatal(err)
	}
	if !expiry(t, e, rot.CredentialID).Equal(child) || !expiry(t, e, first.CredentialID).Equal(short) {
		t.Fatal("revoke or disable changed an expiry")
	}
}

// TestBootstrapRevokeIsTerminalAndRollbackNeverReactivates: a repeat writes
// nothing; a revoked parent cannot be rotated; revoking the successor during
// the overlap (the rollback) leaves the parent's lowered expiry as it is.
func TestBootstrapRevokeIsTerminalAndRollbackNeverReactivates(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, principal := registerWorkload(t, e, []string{"auth-service"})
	old := mint(t)
	first, err := e.writer.CreateBootstrap(ctx, n, old, nil, 24*time.Hour, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	next := mint(t)
	rot, err := e.writer.RotateBootstrap(ctx, n, next, 30*time.Minute, 24*time.Hour, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	parentEdge := expiry(t, e, first.CredentialID)
	if err := e.writer.RevokeBootstrap(ctx, rot.CredentialID, BootstrapReasonOwnerRequest, Actor{}); err != nil {
		t.Fatal(err)
	}
	if !expiry(t, e, first.CredentialID).Equal(parentEdge) {
		t.Fatal("the rollback moved the parent's expiry")
	}
	if _, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(next), time.Now()); RefusalReason(err) != string(ReasonRevoked) {
		t.Fatalf("revoked successor = %v, want revoked", err)
	}
	if _, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(old), parentEdge); RefusalReason(err) != string(ReasonExpired) {
		t.Fatalf("the parent after the overlap = %v, want expired", err)
	}
	audits := auditRows(t, e, principal, "auth.workload.bootstrap_revoked")
	events := bootstrapEvents(t, e, principal)
	if err := e.writer.RevokeBootstrap(ctx, rot.CredentialID, BootstrapReasonCompromised, Actor{}); err != nil {
		t.Fatalf("repeat revoke = %v", err)
	}
	var reason string
	if err := e.admin.QueryRow(ctx, `SELECT revoked_reason FROM auth.credential_registry WHERE id = $1`, rot.CredentialID).Scan(&reason); err != nil || reason != BootstrapReasonOwnerRequest {
		t.Fatalf("a repeat revoke changed the reason to %q (%v)", reason, err)
	}
	if auditRows(t, e, principal, "auth.workload.bootstrap_revoked") != audits || bootstrapEvents(t, e, principal) != events {
		t.Fatal("a repeat revoke wrote an audit row or an event")
	}
	if err := e.writer.RevokeBootstrap(ctx, first.CredentialID, BootstrapReasonCompromised, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.writer.RotateBootstrap(ctx, n, mint(t), 0, time.Hour, Actor{}); !errors.Is(err, ErrBootstrapNone) {
		t.Fatalf("rotate with only revoked values = %v, want ErrBootstrapNone", err)
	}
	if err := e.writer.RevokeBootstrap(ctx, uuid.New(), BootstrapReasonCompromised, Actor{}); !errors.Is(err, ErrBootstrapNotFound) {
		t.Fatalf("revoke of an unknown id = %v", err)
	}
	// Even through audit.Commit (the one path the guards admit), the 0007
	// trigger refuses an un-revoke.
	schema, _ := authschema.NewValidatedIdentifier("auth")
	err = audit.Commit(ctx, e.admin, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.credential_registry SET revoked_at = NULL, revoked_reason = NULL WHERE id = $1`, first.CredentialID)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "credential.test", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamWorkloadRegistry,
	})
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "23514" {
		t.Fatalf("un-revoke through audit.Commit = %v, want the 0007 trigger's 23514", err)
	}
	if status, err := e.writer.BootstrapStatus(ctx, first.CredentialID); err != nil || status != "revoked" {
		t.Fatalf("status %q %v", status, err)
	}
	noSecretIn(t, e, principal, old, next)
}

// TestBootstrapDisableRefusesEveryLiveValueAtOnce revokes both values of a
// rotation in one transaction, leaves the workload enabled, and writes one
// audit row and one event; a repeat writes nothing.
func TestBootstrapDisableRefusesEveryLiveValueAtOnce(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, principal := registerWorkload(t, e, []string{"auth-service"})
	a, b := mint(t), mint(t)
	if _, err := e.writer.CreateBootstrap(ctx, n, a, nil, time.Hour, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.writer.RotateBootstrap(ctx, n, b, BootstrapOverlapCeiling, time.Hour, Actor{}); err != nil {
		t.Fatal(err)
	}
	res, err := e.writer.DisableBootstrap(ctx, n, Actor{})
	if err != nil || res.Revoked != 2 {
		t.Fatalf("disable revoked %d (%v), want 2", res.Revoked, err)
	}
	for _, v := range []string{a, b} {
		if _, err := e.reader.ResolveBootstrap(ctx, BootstrapDigest(v), time.Now()); RefusalReason(err) != string(ReasonRevoked) {
			t.Fatalf("a value after disable = %v, want revoked", err)
		}
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.credential_registry WHERE principal_id = $1 AND revoked_reason = 'compromised'`, principal); c != 2 {
		t.Fatalf("compromised rows = %d", c)
	}
	if info, err := e.writer.Lookup(ctx, n); err != nil || info.Disabled {
		t.Fatal("disable touched the workload itself")
	}
	if auditRows(t, e, principal, "auth.workload.bootstrap_disabled") != 1 {
		t.Fatal("disable audit rows != 1")
	}
	events := bootstrapEvents(t, e, principal)
	if res, err := e.writer.DisableBootstrap(ctx, n, Actor{}); err != nil || res.Revoked != 0 {
		t.Fatalf("repeat disable = %d %v", res.Revoked, err)
	}
	if auditRows(t, e, principal, "auth.workload.bootstrap_disabled") != 1 || bootstrapEvents(t, e, principal) != events {
		t.Fatal("a repeat disable wrote a row")
	}
	if _, err := e.writer.CreateBootstrap(ctx, n, mint(t), nil, time.Hour, Actor{}); err != nil {
		t.Fatalf("recovery by a new create: %v", err)
	}
	noSecretIn(t, e, principal, a, b)
}

func TestBootstrapStoreErrorsCarryNoValueOrDigest(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, principal := registerWorkload(t, e, []string{"auth-service"})
	v := mint(t)
	if _, err := e.writer.CreateBootstrap(ctx, n, v, nil, time.Hour, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.writer.RevokeBootstrap(ctx, uuid.Nil, BootstrapReasonCompromised, Actor{}); !errors.Is(err, ErrBootstrapNotFound) {
		t.Fatal(err)
	}
	// The same value on a second workload hits the unique digest index.
	m, _ := registerWorkload(t, e, []string{"auth-service"})
	_, err := e.writer.CreateBootstrap(ctx, m, v, nil, time.Hour, Actor{})
	if !errors.Is(err, ErrStore) {
		t.Fatalf("a repeated digest = %v, want ErrStore", err)
	}
	d := BootstrapDigest(v)
	for _, leak := range []string{v, hex.EncodeToString(d[:]), "secret_hash"} {
		if strings.Contains(err.Error(), leak) {
			t.Fatalf("the store error %q holds %q", err.Error(), leak)
		}
	}
	if err.Error() != ErrStore.Error()+" (sqlstate 23505)" {
		t.Fatalf("store error text = %q", err.Error())
	}
	noSecretIn(t, e, principal, v)
}
