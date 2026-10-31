//go:build integration

package workloadexchange

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

const uniform400 = "{\"error\":\"invalid_grant\"}\n"

type revEnv struct {
	e     *storeEnv
	h     *harness
	name  string
	prin  uuid.UUID
	value string
	cred  workload.BootstrapResult
	t0    time.Time
}

func newRevEnv(t *testing.T, expiresIn time.Duration) *revEnv {
	t.Helper()
	e := getStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	name := "r-" + uuid.NewString()[:8]
	prin, err := e.writer.Register(ctx, workload.Spec{Name: name, Audiences: []string{"auth-service"}}, workload.Actor{})
	if err != nil {
		t.Fatal(err)
	}
	value, err := workload.MintBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	cred, err := e.writer.CreateBootstrap(ctx, name, value, nil, expiresIn, workload.Actor{})
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, withoutKubernetes(), func(h *harness, d *Deps) {
		d.Bootstrap = storeBootstrap{store: e.reader}
		d.Audit = func(ctx context.Context, event audit.AuditEvent) error {
			return audit.Append(ctx, e.runtime, e.schema, event)
		}
	})
	r := &revEnv{e: e, h: h, name: name, prin: prin, value: value, cred: cred, t0: time.Now().UTC().Truncate(time.Second)}
	h.now = r.t0
	return r
}

func (r *revEnv) at(d time.Duration) { r.h.now = r.t0.Add(d) }

func (r *revEnv) mustIssue(t *testing.T, value string) string {
	t.Helper()
	return decodeAnswer(t, r.h.post(bootstrapForm(value, "auth-service", "").Encode())).AccessToken
}

func (r *revEnv) mustRefuse(t *testing.T, value, when string) {
	t.Helper()
	rec := r.h.post(bootstrapForm(value, "auth-service", "").Encode())
	if rec.Code != http.StatusBadRequest || rec.Body.String() != uniform400 {
		t.Fatalf("%s: %d %q, want the uniform 400", when, rec.Code, rec.Body.String())
	}
}

// verifyAt checks compact with the canonical workload verifier at the injected
// instant now (leeway 0, the authverify default).
func (r *revEnv) verifyAt(t *testing.T, compact string, now time.Time) error {
	t.Helper()
	set, err := r.h.signer.KeySet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	keys := authverify.StaticKeys{}
	for _, key := range set.Published() {
		keys[key.KeyID] = key.PublicKey
	}
	v, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{Issuer: testIssuerURL, Audience: "auth-service", Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	_, err = v.Verify(compact, now)
	return err
}

// requireDiesAt: the token verifies one second before iat+life and is
// refused at iat+life and after, on the injected instants alone.
func (r *revEnv) requireDiesAt(t *testing.T, compact string, life time.Duration) {
	t.Helper()
	if err := r.verifyAt(t, compact, r.t0.Add(life-time.Second)); err != nil {
		t.Fatalf("token at iat+%v-1s: %v, want valid", life, err)
	}
	for _, d := range []time.Duration{0, time.Second, time.Hour} {
		err := r.verifyAt(t, compact, r.t0.Add(life+d))
		var refusal *authverify.RefusalError
		if err == nil || !errors.As(err, &refusal) || refusal.Reason != authverify.ReasonExpired {
			t.Fatalf("token at iat+%v+%v: %v, want expired", life, d, err)
		}
	}
}

func TestRevocationBoundBootstrapDisableRefusesAtOnceAndTokenDiesInFiveMinutes(t *testing.T) {
	r := newRevEnv(t, time.Hour)
	issued := r.mustIssue(t, r.value)
	res, err := r.e.writer.DisableBootstrap(context.Background(), r.name, workload.Actor{})
	if err != nil || res.Revoked != 1 {
		t.Fatalf("bootstrap-disable revoked %d (%v), want 1", res.Revoked, err)
	}
	r.mustRefuse(t, r.value, "at the disable instant")
	r.requireDiesAt(t, issued, 300*time.Second)
	r.at(299 * time.Second)
	r.mustRefuse(t, r.value, "299 s after the disable")
}

func TestRevocationBoundWorkloadDisableRefusesAtOnceAndTokenDiesInFiveMinutes(t *testing.T) {
	r := newRevEnv(t, time.Hour)
	issued := r.mustIssue(t, r.value)
	if err := r.e.writer.Disable(context.Background(), r.name, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	r.mustRefuse(t, r.value, "at the workload disable instant")
	r.requireDiesAt(t, issued, 300*time.Second)
}

func TestRevocationBoundTokenStopsAtTheCredentialExpiry(t *testing.T) {
	r := newRevEnv(t, time.Hour)
	// Put the injected clock 120 s before the credential expiry.
	r.t0 = r.cred.ExpiresAt.Add(-120 * time.Second)
	r.at(0)
	issued := r.mustIssue(t, r.value)
	r.requireDiesAt(t, issued, 120*time.Second)
	r.h.now = r.cred.ExpiresAt
	r.mustRefuse(t, r.value, "at the credential expiry")
}

func TestRevocationBoundRotationParentRevokedRefusesAtOnce(t *testing.T) {
	r := newRevEnv(t, time.Hour)
	ctx := context.Background()
	next, err := workload.MintBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	succ, err := r.e.writer.RotateBootstrap(ctx, r.name, next, 10*time.Minute, time.Hour, workload.Actor{})
	if err != nil {
		t.Fatal(err)
	}
	issued := r.mustIssue(t, r.value)
	if err := r.e.writer.RevokeBootstrap(ctx, r.cred.CredentialID, workload.BootstrapReasonCompromised, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	r.mustRefuse(t, r.value, "revoked rotation parent inside the overlap")
	r.mustIssue(t, next)
	r.requireDiesAt(t, issued, 300*time.Second)
	if err := r.e.writer.RevokeBootstrap(ctx, succ.CredentialID, workload.BootstrapReasonCompromised, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	r.mustRefuse(t, next, "revoked successor")
}

// oldReleaseLive is the exchange's registry read as the release before the
// bootstrap lifecycle verbs ran it (SQL and decision of 46f0604ca5, #5215,
// unchanged since), reduced to live or not.
func oldReleaseLive(t *testing.T, e *storeEnv, digest [sha256.Size]byte, now time.Time) bool {
	t.Helper()
	var class, owner, kind string
	var revoked, wDisabled, pDisabled bool
	var expires *time.Time
	var cAud, wAud []string
	err := e.runtime.QueryRow(context.Background(), `SELECT c.credential_class, c.domain_owner, c.revoked_at IS NOT NULL, c.expires_at,
c.audiences::text[], w.audiences, p.kind, w.disabled_at IS NOT NULL, p.disabled_at IS NOT NULL
FROM auth.credential_registry c
JOIN auth.workloads w ON w.principal_id = c.principal_id
JOIN auth.principals p ON p.id = c.principal_id
WHERE c.secret_hash = $1`, digest[:]).Scan(&class, &owner, &revoked, &expires, &cAud, &wAud, &kind, &wDisabled, &pDisabled)
	if err != nil {
		return false
	}
	if class != workload.BootstrapClass || owner != "platform" || kind != "workload" || revoked || wDisabled || pDisabled {
		return false
	}
	return expires == nil || now.Before(*expires)
}

func (r *revEnv) viaCommit(t *testing.T, sql string, args ...any) error {
	t.Helper()
	return audit.Commit(context.Background(), r.e.admin, r.e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, sql, args...)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "credential.test", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamWorkloadRegistry,
	})
}

func (r *revEnv) revokeState(t *testing.T) (at *time.Time, reason *string) {
	t.Helper()
	if err := r.e.admin.QueryRow(context.Background(), `SELECT revoked_at, revoked_reason FROM auth.credential_registry WHERE id = $1`,
		r.cred.CredentialID).Scan(&at, &reason); err != nil {
		t.Fatal(err)
	}
	return at, reason
}

func TestRollbackOldReleaseReadsEveryRevokedOrDisabledValueAsRefused(t *testing.T) {
	r := newRevEnv(t, time.Hour)
	ctx := context.Background()
	digest := workload.BootstrapDigest(r.value)
	if !oldReleaseLive(t, r.e, digest, r.t0) {
		t.Fatal("control: the old read does not see a live value as live")
	}
	if _, err := r.e.writer.DisableBootstrap(ctx, r.name, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	if oldReleaseLive(t, r.e, digest, r.t0) {
		t.Fatal("the old release reads a revoked value as live")
	}
	r.mustRefuse(t, r.value, "current release, revoked value")
	if err := r.e.writer.Disable(ctx, r.name, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	if oldReleaseLive(t, r.e, digest, r.t0) {
		t.Fatal("the old release reads a revoked value of a disabled workload as live")
	}
	if err := r.e.writer.Enable(ctx, r.name, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	if oldReleaseLive(t, r.e, digest, r.t0) {
		t.Fatal("the old release reads a revoked value as live after the workload is enabled")
	}
	r.mustRefuse(t, r.value, "current release, revoked value after enable")
}

func TestRollbackNoWriteOfAnOlderReleaseReactivatesARevokedValue(t *testing.T) {
	r := newRevEnv(t, time.Hour)
	ctx := context.Background()
	next, err := workload.MintBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.e.writer.RotateBootstrap(ctx, r.name, next, 10*time.Minute, time.Hour, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := r.e.writer.RevokeBootstrap(ctx, r.cred.CredentialID, workload.BootstrapReasonOwnerRequest, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	wasAt, wasReason := r.revokeState(t)
	refused := map[string]struct {
		sql  string
		args []any
	}{
		"clear revoked_at":     {`UPDATE auth.credential_registry SET revoked_at = NULL, revoked_reason = NULL WHERE id = $1`, nil},
		"move revoked_at":      {`UPDATE auth.credential_registry SET revoked_at = revoked_at + interval '1 day' WHERE id = $1`, nil},
		"change the reason":    {`UPDATE auth.credential_registry SET revoked_reason = 'compromised' WHERE id = $1`, nil},
		"re-point the parent":  {`UPDATE auth.credential_registry SET rotation_parent_id = NULL WHERE id = $1`, nil},
		"swap the secret hash": {`UPDATE auth.credential_registry SET secret_hash = $2 WHERE id = $1`, []any{make([]byte, sha256.Size)}},
	}
	for label, w := range refused {
		args := append([]any{r.cred.CredentialID}, w.args...)
		err := r.viaCommit(t, w.sql, args...)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "23514" {
			t.Fatalf("%s: %v, want the 0007 trigger's 23514", label, err)
		}
	}
	// An extended expiry is not refused by the trigger; the value must stay refused.
	if err := r.viaCommit(t, `UPDATE auth.credential_registry SET expires_at = now() + interval '30 days' WHERE id = $1`, r.cred.CredentialID); err != nil {
		t.Fatalf("expiry extension on a revoked row: %v", err)
	}
	gotAt, gotReason := r.revokeState(t)
	if gotAt == nil || !gotAt.Equal(*wasAt) || gotReason == nil || *gotReason != *wasReason {
		t.Fatalf("revoked state moved: %v %v -> %v %v", wasAt, wasReason, gotAt, gotReason)
	}
	r.mustRefuse(t, r.value, "revoked value after every older-release write")
	if oldReleaseLive(t, r.e, workload.BootstrapDigest(r.value), r.t0) {
		t.Fatal("the old release reads the value as live after an expiry extension")
	}
	if got := r.h.bootstrapPoints()["refused/revoked/auth-service"]; got != 1 {
		t.Fatalf("counter refused/revoked = %d; points %v", got, nonZero(r.h.bootstrapPoints()))
	}
}

func TestRollbackReCreateOrRotateWithARevokedValueLeavesItRevoked(t *testing.T) {
	r := newRevEnv(t, time.Hour)
	ctx := context.Background()
	if err := r.e.writer.RevokeBootstrap(ctx, r.cred.CredentialID, workload.BootstrapReasonCompromised, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	digest := workload.BootstrapDigest(r.value)
	rows := func() int {
		var n int
		if err := r.e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.credential_registry WHERE secret_hash = $1`, digest[:]).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if _, err := r.e.writer.CreateBootstrap(ctx, r.name, r.value, nil, time.Hour, workload.Actor{}); err == nil {
		t.Fatal("create with the value of a revoked row succeeded")
	}
	other := "r-" + uuid.NewString()[:8]
	if _, err := r.e.writer.Register(ctx, workload.Spec{Name: other, Audiences: []string{"auth-service"}}, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.e.writer.CreateBootstrap(ctx, other, r.value, nil, time.Hour, workload.Actor{}); err == nil {
		t.Fatal("create for another workload with the value of a revoked row succeeded")
	}
	fresh, err := workload.MintBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.e.writer.CreateBootstrap(ctx, r.name, fresh, nil, time.Hour, workload.Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.e.writer.RotateBootstrap(ctx, r.name, r.value, 10*time.Minute, time.Hour, workload.Actor{}); err == nil {
		t.Fatal("rotate to the value of a revoked row succeeded")
	}
	if n := rows(); n != 1 {
		t.Fatalf("%d rows hold the revoked value's digest, want 1", n)
	}
	var revoked bool
	if err := r.e.admin.QueryRow(ctx, `SELECT revoked_at IS NOT NULL FROM auth.credential_registry WHERE secret_hash = $1`, digest[:]).Scan(&revoked); err != nil || !revoked {
		t.Fatalf("the revoked value's row: revoked %v (%v)", revoked, err)
	}
	r.mustRefuse(t, r.value, "after re-create and rotate attempts")
	r.mustIssue(t, fresh)
}
