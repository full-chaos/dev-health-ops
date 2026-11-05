//go:build integration

package pushprojection

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/customerpush"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile/reconcilejob"
)

// The reconcile cells C30..C35 (CHAOS-10897 PR-C): real reader and engine write the projection from real
// lifecycle events; the reconcile walks the token table and the projection. Every cell runs on its own
// database, because a reconcile counts every row and the shared env of this package holds rows of other cells.

func (e *env) fresh(t *testing.T) *env {
	t.Helper()
	own, err := newEnv(t, true)
	if err != nil {
		t.Fatal(err)
	}
	return own
}

func (e *env) domainToken(t *testing.T, org uuid.UUID, id, scopesJSON string, expires, revoked *time.Time) {
	t.Helper()
	if _, err := e.admin.Exec(context.Background(), `INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_at, expires_at, revoked_at)
		VALUES ($1, $2, NULL, 'reconcile', md5(gen_random_uuid()::text), 'fcpush_rrrrr', $3, now(), $4, $5)`, id, org.String(), scopesJSON, expires, revoked); err != nil {
		t.Fatal(err)
	}
}

func orgCreated(org uuid.UUID, id string, expires *time.Time, scopes ...string) audit.PushLifecycleEvent {
	return audit.PushLifecycleEvent{Binding: externalpush.BindingOrganization, EventType: externalpush.Created, AggregateID: id,
		OrganizationID: "org_" + org.String(), Change: externalpush.Change{Status: "active", Fingerprint: freshFingerprint(), Scopes: scopes, ExpiresAt: expires}}
}

func orgScoped(c audit.PushLifecycleEvent, scopes ...string) audit.PushLifecycleEvent {
	snapshot := c.Change
	return audit.PushLifecycleEvent{Binding: externalpush.BindingOrganization, EventType: externalpush.Scoped, AggregateID: c.AggregateID, OrganizationID: c.OrganizationID,
		Change: externalpush.Change{Scopes: scopes}, Snapshot: &snapshot}
}

func orgRevoked(c audit.PushLifecycleEvent) audit.PushLifecycleEvent {
	snapshot := c.Change
	return audit.PushLifecycleEvent{Binding: externalpush.BindingOrganization, EventType: externalpush.Revoked, AggregateID: c.AggregateID, OrganizationID: c.OrganizationID,
		Change: externalpush.Change{Status: "revoked"}, Snapshot: &snapshot}
}

func orgExpired(c audit.PushLifecycleEvent, at time.Time) audit.PushLifecycleEvent {
	snapshot := c.Change
	return audit.PushLifecycleEvent{Binding: externalpush.BindingOrganization, EventType: externalpush.Expired, AggregateID: c.AggregateID, OrganizationID: c.OrganizationID,
		Change: externalpush.Change{Status: "expired", ExpiresAt: &at}, Snapshot: &snapshot}
}

func micro(d time.Duration) *time.Time {
	v := time.Now().UTC().Add(d).Truncate(time.Microsecond)
	return &v
}

// reconcile runs the two-pass run over the admin pool; between the passes it calls between (may be nil).
func (e *env) reconcile(t *testing.T, between func()) reconcilejob.Report {
	t.Helper()
	pass := reconcilejob.NewPass(e.admin, e.schema, 2, nil)
	rep, err := reconcilejob.Run(context.Background(), pass, time.Millisecond, func(ctx context.Context, d time.Duration) error {
		if between != nil {
			between()
		}
		return reconcilejob.Sleep(ctx, d)
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return rep
}

func wantDrift(t *testing.T, rep reconcilejob.Report, want ...pushreconcile.Finding) {
	t.Helper()
	if len(rep.Drift) == 0 && len(want) == 0 {
		return
	}
	if !slices.Equal(rep.Drift, want) {
		t.Fatalf("drift = %v, want %v (transient %d)", rep.Drift, want, rep.Transient)
	}
}

// C30: create, scope, revoke and expire through the real reader step -> drift total=0.
func TestC30CleanAfterRealLifecycle(t *testing.T) {
	e := (&env{}).fresh(t)
	org := e.org(t)
	ctx := context.Background()
	long, past := micro(72*time.Hour), micro(-time.Hour)

	live, narrowed, revoked, expired := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	e.domainToken(t, org, live, `["ingest:write"]`, long, nil)
	liveEvent := orgCreated(org, live, long, "ingest:write")

	e.domainToken(t, org, narrowed, `["ingest:write"]`, nil, nil)
	narrowedEvent := orgCreated(org, narrowed, nil, "ingest:write", "schema:read")

	now := time.Now().UTC().Truncate(time.Microsecond)
	e.domainToken(t, org, revoked, `["ingest:write"]`, nil, &now)
	revokedEvent := orgCreated(org, revoked, nil, "ingest:write")

	e.domainToken(t, org, expired, `["ingest:write"]`, past, nil)
	expiredEvent := orgCreated(org, expired, past, "ingest:write")

	e.appendCommitted(t, liveEvent, narrowedEvent, revokedEvent, expiredEvent)
	e.appendCommitted(t, orgScoped(narrowedEvent, "ingest:write"), orgRevoked(revokedEvent), orgExpired(expiredEvent, *past))
	e.drain(t, e.reader(t, time.Hour))

	rep := e.reconcile(t, nil)
	wantDrift(t, rep)
	if rep.Tokens != 4 || rep.Projected != 4 {
		t.Fatalf("tokens %d projected %d, want 4 and 4 (%v)", rep.Tokens, rep.Projected, ctx.Err())
	}
}

// C31: a revoked event that lands over a gap (the CHAOS-12699 path) leaves a store with zero diff.
func TestC31TerminalOverGapCleanAfterRealReader(t *testing.T) {
	e := (&env{}).fresh(t)
	org := e.org(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	id := uuid.NewString()
	e.domainToken(t, org, id, `["ingest:write"]`, nil, &now)
	first := orgCreated(org, id, nil, "ingest:write")
	e.appendCommitted(t, first)
	r := e.reader(t, time.Hour)
	if rep, err := r.Step(context.Background(), time.Now()); err != nil || rep.Applied < 1 {
		t.Fatalf("setup step: %+v, %v", rep, err)
	}
	e.appendCommitted(t, orgScoped(first, "ingest:write"), orgScoped(first, "ingest:write"), orgRevoked(first))
	for _, rev := range []int{2, 3} {
		e.seed(t, `INSERT INTO auth.credential_projection_digest (domain_owner, external_reference, revision, digest, outcome) VALUES ('external_push', $1, $2, $3, 'applied')`, id, rev, make([]byte, 32))
	}
	e.drain(t, r)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND status = 'revoked'`, id); n != 1 {
		t.Fatal("the revoked event did not apply over the gap; the setup is wrong")
	}
	wantDrift(t, e.reconcile(t, nil))
}

// C32: a widening refused by Apply, with the domain widened by a direct UPDATE, is drift class scopes.
func TestC32RefusedWidenIsDrift(t *testing.T) {
	e := (&env{}).fresh(t)
	org := e.org(t)
	id := uuid.NewString()
	e.domainToken(t, org, id, `["ingest:write"]`, nil, nil)
	c := orgCreated(org, id, nil, "ingest:write", "schema:read")
	e.appendCommitted(t, c, orgScoped(c, "ingest:write"), orgScoped(c, "ingest:write", "schema:read"))
	e.drain(t, e.reader(t, time.Hour))
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1 AND outcome = 'refused_widen'`, id); n != 1 {
		t.Fatal("the widening was not refused; the setup is wrong")
	}
	if _, err := e.admin.Exec(context.Background(), `UPDATE external_ingest_tokens SET scopes = '["ingest:write","schema:read"]' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	wantDrift(t, e.reconcile(t, nil), pushreconcile.Finding{Class: pushreconcile.ClassScopes, ID: id})
}

// C33: a direct revoke with no event is status drift, and only after the confirm pass: an event that lands between
// the passes clears it.
func TestC33DirectRevokeNoEventIsDriftAppearsOnlyAfterTheConfirmPass(t *testing.T) {
	e := (&env{}).fresh(t)
	org := e.org(t)
	id := uuid.NewString()
	e.domainToken(t, org, id, `["ingest:write"]`, nil, nil)
	c := orgCreated(org, id, nil, "ingest:write")
	e.appendCommitted(t, c)
	r := e.reader(t, time.Hour)
	e.drain(t, r)
	if _, err := e.admin.Exec(context.Background(), `UPDATE external_ingest_tokens SET revoked_at = now() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	rep := e.reconcile(t, func() {
		e.appendCommitted(t, orgRevoked(c))
		e.drain(t, r)
	})
	wantDrift(t, rep)
	if rep.Transient != 1 {
		t.Fatalf("transient %d, want 1: the status finding of pass one was cleared by the event", rep.Transient)
	}
	// Second token: nothing arrives between the passes, so the finding is confirmed.
	other := uuid.NewString()
	e.domainToken(t, org, other, `["ingest:write"]`, nil, nil)
	e.appendCommitted(t, orgCreated(org, other, nil, "ingest:write"))
	e.drain(t, r)
	if _, err := e.admin.Exec(context.Background(), `UPDATE external_ingest_tokens SET revoked_at = now() WHERE id = $1`, other); err != nil {
		t.Fatal(err)
	}
	wantDrift(t, e.reconcile(t, nil), pushreconcile.Finding{Class: pushreconcile.ClassStatus, ID: other})
}

// C34: a post-restore-fence gap parks; the parked row is counted and the state that differs is reported.
func TestC34PostFenceGapParkedRowCounted(t *testing.T) {
	e := (&env{}).fresh(t)
	org := e.org(t)
	id := uuid.NewString()
	e.domainToken(t, org, id, `["ingest:write"]`, nil, nil)
	c := orgCreated(org, id, nil, "ingest:write", "schema:read")
	e.appendCommitted(t, c)
	r := e.reader(t, time.Hour)
	now := time.Now()
	if rep, err := r.Step(context.Background(), now); err != nil || rep.Applied < 1 {
		t.Fatalf("setup step: %+v, %v", rep, err)
	}
	for range 3 {
		e.appendCommitted(t, orgScoped(c, "ingest:write"))
	}
	for _, rev := range []int{2, 3} {
		e.seed(t, `INSERT INTO auth.credential_projection_digest (domain_owner, external_reference, revision, digest, outcome) VALUES ('external_push', $1, $2, $3, 'applied')`, id, rev, make([]byte, 32))
	}
	if rep, err := r.Step(context.Background(), now); err != nil || rep.Parked < 1 {
		t.Fatalf("step: %+v, %v; want the gap parked", rep, err)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.projection_retry WHERE stream = 'external_push' AND last_outcome = 'gap'`); n < 1 {
		t.Fatal("no parked gap row")
	}
	if _, err := e.admin.Exec(context.Background(), `UPDATE external_ingest_tokens SET scopes = '["ingest:write"]' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	wantDrift(t, e.reconcile(t, nil), pushreconcile.Finding{Class: pushreconcile.ClassScopes, ID: id})
}

// C35: a token with no events is missing; the backfill emits created; after the reader step the store is clean.
func TestC35BackfillClearsMissing(t *testing.T) {
	e := (&env{}).fresh(t)
	org := e.org(t)
	id := uuid.NewString()
	e.domainToken(t, org, id, `["ingest:write"]`, nil, nil)
	wantDrift(t, e.reconcile(t, nil), pushreconcile.Finding{Class: pushreconcile.ClassMissing, ID: id})
	if _, err := customerpush.BackfillLifecycle(context.Background(), e.admin, "auth", true, 2); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	e.drain(t, e.reader(t, time.Hour))
	wantDrift(t, e.reconcile(t, nil))
}
