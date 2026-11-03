//go:build integration

package pushprojection

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
)

func revoke(c audit.PushLifecycleEvent) audit.PushLifecycleEvent {
	snapshot := c.Change
	return audit.PushLifecycleEvent{Binding: c.Binding, EventType: externalpush.Revoked, AggregateID: c.AggregateID, OrganizationID: c.OrganizationID, SourceID: c.SourceID,
		Change: externalpush.Change{Status: "revoked"}, Snapshot: &snapshot}
}

func rotation(org uuid.UUID, id, parent string, scopes ...string) audit.PushLifecycleEvent {
	return audit.PushLifecycleEvent{Binding: externalpush.BindingSource, EventType: externalpush.Rotated, AggregateID: id, OrganizationID: "org_" + org.String(), SourceID: "src-r",
		Change: externalpush.Change{Status: "active", Fingerprint: freshFingerprint(), Scopes: scopes, RotationParentID: parent}}
}

// moveLate gives the outbox row of (aggregate, revision) an id above every row written so far, so the reader meets it last.
func (e *env) moveLate(t *testing.T, aggregate string, revision int) {
	t.Helper()
	e.seedOutbox(t, `UPDATE auth.auth_outbox_events SET id = nextval(pg_get_serial_sequence('auth.auth_outbox_events', 'id')) WHERE idempotency_key = $1`,
		fmt.Sprintf("external_push_credential:%s:%d", aggregate, revision))
}

// replayRow appends a copy of the outbox row of (aggregate, revision) under a new id and key: the same delivery again.
func (e *env) replayRow(t *testing.T, aggregate string, revision int) {
	t.Helper()
	e.seedOutbox(t, `INSERT INTO auth.auth_outbox_events (aggregate_type, aggregate_id, event_type, payload, idempotency_key, created_at)
		SELECT aggregate_type, aggregate_id, event_type, payload, idempotency_key || ':replay:' || gen_random_uuid()::text, now() FROM auth.auth_outbox_events WHERE idempotency_key = $1`,
		fmt.Sprintf("external_push_credential:%s:%d", aggregate, revision))
}

type view struct {
	revision int
	status   string
	scopes   string
}

func (e *env) view(t *testing.T, id string) view {
	t.Helper()
	var v view
	err := e.admin.QueryRow(context.Background(), `SELECT revision, status, scopes::text FROM auth.credential_projection WHERE external_reference = $1`, id).Scan(&v.revision, &v.status, &v.scopes)
	if err != nil {
		return view{}
	}
	return v
}

func (e *env) registry(t *testing.T, id string) string {
	t.Helper()
	var s string
	if err := e.admin.QueryRow(context.Background(), `SELECT coalesce(string_agg(coalesce(revoked_at::text,'-')||'|'||coalesce(revoked_reason::text,'-')||'|'||scopes::text||'|'||updated_at::text, ';'), 'none') FROM auth.credential_registry WHERE external_reference = $1`, id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// quiet steps the reader once a minute until it reads nothing and holds no due row, and calls each with the view after every step.
func (e *env) quiet(t *testing.T, r *Reader, id string, each func(view)) {
	t.Helper()
	now := time.Now()
	for i := 0; i < 15; i++ {
		rep, err := r.Step(context.Background(), now)
		if err != nil {
			t.Fatal(err)
		}
		each(e.view(t, id))
		now = now.Add(time.Minute)
		if rep.Seen == 0 && rep.Retried == 0 && rep.Released == 0 && i > 1 {
			return
		}
	}
}

// neverRevives fails when a credential seen revoked is later seen in any other state, or its scopes grow.
func neverRevives(t *testing.T) func(view) {
	var last view
	return func(v view) {
		t.Helper()
		if last.status == "revoked" && v != last {
			t.Fatalf("a revoked credential moved from %+v to %+v", last, v)
		}
		if last.revision > 0 && v.revision < last.revision {
			t.Fatalf("the revision went from %d to %d", last.revision, v.revision)
		}
		last = v
	}
}

// refusedNeverParked fails when the widening (revision 3) is decided as refused_widen while a retry row still names the aggregate.
func (e *env) refusedNeverParked(t *testing.T, id string) {
	t.Helper()
	if e.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1 AND revision = 3 AND outcome = 'refused_widen'`, id) == 1 && e.parkedFor(t, id) != 0 {
		t.Fatal("a refused widening is decided and still parked: it is final, not a retry")
	}
}

func (e *env) parkedFor(t *testing.T, aggregate string) int {
	return e.count(t, `SELECT count(*) FROM auth.projection_retry WHERE stream = 'external_push' AND aggregate_id = $1`, aggregate)
}

// Cell 15: the revoke reaches the reader before the creation; the credential ends revoked and is never seen active after.
func TestDisorderRevokeReadBeforeCreationEndsRevokedNeverActiveAfter(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	e.drain(t, e.reader(t, time.Hour))
	id := uuid.NewString()
	c := created(org, id, "ingest:write", "schema:read")
	e.appendCommitted(t, c)
	e.appendCommitted(t, scoped(c, "ingest:write"))
	e.appendCommitted(t, revoke(c))
	e.moveLate(t, id, 1)
	r := e.reader(t, time.Hour)
	rep, err := r.Step(context.Background(), time.Now())
	if err != nil || rep.Parked < 1 {
		t.Fatalf("step: %+v, %v; want the early rows parked as a gap", rep, err)
	}
	if v := e.view(t, id); v.status == "revoked" || v.revision > 1 {
		t.Fatalf("a row ahead of its predecessors applied: %+v", v)
	}
	e.quiet(t, r, id, neverRevives(t))
	if v := e.view(t, id); v != (view{3, "revoked", "{ingest:write}"}) {
		t.Fatalf("end state %+v, want revoked r3 {ingest:write}", v)
	}
	if n := e.parkedFor(t, id); n != 0 {
		t.Fatalf("%d rows still parked", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND revoked_at IS NOT NULL AND scopes = ARRAY['ingest:write']::auth.credential_scope[]`, id); n != 1 {
		t.Fatal("the registry row is not revoked and narrowed")
	}
}

// Cell 16: a widening that is read before the narrowing it follows is parked, then refused for good.
func TestDisorderWideningReadBeforeTheNarrowingIsRefusedNotApplied(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	e.drain(t, e.reader(t, time.Hour))
	id := uuid.NewString()
	c := created(org, id, "ingest:write", "schema:read")
	e.appendCommitted(t, c)
	e.appendCommitted(t, scoped(c, "ingest:write"))
	e.appendCommitted(t, scoped(c, "ingest:write", "schema:read"))
	e.moveLate(t, id, 2)
	r := e.reader(t, time.Hour)
	e.quiet(t, r, id, func(v view) {
		e.refusedNeverParked(t, id)
		if v.revision > 0 && !slices.Contains([]string{"{ingest:write,schema:read}", "{ingest:write}"}, v.scopes) {
			t.Fatalf("unexpected scopes %+v", v)
		}
		if v.revision >= 3 {
			t.Fatalf("the widening applied: %+v", v)
		}
	})
	if v := e.view(t, id); v != (view{2, "active", "{ingest:write}"}) {
		t.Fatalf("end state %+v, want active r2 {ingest:write}", v)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1 AND revision = 3 AND outcome = 'refused_widen'`, id); n != 1 {
		t.Fatal("the widening is not decided as refused_widen")
	}
	if n := e.parkedFor(t, id); n != 0 {
		t.Fatalf("a refused widening stays parked (%d rows): the reader would read it again", n)
	}
}

// Cell 17: replayed rows of every revision after a revoke are decided and change nothing.
func TestDisorderReplayedRowsAfterRevokeAreDecidedAndChangeNothing(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	r := e.reader(t, time.Hour)
	e.drain(t, r)
	id := uuid.NewString()
	c := created(org, id, "ingest:write", "schema:read")
	e.appendCommitted(t, c)
	e.appendCommitted(t, scoped(c, "ingest:write"))
	e.appendCommitted(t, revoke(c))
	e.quiet(t, r, id, neverRevives(t))
	want := view{3, "revoked", "{ingest:write}"}
	if v := e.view(t, id); v != want {
		t.Fatalf("setup state %+v", v)
	}
	registry := e.registry(t, id)
	for rev := 1; rev <= 3; rev++ {
		e.replayRow(t, id, rev)
	}
	e.replayRow(t, id, 1)
	// A decided revision is never read again, whatever its body.
	if rep, err := r.Step(context.Background(), time.Now()); err != nil || rep.Seen != 0 || rep.Applied != 0 {
		t.Fatalf("step: %+v, %v; want the decided replays not read", rep, err)
	}
	// With the digest rows lost the replays are read, and still decided without a change.
	e.seed(t, `DELETE FROM auth.credential_projection_digest WHERE external_reference = $1`, id)
	rep, err := r.Step(context.Background(), time.Now())
	if err != nil || rep.Seen < 4 || rep.Applied != 0 {
		t.Fatalf("step: %+v, %v; want the replays read and none applied", rep, err)
	}
	e.quiet(t, r, id, neverRevives(t))
	if v := e.view(t, id); v != want {
		t.Fatalf("a replay moved the credential to %+v", v)
	}
	if got := e.registry(t, id); got != registry {
		t.Fatalf("a replay wrote the registry: %q -> %q", registry, got)
	}
	// A replayed row is another row (its own key and stamp): the engine meets a digest of the first reading and
	// answers conflict, which is final and writes nothing.
	if n := e.count(t, `SELECT count(*) FROM auth.projection_retry WHERE stream = 'external_push' AND aggregate_id = $1 AND last_outcome <> 'conflict'`, id); n != 0 {
		t.Fatalf("%d replays parked as something other than a final conflict", n)
	}
	if n := e.parkedFor(t, id); n < 1 {
		t.Fatalf("%d replays parked, want the first conflict parked (later rows of the aggregate wait behind it)", n)
	}
	if rep, err := r.Step(context.Background(), time.Now().Add(3*time.Hour)); err != nil || rep.Seen != 0 {
		t.Fatalf("step: %+v, %v; want nothing left to read", rep, err)
	}
}

// Cell 18: a rotation read before its parent waits; the parent's events land; replays of the parent never revive it.
func TestDisorderRotationReadBeforeItsParentWaitsAndAParentReplayRevivesNothing(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	r := e.reader(t, time.Hour)
	e.drain(t, r)
	parent, child := uuid.NewString(), uuid.NewString()
	p := created(org, parent, "ingest:write", "schema:read")
	e.appendCommitted(t, rotation(org, child, parent, "ingest:write"))
	rep, err := r.Step(context.Background(), time.Now())
	if err != nil || rep.Parked != 1 {
		t.Fatalf("step: %+v, %v; want the child parked as parent_pending", rep, err)
	}
	e.appendCommitted(t, p)
	e.appendCommitted(t, revoke(p))
	e.quiet(t, r, child, neverRevives(t))
	if v := e.view(t, child); v.status != "active" || v.revision != 1 {
		t.Fatalf("child %+v, want active r1", v)
	}
	want := view{2, "revoked", "{ingest:write,schema:read}"}
	if v := e.view(t, parent); v != want {
		t.Fatalf("parent %+v, want %+v", v, want)
	}
	registry := e.registry(t, parent)
	e.replayRow(t, parent, 1)
	e.replayRow(t, parent, 2)
	e.replayRow(t, child, 1)
	e.quiet(t, r, parent, neverRevives(t))
	if v := e.view(t, parent); v != want {
		t.Fatalf("a replay moved the parent to %+v", v)
	}
	if got := e.registry(t, parent); got != registry {
		t.Fatalf("a replay wrote the parent registry row: %q -> %q", registry, got)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, child); n != 1 {
		t.Fatalf("%d registry rows for the child, want 1", n)
	}
}

// Cell 16b: a widening read in order, right after the narrowing, is refused for good and never parked.
func TestDisorderWideningReadInOrderIsRefusedAtOnceAndNeverParked(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	r := e.reader(t, time.Hour)
	e.drain(t, r)
	id := uuid.NewString()
	c := created(org, id, "ingest:write", "schema:read")
	e.appendCommitted(t, c)
	e.appendCommitted(t, scoped(c, "ingest:write"))
	e.quiet(t, r, id, func(view) {})
	e.appendCommitted(t, scoped(c, "ingest:write", "schema:read"))
	e.quiet(t, r, id, func(v view) {
		e.refusedNeverParked(t, id)
		if v.revision >= 3 {
			t.Fatalf("the widening applied: %+v", v)
		}
	})
	if v := e.view(t, id); v != (view{2, "active", "{ingest:write}"}) {
		t.Fatalf("end state %+v, want active r2 {ingest:write}", v)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1 AND revision = 3 AND outcome = 'refused_widen'`, id); n != 1 {
		t.Fatal("the widening is not decided as refused_widen")
	}
	if n := e.parkedFor(t, id); n != 0 {
		t.Fatalf("a refused widening is parked (%d rows)", n)
	}
}
