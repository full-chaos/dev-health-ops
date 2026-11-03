//go:build integration

package pushprojection

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
)

type togView struct {
	revision int
	status   string
	scopes   string
}

func (e *env) togView(t *testing.T, id string) togView {
	t.Helper()
	var v togView
	if err := e.admin.QueryRow(context.Background(), `SELECT revision, status, scopes::text FROM auth.credential_projection WHERE external_reference = $1`, id).Scan(&v.revision, &v.status, &v.scopes); err != nil {
		return togView{}
	}
	return v
}

// togQuiet steps the reader once a simulated minute until it reads and retries nothing. After any step that shows the
// credential revoked, every later step must show it revoked and its registry row revoked.
func (e *env) togQuiet(t *testing.T, r *Reader, id string) {
	t.Helper()
	now, ended := time.Now(), false
	for i := 0; i < 15; i++ {
		rep, err := r.Step(context.Background(), now)
		if err != nil {
			t.Fatal(err)
		}
		v := e.togView(t, id)
		if v.status == "revoked" {
			ended = true
		}
		if ended && (v.status != "revoked" || e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND revoked_at IS NOT NULL`, id) != 1) {
			t.Fatalf("step %d: a revoked credential reads %+v", i, v)
		}
		now = now.Add(time.Minute)
		if rep.Seen == 0 && rep.Retried == 0 && rep.Released == 0 && i > 1 {
			return
		}
	}
	t.Fatal("the reader did not go quiet in 15 steps")
}

func togRevoke(c audit.PushLifecycleEvent) audit.PushLifecycleEvent {
	snapshot := c.Change
	return audit.PushLifecycleEvent{Binding: c.Binding, EventType: externalpush.Revoked, AggregateID: c.AggregateID, OrganizationID: c.OrganizationID, SourceID: c.SourceID,
		Change: externalpush.Change{Status: "revoked"}, Snapshot: &snapshot}
}

func (e *env) togParked(t *testing.T, id string) int {
	t.Helper()
	return e.count(t, `SELECT count(*) FROM auth.projection_retry WHERE stream = 'external_push' AND aggregate_id = $1`, id)
}

// R1: a refused widening read by the reader never parks the later revisions of its credential: a later narrowing and
// a later revoke both reach the projection.
func TestReaderAppliesTheRevisionsAfterARefusedWidening(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	e.drain(t, e.reader(t, time.Hour))
	for name, tc := range map[string]struct {
		last func(audit.PushLifecycleEvent) audit.PushLifecycleEvent
		want togView
	}{
		"narrowing r4": {func(c audit.PushLifecycleEvent) audit.PushLifecycleEvent { return scoped(c, "ingest:write") }, togView{4, "active", "{ingest:write}"}},
		"revoked r4":   {togRevoke, togView{4, "revoked", "{ingest:write}"}},
	} {
		id := uuid.NewString()
		c := created(org, id, "ingest:write", "schema:read")
		for _, ev := range []audit.PushLifecycleEvent{c, scoped(c, "ingest:write"), scoped(c, "ingest:write", "schema:read"), tc.last(c)} {
			e.appendCommitted(t, ev)
		}
		e.togQuiet(t, e.reader(t, time.Hour), id)
		if v := e.togView(t, id); v != tc.want {
			t.Fatalf("%s: end state %+v, want %+v", name, v, tc.want)
		}
		if n := e.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1 AND revision = 3 AND outcome = 'refused_widen'`, id); n != 1 {
			t.Fatalf("%s: the widening is not decided as refused_widen", name)
		}
		if n := e.togParked(t, id); n != 0 {
			t.Fatalf("%s: %d rows parked behind the refused widening", name, n)
		}
		if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND scopes = ARRAY['ingest:write']::auth.credential_scope[]`, id); n != 1 {
			t.Fatalf("%s: the registry scopes are not the narrowed ones", name)
		}
	}
}
