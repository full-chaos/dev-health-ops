//go:build integration

package outboxprojection

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

const decidedAudit = `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.credential_projection.decided'`

// seed runs one statement inside audit.Commit as the migration role: the guarded tables take no other write.
func (e *env) seed(t *testing.T, sql string, args ...any) {
	t.Helper()
	err := audit.Commit(context.Background(), e.admin, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, sql, args...)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "credential.test", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamWorkloadRegistry,
	})
	if err != nil {
		t.Fatalf("seed %q: %v", sql, err)
	}
}

// snapshot reads the registry row and the projection row of one credential as one text, updated_at included, so any write shows.
func (e *env) snapshot(t *testing.T, id string) string {
	t.Helper()
	var reg, proj string
	if err := e.admin.QueryRow(context.Background(), `SELECT coalesce(string_agg(coalesce(revoked_at::text,'-')||'|'||coalesce(revoked_reason::text,'-')||'|'||scopes::text||'|'||coalesce(expires_at::text,'-')||'|'||updated_at::text, ';'), 'none') FROM auth.credential_registry WHERE external_reference = $1`, id).Scan(&reg); err != nil {
		t.Fatal(err)
	}
	if err := e.admin.QueryRow(context.Background(), `SELECT coalesce(string_agg(revision::text||'|'||status||'|'||scopes::text||'|'||coalesce(terminal_at::text,'-')||'|'||updated_at::text, ';'), 'none') FROM auth.credential_projection WHERE external_reference = $1`, id).Scan(&proj); err != nil {
		t.Fatal(err)
	}
	return reg + " / " + proj
}

// revokedCredential applies created r1 (write+status), a narrowing r2 (write) and a revoke r3.
func (e *env) revokedCredential(t *testing.T, source string) (c, narrow, revoke spec) {
	t.Helper()
	c = created(e.org(t), source, "ingest:write", "ingest:status")
	mustOutcome(t, e.project(t, c.input()), pushlifecycle.Applied)
	narrow = c.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}})
	mustOutcome(t, e.project(t, narrow.input()), pushlifecycle.Applied)
	revoke = narrow.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"})
	mustOutcome(t, e.project(t, revoke.input()), pushlifecycle.Applied)
	return c, narrow, revoke
}

func (e *env) mustStayRevoked(t *testing.T, id string) {
	t.Helper()
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND revoked_at IS NOT NULL AND scopes = ARRAY['ingest:write']::auth.credential_scope[]`, id); n != 1 {
		t.Fatal("the registry row is not revoked with the narrowed scopes")
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND status = 'revoked' AND revision = 3 AND scopes = ARRAY['ingest:write']`, id); n != 1 {
		t.Fatal("the projection is not revoked at revision 3 with the narrowed scopes")
	}
}

// Cell 8: replays after a revoke decide as stale, duplicate or conflict and write nothing.
func TestDisorderReplayAfterRevokeWritesNothingAndNeverReactivates(t *testing.T) {
	e := getEnv(t, true)
	c, narrow, revoke := e.revokedCredential(t, "src-dis8")
	before, audits := e.snapshot(t, c.id), e.count(t, decidedAudit)

	wideCreated := c
	wideCreated.change.Scopes = []string{"ingest:write", "ingest:status", "schema:read"}
	wideScoped := narrow
	wideScoped.change.Scopes = []string{"ingest:write", "ingest:status"}
	for name, tc := range map[string]struct {
		in   spec
		want Outcome
	}{
		"created r1":             {c, pushlifecycle.Stale},
		"scoped r2":              {narrow, pushlifecycle.Stale},
		"revoked r3":             {revoke, pushlifecycle.Duplicate},
		"created r1, wider body": {wideCreated, OutcomeConflict},
		"scoped r2, wider body":  {wideScoped, OutcomeConflict},
		"created r1, again":      {c, pushlifecycle.Stale},
		"revoked r3, again":      {revoke, pushlifecycle.Duplicate},
	} {
		res := e.project(t, tc.in.input())
		if res.Outcome != tc.want {
			t.Errorf("%s: outcome %s, want %s", name, res.Outcome, tc.want)
		}
		if res.Outcome == pushlifecycle.Applied {
			t.Fatalf("%s: applied after a revoke", name)
		}
	}
	if after := e.snapshot(t, c.id); after != before {
		t.Fatalf("a replay wrote: %q -> %q", before, after)
	}
	if n := e.count(t, decidedAudit); n != audits {
		t.Fatalf("replays wrote %d audit rows", n-audits)
	}
	e.mustStayRevoked(t, c.id)
}

// Cell 9: with the digest rows lost, the core still decides stale and the registry stays revoked.
func TestDisorderReplayAfterRevokeWithLostDigestsIsStaleAndWritesNoRegistryChange(t *testing.T) {
	e := getEnv(t, true)
	c, narrow, revoke := e.revokedCredential(t, "src-dis9")
	before := e.snapshot(t, c.id)
	e.seed(t, `DELETE FROM auth.credential_projection_digest WHERE external_reference = $1`, c.id)
	wide := c
	wide.change.Scopes = []string{"ingest:write", "ingest:status", "schema:read"}
	for _, tc := range []struct {
		name string
		in   spec
		want Outcome
	}{
		{"wider created", wide, pushlifecycle.Stale},
		{"scoped r2", narrow, pushlifecycle.Stale},
		{"revoked r3", revoke, pushlifecycle.Duplicate},
	} {
		if res := e.project(t, tc.in.input()); res.Outcome != tc.want {
			t.Errorf("%s: outcome %s, want %s", tc.name, res.Outcome, tc.want)
		}
	}
	if after := e.snapshot(t, c.id); after != before {
		t.Fatalf("a replay with lost digests wrote: %q -> %q", before, after)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1 AND outcome IN ('stale','duplicate')`, c.id); n < 1 {
		t.Fatal("the decided replay left no digest row, so the reader would read it again")
	}
	e.mustStayRevoked(t, c.id)
}

// Cell 10: a widening event never widens the registry, as a changed body or as a new revision.
func TestDisorderAWideningEventNeverWidensTheRegistry(t *testing.T) {
	e := getEnv(t, true)
	c := created(e.org(t), "src-dis10", "ingest:write", "ingest:status")
	mustOutcome(t, e.project(t, c.input()), pushlifecycle.Applied)
	narrow := c.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}})
	mustOutcome(t, e.project(t, narrow.input()), pushlifecycle.Applied)
	before := e.snapshot(t, c.id)

	sameRevision := narrow
	sameRevision.change.Scopes = []string{"ingest:write", "ingest:status"}
	mustOutcome(t, e.project(t, sameRevision.input()), OutcomeConflict)
	if after := e.snapshot(t, c.id); after != before {
		t.Fatalf("a conflicting body wrote: %q -> %q", before, after)
	}

	denied := e.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.credential_projection.decided' AND outcome = 'denied' AND attributes ->> 'reason_code' = 'denied_policy'`)
	wide := narrow.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write", "ingest:status"}})
	res := e.project(t, wide.input())
	mustOutcome(t, res, pushlifecycle.RefusedWiden)
	if res.Class != ClassRefused {
		t.Fatalf("class %s, want refused", res.Class)
	}
	if after := e.snapshot(t, c.id); after != before {
		t.Fatalf("a refused widening wrote: %q -> %q", before, after)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.credential_projection.decided' AND outcome = 'denied' AND attributes ->> 'reason_code' = 'denied_policy'`); n != denied+1 {
		t.Fatalf("the refused widening wrote %d denied audit rows, want 1", n-denied)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection_digest WHERE external_reference = $1 AND revision = 3 AND outcome = 'refused_widen'`, c.id); n != 1 {
		t.Fatal("the refusal is not decided, so the reader would read it again")
	}
	mustOutcome(t, e.project(t, wide.input()), pushlifecycle.Duplicate)
}

// Cell 11: replays of the parent and of the rotated child after a rotation revive nothing.
func TestDisorderReplayAroundARotationNeverRevivesTheParent(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	parent := created(org, "src-dis11", "ingest:write", "ingest:status")
	mustOutcome(t, e.project(t, parent.input()), pushlifecycle.Applied)
	rotatedAt := parent.at.Add(2 * time.Second)
	child := spec{org: org, source: "src-dis11", id: uuid.NewString(), revision: 1, kind: contracts.ExternalPushRotated, at: rotatedAt,
		change: contracts.ExternalPushLifecycleChange{Status: "active", Fingerprint: freshFingerprint(), Scopes: []string{"ingest:write"}, RotationParentID: parent.id}}
	mustOutcome(t, e.project(t, child.input()), pushlifecycle.Applied)
	retire := parent.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"})
	retire.at = rotatedAt
	mustOutcome(t, e.project(t, retire.input()), pushlifecycle.Applied)
	parentBefore, childBefore := e.snapshot(t, parent.id), e.snapshot(t, child.id)

	mustOutcome(t, e.project(t, parent.input()), pushlifecycle.Stale)
	mustOutcome(t, e.project(t, retire.input()), pushlifecycle.Duplicate)
	mustOutcome(t, e.project(t, child.input()), pushlifecycle.Duplicate)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, child.id); n != 1 {
		t.Fatalf("the replayed rotation left %d registry rows for the child, want 1", n)
	}
	if after := e.snapshot(t, parent.id); after != parentBefore {
		t.Fatalf("the parent changed: %q -> %q", parentBefore, after)
	}
	if after := e.snapshot(t, child.id); after != childBefore {
		t.Fatalf("the child changed: %q -> %q", childBefore, after)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND revoked_at IS NOT NULL`, parent.id); n != 1 {
		t.Fatal("the parent registry row is not revoked")
	}
	// A second rotation of the retired parent is refused.
	second := spec{org: org, source: "src-dis11", id: uuid.NewString(), revision: 1, kind: contracts.ExternalPushRotated, at: rotatedAt.Add(time.Minute),
		change: contracts.ExternalPushLifecycleChange{Status: "active", Fingerprint: freshFingerprint(), Scopes: []string{"ingest:write"}, RotationParentID: parent.id}}
	mustOutcome(t, e.project(t, second.input()), pushlifecycle.RefusedParent)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, second.id); n != 0 {
		t.Fatal("a refused second rotation wrote a registry row")
	}
}

// Cell 12: a registry row revoked outside the projection makes the credential terminal; no later event writes.
func TestDisorderARegistryRevokedOutOfBandIsTerminalForLaterEvents(t *testing.T) {
	e := getEnv(t, true)
	c := created(e.org(t), "src-dis12", "ingest:write", "ingest:status")
	mustOutcome(t, e.project(t, c.input()), pushlifecycle.Applied)
	e.seed(t, `UPDATE auth.credential_registry SET revoked_at = clock_timestamp(), revoked_reason = 'domain_revoked' WHERE external_reference = $1`, c.id)
	before := e.snapshot(t, c.id)
	narrow := c.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}})
	res := e.project(t, narrow.input())
	mustOutcome(t, res, pushlifecycle.RefusedTerminal)
	if res.Class != ClassRefused {
		t.Fatalf("class %s, want refused", res.Class)
	}
	if after := e.snapshot(t, c.id); after != before {
		t.Fatalf("an event after an out-of-band revoke wrote: %q -> %q", before, after)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND status = 'active'`, c.id); n != 1 {
		t.Fatal("the projection row changed")
	}
}

// Cell 13: the same aggregate id sent under another organization is a conflict or a gap and writes nothing in either.
func TestDisorderTheSameAggregateUnderAnotherOrganizationWritesNothing(t *testing.T) {
	e := getEnv(t, true)
	c := created(e.org(t), "src-dis13", "ingest:write", "ingest:status")
	mustOutcome(t, e.project(t, c.input()), pushlifecycle.Applied)
	narrow := c.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}})
	mustOutcome(t, e.project(t, narrow.input()), pushlifecycle.Applied)
	before := e.snapshot(t, c.id)
	other := e.org(t)

	foreign := c
	foreign.org = other
	mustOutcome(t, e.project(t, foreign.input()), OutcomeConflict)
	foreignNarrow := narrow
	foreignNarrow.org = other
	mustOutcome(t, e.project(t, foreignNarrow.input()), OutcomeConflict)
	foreignRevoke := narrow.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"})
	foreignRevoke.org = other
	res := e.project(t, foreignRevoke.input())
	mustOutcome(t, res, pushlifecycle.Gap)
	if !res.Outcome.Retry() {
		t.Fatal("a gap must be a retry")
	}
	if after := e.snapshot(t, c.id); after != before {
		t.Fatalf("a foreign-organization event changed the credential: %q -> %q", before, after)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND organization_id = $2`, c.id, other); n != 0 {
		t.Fatal("a projection row exists under the other organization")
	}
}
