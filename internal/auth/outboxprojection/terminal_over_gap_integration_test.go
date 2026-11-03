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

const togDecided = `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.credential_projection.decided'`

// togSeed runs one statement inside audit.Commit as the migration role: the guarded tables take no other write.
func (e *env) togSeed(t *testing.T, sql string, args ...any) {
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

// togSnapshot is the registry row and the projection row of one credential as one text, updated_at included.
func (e *env) togSnapshot(t *testing.T, id string) string {
	t.Helper()
	var reg, proj string
	if err := e.admin.QueryRow(context.Background(), `SELECT coalesce(string_agg(coalesce(revoked_at::text,'-')||'|'||scopes::text||'|'||coalesce(expires_at::text,'-')||'|'||updated_at::text, ';'), 'none') FROM auth.credential_registry WHERE external_reference = $1`, id).Scan(&reg); err != nil {
		t.Fatal(err)
	}
	if err := e.admin.QueryRow(context.Background(), `SELECT coalesce(string_agg(revision::text||'|'||status||'|'||scopes::text||'|'||coalesce(terminal_at::text,'-')||'|'||updated_at::text, ';'), 'none') FROM auth.credential_projection WHERE external_reference = $1`, id).Scan(&proj); err != nil {
		t.Fatal(err)
	}
	return reg + " / " + proj
}

// togEnded fails unless the registry row is ended (revoked, or expired by now) and the projection is ended at revision.
func (e *env) togEnded(t *testing.T, id, status string, revision int) {
	t.Helper()
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND (revoked_at IS NOT NULL OR expires_at <= now())`, id); n != 1 {
		t.Fatal("the registry row is still live")
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND status = $2 AND status <> 'active' AND revision = $3 AND max_seen_revision = $3 AND terminal_at IS NOT NULL`, id, status, revision); n != 1 {
		t.Fatalf("the projection is not %s at revision %d", status, revision)
	}
}

func togAt(s spec, revision int) spec {
	s.at = s.at.Add(time.Duration(revision-s.revision) * time.Second)
	s.revision = revision
	return s
}

func togExpiry() *contracts.Timestamp {
	return &contracts.Timestamp{Time: time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)}
}

// I1: a revoke or an expiry over a gap ends a push credential; the skipped revision is then stale and writes nothing.
func TestTerminalOverAGapEndsAPushCredentialAndTheSkippedRevisionIsStale(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	c := created(org, "src-tog1", "ingest:write", "ingest:status")
	mustOutcome(t, e.project(t, c.input()), pushlifecycle.Applied)
	gone := togAt(c.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"}), 3)
	res := e.project(t, gone.input())
	mustOutcome(t, res, pushlifecycle.Applied)
	if res.Class != ClassDone || res.State.Status != pushlifecycle.StatusRevoked {
		t.Fatalf("class %s status %s, want done and revoked", res.Class, res.State.Status)
	}
	e.togEnded(t, c.id, "revoked", 3)
	before, audits := e.togSnapshot(t, c.id), e.count(t, togDecided)
	late := c.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}})
	mustOutcome(t, e.project(t, late.input()), pushlifecycle.Stale)
	if after := e.togSnapshot(t, c.id); after != before || e.count(t, togDecided) != audits {
		t.Fatalf("the skipped revision wrote: %q -> %q", before, after)
	}
	mustOutcome(t, e.project(t, togAt(late, 4).input()), pushlifecycle.RefusedTerminal)
	mustOutcome(t, e.project(t, togAt(late, 7).input()), pushlifecycle.RefusedTerminal)
	e.togEnded(t, c.id, "revoked", 3)

	x := created(org, "src-tog1", "ingest:write")
	mustOutcome(t, e.project(t, x.input()), pushlifecycle.Applied)
	ended := togAt(x.next(contracts.ExternalPushExpired, contracts.ExternalPushLifecycleChange{Status: "expired", ExpiresAt: togExpiry()}), 4)
	mustOutcome(t, e.project(t, ended.input()), pushlifecycle.Applied)
	e.togEnded(t, x.id, "expired", 4)
}

// I2: a refused widening is decided: it writes nothing, and the next narrowing and the revoke after it apply.
func TestARefusedWideningIsDecidedAndLaterRevisionsApply(t *testing.T) {
	e := getEnv(t, true)
	c := created(e.org(t), "src-tog2", "ingest:write", "ingest:status")
	mustOutcome(t, e.project(t, c.input()), pushlifecycle.Applied)
	narrow := c.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}})
	mustOutcome(t, e.project(t, narrow.input()), pushlifecycle.Applied)
	before := e.togSnapshot(t, c.id)
	widen := narrow.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write", "ingest:status"}})
	mustOutcome(t, e.project(t, widen.input()), pushlifecycle.RefusedWiden)
	if after := e.togSnapshot(t, c.id); after != before {
		t.Fatalf("a refused widening wrote: %q -> %q", before, after)
	}
	mustOutcome(t, e.project(t, widen.input()), pushlifecycle.Duplicate)
	again := widen.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write", "ingest:status"}})
	mustOutcome(t, e.project(t, again.input()), pushlifecycle.RefusedWiden)
	narrowAgain := again.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}})
	mustOutcome(t, e.project(t, narrowAgain.input()), pushlifecycle.Applied)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND revision = 5 AND status = 'active' AND scopes = ARRAY['ingest:write']`, c.id); n != 1 {
		t.Fatal("the narrowing after two refused widenings is not applied at revision 5")
	}
	gone := narrowAgain.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"})
	mustOutcome(t, e.project(t, gone.input()), pushlifecycle.Applied)
	e.togEnded(t, c.id, "revoked", 6)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND scopes = ARRAY['ingest:write']::auth.credential_scope[]`, c.id); n != 1 {
		t.Fatal("the registry scopes widened")
	}
}

// I3: the same for acr: a revoke or an expiry over a gap ends the credential, and the route's retry is gone.
func TestAcrTerminalOverAGapEndsTheCredential(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	c := acrCreated(org)
	mustOutcome(t, e.project(t, e.acr(t, c, "sub-1", 1)), pushlifecycle.Applied)
	gone := togAt(c.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"}), 3)
	res := e.project(t, e.acr(t, gone, "", 3))
	mustOutcome(t, res, pushlifecycle.Applied)
	if res.Class != ClassDone {
		t.Fatalf("class %s, want done", res.Class)
	}
	e.togEnded(t, c.id, "revoked", 3)
	mustOutcome(t, e.project(t, e.acr(t, togAt(gone, 2), "", 2)), pushlifecycle.Stale)
	e.togEnded(t, c.id, "revoked", 3)

	x := acrCreated(org)
	mustOutcome(t, e.project(t, e.acr(t, x, "sub-1", 1)), pushlifecycle.Applied)
	ended := togAt(x.next(contracts.ExternalPushExpired, contracts.ExternalPushLifecycleChange{Status: "expired", ExpiresAt: togExpiry()}), 4)
	mustOutcome(t, e.project(t, e.acr(t, ended, "", 4)), pushlifecycle.Applied)
	e.togEnded(t, x.id, "expired", 4)
}

// I4: a refused acr rotation never existed: its later revisions are refused for good (a denied audit row each),
// never a retry, and no registry or projection row is written.
func TestAcrRefusedRotationRefusesTheChildsLaterRevisions(t *testing.T) {
	e := getEnv(t, true)
	parent := acrCreated(e.org(t))
	mustOutcome(t, e.project(t, e.acr(t, parent, "sub-1", 1)), pushlifecycle.Applied)
	child := spec{org: parent.org, source: "acr", id: "cred_" + uuid.NewString()[:12], revision: 1, kind: contracts.ExternalPushRotated, at: parent.at.Add(time.Second),
		change: contracts.ExternalPushLifecycleChange{Status: "active", Scopes: []string{"context:read", "evidence:read"}, RotationParentID: parent.id}}
	child.change.Fingerprint = acrFingerprint(child.id)
	mustOutcome(t, e.project(t, e.acr(t, child, "sub-1", 6)), pushlifecycle.RefusedWiden)
	for _, revision := range []int{2, 3} {
		denied := e.count(t, togDecided+` AND outcome = 'denied'`)
		gone := togAt(child.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"}), revision)
		res := e.project(t, e.acr(t, gone, "", byte(revision)))
		mustOutcome(t, res, pushlifecycle.RefusedTransition)
		if res.Class != ClassRefused || e.count(t, togDecided+` AND outcome = 'denied'`) != denied+1 {
			t.Fatalf("revoked r%d: class %s, or not one denied audit row", revision, res.Class)
		}
	}
	if e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, child.id)+
		e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1`, child.id) != 0 {
		t.Fatal("the refused child has a registry or a projection row")
	}
}

// I5: a refused digest row above a missing revision never moves the decided frontier past the missing revision, and
// once the frontier reaches it, a widening against the applied scopes is still refused.
func TestARefusedDigestAboveAMissingRevisionNeverSkipsItOrWidens(t *testing.T) {
	e := getEnv(t, true)
	c := created(e.org(t), "src-tog5", "ingest:write", "ingest:status")
	mustOutcome(t, e.project(t, c.input()), pushlifecycle.Applied)
	narrow := c.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}})
	third := narrow.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write", "ingest:status"}})
	digest := Digest(third.input())
	e.togSeed(t, `INSERT INTO auth.credential_projection_digest (domain_owner, external_reference, revision, digest, outcome) VALUES ('external_push', $1, 3, $2, 'refused_widen')`, c.id, digest[:])

	before, audits := e.togSnapshot(t, c.id), e.count(t, togDecided)
	fourth := third.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write", "ingest:status"}})
	res := e.project(t, fourth.input())
	mustOutcome(t, res, pushlifecycle.Gap)
	if after := e.togSnapshot(t, c.id); after != before || e.count(t, togDecided) != audits {
		t.Fatalf("revision 4 over the missing revision 2 wrote: %q -> %q", before, after)
	}
	mustOutcome(t, e.project(t, narrow.input()), pushlifecycle.Applied)
	mustOutcome(t, e.project(t, fourth.input()), pushlifecycle.RefusedWiden)
	mustOutcome(t, e.project(t, third.input()), pushlifecycle.Duplicate)
	fifth := fourth.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}})
	mustOutcome(t, e.project(t, fifth.input()), pushlifecycle.Applied)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1 AND revision = 5 AND scopes = ARRAY['ingest:write']`, c.id); n != 1 {
		t.Fatal("the projection is not narrowed at revision 5")
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND scopes = ARRAY['ingest:write']::auth.credential_scope[]`, c.id); n != 1 {
		t.Fatal("the registry scopes widened")
	}
}

// I6: on a terminal credential a refusal over a gap is not decided: a lower revision after it is refused, not stale.
func TestARefusalOverAGapOnATerminalCredentialDoesNotSkipTheLowerRevision(t *testing.T) {
	e := getEnv(t, true)
	c := created(e.org(t), "src-tog6", "ingest:write")
	mustOutcome(t, e.project(t, c.input()), pushlifecycle.Applied)
	gone := c.next(contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"})
	mustOutcome(t, e.project(t, gone.input()), pushlifecycle.Applied)
	later := gone.next(contracts.ExternalPushScoped, contracts.ExternalPushLifecycleChange{Scopes: []string{"ingest:write"}})
	mustOutcome(t, e.project(t, togAt(later, 5).input()), pushlifecycle.RefusedTerminal)
	mustOutcome(t, e.project(t, later.input()), pushlifecycle.RefusedTerminal)
	e.togEnded(t, c.id, "revoked", 2)
}
